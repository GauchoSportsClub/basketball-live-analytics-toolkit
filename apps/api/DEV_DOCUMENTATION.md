# Analytics Engine — Developer Documentation

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Module Layout](#module-layout)
3. [Constants & Configuration](#constants--configuration)
4. [Helper Functions](#helper-functions)
   - [Environment & I/O](#environment--io)
   - [String Normalization](#string-normalization)
   - [HTTP Utilities](#http-utilities)
   - [Path Helpers](#path-helpers)
5. [PDF Parsing Pipeline](#pdf-parsing-pipeline)
6. [ESPN Scraping Pipeline](#espn-scraping-pipeline)
7. [Play-by-Play Pipeline](#play-by-play-pipeline)
8. [Live Stats Computation](#live-stats-computation)
9. [Insights Generation](#insights-generation)
10. [Dataset Context System](#dataset-context-system)
11. [API Route Logic](#api-route-logic)
12. [Common Workflows](#common-workflows)

---

## Architecture Overview

The entire server lives in a single Python file. There is no framework — it uses `http.server.ThreadingHTTPServer` directly. All request routing is done by matching `urlparse(self.path).path` against string literals or regexes inside `do_GET` and `do_POST`.

Data flows in two directions:

- **Ingest**: ESPN Core API or PDF → parse → write CSV/JSON to `data/`
- **Serve**: read CSV/JSON from `data/` → build context dict → serialize to JSON response

The "context dict" is the central data structure. Every dataset, regardless of source, is normalized into the same shape before being sent to API consumers or the OpenAI insights generator:

```python
{
    "team_id": str,
    "dataset": str,           # "team", "players", or "pbp"
    "columns": List[str],
    "rows": List[Dict[str, str]],
    "rows_by_key": Dict[str, Dict],   # row_key -> row, for O(1) evidence lookup
}
```

---

## Module Layout

The file is organized in roughly this order:

1. Constants and global state (`SEASON_LABEL`, rate limit timestamps, athlete name cache)
2. Utility/helper functions (normalization, I/O, HTTP)
3. PDF parsing pipeline
4. ESPN scraping pipeline
5. PBP fetch, filter, and display pipeline
6. Live stats computation from PBP
7. Dataset context builders (the bridge between raw data and API responses)
8. Insights generation (OpenAI call + schema validation)
9. `ApiHandler` class (`do_GET` / `do_POST`)
10. `run_server()` entry point

---

## Constants & Configuration

### Season constants

```python
SEASON_LABEL = "2025-26"        # Human-readable label
SCHEMA_VERSION = "espn-season-v1"
ESPN_PBP_GAME_ID = "401809115"  # Default game for PBP fetches
ESPN_PBP_LEAGUE = "mens-college-basketball"
```

`season_year_from_label(label)` parses `SEASON_LABEL` and returns the end year as an integer (e.g. `2026`). This integer is used in all ESPN API URLs like `.../seasons/2026/...`.

### Rate limit globals

```python
_LAST_REQUEST_AT: float         # Timestamp of last outbound HTTP request
_LAST_PBP_UPDATE_AT: float      # Timestamp of last PBP update call
_ATHLETE_NAME_CACHE: Dict       # athlete_id -> display name
```

These are module-level globals mutated by `_respect_rate_limit()` and `_mark_request_complete()`. The server is single-process (though multi-threaded via `ThreadingHTTPServer`), so these globals are shared across all in-flight requests.

### Player column config

`PLAYER_TABLE_CONFIG` is a dict mapping intended display column names to source field names. Two sentinel values trigger computed columns:

```python
_COMPUTED_RPG = "__computed_rpg__"   # rebounds / GP
_COMPUTED_APG = "__computed_apg__"   # assists / GP
```

These are resolved at serve time in `_apply_player_column_config()`, not at ingest time.

---

## Helper Functions

### Environment & I/O

#### `load_dotenv(path: Path) -> None`

Reads a `.env` file line by line. Skips comments and blank lines. Sets env vars only if they are not already set, so real environment variables always win. Called once in `run_server()`.

#### `now_iso() -> str`

Returns the current UTC time as an ISO 8601 string. Used for `last_updated` and `updated_at` fields throughout.

#### `read_json_file(path: Path) -> Optional[Dict]`

Reads and parses a JSON file. Returns `None` on any error (missing file, parse error). Never raises. Used for reading cached manifests and team lists.

#### `write_json_file(path: Path, payload: Any) -> None`

Writes JSON atomically: writes to a `.tmp` sibling file first, then renames. Creates parent directories as needed. This prevents partial writes from corrupting the cache.

#### `read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]`

Reads a CSV file using `csv.DictReader`. Returns `(columns, rows)`. Columns come from the header row; rows are plain dicts.

#### `write_csv(path: Path, rows: Sequence[Dict[str, str]]) -> Dict[str, Any]`

Writes rows to CSV, always placing `row_key` first. Discovers all column names by iterating all rows (so sparse rows don't lose fields). Also writes atomically via a `.tmp` file. Returns a metadata dict `{filename, row_count, columns}` used in manifests.

#### `rows_to_csv_text(columns, rows) -> str`

In-memory version of `write_csv`. Produces a CSV string (no file). Used to format dataset context for the OpenAI prompt.

---

### String Normalization

These are the building blocks for all key/ID normalization and are used pervasively. Understand these before editing any parsing or routing logic.

#### `normalize_space(value: str) -> str`

Collapses all runs of whitespace to a single space and strips leading/trailing whitespace. Used on every cell read from PDFs or CSV.

#### `normalize_team_id(value: str) -> str`

Lowercases, strips all characters except `[a-z0-9-]`, and strips leading/trailing hyphens. Raises `ValueError` if the result is empty. Used to canonicalize every team ID before use as a dict key or path component. **Always call this before comparing team IDs.**

#### `normalize_token(value: str) -> str`

Lowercases and replaces runs of non-alphanumeric characters with underscores. Used for general-purpose token generation.

#### `normalize_column_name(value: str) -> str`

Like `normalize_token` but guarantees a non-empty fallback of `"col"`. Used for column names derived from raw strings (e.g. ESPN stat keys, PDF headers).

#### `normalize_row_key(value: str) -> str`

Calls `normalize_token` and falls back to `"row"` if empty. Used as the base for all row key generation.

#### `normalize_dataset_name(dataset: str) -> str`

Maps raw dataset strings to canonical values: `"team"`, `"players"`, or `"pbp"`. Raises `ValueError` for unknown datasets.

#### `ensure_unique_keys(values: Sequence[str]) -> List[str]`

Takes a list of (potentially duplicate) column names and appends `_2`, `_3`, etc. to duplicates. Used when parsing PDF player table headers, which may have repeated `AVG` columns.

#### `unique_row_key(base: str, seen: Dict[str, int]) -> str`

Generates a unique row key by tracking how many times `base` has been seen. Returns `base` for the first occurrence, `base_2` for the second, etc. The `seen` dict must be passed in and maintained by the caller.

#### `to_text(value: Any) -> str`

Converts any value to a display string. Handles `None`, booleans, numbers, and strings. Always calls `normalize_space` on string values.

---

### HTTP Utilities

#### `_respect_rate_limit() -> None`

Sleeps if less than `RATE_LIMIT_SECONDS` (1.0s) has elapsed since the last outbound request. Called before every `requests.get`.

#### `_mark_request_complete() -> None`

Updates `_LAST_REQUEST_AT` to the current time. Called after every successful request.

#### `fetch_json(url: str, retries: int = 3) -> Dict[str, Any]`

Fetches a URL, respecting the rate limit, and parses the response as JSON. Retries up to `retries` times with increasing sleep between attempts (`time.sleep(attempt)`). Raises `RuntimeError` listing all attempt errors if all retries fail. Requires the response to be a JSON object (not an array).

#### `fetch_binary(url: str, retries: int = 3) -> bytes`

Same retry logic as `fetch_json` but returns raw bytes. Used for downloading PDF files.

#### `collect_ref_urls(node: Any, out: List[str]) -> List[str]`

Recursively walks any JSON-decoded Python structure (dicts and lists) and collects all `"$ref"` string values. Used to discover ESPN API links nested anywhere in a response payload.

---

### Path Helpers

#### `team_data_dir(team_id: str) -> Path`

Returns `data/espn/<team_id>/`. This is where `team.csv`, `player.csv`, and `manifest.json` are stored for ESPN-scraped teams.

#### `espn_teams_cache_path() -> Path`

Returns the path to the shared teams cache: `data/espn/teams-2025-26.json`.

#### `pbp_data_path(game_id: Optional[str]) -> Path`

Returns `data/pbp-<game_id>.json`. If `game_id` is `None`, falls back to `ESPN_PBP_GAME_ID`.

#### `get_pdf_path_for_team(team_id: str) -> Optional[Path]`

Looks up whether a PDF stat file exists for the given team ID. Returns the `Path` if it exists, `None` otherwise. This is the primary check used to decide whether to use PDF parsing vs. ESPN scraping for a team.

#### `dataset_filename(dataset: str) -> str`

Maps canonical dataset names to filenames: `team` → `team.csv`, `players` → `player.csv`, `pbp` → `pbp-<game_id>.json`.

---

## PDF Parsing Pipeline

Used for teams (UCSB, UCR) where ESPN data is unreliable or unavailable. The pipeline has two stages: extraction and parsing.

### Extraction

#### `extract_first_page_tables(pdf_path) -> Tuple[team_table, player_table]`

Top-level entry point. Tries `_parse_first_page_text_tables` first (text-based, more reliable for Sidearm-format PDFs). Falls back to `extract_pdf_tables` + `select_best_table` if text parsing produces nothing.

#### `_parse_first_page_text_tables(pdf_path) -> Tuple[team_table, player_table]`

Uses `pdfplumber` to extract the raw text of the first page, then splits it into lines. Identifies the player table by finding a line containing both `"player"` and `"gp-gs"`. Parses subsequent lines by finding the first `DD-DD` token (the GP-GS field) to split the jersey number + name from the stats. Identifies the team stats block by finding a line containing `"team statistics"`.

Returns two tables as lists of lists of strings:
- `player_table`: rows are `[number, name, stat1, stat2, ...]`
- `team_table`: rows are `[metric, team_value, opponent_value]`

#### `extract_pdf_tables(pdf_path) -> List[List[List[str]]]`

Fallback: uses `pdfplumber`'s `extract_tables()` on every page, cleans each cell with `normalize_space`, and returns all non-empty tables.

#### `select_best_table(tables, scorer) -> Optional[List[List[str]]]`

Scores every table using a scorer function and returns the highest-scoring one, or `None` if the best score is ≤ 0.

#### `score_team_table(table)` / `score_player_table(table)`

Heuristic scorers. `score_team_table` rewards keywords like `"scoring"`, `"points per game"`, `"field goal pct"` and penalizes tables that look like player tables. `score_player_table` rewards `"player"`, `"gp-gs"`, `"pts"`, `"avg"`.

### Parsing

#### `parse_team_table_rows(table) -> List[Dict[str, str]]`

Converts the raw team table into a list of dicts. Each row becomes `{row_key, metric, team, opp, extra_1, extra_2}`. Skips header-like rows (`"team"`, `"opp"`, `"metric"`). Generates unique row keys from the metric name.

#### `parse_player_table_rows(table) -> List[Dict[str, str]]`

Scans the table for a header row containing `"player"` and `"gp-gs"`. Maps each header cell to a canonical column name via `canonical_player_header()`. Parses subsequent rows into dicts, skipping Total/Opponents rows. Appends a computed `"Team"` row by calling `_compute_team_row_from_parsed_players()`.

#### `canonical_player_header(label: str) -> str`

Maps raw header text to standard internal column names. For example: `"GP-GS"` → `"gp_gs"`, `"FG%"` → `"fg_pct"`, `"A"` → `"a"`, `"AVG"` → `"avg"`. Repeated `"AVG"` columns end up disambiguated by `ensure_unique_keys` as `avg`, `avg_2`, `avg_3`.

#### `_compute_team_row_from_parsed_players(player_rows, header) -> Optional[Dict]`

Aggregates all player rows to produce a team totals row. Sums cumulative stats (MIN, PTS, REB, AST, etc.), takes the maximum GP, computes FG/3FG/FT percentages from made/attempted totals, and computes per-game averages. Returns `None` if no valid GP is found.

---

## ESPN Scraping Pipeline

Used for any team without a PDF source. Navigates the ESPN Core API's `$ref`-based hypermedia structure.

### Team Discovery

#### `fetch_espn_teams(force_refresh: bool) -> Dict`

Checks the cache at `data/espn/teams-2025-26.json` first (unless `force_refresh`). If fetching, it:
1. Fetches the league root URL
2. Calls `_discover_season_ref()` to find the season's URL
3. Fetches the season, finds the `teams.$ref`, paginates through all team items
4. Fetches each team's detail page for `displayName`, `abbreviation`, etc.

Falls back to `DEFAULT_ESPN_TEAMS` if the fetch fails and there is no cache.

#### `_discover_season_ref(league_root, target_year) -> str`

Looks through the `seasons.$ref` items list for one matching `/seasons/{year}`. Falls back to constructing the URL directly.

### Team Scraping

#### `scrape_team(team_id: str) -> Dict`

The main ESPN scrape entry point for a single team. Steps:

1. Call `_discover_team_ref()` to get the team's ESPN URL
2. Fetch the team payload
3. Find stats `$ref` links in the payload and expand them via `_expand_ref_collection()`
4. Call `_extract_team_stat_rows()` to flatten the stats into rows
5. Find roster/athletes `$ref` links
6. For each athlete: fetch the athlete detail page and their stats `$ref`
7. Call `_extract_player_stat_map()` to get a flat key→value stat map per player
8. Write `team.csv`, `player.csv`, and `manifest.json`

#### `_expand_ref_collection(ref_url, max_items) -> List[Dict]`

Fetches a paginated ESPN collection URL. If the response has an `items` array, fetches each item's `$ref` individually and returns the list of expanded payloads. If there is no `items` array, returns the top-level payload wrapped in a list.

#### `_extract_team_stat_rows(team_id, stats_payloads) -> List[Dict]`

Recursively walks the stats payload tree looking for `statistics` arrays. Each stat object becomes a row with fields: `row_key`, `team_id`, `split`, `stat_key`, `stat_name`, `value`, `numeric_value`, `display_value`, `rank`, `abbreviation`. The `split` comes from the nearest parent node's `displayName` or `name` (e.g. `"overall"`, `"home"`, `"away"`). Deduplicates by `(split, stat_key)` pairs.

#### `_extract_player_stat_map(stats_payloads) -> Dict[str, str]`

Similar recursive walk, but returns a flat `{stat_key: display_value}` dict for a single player. Prefixes keys with the split name if the split is not `"overall"`. Used to produce the player CSV row alongside the athlete's name, jersey, and position.

#### `ensure_team_data(team_id: str) -> None`

Checks if `team.csv` and `player.csv` already exist for the team. Calls `scrape_team()` only if they don't. This is the lazy-loading gate used by `build_dataset_context()`.

---

## Play-by-Play Pipeline

### Fetching

#### `fetch_espn_pbp_rows(league, game_id) -> List[Dict]`

Paginates through the ESPN Core API plays endpoint (`/events/{id}/competitions/{id}/plays`) using `limit=1000` and incrementing `pageIndex`. Normalizes each play object into a flat dict with fields: `id`, `sequence`, `period`, `clock`, `text`, `type`, `team_id`, `home_score`, `away_score`, `scoring_play`, `shooting_play`, `score_value`, `points_attempted`, `wallclock`, `athlete_id`, `assist_athlete_id`.

Team IDs and athlete IDs are extracted from `$ref` URLs using `_extract_ref_id()`.

#### `update_pbp_data(force, game_id) -> Dict`

Calls `fetch_espn_pbp_rows()` and writes the result to `data/pbp-<game_id>.json`. Enforces a 10-second rate limit between calls unless `force=True`.

#### `load_pbp_rows(game_id) -> List[Dict]`

Reads the cached JSON file and validates it is a non-empty list of dicts. Raises descriptive `RuntimeError` messages if the file is missing or empty, which propagate to the API as 500 errors with actionable messages.

### Filtering

#### `parse_pbp_filters(params: Dict[str, List[str]]) -> PbpFilters`

Parses raw query string parameters into a `PbpFilters` dataclass. Validates:
- `period` values must be `1`–`4` or `OT`
- `team_id` values must be alphanumeric/hyphen/underscore
- `clock_mode` must be `"last_n"` or `"range"` if present
- `clock_last_n` requires a positive float; `clock_range` requires both `clock_from` and `clock_to` in `MM:SS` format with `from >= to` (time remaining counts down)

Raises `PbpFilterValidationError` (a `ValueError` subclass) on any invalid input. The handler catches this and returns HTTP 400.

#### `apply_pbp_filters(rows, filters, ucsb_team_id) -> List[Dict]`

Applies all filters from a `PbpFilters` object to a list of PBP rows. Filters are AND-combined. Team ID filtering resolves `"UCSB"` and `"Opponent"` labels as well as raw numeric IDs. Clock filtering converts clock strings to total seconds for comparison.

### Display

#### `_build_pbp_display_rows(rows, columns, ucsb_team_id) -> Tuple[List[str], List[Dict]]`

Prepares rows for API output. Hides internal columns (`id`, `sequence`, `scoring_play`, `shooting_play`, `wallclock`). Replaces raw numeric `team_id` values with `"UCSB"` or `"Opponent"` labels. Reorders columns so `team_id`, `type`, `text` appear first.

#### `row_keys_for_pbp_rows(rows) -> List[str]`

Generates stable, unique row keys for PBP rows. Prefers the ESPN play `id` field (`play_<id>`). Falls back to a SHA-1 hash of `period|clock|text` if no ID is present.

---

## Live Stats Computation

These functions derive in-game statistics directly from PBP rows, without relying on any external stats endpoint.

#### `_compute_live_team_stats(team_plays) -> Dict[str, int]`

Iterates plays for one team and accumulates: `pts`, `fgm/fga`, `2pm/2pa`, `3pm/3pa`, `ftm/fta`, `oreb`, `dreb`, `ast`, `to`, `stl`, `blk`, `pf`. Scoring plays increment `pts` by `score_value`. Shooting plays with `points_attempted` of 2 or 3 increment FGA. Free throw detection uses `_is_free_throw_text()` on the play description. Assist credit goes to `assist_athlete_id` on any `scoring_play`.

#### `_compute_live_player_stats(team_plays) -> Dict[str, Dict[str, int]]`

Same logic as `_compute_live_team_stats` but broken out per `athlete_id`. Returns a dict of `athlete_id -> stat_dict`. All athletes appearing in `athlete_id` or `assist_athlete_id` fields are included.

#### `resolve_athlete_name(athlete_id: str) -> str`

Looks up an athlete's display name from `_ATHLETE_NAME_CACHE`. On a cache miss, fetches `ESPN_LEAGUE_ROOT_URL/seasons/{year}/athletes/{id}` and caches the result. Falls back to `"Player {id}"` on any error.

#### `_live_team_rows(team_id, rows) -> List[Dict]`

Calls `_compute_live_team_stats()` and converts the result to a list of stat rows in the same format as ESPN season team rows (`row_key`, `team_id`, `split`, `stat_key`, `stat_name`, `value`, etc.).

#### `_live_player_rows(team_id, rows) -> List[Dict]`

Calls `_compute_live_player_stats()` and converts to player rows in the same format as season player rows (`Player`, `PTS`, `REB`, `AST`, `FG_PCT`, etc.). Calls `resolve_athlete_name()` for each athlete.

#### `build_live_stats_from_pbp(ucsb_team_id, opponent_team_id, game_id) -> Dict`

Top-level entry point for the `/api/pbp/live-stats` route. Infers the opponent team ID from unique `team_id` values in the PBP data if not explicitly provided. Returns four datasets: `ucsb_team`, `ucsb_players`, `opponent_team`, `opponent_players`.

---

## Insights Generation

### Schema & Validation

#### `openai_schema(allowed_pairs) -> Dict`

Builds a JSON Schema object for OpenAI's structured output (`response_format: json_schema`). When `allowed_pairs` is provided, the `evidence` item schema uses `anyOf` with one entry per `(team_id, dataset)` pair, each with a `const` constraint. This prevents the model from hallucinating team IDs or dataset names.

#### `validate_insights_payload(payload, datasets) -> Tuple[bool, List[str]]`

Validates a candidate insights payload against the actual loaded datasets. Checks:
- Correct top-level shape (`{"insights": [...]}`)
- Each insight has `insight` (non-empty string) and `evidence` (array)
- Each evidence ref has valid `team_id`, `dataset`, `row_key`, and `fields`
- `row_key` exists in the dataset's `rows_by_key`
- All `fields` exist in the dataset's `columns`

Returns `(True, [])` on success or `(False, [error_messages])` on failure.

#### `canonicalize_insights_payload(payload, datasets) -> Dict`

Normalizes team IDs and dataset names in evidence references to match the allowed pairs. Handles the case where the model uses a different team ID for a PBP reference (e.g. the UCSB numeric ID instead of `"pbp"`) by remapping it if there is only one PBP context.

### Generation

#### `generate_insights(prompt, dataset_contexts) -> Dict`

Orchestrates the full insights call:

1. Formats all dataset contexts as CSV text sections
2. Builds the system prompt from `LIVE_GAME_INSIGHT_RULES`
3. Builds the user prompt including the analyst's request, output schema, allowed evidence pairs, and all data
4. Calls `_call_openai_chat()` with structured output enforced
5. Calls `canonicalize_insights_payload()` then `validate_insights_payload()`
6. On validation failure, retries up to 3 times, feeding errors back to the model in a repair prompt
7. Returns the validated payload or raises `RuntimeError` with all validation errors

#### `_call_openai_chat(messages, schema) -> Dict`

Makes a single call to the OpenAI chat completions endpoint. Uses `temperature: 0` and the `json_schema` response format. Parses the response content as JSON and validates it is a dict.

---

## Dataset Context System

This is the central abstraction. All API routes that serve data call `build_dataset_context()`, which returns a normalized context dict.

#### `build_dataset_context(team_id, dataset) -> Dict`

The main entry point. Decision tree:

1. If `dataset == "pbp"`, delegate to `build_pbp_context()`
2. Check `get_pdf_path_for_team(team_id)` — if a PDF exists, delegate to `build_dataset_context_from_pdf()`
3. Otherwise, call `ensure_team_data()` (lazy ESPN scrape), read the CSV, refresh if needed
4. Call `_apply_player_column_config()` if dataset is `"players"`

#### `build_dataset_context_from_pdf(team_id, dataset) -> Optional[Dict]`

Calls `extract_first_page_tables()`, then the appropriate parser (`parse_team_table_rows` or `parse_player_table_rows`), and assembles the context dict. Returns `None` if no PDF exists for the team (so the caller falls through to ESPN).

#### `build_pbp_context(team_id, game_id) -> Dict`

Loads PBP rows from the cache, generates row keys, builds display rows, and returns the context dict.

#### `build_pbp_context_filtered(team_id, game_id, filters) -> Dict`

Same as `build_pbp_context` but applies `apply_pbp_filters()` before building display rows.

#### `_apply_player_column_config(context) -> None`

Mutates a player context dict in-place. Reads the column config, reorders and renames columns, computes RPG and APG from raw totals and GP, and rebuilds `rows_by_key`. This is called after loading both PDF and ESPN player data.

#### `dataset_needs_refresh(dataset, columns, rows) -> bool`

Heuristic check on loaded CSV data. Returns `True` (needs re-scrape) if the column list is near-empty, the row list is empty, or a dataset-specific sentinel column is missing (`stat_key` for team, `player` for players, `text`/`clock` for PBP).

---

## API Route Logic

### `GET /api/health`

Immediately returns `{ok: true, timestamp}`. No data access.

### `GET /api/espn/teams` and `GET /api/schools`

Both call `fetch_espn_teams()`. The `/teams` route also appends UCR to the team list if it is absent (since UCR is PDF-only and not in the ESPN cache). The `/schools` route reformats the response for school picker UIs.

### `GET /api/espn/season/<team_id>/<dataset>` and `GET /api/data/<team_id>/<dataset>`

Both follow the same logic:

1. Normalize `team_id` and `dataset`
2. Call `build_dataset_context(team_id, dataset)`
3. Read `manifest.json` for metadata like `school_name`, `source_urls`, `last_updated`
4. Strip `row_key` from the visible columns list (keep it in rows for client-side use)
5. Return the context dict with metadata fields added

The ESPN route (`/api/espn/season/...`) includes `source_urls` and `schema_version` from the manifest. The data route (`/api/data/...`) is a simpler alias that omits those.

### `POST /api/espn/season/<team_id>/update`

If the team has a PDF source, returns immediately with `{source: "pdf"}` — PDFs don't need refreshing via this endpoint. Otherwise calls `scrape_team(team_id)` to re-fetch from ESPN.

### `POST /api/scrape/<team_id>`

Directly calls `scrape_team(team_id)` unconditionally, bypassing the PDF check. Use this to force a re-scrape regardless of current cache state.

### `GET /api/pbp`

1. Parse query string into a `PbpFilters` object via `parse_pbp_filters()` — returns HTTP 400 on `PbpFilterValidationError`
2. Call `build_pbp_context_filtered()` with the parsed filters and `game_id`
3. Strip `row_key` from visible columns
4. Return rows with PBP metadata

### `GET /api/pbp/live-stats`

1. Parse `ucsb`, `opponent`, `game_id` from query string (manual split, not `parse_qs`)
2. Call `build_live_stats_from_pbp()`
3. Strip `row_key` from all four datasets' column lists
4. Return all four datasets in one response

### `POST /api/pbp/update`

Reads `force` and `game_id` from the request body. Calls `update_pbp_data(force, game_id)`. The `game_id` from the request body is used directly, so the UI's currently selected game determines which ESPN endpoint is queried.

### `GET /api/gameids`

Fetches the UCSB team's events list from the ESPN Core API and extracts game IDs by regex-searching `$ref` URLs for `/events/(40\d+)`. Returns the list of IDs.

### `POST /api/evidence/validate`

Loads each unique `(team_id, dataset)` pair from the `refs` array exactly once, caching the context in a local dict. For each ref, checks that `row_key` exists in `rows_by_key` and that all `fields` are present in `columns`. Returns a per-ref results array.

### `POST /api/insights`

1. Parse `prompt` and `contexts` from request body
2. For each context: build the appropriate dataset context (PBP or season)
3. Call `generate_insights(prompt, dataset_contexts)`
4. Return `{ok: true, insights: [...]}`

OpenAI `HTTPError` responses are forwarded with their original status code. All other errors return 500.

---

## Common Workflows

### Adding a new PDF-sourced team

1. Place the PDF in `analytics_engine/data/<filename>.pdf`
2. Add an entry to `PDF_TEAM_FILES`: `"new_team_id": "new-filename.pdf"`
3. Add an entry to `PDF_TEAM_NAMES`: `"new_team_id": "Full Team Name"`
4. The team will now be served via the PDF pipeline for both `team` and `players` datasets

### Adding a new ESPN-only team

No code change needed. Any valid ESPN team ID can be passed to `/api/espn/season/<team_id>/team` or `/api/data/<team_id>/team`. The scraper discovers all data dynamically via the ESPN hypermedia API.

### Changing the default game for PBP

Update `ESPN_PBP_GAME_ID` at the top of the file. All routes that default to this value will use the new ID. Alternatively, callers can pass `game_id` explicitly in all PBP routes.

### Adding a new player stat column

1. Add the column to `PLAYER_TABLE_CONFIG` with the key as the display name and the value as the source field name (must exist in the CSV or PDF-parsed rows)
2. If the column requires a computation (like RPG/APG), add a new sentinel constant and handle it in `_apply_player_column_config()`

### Adding a new API route

1. In `do_GET` or `do_POST`, add a new `if path == "/api/new-route":` block before the final `404` fallback
2. For path parameters, use `re.match(r"^/api/.../([\w-]+)$", path)` and call `normalize_team_id()` on any team ID capture group
3. Wrap the entire body in `try/except Exception as exc` and call `self._send_json(500, {"error": str(exc)})` in the except clause
4. Use `self._send_json(200, payload)` for success responses

### Debugging ESPN scrape failures

`scrape_team()` raises `RuntimeError` with descriptive messages at each stage. To debug interactively:

```python
from analytics_engine.server import scrape_team, fetch_espn_teams, _discover_team_ref

fetch_espn_teams(force_refresh=True)          # Refresh team cache
ref, name = _discover_team_ref("2540")        # Inspect discovered team URL
print(ref)
```

The manifest at `data/espn/<team_id>/manifest.json` lists every `source_url` fetched during a successful scrape, which is useful for tracing what the API returned.

### Debugging PBP data issues

The raw PBP cache is plain JSON at `data/pbp-<game_id>.json`. Inspect it directly to check what ESPN returned before any filtering or display transformation is applied:

```python
import json
from pathlib import Path
rows = json.loads(Path("data/pbp-401809115.json").read_text())
print(rows[0])   # First play
```

To test filters without running the server:

```python
from analytics_engine.server import load_pbp_rows, parse_pbp_filters, apply_pbp_filters

rows = load_pbp_rows("401809115")
filters = parse_pbp_filters({"clock_mode": ["last_n"], "clock_last_n_minutes": ["5"]})
filtered = apply_pbp_filters(rows, filters)
print(len(filtered), "plays in last 5 minutes")
```

### Updating the insights rules

The full set of rules governing how the AI produces insights lives in the `LIVE_GAME_INSIGHT_RULES` string constant. Edit this string to change recency requirements, output count, quantification rules, or commentary style. The string is injected verbatim as the system prompt in `generate_insights()`.