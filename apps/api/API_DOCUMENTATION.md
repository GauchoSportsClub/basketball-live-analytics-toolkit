# Analytics Engine API — Documentation
##### Generated with Claude.ai
## Overview

The Analytics Engine is a Python HTTP server that serves basketball statistics for UCSB and opponent teams. It ingests data from two sources: the **ESPN Core API** (season stats, rosters) and **Sidearm PDF stat sheets** (for teams like UCSB and UCR). It also fetches and caches live **play-by-play (PBP)** data from ESPN, and can generate AI-powered game insights via the OpenAI API.

The server runs on `http://0.0.0.0:8000` by default and exposes a JSON REST API.

---

## Configuration

Configuration is read from environment variables. A `.env` file at the repository root is loaded automatically on startup.

| Variable | Default | Description |
|---|---|---|
| `API_HOST` | `0.0.0.0` | Host address to bind |
| `API_PORT` | `8000` | Port to listen on |
| `OPENAI_API_KEY` | *(required for insights)* | OpenAI API key |
| `OPENAI_MODEL` | `gpt-4.1-mini` | OpenAI model to use |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible endpoint base URL |
| `PLAYER_TABLE_CONFIG` | *(see below)* | Override player column mapping (`key:val,key2:val2`) |

### Data Paths

| Path | Purpose |
|---|---|
| `<repo_root>/data/espn/<team_id>/` | ESPN-scraped CSVs and manifest per team |
| `<repo_root>/data/pbp-<game_id>.json` | Cached play-by-play data |
| `<repo_root>/analytics_engine/data/*.pdf` | Sidearm PDF stat sheets |
| `<repo_root>/data/espn/teams-2025-26.json` | Cached ESPN team list |

---

## Running the Server

```bash
python analytics_engine/server.py
```

The server loads `.env` from the repo root, creates the `data/` directory if needed, and starts listening.

---

## API Reference

### Health

#### `GET /api/health`

Returns server status.

**Response**
```json
{ "ok": true, "timestamp": "2025-11-01T12:00:00+00:00" }
```

---

### Teams

#### `GET /api/espn/teams`

Returns the full list of ESPN teams for the current season. Appends UCR (UC Riverside) if not already present, since it is sourced from a PDF rather than ESPN.

**Query Parameters**

| Parameter | Description |
|---|---|
| `force=1` or `refresh=1` | Force re-fetch from ESPN, bypassing cache |

**Response**
```json
{
  "season": "2025-26",
  "season_year": 2026,
  "league": "mens-college-basketball",
  "teams": [
    {
      "team_id": "2540",
      "school_name": "UC Santa Barbara Gauchos",
      "abbreviation": "UCSB",
      "team_ref": "https://..."
    }
  ]
}
```

---

#### `GET /api/schools`

Alias for the team list, formatted for school-picker UIs.

**Response**
```json
{
  "season": "2025-26",
  "season_year": 2026,
  "schools": [ ... ],
  "source": "espn-core-api-cache",
  "last_updated": "...",
  "warning": ""
}
```

---

### Season Stats

#### `GET /api/espn/season/<team_id>/<dataset>`

Returns season stats for a team. For teams with a PDF source (e.g. `ucsb`, `ucr`), data is parsed from the PDF. Otherwise, data is fetched from the ESPN Core API and cached as CSV.

**Path Parameters**

| Parameter | Values | Description |
|---|---|---|
| `team_id` | e.g. `2540`, `ucsb`, `ucr` | Team identifier |
| `dataset` | `team`, `player` / `players` | Stat table to return |

**Response**
```json
{
  "team_id": "2540",
  "school_name": "UC Santa Barbara Gauchos",
  "dataset": "players",
  "columns": ["Player", "GP-GS", "MIN", "PTS", ...],
  "rows": [ { "Player": "John Smith", "PTS": "14.2", ... } ],
  "row_key": "row_key",
  "updated_at": "...",
  "source_urls": ["https://..."],
  "schema_version": "espn-season-v1"
}
```

> **Note:** The `row_key` field is present in each row object but omitted from `columns` in the response. It is used internally for evidence validation.

---

#### `GET /api/data/<team_id>/<dataset>`

Identical to `/api/espn/season/<team_id>/<dataset>`. Included as an alternate route.

---

#### `POST /api/espn/season/<team_id>/update`

Triggers a re-scrape of ESPN season data for the given team. For PDF-sourced teams, this is a no-op.

**Request Body**
```json
{ "force": true }
```

**Response**
```json
{ "ok": true, "summary": { "team_id": "2540", "school_name": "...", "files": [...] } }
```

---

#### `POST /api/scrape/<team_id>`

Directly triggers a full ESPN scrape for the team (team stats + roster + player stats).

**Response**
```json
{ "ok": true, "summary": { "team_id": "...", "parse_meta": { "team_stat_rows": 42, "player_rows": 14 } } }
```

---

### Play-by-Play (PBP)

#### `GET /api/pbp`

Returns play-by-play rows from the local cache for a given game. Supports extensive filtering.

**Query Parameters**

| Parameter | Description |
|---|---|
| `game_id` | ESPN game ID (default: configured `ESPN_PBP_GAME_ID`) |
| `q` | Full-text search across all fields |
| `text` | Filter by play description text |
| `type` | Filter by play type (comma-separated for multiple) |
| `team_id` | Filter by team (`UCSB` or `Opponent`, or numeric ID) |
| `period` | Filter by period: `1`, `2`, `3`, `4`, or `OT` (comma-separated) |
| `clock_mode` | `last_n` or `range` |
| `clock_last_n_minutes` | Used with `clock_mode=last_n`; returns plays in the last N minutes of the clock |
| `clock_from` | Used with `clock_mode=range`; start of clock range in `MM:SS` (higher value = more time remaining) |
| `clock_to` | Used with `clock_mode=range`; end of clock range in `MM:SS` |

**Clock Range Example**

To get plays between 5:00 and 2:00 remaining:
```
?clock_mode=range&clock_from=05:00&clock_to=02:00
```

**Response**
```json
{
  "team_id": "pbp",
  "dataset": "pbp",
  "columns": ["team_id", "type", "text", "period", "clock", ...],
  "rows": [ { "team_id": "UCSB", "type": "Made Shot", "text": "...", ... } ],
  "row_key": "row_key",
  "updated_at": "...",
  "source_url": "https://..."
}
```

---

#### `GET /api/pbp/live-stats`

Computes live in-game statistics derived from the cached PBP data, broken out by team and player.

**Query Parameters**

| Parameter | Default | Description |
|---|---|---|
| `ucsb` | `2540` | UCSB team ID |
| `opponent` | *(inferred)* | Opponent team ID |
| `game_id` | *(configured default)* | ESPN game ID |

**Response**
```json
{
  "ucsb_team": { "columns": [...], "rows": [...] },
  "ucsb_players": { "columns": [...], "rows": [...] },
  "opponent_team": { "columns": [...], "rows": [...] },
  "opponent_players": { "columns": [...], "rows": [...] }
}
```

Team rows include: `pts`, `fgm`, `fga`, `fg_pct`, `3pm`, `3pa`, `3p_pct`, `ftm`, `fta`, `ft_pct`, `oreb`, `dreb`, `reb`, `ast`, `to`, `stl`, `blk`, `pf`.

Player rows include: `Player`, `PTS`, `REB`, `AST`, `STL`, `BLK`, `TO`, `FG_PCT`, `FG3_PCT`, `FT_PCT`, `PF`.

---

#### `POST /api/pbp/update`

Fetches fresh PBP data from ESPN and saves it to the local cache. Rate-limited to once every 10 seconds (bypass with `force: true`).

**Request Body**
```json
{ "force": false, "game_id": "401809115" }
```

**Response**
```json
{
  "ok": true,
  "summary": {
    "rows": 312,
    "columns": [...],
    "source_url": "https://...",
    "file": "data/pbp-401809115.json",
    "updated_at": "...",
    "schema_version": "espn-pbp-v1"
  }
}
```

---

#### `GET /api/gameids`

Returns ESPN game IDs for UCSB's current-season schedule.

**Response**
```json
{ "games": ["401809115", "401809116", ...] }
```

---

### Evidence & Insights

#### `POST /api/evidence/validate`

Validates a list of evidence references against loaded datasets. Useful for checking that row keys and field names are correct before submitting to the insights endpoint.

**Request Body**
```json
{
  "refs": [
    {
      "team_id": "2540",
      "dataset": "players",
      "row_key": "john_smith_12",
      "fields": ["PTS", "REB"]
    }
  ]
}
```

**Response**
```json
{
  "results": [
    { "valid": true, "ref": { ... } },
    { "valid": false, "reason": "row_key not found", "ref": { ... } }
  ]
}
```

---

#### `POST /api/insights`

Generates AI-powered live game insights using the OpenAI API. Requires at least one PBP context. Validates and retries the model response up to 3 times to ensure schema and evidence correctness.

**Request Body**
```json
{
  "prompt": "What scoring trends have emerged in the last 5 minutes?",
  "contexts": [
    { "dataset": "pbp", "team_id": "pbp", "game_id": "401809115" },
    { "dataset": "players", "team_id": "2540" }
  ]
}
```

**Response**
```json
{
  "ok": true,
  "insights": [
    {
      "insight": "UCSB has scored 11 points in the last 5 minutes, going 4-of-6 from the field (67%)...",
      "evidence": [
        {
          "team_id": "pbp",
          "dataset": "pbp",
          "row_key": "play_401809115_88",
          "fields": ["text", "clock", "home_score"]
        }
      ]
    }
  ]
}
```

Each insight includes 3–6 data-backed observations. Evidence references are validated against the provided context datasets — fabricated row keys or field names are rejected.

---

## Data Sources

### ESPN Core API

Season stats, team metadata, and rosters are fetched from `https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball`. The scraper traverses `$ref` links to resolve team stats, athlete rosters, and per-player statistics, then caches results as CSV files.

### Sidearm PDFs

For UCSB and UCR, season stats are parsed from PDF stat sheets located in `analytics_engine/data/`. The parser uses `pdfplumber` to extract text from the first page and heuristically identifies player and team stat tables using column header patterns like `GP-GS` and `Player`.

The following teams use PDF sources:

| Key | File | Team Name |
|---|---|---|
| `ucsb` / `2540` | `ucsb-season-stats.pdf` | UC Santa Barbara |
| `ucr` | `ucr-season-stats.pdf` | UC Riverside |

### ESPN Play-by-Play (CDN)

PBP data is fetched from `https://cdn.espn.com/core/mens-college-basketball/playbyplay?gameid=<id>` and also paginated via the ESPN Core API plays endpoint. Data is saved locally as a JSON array of play objects.

---

## Player Table Columns

The default player table display applies the following column mapping and order (configurable via `PLAYER_TABLE_CONFIG` env var):

| Display Column | Source Field | Notes |
|---|---|---|
| `Player` | `player` | |
| `GP-GS` | `gp_gs` | |
| `MIN` | `min` | |
| `PTS` | `pts` | |
| `PPG` | `avg_3` | Points per game |
| `REB` | `tot` | Total rebounds |
| `RPG` | *(computed)* | `tot / GP` |
| `AST` | `a` | |
| `APG` | *(computed)* | `a / GP` |
| `TO` | `to` | |
| `STL` | `stl` | |
| `BLK` | `blk` | |
| `PF` | `pf` | |
| `FG%` | `fg_pct` | |
| `FT%` | `ft_pct` | |
| `3P%` | `fg3_pct` | |

---

## Error Responses

All error responses share the same shape:

```json
{ "error": "Human-readable error message" }
```

| HTTP Status | Meaning |
|---|---|
| `400` | Invalid filter parameters (PBP filters) |
| `404` | Route not found |
| `500` | Internal server error (scrape failure, missing data, OpenAI error, etc.) |

---

## CORS

All responses include permissive CORS headers:

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

`OPTIONS` preflight requests return `204 No Content`.