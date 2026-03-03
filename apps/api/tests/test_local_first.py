import tempfile
import time
import unittest
from pathlib import Path

from apps.api.local_first import LocalFirstService, classify_shot_zone, format_pct


class ShotZoneHeuristicTests(unittest.TestCase):
    def test_classify_shot_zone(self) -> None:
        self.assertEqual(classify_shot_zone("Player makes layup"), "layup")
        self.assertEqual(classify_shot_zone("Player makes driving dunk"), "dunk")
        self.assertEqual(classify_shot_zone("Player hits pullup jumper"), "mid")
        self.assertIsNone(classify_shot_zone("Player makes three point jumper"))


class PercentageFromMakesAttemptsTests(unittest.TestCase):
    def test_format_pct(self) -> None:
        self.assertEqual(format_pct(7, 10), ".700")
        self.assertEqual(format_pct(0, 0), "")

    def test_player_dataset_uses_makes_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = LocalFirstService(db_path=root / "state.sqlite3", object_store_root=root / "object_store")
            with service.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO season_player_stats (
                        season_id, season_type, team_id, player_key, athlete_id, player_name, games_played, points, rebounds, assists,
                        turnovers, steals, blocks, personal_fouls, fgm, fga, fg3m, fg3a, ftm, fta,
                        layup_m, layup_a, dunk_m, dunk_a, mid_m, mid_a
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "2025-2026",
                        "regular",
                        "2540",
                        "1",
                        "1",
                        "Guard One",
                        2,
                        18,
                        6,
                        4,
                        2,
                        1,
                        0,
                        3,
                        7,
                        10,
                        3,
                        5,
                        1,
                        2,
                        2,
                        3,
                        0,
                        0,
                        2,
                        4,
                    ),
                )
            payload = service.player_dataset("2540")
            self.assertEqual(payload["rows"][0]["FG%"], ".700")
            self.assertEqual(payload["rows"][0]["3P%"], ".600")
            self.assertEqual(payload["rows"][-1]["Player"], "Team")


class ScheduleValidationTests(unittest.TestCase):
    def test_validate_schedule_reference(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = LocalFirstService(db_path=root / "state.sqlite3", object_store_root=root / "object_store")
            reference = service.load_schedule_reference()["games"]
            service.validate_schedule(reference)
            with self.assertRaises(RuntimeError):
                bad = list(reference)
                bad[0] = {**bad[0], "home_away": "away"}
                service.validate_schedule(bad)

    def test_parse_schedule_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = LocalFirstService(db_path=root / "state.sqlite3", object_store_root=root / "object_store")
            payload = {
                "events": [
                    {
                        "id": "401809115",
                        "date": "2026-02-08T03:00Z",
                        "competitions": [
                            {
                                "competitors": [
                                    {"team": {"id": "2540", "displayName": "UC Santa Barbara Gauchos"}, "homeAway": "home"},
                                    {"team": {"id": "300", "displayName": "UC Irvine Anteaters"}, "homeAway": "away"},
                                ]
                            }
                        ],
                    }
                ]
            }
            games = service.parse_schedule_payload(payload)
            self.assertEqual(games[0]["game_id"], "401809115")
            self.assertEqual(games[0]["opponent_team_id"], "300")
            self.assertEqual(games[0]["home_away"], "home")


class BuildJobTransitionTests(unittest.TestCase):
    def test_build_job_transitions_to_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = LocalFirstService(db_path=root / "state.sqlite3", object_store_root=root / "object_store")
            service.verify_and_persist_schedule = lambda force=False: [  # type: ignore[assignment]
                {"game_id": "1", "date": "2026-01-01", "opponent_team_id": "27", "opponent_name": "UC Riverside", "home_away": "home"}
            ]
            service.ingest_game = lambda game_id, force=False: {"rows": 10}  # type: ignore[assignment]
            service.derive_game_stats = lambda game_id: None  # type: ignore[assignment]
            service.aggregate_season = lambda: None  # type: ignore[assignment]
            job = service.start_build("season", "2540", force=False)
            deadline = time.time() + 5
            current = job
            while time.time() < deadline:
                current = service.get_job(job["job_id"])
                if current["status"] in {"succeeded", "failed"}:
                    break
                time.sleep(0.05)
            self.assertEqual(current["status"], "succeeded")
            self.assertEqual(current["stage"], "aggregate_season_stats")


if __name__ == "__main__":
    unittest.main()
