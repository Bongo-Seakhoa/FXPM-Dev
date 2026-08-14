"""Protocol guarantees for the Claude<->Codex agent bus (tools/agent_bus.py)."""

import json
from concurrent.futures import ProcessPoolExecutor
import shutil
import unittest
from pathlib import Path
from unittest import mock

import tools.agent_bus as bus


def _process_post(comms: str, ordinal: int) -> int:
    from pathlib import Path
    import tools.agent_bus as process_bus

    process_bus.COMMS_DIR = Path(comms)
    process_bus.BUS_PATH = process_bus.COMMS_DIR / "BUS.jsonl"
    process_bus.LANES_PATH = process_bus.COMMS_DIR / "LANES.json"
    return process_bus.main(
        [
            "post",
            "--from",
            "codex",
            "--type",
            "status",
            "--body",
            f"concurrent-{ordinal}",
        ]
    )


def _process_claim(comms: str, agent: str) -> int:
    from pathlib import Path
    import tools.agent_bus as process_bus

    process_bus.COMMS_DIR = Path(comms)
    process_bus.BUS_PATH = process_bus.COMMS_DIR / "BUS.jsonl"
    process_bus.LANES_PATH = process_bus.COMMS_DIR / "LANES.json"
    return process_bus.main(
        [
            "claim",
            "--from",
            agent,
            "--paths",
            "pm_pipeline.py",
            "--purpose",
            "concurrent claim",
        ]
    )


class AgentBusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1] / ".tmp_agent_bus_tests"
        self.comms = self.root / "agent_comms"
        self.comms.mkdir(parents=True, exist_ok=True)
        self._patches = [
            mock.patch.object(bus, "COMMS_DIR", self.comms),
            mock.patch.object(bus, "BUS_PATH", self.comms / "BUS.jsonl"),
            mock.patch.object(bus, "LANES_PATH", self.comms / "LANES.json"),
        ]
        for patcher in self._patches:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in self._patches:
            patcher.stop()
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    def _run(self, *argv) -> int:
        return bus.main(list(argv))

    def _bus_records(self):
        return bus._read_bus_lines()

    def test_post_assigns_monotonic_per_agent_ids(self):
        self._run("post", "--from", "claude", "--type", "status", "--body", "one")
        self._run("post", "--from", "codex", "--type", "status", "--body", "two")
        self._run("post", "--from", "claude", "--type", "finding", "--body", "three")
        ids = [record["id"] for record in self._bus_records()]
        self.assertEqual(ids, ["CC-001", "CX-001", "CC-002"])

    def test_claim_conflict_blocks_overlapping_lane(self):
        self.assertEqual(
            self._run("claim", "--from", "codex", "--paths", "pm_pipeline.py",
                      "--purpose", "per-cell contract"),
            0,
        )
        # Same file by the other agent must be refused.
        self.assertEqual(
            self._run("claim", "--from", "claude", "--paths", "pm_pipeline.py",
                      "--purpose", "would collide"),
            3,
        )
        # Directory prefix overlap is also a conflict.
        self.assertEqual(
            self._run("claim", "--from", "codex", "--paths", "pm_dashboard",
                      "--purpose", "dash lane"),
            0,
        )
        self.assertEqual(
            self._run("claim", "--from", "claude", "--paths", "pm_dashboard/analytics.py",
                      "--purpose", "would collide"),
            3,
        )
        # Disjoint path is fine.
        self.assertEqual(
            self._run("claim", "--from", "claude", "--paths", "pm_storage.py",
                      "--purpose", "storage bounds"),
            0,
        )

    def test_release_frees_lane_for_other_agent(self):
        self._run("claim", "--from", "codex", "--paths", "pm_mt5.py", "--purpose", "acq")
        payload = bus._load_lanes()
        lane_id = payload["lanes"][0]["id"]
        # Wrong owner cannot release.
        self.assertEqual(self._run("release", "--from", "claude", "--lane", lane_id), 2)
        self.assertEqual(
            self._run("release", "--from", "codex", "--lane", lane_id, "--note", "done"),
            0,
        )
        self.assertEqual(
            self._run("claim", "--from", "claude", "--paths", "pm_mt5.py",
                      "--purpose", "audit pass"),
            0,
        )
        types = [record["type"] for record in self._bus_records()]
        self.assertEqual(types, ["claim", "release", "claim"])

    def test_bus_rotation_preserves_id_continuity(self):
        self._run("post", "--from", "claude", "--type", "status", "--body", "before")
        with mock.patch.object(bus, "BUS_MAX_BYTES", 10):
            self._run("post", "--from", "claude", "--type", "status", "--body", "after rotation")
        archives = list(self.comms.glob("BUS.archive-*.jsonl"))
        self.assertEqual(len(archives), 1)
        live = self._bus_records()
        self.assertEqual(len(live), 1)
        self.assertEqual(live[0]["id"], "CC-002", "archived ids must not be reused")

    def test_invalid_type_and_agent_rejected(self):
        with self.assertRaises(SystemExit):
            self._run("post", "--from", "claude", "--type", "gossip", "--body", "x")
        with self.assertRaises(SystemExit):
            self._run("post", "--from", "gpt", "--type", "status", "--body", "x")

    def test_blocking_posts_require_one_named_recipient(self):
        for kind in sorted(bus.BLOCKING_MESSAGE_TYPES):
            with self.subTest(kind=kind):
                self.assertEqual(
                    self._run(
                        "post",
                        "--from",
                        "codex-pm",
                        "--to",
                        "all",
                        "--type",
                        kind,
                        "--body",
                        "unowned",
                    ),
                    2,
                )
        self.assertEqual(self._bus_records(), [])
        self.assertEqual(
            self._run(
                "post",
                "--from",
                "codex-pm",
                "--to",
                "claude-pm",
                "--type",
                "review_request",
                "--body",
                "owned",
            ),
            0,
        )
        self.assertEqual(
            self._run(
                "post",
                "--from",
                "codex-pm",
                "--to",
                "all",
                "--type",
                "review",
                "--body",
                "nonblocking verdict",
            ),
            0,
        )

    def test_pm_identities_cannot_claim_or_release_implementation_lanes(self):
        self.assertEqual(
            self._run(
                "claim",
                "--from",
                "codex-pm",
                "--paths",
                "pm_pipeline.py",
                "--purpose",
                "wrong identity",
            ),
            2,
        )
        self.assertEqual(
            self._run(
                "post",
                "--from",
                "claude-pm",
                "--type",
                "claim",
                "--body",
                "manual wrong identity",
            ),
            2,
        )
        self.assertEqual(self._bus_records(), [])

        self.assertEqual(
            self._run(
                "claim",
                "--from",
                "codex",
                "--paths",
                "pm_pipeline.py",
                "--purpose",
                "implementation",
            ),
            0,
        )
        lane_id = bus._load_lanes()["lanes"][0]["id"]
        self.assertEqual(
            self._run("release", "--from", "codex-pm", "--lane", lane_id),
            2,
        )
        self.assertEqual(
            self._run("release", "--from", "codex", "--lane", lane_id),
            0,
        )

    def test_lane_history_stays_bounded(self):
        payload = bus._load_lanes()
        for index in range(150):
            payload["lanes"].append(
                {
                    "id": f"L-{index:04d}",
                    "owner": "claude",
                    "paths": [f"file_{index}.py"],
                    "status": "released",
                    "released_at": "2026-07-12T00:00:00+00:00",
                }
            )
        bus._save_lanes(payload)
        reloaded = bus._load_lanes()
        self.assertLessEqual(len(reloaded["lanes"]), bus.LANE_HISTORY_MAX)

    def test_concurrent_process_posts_keep_every_record_and_unique_id(self):
        with ProcessPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    _process_post,
                    [str(self.comms)] * 24,
                    range(24),
                )
            )

        self.assertEqual(results, [0] * 24)
        records = self._bus_records()
        self.assertEqual(len(records), 24)
        ids = [record["id"] for record in records]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(
            sorted(int(identifier.split("-", 1)[1]) for identifier in ids),
            list(range(1, 25)),
        )

    def test_concurrent_process_claims_have_exactly_one_winner(self):
        with ProcessPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(_process_claim, str(self.comms), agent)
                for agent in ("claude", "codex")
            ]
            results = [future.result() for future in futures]

        self.assertEqual(sorted(results), [0, 3])
        active = [
            lane
            for lane in bus._load_lanes()["lanes"]
            if lane.get("status") == "active"
        ]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["paths"], ["pm_pipeline.py"])
        claims = [
            record for record in self._bus_records() if record.get("type") == "claim"
        ]
        self.assertEqual(len(claims), 1)


if __name__ == "__main__":
    unittest.main()
