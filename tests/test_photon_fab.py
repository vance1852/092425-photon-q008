from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path

from photon_fab.api import Handler
from photon_fab.errors import Conflict, ValidationFailed
from photon_fab.service import PhotonService
from photon_fab.storage import connect


class ApprovalTestMixin:
    def setUp(self) -> None:  # noqa: D401
        self.service = PhotonService(":memory:")
        self.service.bootstrap_admin()
        self.admin = self.service.login("admin", "photon-admin")
        self.service.auth.create_user("qa-a", "password1", "quality")
        self.service.auth.create_user("qa-b", "password2", "quality")
        self.qa_a = self.service.login("qa-a", "password1")
        self.qa_b = self.service.login("qa-b", "password2")
        self.service.create_lot(self.admin, "LOT-1", "CMOS image sensor", "P3.2", 10)

    def _seed_measurements(self, lot_id: str = "LOT-1") -> None:
        for wavelength, response in ((450, 0.71), (520, 0.93), (650, 0.84)):
            self.service.add_measurement(self.admin, lot_id, wavelength, response, 0.01, "spec-1")


class ApprovalServiceTests(ApprovalTestMixin, unittest.TestCase):
    def test_first_decision_transitions_pending_and_bumps_version(self) -> None:
        lot = self.service.approve(self.qa_a, "LOT-1", "release", "指标达标")
        self.assertEqual(lot["status"], "released")
        self.assertEqual(lot["version"], 2)
        approvals = self.service.db.execute("SELECT reviewer,decision,effective FROM approvals").fetchall()
        self.assertEqual([tuple(r) for r in approvals], [("qa-a", "release", 1)])

    def test_opposing_decision_conflicts_and_leaves_state_untouched(self) -> None:
        self.service.approve(self.qa_a, "LOT-1", "release", "指标达标")
        with self.assertRaises(Conflict) as ctx:
            self.service.approve(self.qa_b, "LOT-1", "reject", "复测异常")
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.details["current_status"], "released")
        self.assertEqual(ctx.exception.details["effective_decision"], "release")
        self.assertEqual(ctx.exception.details["effective_reviewer"], "qa-a")

        lot = self.service.get_lot(self.admin, "LOT-1")
        self.assertEqual(lot["status"], "released")
        self.assertEqual(lot["version"], 2)
        # 失败的决定不得写入生效审批表。
        rows = self.service.db.execute("SELECT reviewer FROM approvals WHERE effective=1").fetchall()
        self.assertEqual([r[0] for r in rows], ["qa-a"])

    def test_conflict_is_visible_in_audit(self) -> None:
        self.service.approve(self.qa_a, "LOT-1", "release", "指标达标")
        with self.assertRaises(Conflict):
            self.service.approve(self.qa_b, "LOT-1", "reject", "复测异常")
        events = self.service.audit(self.admin, "LOT-1")
        kinds = [(e["event_type"], e["actor"]) for e in events]
        self.assertEqual(kinds[-2], ("approval", "qa-a"))
        self.assertEqual(kinds[-1], ("approval.conflicted", "qa-b"))
        payload = json.loads(events[-1]["payload"])
        self.assertEqual(payload["decision"], "reject")
        self.assertEqual(payload["reason_code"], "already_decided")
        self.assertEqual(payload["current_status"], "released")
        self.assertEqual(payload["current_version"], 2)
        self.assertEqual(payload["effective_decision"], "release")

    def test_same_decision_replay_is_idempotent(self) -> None:
        first = self.service.approve(self.qa_a, "LOT-1", "hold", "等待复核")
        second = self.service.approve(self.qa_b, "LOT-1", "hold", "重复提交同一决定")
        third = self.service.approve(self.qa_a, "LOT-1", "hold", "再试一次")
        self.assertEqual(first, second)
        self.assertEqual(first, third)
        events = self.service.audit(self.admin, "LOT-1")
        self.assertEqual([e["event_type"] for e in events], ["created", "approval"])
        self.assertEqual(self.service.get_lot(self.admin, "LOT-1")["version"], 2)

    def test_expected_version_optimistic_lock(self) -> None:
        with self.assertRaises(Conflict) as ctx:
            self.service.approve(self.qa_a, "LOT-1", "release", "基于旧版本", expected_version=99)
        self.assertEqual(ctx.exception.details["reason_code"], "version_mismatch")
        self.assertEqual(self.service.get_lot(self.admin, "LOT-1")["status"], "engineering")
        events = self.service.audit(self.admin, "LOT-1")
        self.assertEqual(events[-1]["event_type"], "approval.conflicted")
        # 正确版本可以正常推进。
        lot = self.service.approve(self.qa_b, "LOT-1", "hold", "版本匹配", expected_version=1)
        self.assertEqual(lot["status"], "hold")
        self.assertEqual(lot["version"], 2)

    def test_invalid_decision_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.approve(self.qa_a, "LOT-1", "ship", "理由")
        with self.assertRaises(ValidationFailed):
            self.service.approve(self.qa_a, "LOT-1", "release", "  ")

    def test_concurrent_opposing_decisions_have_single_winner(self) -> None:
        barrier = threading.Barrier(2)

        def decide(token: str, decision: str) -> tuple[str, dict | None]:
            barrier.wait()
            try:
                return "ok", self.service.approve(token, "LOT-1", decision, f"并发{decision}")
            except Conflict as exc:
                return "conflict", exc.details

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(decide, [self.qa_a, self.qa_b], ["release", "reject"]))

        outcomes = {r[0] for r in results}
        self.assertEqual(outcomes, {"ok", "conflict"})
        winner = next(r[1] for r in results if r[0] == "ok")
        loser = next(r[1] for r in results if r[0] == "conflict")
        self.assertEqual(winner["version"], 2)
        self.assertEqual(loser["current_version"], 2)
        self.assertEqual(loser["reason_code"], "already_decided")
        winning_decision = {"released": "release", "hold": "hold", "rejected": "reject"}[winner["status"]]
        self.assertEqual(loser["effective_decision"], winning_decision)

        lot = self.service.get_lot(self.admin, "LOT-1")
        self.assertIn(lot["status"], {"released", "rejected"})
        self.assertEqual(lot["version"], 2)
        effective = self.service.db.execute("SELECT count(*) FROM approvals WHERE effective=1").fetchone()[0]
        self.assertEqual(effective, 1)
        events = self.service.audit(self.admin, "LOT-1")
        self.assertEqual([e["event_type"] for e in events], ["created", "approval", "approval.conflicted"])

    def test_concurrent_same_decision_both_see_original_result(self) -> None:
        barrier = threading.Barrier(2)

        def decide(token: str) -> dict:
            barrier.wait()
            return self.service.approve(token, "LOT-1", "hold", "一致决定")

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(decide, [self.qa_a, self.qa_b]))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0]["status"], "hold")
        events = self.service.audit(self.admin, "LOT-1")
        self.assertEqual([e["event_type"] for e in events], ["created", "approval"])


class ApprovalPersistenceTests(ApprovalTestMixin, unittest.TestCase):
    def _fresh_file(self) -> str:
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        return tmp.name

    def test_state_and_audit_survive_restart(self) -> None:
        path = self._fresh_file()
        first = PhotonService(path)
        first.bootstrap_admin()
        admin = first.login("admin", "photon-admin")
        first.auth.create_user("qa-a", "password1", "quality")
        first.auth.create_user("qa-b", "password2", "quality")
        qa_a = first.login("qa-a", "password1")
        qa_b = first.login("qa-b", "password2")
        first.create_lot(admin, "LOT-P", "sensor", "P1", 4)
        first.approve(qa_a, "LOT-P", "release", "重启前放行")
        with self.assertRaises(Conflict):
            first.approve(qa_b, "LOT-P", "reject", "重启前冲突")
        before_events = first.audit(admin, "LOT-P")
        first.db.close()

        restarted = PhotonService(path)
        admin2 = restarted.login("admin", "photon-admin")
        qa_b2 = restarted.login("qa-b", "password2")
        lot = restarted.get_lot(admin2, "LOT-P")
        self.assertEqual(lot["status"], "released")
        self.assertEqual(lot["version"], 2)
        with self.assertRaises(Conflict):
            restarted.approve(qa_b2, "LOT-P", "reject", "重启后再冲突")
        after_events = restarted.audit(admin2, "LOT-P")
        self.assertEqual(
            [(e["event_type"], e["actor"], e["payload"]) for e in before_events],
            [(e["event_type"], e["actor"], e["payload"]) for e in after_events[: len(before_events)]],
        )
        self.assertEqual(after_events[-1]["event_type"], "approval.conflicted")
        effective = restarted.db.execute(
            "SELECT reviewer,decision FROM approvals WHERE lot_id=? AND effective=1", ("LOT-P",)
        ).fetchall()
        self.assertEqual([tuple(r) for r in effective], [("qa-a", "release")])
        restarted.db.close()

    def test_pre_version_schema_is_migrated_on_connect(self) -> None:
        path = self._fresh_file()
        # 按基线旧结构手工建库：无 version 列、approvals 无 effective 列，
        # 并模拟旧逻辑下后写覆盖先写的终态。
        legacy = sqlite3.connect(path)
        legacy.executescript(
            """
            CREATE TABLE chip_lots(lot_id TEXT PRIMARY KEY, product TEXT NOT NULL,
             process_rev TEXT NOT NULL, wafer_count INTEGER NOT NULL, status TEXT NOT NULL,
             owner TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE approvals(lot_id TEXT NOT NULL, reviewer TEXT NOT NULL,
             decision TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
             PRIMARY KEY(lot_id,reviewer));
            CREATE TABLE lot_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT NOT NULL,
             event_type TEXT NOT NULL, actor TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            INSERT INTO chip_lots VALUES('LOT-O','sensor','P1',3,'rejected','admin','t0','t1');
            INSERT INTO approvals VALUES('LOT-O','qa-a','release','ok','t0');
            INSERT INTO approvals VALUES('LOT-O','qa-b','reject','bad','t1');
            """
        )
        legacy.commit()
        legacy.close()

        service = connect(path)
        lot = service.execute("SELECT status,version FROM chip_lots WHERE lot_id='LOT-O'").fetchone()
        self.assertEqual(lot["version"], 1)
        rows = service.execute("SELECT reviewer,effective FROM approvals ORDER BY reviewer").fetchall()
        self.assertEqual([tuple(r) for r in rows], [("qa-a", 0), ("qa-b", 1)])
        service.close()


class ApprovalHttpTests(ApprovalTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))
        Handler.service = PhotonService(tmp.name)
        Handler.service.bootstrap_admin()
        Handler.service.auth.create_user("qa-a", "password1", "quality")
        Handler.service.auth.create_user("qa-b", "password2", "quality")
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)

    def _shutdown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    def _post(self, path: str, token: str | None, body: dict) -> tuple[int, dict]:
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json", "Content-Length": str(len(data))}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def _login(self, user_id: str, password: str) -> str:
        status, body = self._post("/login", None, {"user_id": user_id, "password": password})
        self.assertEqual(status, 200)
        return body["token"]

    def _get(self, path: str, token: str) -> tuple[int, dict]:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            headers={"Authorization": f"Bearer {token}"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_http_approval_success_conflict_and_replay(self) -> None:
        qa_a = self._login("qa-a", "password1")
        qa_b = self._login("qa-b", "password2")
        admin = self._login("admin", "photon-admin")
        self._post("/lots", admin, {"lot_id": "LOT-H", "product": "sensor", "process_rev": "P9", "wafer_count": 2})

        status, first = self._post("/lots/LOT-H/approvals", qa_a, {"decision": "release", "reason": "合格"})
        self.assertEqual(status, 200)
        self.assertEqual(first["status"], "released")
        self.assertEqual(first["version"], 2)

        status, body = self._post("/lots/LOT-H/approvals", qa_b, {"decision": "reject", "reason": "不合格"})
        self.assertEqual(status, 409)
        self.assertEqual(body["code"], "conflict")
        self.assertEqual(body["details"]["effective_decision"], "release")

        status, replay = self._post("/lots/LOT-H/approvals", qa_b, {"decision": "release", "reason": "重试"})
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)

        _, events_body = self._get("/lots/LOT-H/audit", admin)
        events = events_body["events"]
        self.assertEqual([e["event_type"] for e in events], ["created", "approval", "approval.conflicted"])

    def test_http_concurrent_opposing_decisions(self) -> None:
        qa_a = self._login("qa-a", "password1")
        qa_b = self._login("qa-b", "password2")
        admin = self._login("admin", "photon-admin")
        self._post("/lots", admin, {"lot_id": "LOT-C", "product": "sensor", "process_rev": "P9", "wafer_count": 2})
        barrier = threading.Barrier(2)

        def post(token: str, decision: str) -> tuple[int, dict]:
            barrier.wait()
            return self._post("/lots/LOT-C/approvals", token, {"decision": decision, "reason": f"http{decision}"})

        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(post, [qa_a, qa_b], ["release", "reject"]))
        codes = sorted(code for code, _ in statuses)
        self.assertEqual(codes, [200, 409])


if __name__ == "__main__":
    unittest.main()
