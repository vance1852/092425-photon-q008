"""质量审批的条件更新、幂等重放与并发冲突测试。"""

from __future__ import annotations

import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

from photon_fab.api import Handler
from photon_fab.errors import Conflict
from photon_fab.service import PhotonService


class ApprovalServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = PhotonService()
        self.service.bootstrap_admin()
        for user_id in ("qa-a", "qa-b"):
            self.service.auth.create_user(user_id, f"password-{user_id}", "quality")
        self.admin = self.service.auth.login("admin", "photon-admin")
        self.token_a = self.service.auth.login("qa-a", "password-qa-a")
        self.token_b = self.service.auth.login("qa-b", "password-qa-b")

    def tearDown(self) -> None:
        self.service.close()

    def _lot(self, lot_id: str = "LOT-1") -> str:
        self.service.create_lot(self.admin, lot_id, "CMOS image sensor", "P1.0", 5)
        return lot_id

    def _events(self, lot_id: str) -> list[str]:
        return [e["event_type"] for e in self.service.audit(self.admin, lot_id)]

    def test_opposite_decision_conflicts_and_is_audited(self) -> None:
        self._lot()
        released = self.service.approve(self.token_a, "LOT-1", "release", "meets spec")
        self.assertEqual(released["status"], "released")
        self.assertEqual(released["version"], 2)

        with self.assertRaises(Conflict) as ctx:
            self.service.approve(self.token_b, "LOT-1", "reject", "defect found")
        self.assertEqual(ctx.exception.current["status"], "released")

        lot = self.service.get_lot(self.admin, "LOT-1")
        self.assertEqual(lot["status"], "released")
        self.assertEqual(lot["version"], 2)

        approvals = self.service.db.execute("SELECT * FROM approvals WHERE lot_id='LOT-1'").fetchall()
        self.assertEqual(len(approvals), 1)
        self.assertEqual(approvals[0]["reviewer"], "qa-a")
        self.assertEqual(approvals[0]["decision"], "release")

        # 审计同时保留生效决定与被拒绝的冲突尝试。
        self.assertEqual(self._events("LOT-1"), ["created", "approval", "approval.conflict"])
        conflict_event = self.service.audit(self.admin, "LOT-1")[-1]
        self.assertEqual(conflict_event["actor"], "qa-b")
        payload = json.loads(conflict_event["payload"])
        self.assertEqual(payload["attempted_decision"], "reject")
        self.assertEqual(payload["current_status"], "released")
        self.assertEqual(payload["winner_reviewer"], "qa-a")
        self.assertEqual(payload["winner_decision"], "release")

    def test_same_decision_replay_returns_original_result(self) -> None:
        self._lot()
        first = self.service.approve(self.token_a, "LOT-1", "hold", "awaiting review")
        replay = self.service.approve(self.token_a, "LOT-1", "hold", "awaiting review")
        self.assertEqual(first, replay)
        # 重放不产生新的状态变化或审计事件。
        self.assertEqual(self._events("LOT-1"), ["created", "approval"])
        count = self.service.db.execute("SELECT count(*) FROM approvals WHERE lot_id='LOT-1'").fetchone()[0]
        self.assertEqual(count, 1)

    def test_same_reviewer_changed_request_conflicts(self) -> None:
        self._lot()
        self.service.approve(self.token_a, "LOT-1", "hold", "awaiting review")
        with self.assertRaises(Conflict):
            self.service.approve(self.token_a, "LOT-1", "release", "actually fine")
        with self.assertRaises(Conflict):
            self.service.approve(self.token_a, "LOT-1", "hold", "different reason")
        self.assertEqual(self.service.get_lot(self.admin, "LOT-1")["status"], "hold")

    def test_expected_version_is_enforced(self) -> None:
        self._lot()
        lot = self.service.get_lot(self.admin, "LOT-1")
        self.assertEqual(lot["version"], 1)
        with self.assertRaises(Conflict):
            self.service.approve(self.token_a, "LOT-1", "release", "stale client", expected_version=7)
        decided = self.service.approve(self.token_a, "LOT-1", "release", "meets spec", expected_version=1)
        self.assertEqual(decided["status"], "released")
        self.assertEqual(decided["version"], 2)
        # 决定形成后旧版本号不再可用。
        with self.assertRaises(Conflict):
            self.service.approve(self.token_b, "LOT-1", "reject", "late", expected_version=1)

    def test_concurrent_opposite_decisions_have_exactly_one_winner(self) -> None:
        for round_no in range(20):
            lot_id = self._lot(f"LOT-C{round_no}")
            barrier = threading.Barrier(2)
            outcomes: list[tuple[str, str | None]] = []

            def decide(token: str, decision: str) -> None:
                barrier.wait()
                try:
                    lot = self.service.approve(token, lot_id, decision, f"{decision} by reviewer")
                    outcomes.append(("ok", lot["status"]))
                except Conflict:
                    outcomes.append(("conflict", None))

            threads = [
                threading.Thread(target=decide, args=(self.token_a, "release")),
                threading.Thread(target=decide, args=(self.token_b, "reject")),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(sorted(kind for kind, _ in outcomes), ["conflict", "ok"])
            winner_status = next(status for kind, status in outcomes if kind == "ok")
            final = self.service.get_lot(self.admin, lot_id)
            self.assertEqual(final["status"], winner_status)
            self.assertEqual(final["version"], 2)
            self.assertEqual(self._events(lot_id), ["created", "approval", "approval.conflict"])
            count = self.service.db.execute(
                "SELECT count(*) FROM approvals WHERE lot_id=?", (lot_id,)
            ).fetchone()[0]
            self.assertEqual(count, 1)


class ApprovalPersistenceTests(unittest.TestCase):
    def test_decision_replay_and_conflict_survive_restart(self) -> None:
        with TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "photon.sqlite3")
            service = PhotonService(path)
            service.bootstrap_admin()
            service.auth.create_user("qa-a", "password-qa-a", "quality")
            admin = service.auth.login("admin", "photon-admin")
            token = service.auth.login("qa-a", "password-qa-a")
            service.create_lot(admin, "LOT-R", "CMOS image sensor", "P1.0", 5)
            service.approve(token, "LOT-R", "release", "meets spec")
            audit_before = service.audit(admin, "LOT-R")
            service.close()

            reopened = PhotonService(path)
            try:
                admin2 = reopened.auth.login("admin", "photon-admin")
                token2 = reopened.auth.login("qa-a", "password-qa-a")
                lot = reopened.get_lot(admin2, "LOT-R")
                self.assertEqual(lot["status"], "released")
                self.assertEqual(lot["version"], 2)
                self.assertEqual(reopened.audit(admin2, "LOT-R"), audit_before)

                replay = reopened.approve(token2, "LOT-R", "release", "meets spec")
                self.assertEqual(replay["status"], "released")
                # 重放不改变重启后的审计内容。
                self.assertEqual(reopened.audit(admin2, "LOT-R"), audit_before)

                with self.assertRaises(Conflict):
                    reopened.approve(token2, "LOT-R", "reject", "late defect")
                events = reopened.audit(admin2, "LOT-R")
                self.assertEqual(events[-1]["event_type"], "approval.conflict")
                self.assertEqual(reopened.get_lot(admin2, "LOT-R")["status"], "released")
            finally:
                reopened.close()

    def test_concurrent_services_on_shared_database_file(self) -> None:
        # 两个服务实例（两条连接）模拟重启前后的进程并发，靠 BEGIN IMMEDIATE
        # 与条件更新保证仍然只有一个决定生效。
        with TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "photon.sqlite3")
            services = [PhotonService(path), PhotonService(path)]
            try:
                services[0].bootstrap_admin()
                services[0].auth.create_user("qa-a", "password-qa-a", "quality")
                services[0].auth.create_user("qa-b", "password-qa-b", "quality")
                admin = services[0].auth.login("admin", "photon-admin")
                tokens = [
                    services[0].auth.login("qa-a", "password-qa-a"),
                    services[1].auth.login("qa-b", "password-qa-b"),
                ]
                services[0].create_lot(admin, "LOT-X", "CMOS image sensor", "P1.0", 5)

                barrier = threading.Barrier(2)
                outcomes: list[str] = []

                def decide(service: PhotonService, token: str, decision: str) -> None:
                    barrier.wait()
                    try:
                        service.approve(token, "LOT-X", decision, f"{decision} by reviewer")
                        outcomes.append("ok")
                    except Conflict:
                        outcomes.append("conflict")

                threads = [
                    threading.Thread(target=decide, args=(services[0], tokens[0], "release")),
                    threading.Thread(target=decide, args=(services[1], tokens[1], "reject")),
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                self.assertEqual(sorted(outcomes), ["conflict", "ok"])
                final = services[1].get_lot(tokens[1], "LOT-X")
                self.assertIn(final["status"], ("released", "rejected"))
                self.assertEqual(final["version"], 2)
                events = [e["event_type"] for e in services[1].audit(tokens[1], "LOT-X")]
                self.assertEqual(events, ["created", "approval", "approval.conflict"])
            finally:
                for service in services:
                    service.close()


class ApprovalApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        service = PhotonService(str(Path(self.tmp.name) / "photon.sqlite3"))
        service.bootstrap_admin()
        service.auth.create_user("qa-a", "password-qa-a", "quality")
        service.auth.create_user("qa-b", "password-qa-b", "quality")
        self.service = service

        class _Handler(Handler):
            pass

        _Handler.service = service
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.service.close()
        self.tmp.cleanup()

    def _request(self, method: str, path: str, body: dict | None = None, token: str | None = None) -> tuple[int, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        connection.request(method, path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        return response.status, payload

    def _login(self, user_id: str) -> str:
        status, body = self._request("POST", "/login", {"user_id": user_id, "password": f"password-{user_id}" if user_id != "admin" else "photon-admin"})
        self.assertEqual(status, 200)
        return body["token"]

    def test_concurrent_approvals_over_http(self) -> None:
        admin = self._login("admin")
        token_a = self._login("qa-a")
        token_b = self._login("qa-b")
        status, _ = self._request(
            "POST", "/lots",
            {"lot_id": "LOT-HTTP", "product": "CMOS image sensor", "process_rev": "P1.0", "wafer_count": 5},
            admin,
        )
        self.assertEqual(status, 201)

        barrier = threading.Barrier(2)
        statuses: list[int] = []

        def decide(token: str, decision: str) -> None:
            barrier.wait()
            code, _ = self._request(
                "POST", "/lots/LOT-HTTP/approval",
                {"decision": decision, "reason": f"{decision} reason"}, token,
            )
            statuses.append(code)

        threads = [
            threading.Thread(target=decide, args=(token_a, "release")),
            threading.Thread(target=decide, args=(token_b, "reject")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(statuses), [200, 409])

        status, lot = self._request("GET", "/lots/LOT-HTTP", token=admin)
        self.assertEqual(status, 200)
        self.assertIn(lot["status"], ("released", "rejected"))

        status, audit = self._request("GET", "/lots/LOT-HTTP/audit", token=admin)
        self.assertEqual(status, 200)
        types = [e["event_type"] for e in audit["events"]]
        self.assertEqual(types, ["created", "approval", "approval.conflict"])

        # 与生效决定一致的重复提交返回原结果。
        winning = "release" if lot["status"] == "released" else "reject"
        winning_token = token_a if winning == "release" else token_b
        status, replayed = self._request(
            "POST", "/lots/LOT-HTTP/approval",
            {"decision": winning, "reason": f"{winning} reason"}, winning_token,
        )
        self.assertEqual(status, 200)
        self.assertEqual(replayed["status"], lot["status"])

        # 失败方的重复尝试仍是 409，且不再新增审计事件。
        losing = "reject" if winning == "release" else "release"
        losing_token = token_b if winning == "release" else token_a
        status, body = self._request(
            "POST", "/lots/LOT-HTTP/approval",
            {"decision": losing, "reason": f"{losing} reason"}, losing_token,
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["status"], lot["status"])
        _, audit = self._request("GET", "/lots/LOT-HTTP/audit", token=admin)
        types = [e["event_type"] for e in audit["events"]]
        self.assertEqual(types.count("approval"), 1)


if __name__ == "__main__":
    unittest.main()
