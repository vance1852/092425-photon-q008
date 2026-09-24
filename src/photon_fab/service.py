"""协调认证、批次、测试和放行门禁的应用服务。"""

from __future__ import annotations

import threading
import uuid

from .analytics import confidence_interval, summarize_spectrum, yield_rate
from .auth import Auth
from .errors import Conflict
from .storage import connect, event, transaction, utcnow

# 审批决定 -> 批次终态。
TERMINAL_STATUS = {"release": "released", "hold": "hold", "reject": "rejected"}
# 可以提交审批的唯一前置状态。
REVIEWABLE_STATUS = "engineering"


class PhotonService:
    def __init__(self, database: str = ":memory:"):
        # HTTP 服务在多个请求线程间共享同一个连接，所有写操作经此锁串行化。
        self._lock = threading.RLock()
        self.db = connect(database)
        self.auth = Auth(self.db, lock=self._lock)

    def bootstrap_admin(self, user_id: str = "admin", password: str = "photon-admin") -> None:
        try:
            self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, wafer_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if wafer_count <= 0 or not lot_id.strip() or not process_rev.strip():
            raise ValueError("lot fields are invalid")
        now = utcnow()
        with transaction(self.db, lock=self._lock):
            self.db.execute("INSERT INTO chip_lots VALUES(?,?,?,?,?,?,?,?,1)", (lot_id, product, process_rev, wafer_count, REVIEWABLE_STATUS, actor.user_id, now, now))
            event(self.db, lot_id, "created", actor.user_id, {"product": product, "process_rev": process_rev})
        return self.get_lot(token, lot_id)

    def get_lot(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "read")
        with self._lock:
            return self._snapshot(lot_id)

    def add_measurement(self, token: str, lot_id: str, wavelength_nm: float, response: float, noise: float, instrument: str) -> dict:
        actor = self.auth.require(token, "measure")
        measurement_id = uuid.uuid4().hex
        with transaction(self.db, lock=self._lock):
            if not self.db.execute("SELECT 1 FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone():
                raise KeyError(lot_id)
            self.db.execute("INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?)", (measurement_id, lot_id, float(wavelength_nm), float(response), float(noise), instrument, actor.user_id, utcnow()))
            event(self.db, lot_id, "measurement", actor.user_id, {"measurement_id": measurement_id, "wavelength_nm": wavelength_nm})
        return {"measurement_id": measurement_id, "lot_id": lot_id}

    def analyze(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "analyze")
        with self._lock:
            rows = self.db.execute("SELECT wavelength_nm,response FROM measurements WHERE lot_id=? ORDER BY wavelength_nm", (lot_id,)).fetchall()
            if len(rows) < 3:
                raise ValueError("three measurements are required")
            summary = summarize_spectrum([r[0] for r in rows], [r[1] for r in rows])
            rates = yield_rate(self._snapshot(lot_id)["wafer_count"], sum(1 for r in rows if r[1] >= 0.8), 0)
            ci = confidence_interval([r[1] for r in rows])
        return {"lot_id": lot_id, "spectrum": summary.__dict__, "yield": rates, "response_ci": ci}

    def approve(
        self,
        token: str,
        lot_id: str,
        decision: str,
        reason: str,
        expected_version: int | None = None,
    ) -> dict:
        """记录质量决定。

        只有一个请求可以把批次从待审核状态推进到终态：终态写入采用带
        ``status='engineering'`` 条件的更新（或客户端提供的版本号），并在
        ``BEGIN IMMEDIATE`` 事务内执行。对同一评审人重复提交的相同决定做
        幂等重放；对立决定收到 :class:`Conflict` 且不改变批次状态，冲突本身
        会写入审计。
        """
        actor = self.auth.require(token, "approve")
        if decision not in TERMINAL_STATUS or not reason.strip():
            raise ValueError("decision and reason are required")
        result: dict | None = None
        conflict_error: Conflict | None = None
        # 冲突事件必须在事务内提交、在事务外抛出，否则异常回滚会把审计抹掉。
        with transaction(self.db, lock=self._lock):
            row = self.db.execute(
                "SELECT status,version FROM chip_lots WHERE lot_id=?", (lot_id,)
            ).fetchone()
            if row is None:
                raise KeyError(lot_id)
            status, version = row["status"], row["version"]

            existing = self.db.execute(
                "SELECT decision,reason FROM approvals WHERE lot_id=? AND reviewer=?",
                (lot_id, actor.user_id),
            ).fetchone()
            if existing is not None:
                if existing["decision"] == decision and existing["reason"] == reason:
                    # 幂等重放：返回既有结果，不改状态、不追加审计事件。
                    result = self._snapshot(lot_id)
                else:
                    self._record_conflict(lot_id, actor.user_id, decision, reason, status)
                    conflict_error = self._conflict(lot_id, status, version)
            elif status != REVIEWABLE_STATUS:
                # 其他评审人已抢先给出终态：记录冲突但保持状态不变。
                self._record_conflict(lot_id, actor.user_id, decision, reason, status)
                conflict_error = self._conflict(lot_id, status, version)
            elif expected_version is not None and expected_version != version:
                conflict_error = self._conflict(lot_id, status, version)
            else:
                now = utcnow()
                terminal = TERMINAL_STATUS[decision]
                cursor = self.db.execute(
                    "UPDATE chip_lots SET status=?,updated_at=?,version=version+1 "
                    "WHERE lot_id=? AND status=?",
                    (terminal, now, lot_id, REVIEWABLE_STATUS),
                )
                if cursor.rowcount != 1:
                    # 跨连接并发：事务拿到写锁后状态已被其他进程推进。
                    winner = self.db.execute(
                        "SELECT status,version FROM chip_lots WHERE lot_id=?", (lot_id,)
                    ).fetchone()
                    self._record_conflict(lot_id, actor.user_id, decision, reason, winner["status"])
                    conflict_error = self._conflict(lot_id, winner["status"], winner["version"])
                else:
                    self.db.execute(
                        "INSERT INTO approvals VALUES(?,?,?,?,?)",
                        (lot_id, actor.user_id, decision, reason, now),
                    )
                    event(self.db, lot_id, "approval", actor.user_id, {"decision": decision, "reason": reason})
                    result = self._snapshot(lot_id)
        if conflict_error is not None:
            raise conflict_error
        return result

    def audit(self, token: str, lot_id: str) -> list[dict]:
        self.auth.require(token, "read")
        with self._lock:
            return [dict(r) for r in self.db.execute("SELECT * FROM lot_events WHERE lot_id=? ORDER BY event_id", (lot_id,)).fetchall()]

    def _snapshot(self, lot_id: str) -> dict:
        row = self.db.execute("SELECT * FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if row is None:
            raise KeyError(lot_id)
        return dict(row)

    def _record_conflict(self, lot_id: str, reviewer: str, attempted: str, reason: str, current_status: str) -> None:
        winner = self.db.execute(
            "SELECT reviewer,decision FROM approvals WHERE lot_id=? LIMIT 1", (lot_id,)
        ).fetchone()
        payload = {
            "attempted_decision": attempted,
            "reason": reason,
            "current_status": current_status,
        }
        if winner is not None:
            payload["winner_reviewer"] = winner["reviewer"]
            payload["winner_decision"] = winner["decision"]
        event(self.db, lot_id, "approval.conflict", reviewer, payload)

    def _conflict(self, lot_id: str, status: str, version: int) -> Conflict:
        return Conflict(
            f"lot {lot_id} is already {status}",
            current={"lot_id": lot_id, "status": status, "version": version},
        )
