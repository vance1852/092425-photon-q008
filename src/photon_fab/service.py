"""协调认证、批次、测试和放行门禁的应用服务。"""

from __future__ import annotations

import threading
import uuid

from .analytics import confidence_interval, summarize_spectrum, yield_rate
from .auth import Auth
from .errors import Conflict, NotFound, ValidationFailed
from .storage import connect, event, transaction, utcnow


# 每个审批决定对应的批次终态。
DECISION_STATUS = {"release": "released", "hold": "hold", "reject": "rejected"}
# 只有待审核状态的批次允许接受第一个审批决定。
PENDING_STATUS = "engineering"


class PhotonService:
    def __init__(self, database: str = ":memory:"):
        self.db = connect(database)
        self.auth = Auth(self.db)
        # ThreadingHTTPServer 会在多个线程里共用同一个 SQLite 连接，
        # 用可重入锁把读改写序列化为单个临界区；跨进程时则由
        # BEGIN IMMEDIATE 加 busy timeout 在数据库层串行化。
        self._lock = threading.RLock()

    def bootstrap_admin(self, user_id: str = "admin", password: str = "photon-admin") -> None:
        try:
            self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def login(self, user_id: str, password: str) -> str:
        with self._lock:
            return self.auth.login(user_id, password)

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, wafer_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if wafer_count <= 0 or not lot_id.strip() or not process_rev.strip():
            raise ValidationFailed("lot fields are invalid")
        now = utcnow()
        with self._lock, transaction(self.db):
            self.db.execute(
                "INSERT INTO chip_lots VALUES(?,?,?,?,?,?,?,?,1)",
                (lot_id, product, process_rev, wafer_count, PENDING_STATUS, actor.user_id, now, now),
            )
            event(self.db, lot_id, "created", actor.user_id, {"product": product, "process_rev": process_rev})
        return self.get_lot(token, lot_id)

    def get_lot(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "read")
        with self._lock:
            row = self.db.execute("SELECT * FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if not row:
            raise NotFound(lot_id)
        return dict(row)

    def add_measurement(self, token: str, lot_id: str, wavelength_nm: float, response: float, noise: float, instrument: str) -> dict:
        actor = self.auth.require(token, "measure")
        measurement_id = uuid.uuid4().hex
        with self._lock, transaction(self.db):
            if not self.db.execute("SELECT 1 FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone():
                raise NotFound(lot_id)
            self.db.execute(
                "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?)",
                (measurement_id, lot_id, float(wavelength_nm), float(response), float(noise), instrument, actor.user_id, utcnow()),
            )
            event(self.db, lot_id, "measurement", actor.user_id, {"measurement_id": measurement_id, "wavelength_nm": wavelength_nm})
        return {"measurement_id": measurement_id, "lot_id": lot_id}

    def analyze(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "analyze")
        with self._lock:
            rows = self.db.execute(
                "SELECT wavelength_nm,response FROM measurements WHERE lot_id=? ORDER BY wavelength_nm", (lot_id,)
            ).fetchall()
            lot = self.db.execute("SELECT wafer_count FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if not lot:
            raise NotFound(lot_id)
        if len(rows) < 3:
            raise ValidationFailed("three measurements are required")
        summary = summarize_spectrum([r[0] for r in rows], [r[1] for r in rows])
        rates = yield_rate(lot["wafer_count"], sum(1 for r in rows if r[1] >= 0.8), 0)
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
        """提交审批决定。

        条件更新：只有批次仍处于待审核状态（且在提供 expected_version 时
        版本一致）才能把状态推进到终态，并将批次版本加一。重复提交同一个
        已生效决定时返回原结果；与已生效决定相反或基于过期版本的请求得到
        Conflict，批次状态不变，但冲突尝试会进入审计日志。
        """
        actor = self.auth.require(token, "approve")
        if decision not in DECISION_STATUS or not reason.strip():
            raise ValidationFailed("decision and reason are required")
        with self._lock, transaction(self.db):
            row = self.db.execute(
                "SELECT status,version FROM chip_lots WHERE lot_id=?", (lot_id,)
            ).fetchone()
            if not row:
                raise NotFound(lot_id)
            current_status, current_version = row["status"], row["version"]
            effective = self.db.execute(
                "SELECT reviewer,decision,reason FROM approvals WHERE lot_id=? AND effective=1",
                (lot_id,),
            ).fetchone()

            # 幂等重放：同一个决定已经生效（无论是谁提交），原样返回，
            # 不写状态也不写审计，重复提交结果稳定。
            if effective is not None and effective["decision"] == decision:
                return self._load_lot(lot_id)

            if expected_version is not None and expected_version != current_version:
                self._record_conflict(
                    lot_id, actor.user_id, decision, reason, current_status, current_version, effective, "version_mismatch"
                )
                raise Conflict(
                    f"lot {lot_id} version {current_version} does not match expected {expected_version}",
                    self._conflict_details(lot_id, current_status, current_version, effective, "version_mismatch"),
                )

            # 条件更新：仅允许从待审核状态转为终态。
            # WHERE 条件是最后一道防线，并发下未抢到的事务 rowcount 为 0。
            cursor = self.db.execute(
                "UPDATE chip_lots SET status=?,version=version+1,updated_at=? "
                "WHERE lot_id=? AND status=?",
                (DECISION_STATUS[decision], utcnow(), lot_id, PENDING_STATUS),
            )
            if cursor.rowcount != 1:
                self._record_conflict(
                    lot_id, actor.user_id, decision, reason, current_status, current_version, effective, "already_decided"
                )
                raise Conflict(
                    f"lot {lot_id} is already {current_status}",
                    self._conflict_details(lot_id, current_status, current_version, effective, "already_decided"),
                )

            self.db.execute(
                "INSERT INTO approvals VALUES(?,?,?,?,1,?)",
                (lot_id, actor.user_id, decision, reason, utcnow()),
            )
            event(
                self.db,
                lot_id,
                "approval",
                actor.user_id,
                {
                    "decision": decision,
                    "reason": reason,
                    "from_status": current_status,
                    "to_status": DECISION_STATUS[decision],
                    "from_version": current_version,
                    "to_version": current_version + 1,
                },
            )
            return self._load_lot(lot_id)

    def _load_lot(self, lot_id: str) -> dict:
        row = self.db.execute("SELECT * FROM chip_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if not row:
            raise NotFound(lot_id)
        return dict(row)

    def _record_conflict(
        self, lot_id: str, actor: str, decision: str, reason: str,
        status: str, version: int, effective, reason_code: str,
    ) -> None:
        payload: dict = {
            "decision": decision,
            "reason": reason,
            "reason_code": reason_code,
            "current_status": status,
            "current_version": version,
        }
        if effective is not None:
            payload["effective_decision"] = effective["decision"]
            payload["effective_reviewer"] = effective["reviewer"]
        event(self.db, lot_id, "approval.conflicted", actor, payload)
        # 冲突尝试也要留在审计里：先落盘再抛错，
        # 避免外层 transaction 的回滚把这条记录带走。
        self.db.commit()

    def _conflict_details(self, lot_id: str, status: str, version: int, effective, reason_code: str) -> dict:
        details = {"lot_id": lot_id, "current_status": status, "current_version": version, "reason_code": reason_code}
        if effective is not None:
            details["effective_decision"] = effective["decision"]
            details["effective_reviewer"] = effective["reviewer"]
        return details

    def audit(self, token: str, lot_id: str) -> list[dict]:
        self.auth.require(token, "read")
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM lot_events WHERE lot_id=? ORDER BY event_id", (lot_id,)
            ).fetchall()
        return [dict(r) for r in rows]
