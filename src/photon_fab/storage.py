"""芯片批次和测量记录的 SQLite 结构及事务辅助函数。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS chip_lots(
 lot_id TEXT PRIMARY KEY, product TEXT NOT NULL, process_rev TEXT NOT NULL,
 wafer_count INTEGER NOT NULL, status TEXT NOT NULL, owner TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS measurements(
 measurement_id TEXT PRIMARY KEY, lot_id TEXT NOT NULL REFERENCES chip_lots(lot_id),
 wavelength_nm REAL NOT NULL, response REAL NOT NULL, noise REAL NOT NULL,
 instrument TEXT NOT NULL, operator TEXT NOT NULL, measured_at TEXT NOT NULL,
 UNIQUE(lot_id,measurement_id));
CREATE TABLE IF NOT EXISTS lot_events(
 event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT NOT NULL,
 event_type TEXT NOT NULL, actor TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(
 lot_id TEXT NOT NULL, reviewer TEXT NOT NULL, decision TEXT NOT NULL,
 reason TEXT NOT NULL, effective INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL,
 PRIMARY KEY(lot_id,reviewer));
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(path: str = ":memory:") -> sqlite3.Connection:
    # 同一个服务实例可能被 ThreadingHTTPServer 的多个工作线程共用，
    # 关闭同线程检查，由 BEGIN IMMEDIATE 与服务层写锁保证原子性。
    db = sqlite3.connect(path, timeout=10, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(SCHEMA)
    _migrate(db)
    db.commit()
    return db


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def _migrate(db: sqlite3.Connection) -> None:
    """对基线后新增的列做增量迁移，旧库重启后语义保持一致。"""
    if "version" not in _columns(db, "chip_lots"):
        db.execute("ALTER TABLE chip_lots ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
    approval_cols = _columns(db, "approvals")
    if approval_cols and "effective" not in approval_cols:
        # 旧版本中最后一次无条件写入决定了批次状态，
        # 与当前状态一致的审批行回填为生效，其余标记为未生效。
        db.execute("ALTER TABLE approvals ADD COLUMN effective INTEGER NOT NULL DEFAULT 0")
        db.execute(
            """UPDATE approvals SET effective=CASE
                WHEN decision='release'
                 AND (SELECT status FROM chip_lots WHERE chip_lots.lot_id=approvals.lot_id)='released' THEN 1
                WHEN decision='hold'
                 AND (SELECT status FROM chip_lots WHERE chip_lots.lot_id=approvals.lot_id)='hold' THEN 1
                WHEN decision='reject'
                 AND (SELECT status FROM chip_lots WHERE chip_lots.lot_id=approvals.lot_id)='rejected' THEN 1
                ELSE 0 END"""
        )


@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        db.execute("BEGIN IMMEDIATE")
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


def event(
    db: sqlite3.Connection,
    lot_id: str,
    event_type: str,
    actor: str,
    payload: dict,
    created_at: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO lot_events(lot_id,event_type,actor,payload,created_at) VALUES(?,?,?,?,?)",
        (lot_id, event_type, actor, json.dumps(payload, sort_keys=True, ensure_ascii=False), created_at or utcnow()),
    )
