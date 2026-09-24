"""服务层可观察错误。"""

from __future__ import annotations


class PhotonServiceError(RuntimeError):
    """所有可映射到 HTTP 状态码的服务错误基类。"""

    status = 400


class Conflict(PhotonServiceError):
    """批次状态已被并发决定推进，或提交了与已记录终态冲突的决定。"""

    status = 409

    def __init__(self, message: str, current: dict | None = None):
        super().__init__(message)
        self.current = current or {}
