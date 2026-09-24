"""服务层可观察错误。"""

from __future__ import annotations

from typing import Any


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class NotFound(ServiceError):
    code = "not_found"
    status = 404


class Conflict(ServiceError):
    code = "conflict"
    status = 409


class ValidationFailed(ServiceError):
    code = "validation_failed"
    status = 422
