"""用于离线验收的无依赖 JSON HTTP API。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import ServiceError
from .service import PhotonService


class Handler(BaseHTTPRequestHandler):
    service = PhotonService()

    def _json(self, status: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _token(self) -> str:
        return self.headers.get("Authorization", "").removeprefix("Bearer ")

    def _read_body(self) -> dict:
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if not raw:
            return {}
        return json.loads(raw)

    def _error(self, exc: Exception) -> None:
        if isinstance(exc, ServiceError):
            body = {"error": str(exc), "code": exc.code}
            if exc.details:
                body["details"] = exc.details
            return self._json(exc.status, body)
        if isinstance(exc, PermissionError):
            return self._json(403, {"error": str(exc), "code": "forbidden"})
        if isinstance(exc, KeyError):
            return self._json(404, {"error": str(exc.args[0]), "code": "not_found"})
        return self._json(400, {"error": str(exc), "code": "bad_request"})

    def do_GET(self):
        try:
            if self.path == "/health":
                return self._json(200, {"status": "ok", "service": "photon-fab"})
            if self.path.startswith("/lots/"):
                parts = self.path.strip("/").split("/")
                token = self._token()
                if len(parts) == 2:
                    return self._json(200, self.service.get_lot(token, parts[1]))
                if len(parts) == 3 and parts[2] == "audit":
                    return self._json(200, {"events": self.service.audit(token, parts[1])})
            return self._json(404, {"error": "not found", "code": "not_found"})
        except Exception as exc:
            return self._error(exc)

    def do_POST(self):
        try:
            body = self._read_body()
            if self.path == "/login":
                return self._json(200, {"token": self.service.login(body["user_id"], body["password"])})
            token = self._token()
            if self.path == "/lots":
                return self._json(
                    201,
                    self.service.create_lot(token, body["lot_id"], body["product"], body["process_rev"], body["wafer_count"]),
                )
            if self.path.startswith("/lots/"):
                parts = self.path.strip("/").split("/")
                if len(parts) == 3 and parts[2] == "measurements":
                    return self._json(
                        201,
                        self.service.add_measurement(
                            token, parts[1], body["wavelength_nm"], body["response"], body.get("noise", 0.0), body["instrument"]
                        ),
                    )
                if len(parts) == 3 and parts[2] == "analysis":
                    return self._json(200, self.service.analyze(token, parts[1]))
                if len(parts) == 3 and parts[2] == "approvals":
                    expected_version = body.get("expected_version")
                    return self._json(
                        200,
                        self.service.approve(
                            token,
                            parts[1],
                            body["decision"],
                            body["reason"],
                            int(expected_version) if expected_version is not None else None,
                        ),
                    )
            return self._json(404, {"error": "not found", "code": "not_found"})
        except Exception as exc:
            return self._error(exc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=":memory:")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    Handler.service = PhotonService(args.database)
    Handler.service.bootstrap_admin()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
