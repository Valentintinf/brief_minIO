#!/usr/bin/env python3
"""
Jours 6-7 — Collecteur des logs d'audit MinIO.

MinIO n'écrit pas ses logs d'audit dans un fichier : il les POSTe sur un webhook
HTTP, un événement JSON par appel S3. Ce serveur reçoit ces événements et les
écrit en JSON Lines, format directement rejouable par `analyze_audit_logs.py`.

Volontairement sans dépendance : il tourne sur l'image `python:3.12-slim` sans
étape de build.

Usage local (hors Docker) :
    python audit_collector.py --port 9999 --output audit-logs/audit.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# Un seul verrou pour toutes les requêtes : le serveur est multi-thread et les
# écritures doivent rester des lignes entières, non entrelacées.
WRITE_LOCK = threading.Lock()


class AuditHandler(BaseHTTPRequestHandler):
    output_path: Path
    event_count = 0

    def do_POST(self) -> None:  # noqa: N802 — nom imposé par BaseHTTPRequestHandler
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""

        # Répondre avant d'écrire : MinIO abandonne les événements si le
        # webhook tarde, et une perte de log est pire qu'un log en retard.
        self.send_response(200)
        self.end_headers()

        if not body:
            return

        try:
            self._persist(body)
        except Exception as error:  # noqa: BLE001 — ne jamais tuer le collecteur
            print(f"! échec d'écriture : {error}", file=sys.stderr, flush=True)

    def _persist(self, body: bytes) -> None:
        # MinIO envoie un objet JSON par requête, mais peut en grouper
        # plusieurs dans un tableau selon la version.
        payload = json.loads(body)
        events = payload if isinstance(payload, list) else [payload]

        received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with WRITE_LOCK:
            with self.output_path.open("a", encoding="utf-8") as handle:
                for event in events:
                    event["_received_at"] = received_at
                    handle.write(json.dumps(event, ensure_ascii=False) + "\n")

        AuditHandler.event_count += len(events)
        for event in events:
            api = (event.get("api") or {}).get("name", "?")
            bucket = (event.get("api") or {}).get("bucket") or "—"
            user = (event.get("requestClaims") or {}).get("accessKey") \
                or event.get("accessKey") or "anonyme"
            status = (event.get("api") or {}).get("statusCode", "?")
            print(f"[{AuditHandler.event_count:5d}] {api:24s} {bucket:10s} "
                  f"{user:16s} → {status}", flush=True)

    def do_GET(self) -> None:  # noqa: N802
        """Sonde de vivacité, utilisée par le healthcheck Docker."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "status": "ok",
            "events": AuditHandler.event_count,
            "output": str(self.output_path),
        }).encode())

    def log_message(self, *_args) -> None:
        """Silence le log d'accès par défaut : seuls les événements comptent."""


def main() -> int:
    parser = argparse.ArgumentParser(description="Collecteur de logs d'audit MinIO.")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--output", default="/audit-logs/audit.jsonl")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.touch(exist_ok=True)

    AuditHandler.output_path = output_path
    server = ThreadingHTTPServer(("0.0.0.0", args.port), AuditHandler)
    print(f"✓ Collecteur d'audit à l'écoute sur :{args.port} "
          f"→ {output_path}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n✓ Arrêt — {AuditHandler.event_count} événement(s) collecté(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
