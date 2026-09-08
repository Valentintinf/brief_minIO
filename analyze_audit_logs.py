#!/usr/bin/env python3
"""
Jours 6-7 (C21) — Analyse des logs d'audit MinIO.

Lit le JSON Lines produit par `audit_collector.py` et en tire une lecture de
gouvernance : qui a accédé à quoi, avec quel résultat, et quelles tentatives ont
été refusées.

Usage :
    python analyze_audit_logs.py                       # tout le journal
    python analyze_audit_logs.py --user data-analyst    # une identité
    python analyze_audit_logs.py --denied-only          # seuls les refus
    python analyze_audit_logs.py --last 20              # dernières entrées
    python analyze_audit_logs.py --report JOUR7_AUDIT.md
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

# Opérations de lecture seule : distinguer consultation et modification est le
# premier découpage utile pour une revue d'accès.
READ_APIS = {
    "GetObject", "HeadObject", "ListObjectsV2", "ListObjects", "ListBuckets",
    "GetBucketLocation", "GetBucketEncryption", "GetBucketLifecycle",
    "GetBucketPolicy", "GetObjectTagging", "HeadBucket",
}


@dataclass(frozen=True)
class AuditEvent:
    time: str
    user: str
    api: str
    bucket: str
    object_key: str
    status: str
    status_code: int
    remote_host: str
    user_agent: str

    @property
    def is_denied(self) -> bool:
        return self.status_code == 403

    @property
    def is_write(self) -> bool:
        return self.api not in READ_APIS

    @property
    def client(self) -> str:
        """Nom d'outil déduit du User-Agent, sans le détail de version."""
        agent = self.user_agent
        if not agent:
            return "—"
        if "Boto3" in agent or "Botocore" in agent:
            return "boto3"
        if agent.startswith("MinIO"):
            return "mc"
        if "Mozilla" in agent:
            return "console web"
        return agent.split("/")[0][:20]


def is_internal_probe(bucket: str) -> bool:
    """MinIO sonde sa propre signature via des buckets `probe-bsign-<alea>`.

    Ce ne sont pas des buckets du data lake : les garder fausserait le décompte
    par bucket sans rien apprendre sur les accès aux données.
    """
    return bucket.startswith("probe-bsign-")


def load_events(path: Path) -> list[AuditEvent]:
    if not path.exists():
        raise FileNotFoundError(
            f"Journal d'audit introuvable : {path}\n"
            "Le collecteur tourne-t-il ? docker-compose up -d audit-collector"
        )

    events: list[AuditEvent] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            # Une ligne tronquée (collecteur arrêté en pleine écriture) ne doit
            # pas invalider tout le journal.
            print(f"! ligne {line_number} illisible, ignorée", file=sys.stderr)
            continue

        api = raw.get("api") or {}
        if is_internal_probe(api.get("bucket") or ""):
            continue
        events.append(AuditEvent(
            time=raw.get("time", "")[:19].replace("T", " "),
            user=raw.get("accessKey") or "anonyme",
            api=api.get("name") or "?",
            bucket=api.get("bucket") or "—",
            object_key=api.get("object") or "",
            status=api.get("status") or "?",
            status_code=int(api.get("statusCode") or 0),
            remote_host=raw.get("remotehost") or "—",
            user_agent=raw.get("userAgent") or "",
        ))
    return events


def print_summary(events: list[AuditEvent]) -> None:
    denied = [e for e in events if e.is_denied]
    writes = [e for e in events if e.is_write and not e.is_denied]

    print(f"\n── Session d'accès : {len(events)} appel(s) S3 ──")
    if events:
        print(f"   du {events[0].time} au {events[-1].time} (UTC)")
    print(f"   {len(events) - len(denied)} autorisé(s), {len(denied)} refusé(s), "
          f"dont {len(writes)} écriture(s) effective(s)")

    print("\n── Par identité ──")
    print(f"   {'compte':16s} {'appels':>7s} {'refus':>7s} {'écritures':>10s}  buckets touchés")
    for user in sorted({e.user for e in events}):
        user_events = [e for e in events if e.user == user]
        user_denied = sum(1 for e in user_events if e.is_denied)
        user_writes = sum(1 for e in user_events if e.is_write and not e.is_denied)
        buckets = sorted({e.bucket for e in user_events if e.bucket != "—"})
        print(f"   {user:16s} {len(user_events):7d} {user_denied:7d} "
              f"{user_writes:10d}  {', '.join(buckets) or '—'}")

    print("\n── Par opération ──")
    for api, count in Counter(e.api for e in events).most_common():
        api_denied = sum(1 for e in events if e.api == api and e.is_denied)
        suffix = f"  ({api_denied} refusé(s))" if api_denied else ""
        print(f"   {api:24s} {count:5d}{suffix}")

    print("\n── Par bucket ──")
    for bucket, count in Counter(e.bucket for e in events).most_common():
        bucket_denied = sum(1 for e in events if e.bucket == bucket and e.is_denied)
        suffix = f"  ({bucket_denied} refusé(s))" if bucket_denied else ""
        print(f"   {bucket:24s} {count:5d}{suffix}")

    if denied:
        print(f"\n── Tentatives refusées : {len(denied)} ──")
        grouped: dict[str, list[str]] = defaultdict(list)
        for event in denied:
            grouped[event.user].append(f"{event.api} sur {event.bucket}")
        for user, attempts in sorted(grouped.items()):
            print(f"   {user} :")
            for attempt, count in sorted(Counter(attempts).items()):
                print(f"       ✗ {attempt}" + (f" × {count}" if count > 1 else ""))
        print("\n   Ces refus sont le fonctionnement attendu des policies : chaque")
        print("   compte est arrêté au-delà de son périmètre, et la tentative est")
        print("   tracée. Un refus inattendu ici serait le signal d'une policy trop")
        print("   étroite ; un accès inattendu, celui d'une policy trop large.")


def print_timeline(events: list[AuditEvent], limit: int) -> None:
    selected = events[-limit:] if limit else events
    print(f"\n── Chronologie ({len(selected)} appel(s)) ──")
    print(f"   {'heure':20s} {'compte':15s} {'opération':20s} "
          f"{'bucket':9s} {'client':12s} statut")
    for event in selected:
        flag = "✗" if event.is_denied else " "
        print(f" {flag} {event.time:20s} {event.user:15s} {event.api:20s} "
              f"{event.bucket:9s} {event.client:12s} {event.status_code}")


def write_report(events: list[AuditEvent], output_path: Path, source: Path) -> None:
    denied = [e for e in events if e.is_denied]
    lines = [
        "# Analyse des logs d'audit MinIO — Jours 6-7 (C21)",
        "",
        "Généré par `analyze_audit_logs.py` à partir de "
        f"`{source}` ({len(events)} événements).",
        "",
        "## Comment les logs sont produits",
        "",
        "MinIO n'écrit pas de fichier de log d'audit : il **POSTe** un événement JSON",
        "sur un webhook HTTP à chaque appel S3. Le service `audit-collector` reçoit ces",
        "événements et les écrit en JSON Lines. La configuration tient en deux",
        "variables d'environnement sur le conteneur MinIO :",
        "",
        "```yaml",
        'MINIO_AUDIT_WEBHOOK_ENABLE_datalake: "on"',
        "MINIO_AUDIT_WEBHOOK_ENDPOINT_datalake: http://audit-collector:9999/audit",
        "```",
        "",
        "Chaque événement porte l'identité appelante (`accessKey`), l'opération",
        "(`api.name`), la cible (`api.bucket`, `api.object`), le code de retour",
        "(`api.statusCode`), l'IP source (`remotehost`) et le client (`userAgent`).",
        "",
        "## Session analysée",
        "",
        "| Indicateur | Valeur |",
        "|---|---|",
        f"| Appels S3 | {len(events)} |",
    ]
    if events:
        lines.append(f"| Période (UTC) | {events[0].time} → {events[-1].time} |")
    lines += [
        f"| Autorisés | {len(events) - len(denied)} |",
        f"| Refusés (403) | {len(denied)} |",
        f"| Identités distinctes | {len({e.user for e in events})} |",
        "",
        "## Répartition par identité",
        "",
        "| Compte | Appels | Refus | Écritures | Buckets touchés |",
        "|---|---:|---:|---:|---|",
    ]
    for user in sorted({e.user for e in events}):
        user_events = [e for e in events if e.user == user]
        user_denied = sum(1 for e in user_events if e.is_denied)
        user_writes = sum(1 for e in user_events if e.is_write and not e.is_denied)
        buckets = sorted({e.bucket for e in user_events if e.bucket != "—"})
        lines.append(
            f"| `{user}` | {len(user_events)} | {user_denied} | {user_writes} "
            f"| {', '.join(f'`{b}`' for b in buckets) or '—'} |"
        )

    lines += [
        "",
        "## Répartition par opération",
        "",
        "| Opération | Appels | dont refusés |",
        "|---|---:|---:|",
    ]
    for api, count in Counter(e.api for e in events).most_common():
        api_denied = sum(1 for e in events if e.api == api and e.is_denied)
        lines.append(f"| `{api}` | {count} | {api_denied} |")

    if denied:
        lines += [
            "",
            "## Tentatives refusées",
            "",
            "| Compte | Opération | Bucket | Occurrences |",
            "|---|---|---|---:|",
        ]
        counted = Counter((e.user, e.api, e.bucket) for e in denied)
        for (user, api, bucket), count in sorted(counted.items()):
            lines.append(f"| `{user}` | `{api}` | `{bucket}` | {count} |")

        lines += [
            "",
            "### Lecture de gouvernance",
            "",
            "Ces refus sont le fonctionnement **attendu** des policies : chaque compte",
            "est arrêté dès qu'il sort de son périmètre, et la tentative reste tracée.",
            "",
            "- `data-analyst` refusé sur `raw`, `staging` et `archive` : conforme, son",
            "  périmètre est la seule couche `curated`.",
            "- `data-analyst` refusé en `PutObject`/`DeleteObject` sur `curated` :",
            "  conforme, son accès y est en lecture seule.",
            "- `data-engineer` refusé sur `archive` : conforme, l'archivage est une",
            "  opération de cycle de vie, pas de pipeline.",
            "",
            "Un refus **inattendu** signalerait une policy trop étroite, bloquant un",
            "traitement légitime. Un accès **inattendu** signalerait une policy trop",
            "large, donc une exposition de données.",
            "",
            "## Ce que l'audit permet de détecter",
            "",
            "| Signal | Interprétation |",
            "|---|---|",
            "| Rafale de 403 sur un même compte | identifiants compromis, ou pipeline mal configuré |",
            "| Écriture par un compte en lecture seule | policy trop large, à corriger |",
            "| `GetObject` massif sur `curated` | extraction anormale de données |",
            "| `remotehost` inhabituel | accès depuis un poste non prévu |",
            "| `DeleteObject` hors DAG d'archivage | suppression non planifiée à investiguer |",
            "",
        ]

    lines += [
        "## Rejouer l'analyse",
        "",
        "```bash",
        "python analyze_audit_logs.py                    # session complète",
        "python analyze_audit_logs.py --denied-only      # uniquement les refus",
        "python analyze_audit_logs.py --user data-analyst",
        "```",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n✓ Rapport écrit : {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyse les logs d'audit MinIO.")
    parser.add_argument("--source", default="audit-logs/audit.jsonl")
    parser.add_argument("--user", help="Ne garder qu'une identité.")
    parser.add_argument("--bucket", help="Ne garder qu'un bucket.")
    parser.add_argument("--denied-only", action="store_true",
                        help="Ne garder que les tentatives refusées.")
    parser.add_argument("--last", type=int, default=15,
                        help="Nombre d'appels dans la chronologie (0 = tous).")
    parser.add_argument("--report", help="Écrire aussi un rapport Markdown.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        events = load_events(Path(args.source))
    except FileNotFoundError as error:
        print(f"✗ {error}", file=sys.stderr)
        return 1

    if not events:
        print("Le journal d'audit est vide. Générez du trafic, par exemple :\n"
              "  python minio_governance_setup.py --check")
        return 0

    if args.user:
        events = [e for e in events if e.user == args.user]
    if args.bucket:
        events = [e for e in events if e.bucket == args.bucket]
    if args.denied_only:
        events = [e for e in events if e.is_denied]

    if not events:
        print("Aucun événement ne correspond aux filtres demandés.")
        return 0

    print_summary(events)
    print_timeline(events, args.last)

    if args.report:
        write_report(events, Path(args.report), Path(args.source))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
