"""Business logic: manifests, fragment uploads, reconciliation and publishing.

All mutating operations run inside a single ``BEGIN IMMEDIATE`` SQLite
transaction so that manifest writes, uploads and publications serialize on one
consistent snapshot.  Any error rolls the whole transaction back — no
half-written manifests or publications are ever left behind.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from .config import SAMPLES_PER_MINUTE
from .database import get_conn


# --------------------------------------------------------------------------- #
# Errors (mapped to HTTP status codes in the router)
# --------------------------------------------------------------------------- #
class ServiceError(Exception):
    status_code = 400


class BoundaryError(ServiceError):
    status_code = 422


class StaleRevisionError(ServiceError):
    """Incoming revision is not higher than the one already stored."""

    status_code = 409


class ContentConflictError(ServiceError):
    """Same fragment_no + revision but different payload."""

    status_code = 409


class ManifestRevisionError(ServiceError):
    """Incoming manifest revision is not higher than the current one."""

    status_code = 409


class ManifestConflictError(ServiceError):
    """Same manifest revision resubmitted with different content."""

    status_code = 409


class NotPublishableError(ServiceError):
    status_code = 409


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def minute_key(dt: datetime) -> str:
    """Canonical UTC minute string, e.g. ``2026-11-17T05:06:00Z``."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:00Z")


def dump_samples(samples: list[int]) -> str:
    return json.dumps(samples, separators=(",", ":"))


def load_samples(raw: str) -> list[int]:
    return json.loads(raw)


def content_hash(start_sample: int, samples: list[int]) -> str:
    payload = json.dumps(
        {"start_sample": start_sample, "samples": samples}, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def manifest_hash(entries: list[dict[str, Any]]) -> str:
    """Stable SHA-256 over the manifest's canonical entry list."""
    canonical = sorted(
        (
            {
                "fragment_no": e["fragment_no"],
                "revision": e["revision"],
                "content_hash": e["content_hash"],
            }
            for e in entries
        ),
        key=lambda e: e["fragment_no"],
    )
    blob = json.dumps(canonical, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _dump_entries(entries: list[dict[str, Any]]) -> str:
    return json.dumps(
        sorted(entries, key=lambda e: e["fragment_no"]), separators=(",", ":")
    )


def _load_entries(raw: str) -> list[dict[str, Any]]:
    return json.loads(raw)


def _fragment_set_hash(rows: list[sqlite3.Row]) -> str:
    """Stable hash of the *effective* fragment set of a station-minute."""
    canonical = [
        {
            "fragment_no": r["fragment_no"],
            "revision": r["revision"],
            "start_sample": r["start_sample"],
            "samples": load_samples(r["samples"]),
            "content_hash": r["content_hash"],
        }
        for r in rows
    ]
    canonical.sort(key=lambda f: f["fragment_no"])
    blob = json.dumps(canonical, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _parse_minute(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _row_to_fragment(r: sqlite3.Row) -> dict[str, Any]:
    return {
        "station": r["station"],
        "minute_utc": _parse_minute(r["minute_utc"]),
        "fragment_no": r["fragment_no"],
        "revision": r["revision"],
        "start_sample": r["start_sample"],
        "samples": load_samples(r["samples"]),
        "content_hash": r["content_hash"],
    }


def _get_fragments(
    conn: sqlite3.Connection, station: str, minute: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
        "ORDER BY fragment_no",
        (station, minute),
    ).fetchall()


def _get_latest_manifest(
    conn: sqlite3.Connection, station: str, minute: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM manifests WHERE station = ? AND minute_utc = ? "
        "ORDER BY manifest_revision DESC, id DESC LIMIT 1",
        (station, minute),
    ).fetchone()


def _row_to_manifest(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "station": row["station"],
        "minute_utc": _parse_minute(row["minute_utc"]),
        "manifest_revision": row["manifest_revision"],
        "entries": _load_entries(row["entries"]),
        "manifest_hash": row["manifest_hash"],
        "created_at": row["created_at"],
    }


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def compute_assembly(rows: list[sqlite3.Row]) -> dict[str, Any]:
    """Compute coverage, gaps and value conflicts for one station-minute."""
    # position -> list of (fragment_no, value)
    covered: dict[int, list[tuple[int, int]]] = {p: [] for p in range(SAMPLES_PER_MINUTE)}
    for r in rows:
        start = r["start_sample"]
        for offset, value in enumerate(load_samples(r["samples"])):
            covered[start + offset].append((r["fragment_no"], value))

    gaps: list[int] = []
    conflicts: list[dict[str, Any]] = []
    coverage = 0
    for pos in range(SAMPLES_PER_MINUTE):
        entries = covered[pos]
        if not entries:
            gaps.append(pos)
            continue
        coverage += 1
        distinct_values = {value for _, value in entries}
        if len(distinct_values) > 1:
            conflicts.append(
                {
                    "position": pos,
                    "fragment_nos": [frag_no for frag_no, _ in entries],
                    "values": [value for _, value in entries],
                }
            )
    return {
        "complete": coverage == SAMPLES_PER_MINUTE and not conflicts,
        "coverage": coverage,
        "gaps": gaps,
        "conflicts": conflicts,
    }


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #
def _reconcile(
    manifest: sqlite3.Row | None, fragment_rows: list[sqlite3.Row]
) -> dict[str, Any]:
    """Compare the current manifest against the effective fragment set.

    Every manifest line ends in exactly one bucket:
    ``matched`` / ``missing`` / ``revision_mismatch`` / ``content_mismatch``.
    Uploaded fragments without a manifest line are reported as ``extras``.
    """
    fragments_by_no = {r["fragment_no"]: r for r in fragment_rows}
    matched: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    revision_mismatches: list[dict[str, Any]] = []
    content_mismatches: list[dict[str, Any]] = []

    expected_nos: set[int] = set()
    if manifest is not None:
        for entry in _load_entries(manifest["entries"]):
            no = entry["fragment_no"]
            expected_nos.add(no)
            item = {
                "fragment_no": no,
                "expected_revision": entry["revision"],
                "expected_content_hash": entry["content_hash"],
            }
            frag = fragments_by_no.get(no)
            if frag is None:
                missing.append(item)
                continue
            item["actual_revision"] = frag["revision"]
            item["actual_content_hash"] = frag["content_hash"]
            if frag["revision"] != entry["revision"]:
                revision_mismatches.append(item)
            elif frag["content_hash"] != entry["content_hash"]:
                content_mismatches.append(item)
            else:
                matched.append(item)

    extras = [
        _row_to_fragment(r) for r in fragment_rows if r["fragment_no"] not in expected_nos
    ]
    reconciled = (
        manifest is not None
        and not missing
        and not revision_mismatches
        and not content_mismatches
        and not extras
    )
    return {
        "matched": matched,
        "missing": missing,
        "revision_mismatches": revision_mismatches,
        "content_mismatches": content_mismatches,
        "extra_fragments": extras,
        "reconciled": reconciled,
    }


def get_reconciliation(station: str, minute_dt: datetime) -> dict[str, Any]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        # One read-only snapshot: manifest and fragments come from the same
        # committed state even while writers interleave around this request.
        manifest = _get_latest_manifest(conn, station, minute)
        rows = _get_fragments(conn, station, minute)
        recon = _reconcile(manifest, rows)
        assembly = compute_assembly(rows)

    return {
        "station": station,
        "minute_utc": _parse_minute(minute),
        "manifest_present": manifest is not None,
        "manifest_revision": None if manifest is None else manifest["manifest_revision"],
        "manifest_hash": None if manifest is None else manifest["manifest_hash"],
        **recon,
        **assembly,
        "publishable": bool(
            manifest is not None and recon["reconciled"] and assembly["complete"]
        ),
    }


# --------------------------------------------------------------------------- #
# Manifest writes
# --------------------------------------------------------------------------- #
def submit_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    station = payload["station"]
    minute = minute_key(payload["minute_utc"])
    revision = payload["manifest_revision"]
    entries = [dict(e) for e in payload["entries"]]
    new_hash = manifest_hash(entries)
    entries_json = _dump_entries(entries)

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            current = _get_latest_manifest(conn, station, minute)
            if current is not None:
                if revision < current["manifest_revision"]:
                    raise ManifestRevisionError(
                        f"manifest for this station-minute is already at revision "
                        f"{current['manifest_revision']}; incoming revision "
                        f"{revision} is not higher"
                    )
                if revision == current["manifest_revision"]:
                    # Same version: identical content is an idempotent retry and
                    # returns the original manifest; different text is refused.
                    if current["manifest_hash"] == new_hash:
                        result = _row_to_manifest(current)
                        result["status"] = "unchanged"
                        conn.commit()
                        return result
                    raise ManifestConflictError(
                        f"manifest revision {revision} already exists with "
                        "different entries"
                    )
                # Higher revision: persist the new list and supersede older ones.
            cur = conn.execute(
                "INSERT INTO manifests (station, minute_utc, manifest_revision, "
                "entries, manifest_hash) VALUES (?, ?, ?, ?, ?)",
                (station, minute, revision, entries_json, new_hash),
            )
            # A newer handover list supersedes every earlier publication of this
            # minute: its frozen wave and source stay readable, but it is no
            # longer the current publication.
            conn.execute(
                "UPDATE publications SET stale = 1 WHERE station = ? "
                "AND minute_utc = ? AND stale = 0",
                (station, minute),
            )
            row = conn.execute(
                "SELECT * FROM manifests WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            conn.commit()
            result = _row_to_manifest(row)
            result["status"] = "accepted" if current is None else "superseded"
            return result
        except Exception:
            conn.rollback()
            raise


def get_manifest(station: str, minute_dt: datetime) -> dict[str, Any] | None:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        row = _get_latest_manifest(conn, station, minute)
        return None if row is None else _row_to_manifest(row)


def list_manifest_revisions(
    station: str, minute_dt: datetime
) -> list[dict[str, Any]]:
    """Full revision history, newest first (source provenance)."""
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM manifests WHERE station = ? AND minute_utc = ? "
            "ORDER BY manifest_revision DESC",
            (station, minute),
        ).fetchall()
        return [_row_to_manifest(r) for r in rows]


# --------------------------------------------------------------------------- #
# Fragment upload
# --------------------------------------------------------------------------- #
def upload_fragment(payload: dict[str, Any]) -> dict[str, Any]:
    station = payload["station"]
    minute = minute_key(payload["minute_utc"])
    fragment_no = payload["fragment_no"]
    revision = payload["revision"]
    start_sample = payload["start_sample"]
    samples = payload["samples"]

    # A fragment must stay inside the 60 one-hertz sample slots.
    if start_sample + len(samples) > SAMPLES_PER_MINUTE:
        raise BoundaryError(
            f"fragment exceeds minute boundary: start_sample={start_sample} "
            f"length={len(samples)} would reach slot {start_sample + len(samples)} "
            f"(limit {SAMPLES_PER_MINUTE})"
        )

    new_hash = content_hash(start_sample, samples)
    samples_json = dump_samples(samples)

    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            existing = conn.execute(
                "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
                "AND fragment_no = ?",
                (station, minute, fragment_no),
            ).fetchone()

            changed = False
            if existing is not None:
                if revision < existing["revision"]:
                    raise StaleRevisionError(
                        f"fragment_no={fragment_no} already at revision "
                        f"{existing['revision']}; incoming revision {revision} "
                        "is not higher"
                    )
                if revision == existing["revision"]:
                    # Same version: identical content is an idempotent retry and
                    # must return the original result; different content is refused.
                    if existing["content_hash"] == new_hash:
                        result = _row_to_fragment(existing)
                        result["status"] = "unchanged"
                        conn.commit()
                        return result
                    raise ContentConflictError(
                        f"fragment_no={fragment_no} revision={revision} already "
                        "exists with different content"
                    )
                # Higher revision accepted.
                conn.execute(
                    "UPDATE fragments SET revision = ?, start_sample = ?, "
                    "samples = ?, content_hash = ? WHERE id = ?",
                    (revision, start_sample, samples_json, new_hash, existing["id"]),
                )
                changed = True
            else:
                conn.execute(
                    "INSERT INTO fragments (station, minute_utc, fragment_no, "
                    "revision, start_sample, samples, content_hash) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        station,
                        minute,
                        fragment_no,
                        revision,
                        start_sample,
                        samples_json,
                        new_hash,
                    ),
                )
                changed = True

            # The effective fragment set changed: every earlier publication of
            # this minute is now superseded and marked stale in the same txn.
            if changed:
                conn.execute(
                    "UPDATE publications SET stale = 1 WHERE station = ? "
                    "AND minute_utc = ? AND stale = 0",
                    (station, minute),
                )

            row = conn.execute(
                "SELECT * FROM fragments WHERE station = ? AND minute_utc = ? "
                "AND fragment_no = ?",
                (station, minute, fragment_no),
            ).fetchone()
            conn.commit()
            result = _row_to_fragment(row)
            result["status"] = "accepted" if existing is None else "superseded"
            return result
        except Exception:
            conn.rollback()
            raise


# --------------------------------------------------------------------------- #
# Reads
# --------------------------------------------------------------------------- #
def get_assembly(station: str, minute_dt: datetime) -> dict[str, Any]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        rows = _get_fragments(conn, station, minute)
        fragments = [_row_to_fragment(r) for r in rows]
        assembly = compute_assembly(rows)
    return {
        "station": station,
        "minute_utc": _parse_minute(minute),
        **assembly,
        "fragments": fragments,
    }


def list_fragments(station: str, minute_dt: datetime) -> list[dict[str, Any]]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        rows = _get_fragments(conn, station, minute)
        return [_row_to_fragment(r) for r in rows]


def _refresh_staleness(conn: sqlite3.Connection, pub: sqlite3.Row) -> sqlite3.Row:
    """Self-heal the stale flag from the current fragment set on read."""
    rows = _get_fragments(conn, pub["station"], pub["minute_utc"])
    current_hash = _fragment_set_hash(rows)
    if not pub["stale"] and current_hash != pub["fragment_set_hash"]:
        conn.execute("UPDATE publications SET stale = 1 WHERE id = ?", (pub["id"],))
        pub = conn.execute(
            "SELECT * FROM publications WHERE id = ?", (pub["id"],)
        ).fetchone()
    return pub


def _hydrate_publication(conn: sqlite3.Connection, pub: sqlite3.Row) -> dict[str, Any]:
    items = conn.execute(
        "SELECT * FROM publication_items WHERE publication_id = ? ORDER BY fragment_no",
        (pub["id"],),
    ).fetchall()
    keys = pub.keys()
    return {
        "id": pub["id"],
        "station": pub["station"],
        "minute_utc": _parse_minute(pub["minute_utc"]),
        "wave": load_samples(pub["wave"]),
        "stale": bool(pub["stale"]),
        "fragment_set_hash": pub["fragment_set_hash"],
        "manifest_revision": pub["manifest_revision"] if "manifest_revision" in keys else None,
        "created_at": pub["created_at"],
        "items": [
            {
                "fragment_no": it["fragment_no"],
                "revision": it["revision"],
                "start_sample": it["start_sample"],
                "samples": load_samples(it["samples"]),
                "content_hash": it["content_hash"],
            }
            for it in items
        ],
    }


def list_publications(station: str | None, minute_dt: datetime | None) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if station is not None:
        clauses.append("station = ?")
        params.append(station)
    if minute_dt is not None:
        clauses.append("minute_utc = ?")
        params.append(minute_key(minute_dt))
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    with get_conn() as conn:
        pubs = conn.execute(
            f"SELECT * FROM publications{where} ORDER BY id", params
        ).fetchall()
        out = []
        for pub in pubs:
            pub = _refresh_staleness(conn, pub)
            out.append(_hydrate_publication(conn, pub))
        conn.commit()
        return out


def get_publication(pub_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        pub = conn.execute(
            "SELECT * FROM publications WHERE id = ?", (pub_id,)
        ).fetchone()
        if pub is None:
            return None
        pub = _refresh_staleness(conn, pub)
        result = _hydrate_publication(conn, pub)
        conn.commit()
        return result


def get_latest_publication(station: str, minute_dt: datetime) -> dict[str, Any] | None:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        pub = conn.execute(
            "SELECT * FROM publications WHERE station = ? AND minute_utc = ? "
            "ORDER BY id DESC LIMIT 1",
            (station, minute),
        ).fetchone()
        if pub is None:
            return None
        pub = _refresh_staleness(conn, pub)
        result = _hydrate_publication(conn, pub)
        conn.commit()
        return result


# --------------------------------------------------------------------------- #
# Publish
# --------------------------------------------------------------------------- #
def _reconciliation_blockers(recon: dict[str, Any]) -> str:
    parts: list[str] = []
    if recon["missing"]:
        parts.append(
            "missing fragments: "
            + ",".join(str(i["fragment_no"]) for i in recon["missing"])
        )
    if recon["revision_mismatches"]:
        parts.append(
            "revision mismatches: "
            + ",".join(
                f"{i['fragment_no']}(expected {i['expected_revision']}, "
                f"got {i['actual_revision']})"
                for i in recon["revision_mismatches"]
            )
        )
    if recon["content_mismatches"]:
        parts.append(
            "content mismatches: "
            + ",".join(str(i["fragment_no"]) for i in recon["content_mismatches"])
        )
    if recon["extra_fragments"]:
        parts.append(
            "fragments outside manifest: "
            + ",".join(str(f["fragment_no"]) for f in recon["extra_fragments"])
        )
    return "; ".join(parts)


def publish_minute(station: str, minute_dt: datetime) -> dict[str, Any]:
    minute = minute_key(minute_dt)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Everything below reads one single committed snapshot: the manifest,
            # the fragment set, the assembly and any existing publication.
            rows = _get_fragments(conn, station, minute)
            assembly = compute_assembly(rows)
            manifest = _get_latest_manifest(conn, station, minute)
            recon = _reconcile(manifest, rows)

            if manifest is None:
                raise NotPublishableError(
                    "no handover manifest for this station-minute"
                )
            if not recon["reconciled"]:
                raise NotPublishableError(
                    "manifest not fully matched: " + _reconciliation_blockers(recon)
                )
            if not rows:
                raise NotPublishableError("no fragments for this station-minute")
            if assembly["gaps"]:
                raise NotPublishableError(
                    f"minute incomplete, gaps at positions: "
                    f"{','.join(map(str, assembly['gaps']))}"
                )
            if assembly["conflicts"]:
                detail = "; ".join(
                    f"position {c['position']} fragments {c['fragment_nos']} "
                    f"values {c['values']}"
                    for c in assembly["conflicts"]
                )
                raise NotPublishableError(f"unresolved conflicts: {detail}")

            set_hash = _fragment_set_hash(rows)
            manifest_rev = manifest["manifest_revision"]

            # A fresh publication for this exact fragment set *and* manifest
            # revision already exists: return it instead of duplicating.
            existing = conn.execute(
                "SELECT * FROM publications WHERE station = ? AND minute_utc = ? "
                "AND fragment_set_hash = ? AND manifest_revision = ? AND stale = 0 "
                "ORDER BY id DESC LIMIT 1",
                (station, minute, set_hash, manifest_rev),
            ).fetchone()
            if existing is not None:
                result = _hydrate_publication(conn, existing)
                conn.commit()
                return result

            # Build the 60-sample wave from the committed fragment set.
            wave = [0] * SAMPLES_PER_MINUTE
            for r in rows:
                start = r["start_sample"]
                for offset, value in enumerate(load_samples(r["samples"])):
                    wave[start + offset] = value

            cur = conn.execute(
                "INSERT INTO publications (station, minute_utc, wave, "
                "fragment_set_hash, manifest_revision, stale) "
                "VALUES (?, ?, ?, ?, ?, 0)",
                (station, minute, dump_samples(wave), set_hash, manifest_rev),
            )
            pub_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO publication_items (publication_id, fragment_no, "
                "revision, start_sample, samples, content_hash) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        pub_id,
                        r["fragment_no"],
                        r["revision"],
                        r["start_sample"],
                        r["samples"],
                        r["content_hash"],
                    )
                    for r in rows
                ],
            )
            pub = conn.execute(
                "SELECT * FROM publications WHERE id = ?", (pub_id,)
            ).fetchone()
            result = _hydrate_publication(conn, pub)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
