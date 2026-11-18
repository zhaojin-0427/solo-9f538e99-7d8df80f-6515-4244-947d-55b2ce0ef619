#!/usr/bin/env python3
"""End-to-end smoke test against a running server.

Usage: python scripts/smoke.py [base_url]
Exits non-zero on the first failed expectation.
"""
from __future__ import annotations

import hashlib
import json
import sys

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8011"
MIN = "2026-11-17T05:06:00Z"
ST = "SMOKE"

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


def chash(start: int, samples: list[int]) -> str:
    payload = json.dumps(
        {"start_sample": start, "samples": samples}, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def main() -> int:
    http = httpx.Client(base_url=BASE, timeout=10)

    def frag(no, rev, start, samples, station=ST, minute=MIN):
        return {
            "station": station,
            "minute_utc": minute,
            "fragment_no": no,
            "revision": rev,
            "start_sample": start,
            "samples": samples,
        }

    def manifest(rev, entries, station=ST, minute=MIN):
        return {
            "station": station,
            "minute_utc": minute,
            "manifest_revision": rev,
            "entries": [
                {
                    "fragment_no": no,
                    "revision": r,
                    "content_hash": chash(start, samples),
                }
                for no, r, start, samples in entries
            ],
        }

    f1 = (1, 1, 0, list(range(0, 30)))
    f2 = (2, 1, 30, list(range(30, 60)))
    full_set = [f1, f2]

    # 1. Publishing before any handover manifest exists is refused.
    r = http.post("/fragments", json=frag(2, 1, 30, list(range(30, 60))))
    check("upload fragment 2 first", r.status_code == 201, r.json().get("status", ""))
    r = http.post("/publications", params={"station": ST, "minute_utc": MIN})
    check("publish without manifest refused", r.status_code == 409, r.text)

    # 2. Reconciliation without a manifest flags everything as extra.
    body = http.get(
        "/reconciliation", params={"station": ST, "minute_utc": MIN}
    ).json()
    check("reconciliation without manifest", body["manifest_present"] is False
          and body["publishable"] is False
          and [f["fragment_no"] for f in body["extra_fragments"]] == [2])

    # 3. Manifest submission: first revision accepted; missing fragment listed.
    r = http.post("/manifests", json=manifest(1, full_set))
    check("manifest revision 1 accepted", r.status_code == 201
          and r.json()["status"] == "accepted")
    r = http.post("/fragments", json=frag(1, 1, 0, list(range(0, 30))))
    check("upload fragment 1", r.status_code == 201)
    body = http.get(
        "/reconciliation", params={"station": ST, "minute_utc": MIN}
    ).json()
    check("fully reconciled", body["reconciled"] is True
          and sorted(i["fragment_no"] for i in body["matched"]) == [1, 2])
    check("publishable flag", body["publishable"] is True)

    # 4. Manifest revision rules.
    r = http.post("/manifests", json=manifest(1, full_set))
    check("same manifest revision same content idempotent",
          r.status_code == 201 and r.json()["status"] == "unchanged")
    changed = [(1, 1, 0, [999] + list(range(1, 30))), f2]
    r = http.post("/manifests", json=manifest(1, changed))
    check("same manifest revision different content refused",
          r.status_code == 409)
    r = http.post("/manifests", json=manifest(2, full_set))
    check("higher manifest revision accepted",
          r.status_code == 201 and r.json()["status"] == "superseded")
    r = http.post("/manifests", json=manifest(1, full_set))
    check("lower manifest revision refused", r.status_code == 409)
    revs = http.get("/manifests/revisions",
                    params={"station": ST, "minute_utc": MIN}).json()
    check("manifest revision history retained",
          [m["manifest_revision"] for m in revs] == [2, 1])

    # 5. Assembly is complete and conflict free.
    body = http.get("/assembly", params={"station": ST, "minute_utc": MIN}).json()
    check("assembly complete", body["complete"] is True, f"coverage={body['coverage']}")
    check("no gaps", body["gaps"] == [])
    check("no conflicts", body["conflicts"] == [])

    # 6. Boundary rejection.
    r = http.post("/fragments", json=frag(9, 1, 58, [1, 2, 3]))
    check("out-of-bounds rejected", r.status_code == 422, r.json()["detail"])

    # 7. Idempotent fragment retry: same version + same content -> unchanged.
    r = http.post("/fragments", json=frag(1, 1, 0, list(range(0, 30))))
    check("same-version same-content retry idempotent",
          r.status_code == 201 and r.json()["status"] == "unchanged")
    rows = http.get("/fragments", params={"station": ST, "minute_utc": MIN}).json()
    check("no duplicate fragment row", len(rows) == 2)

    # 8. Same version, different content -> rejected.
    r = http.post("/fragments", json=frag(1, 1, 0, [999] + list(range(1, 30))))
    check("same-version different-content rejected", r.status_code == 409)

    # 9. Publish the reconciled complete minute (pins manifest revision 2).
    r = http.post("/publications", params={"station": ST, "minute_utc": MIN})
    check("publish succeeds", r.status_code == 201)
    pub1 = r.json()
    check("wave is 60 samples", pub1["wave"] == list(range(60)))
    check("published as fresh", pub1["stale"] is False)
    check("pins manifest revision 2", pub1["manifest_revision"] == 2)
    check("pins two fragment revisions",
          sorted((i["fragment_no"], i["revision"]) for i in pub1["items"])
          == [(1, 1), (2, 1)])

    # Re-publishing the identical state returns the same record.
    r = http.post("/publications", params={"station": ST, "minute_utc": MIN})
    check("republish identical state is idempotent",
          r.status_code == 201 and r.json()["id"] == pub1["id"])

    # 10. Correction arrives: higher fragment revision breaks the match.
    f1_v2 = (1, 2, 0, [42] + list(range(1, 30)))
    r = http.post("/fragments", json=frag(1, 2, 0, [42] + list(range(1, 30))))
    check("higher fragment revision accepted", r.status_code == 201
          and r.json()["status"] == "superseded")
    body = http.get(
        "/reconciliation", params={"station": ST, "minute_utc": MIN}
    ).json()
    check("correction opens revision mismatch",
          [i["fragment_no"] for i in body["revision_mismatches"]] == [1]
          and body["publishable"] is False)
    r = http.post("/publications", params={"station": ST, "minute_utc": MIN})
    check("publish while unmatched refused", r.status_code == 409)

    # 11. Old record is still readable, frozen, and marked stale.
    old = http.get(f"/publications/{pub1['id']}").json()
    check("old record still readable", old["id"] == pub1["id"])
    check("old wave frozen", old["wave"] == list(range(60)))
    check("old revisions frozen", old["items"][0]["revision"] == 1)
    check("old manifest source frozen", old["manifest_revision"] == 2)
    check("old record marked stale", old["stale"] is True)

    # 12. A matching new manifest restores eligibility; new publication works.
    r = http.post("/manifests", json=manifest(3, [f1_v2, f2]))
    check("manifest revision 3 matches correction", r.status_code == 201)
    body = http.get(
        "/reconciliation", params={"station": ST, "minute_utc": MIN}
    ).json()
    check("reconciled again", body["reconciled"] is True
          and body["publishable"] is True)
    r = http.post("/publications", params={"station": ST, "minute_utc": MIN})
    check("republish after correction", r.status_code == 201)
    pub2 = r.json()
    check("new record id differs", pub2["id"] != pub1["id"])
    check("new wave contains correction", pub2["wave"][0] == 42
          and pub2["wave"][1:] == list(range(1, 60)))
    check("new record fresh", pub2["stale"] is False)
    check("new record pins manifest revision 3", pub2["manifest_revision"] == 3)
    latest = http.get("/publications/latest",
                      params={"station": ST, "minute_utc": MIN}).json()
    check("latest endpoint returns new record", latest["id"] == pub2["id"])
    listing = http.get("/publications",
                       params={"station": ST, "minute_utc": MIN}).json()
    check("both records retained", [p["id"] for p in listing]
          == [pub1["id"], pub2["id"]])
    check("staleness flags", [p["stale"] for p in listing] == [True, False])

    # 13. Extra fragment outside the manifest blocks publication.
    http.post("/fragments", json=frag(1, 1, 0, [7, 8, 9], station="EXTRA"))
    http.post("/fragments", json=frag(2, 1, 30, list(range(30, 60)), station="EXTRA"))
    http.post("/fragments", json=frag(3, 1, 0, [7] + [0] * 59, station="EXTRA"))
    http.post("/manifests", json=manifest(
        1,
        [(1, 1, 0, [7, 8, 9]), (2, 1, 30, list(range(30, 60)))],
        station="EXTRA",
    ))
    body = http.get("/reconciliation",
                    params={"station": "EXTRA", "minute_utc": MIN}).json()
    check("extra fragment detected",
          [f["fragment_no"] for f in body["extra_fragments"]] == [3]
          and body["reconciled"] is False)
    r = http.post("/publications", params={"station": "EXTRA", "minute_utc": MIN})
    check("extra fragment blocks publish", r.status_code == 409)

    # 14. Conflict reporting on another station-minute.
    http.post("/fragments", json=frag(1, 1, 0, [7, 8, 9], station="BAD"))
    http.post("/fragments", json=frag(2, 1, 2, [1, 2], station="BAD"))
    body = http.get("/assembly", params={"station": "BAD", "minute_utc": MIN}).json()
    check("conflict located with fragment numbers",
          body["conflicts"] == [{"position": 2, "fragment_nos": [1, 2],
                                 "values": [9, 1]}])
    check("gaps listed", body["gaps"] == list(range(4, 60)))
    r = http.post("/publications", params={"station": "BAD", "minute_utc": MIN})
    check("conflicted minute cannot publish", r.status_code == 409)
    r = http.get("/publications", params={"station": "BAD"})
    check("failed publish left no rows", r.json() == [])

    # 15. Missing minute lookups -> 404.
    r = http.get("/publications/latest",
                 params={"station": "NOPE", "minute_utc": MIN})
    check("unknown publication 404", r.status_code == 404)
    r = http.get("/manifests", params={"station": "NOPE", "minute_utc": MIN})
    check("unknown manifest 404", r.status_code == 404)

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S): {failures}")
        return 1
    print("all smoke checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
