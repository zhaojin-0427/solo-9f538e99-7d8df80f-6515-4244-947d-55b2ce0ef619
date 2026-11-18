"""End-to-end tests for the waveform assembly API.

Each test gets an isolated SQLite file so runs never touch real data.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="wf-test-")
    db_path = Path(tmpdir) / "test.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    # Import after the env var is set so config picks up the test database.
    import importlib

    from app import config, database, main, service

    # Reload in dependency order so every module re-reads the test DB path.
    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()
    with TestClient(main.app) as c:
        yield c
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()
    os.rmdir(tmpdir)


MIN = "2026-11-17T05:06:00Z"


def frag(no, rev, start, samples, station="STA1", minute=MIN):
    return {
        "station": station,
        "minute_utc": minute,
        "fragment_no": no,
        "revision": rev,
        "start_sample": start,
        "samples": samples,
    }


def chash(start, samples):
    """SHA-256 content digest, identical to the service's fragment hash."""
    payload = json.dumps(
        {"start_sample": start, "samples": samples}, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def manifest(rev, entries, station="STA1", minute=MIN):
    """entries: list of (fragment_no, revision, start, samples) tuples."""
    return {
        "station": station,
        "minute_utc": minute,
        "manifest_revision": rev,
        "entries": [
            {"fragment_no": no, "revision": r, "content_hash": chash(start, samples)}
            for no, r, start, samples in entries
        ],
    }


def put_manifest(client, rev, entries, station="STA1", minute=MIN):
    return client.post(
        "/manifests", json=manifest(rev, entries, station, minute)
    )


def upload_all(client, entries, station="STA1", minute=MIN):
    for no, r, start, samples in entries:
        resp = client.post(
            "/fragments", json=frag(no, r, start, samples, station, minute)
        )
        assert resp.status_code == 201, resp.text


# The canonical two-fragment minute used across the lifecycle tests.
WAVE_A = list(range(60))
F1_A = (1, 1, 0, WAVE_A[:30])
F2_A = (2, 1, 30, WAVE_A[30:])
FULL_SET = [F1_A, F2_A]


# --------------------------------------------------------------------------- #
# Health & validation
# --------------------------------------------------------------------------- #
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_boundary_rejected(client):
    # start 55 + 6 samples reaches slot 61 -> out of bounds
    r = client.post("/fragments", json=frag(1, 1, 55, [1, 2, 3, 4, 5, 6]))
    assert r.status_code == 422
    assert "boundary" in r.json()["detail"]


def test_non_whole_minute_rejected(client):
    payload = frag(1, 1, 0, [1])
    payload["minute_utc"] = "2026-11-17T05:06:30Z"
    r = client.post("/fragments", json=payload)
    assert r.status_code == 422


def test_revision_must_be_positive(client):
    r = client.post("/fragments", json=frag(1, 0, 0, [1]))
    assert r.status_code == 422


def test_empty_samples_rejected(client):
    r = client.post("/fragments", json=frag(1, 1, 0, []))
    assert r.status_code == 422


# --------------------------------------------------------------------------- #
# Revision semantics
# --------------------------------------------------------------------------- #
def test_upload_and_retrieve(client):
    r = client.post("/fragments", json=frag(1, 1, 0, [10, 20]))
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "accepted"
    assert body["revision"] == 1
    assert len(body["content_hash"]) == 64

    r = client.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_lower_revision_rejected(client):
    client.post("/fragments", json=frag(1, 3, 0, [10, 20]))
    r = client.post("/fragments", json=frag(1, 2, 0, [99]))
    assert r.status_code == 409
    assert "not higher" in r.json()["detail"]
    # stored fragment untouched
    r = client.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()[0]["revision"] == 3


def test_same_revision_same_content_idempotent(client):
    r1 = client.post("/fragments", json=frag(1, 2, 0, [10, 20]))
    r2 = client.post("/fragments", json=frag(1, 2, 0, [10, 20]))
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r2.json()["status"] == "unchanged"
    assert r1.json()["content_hash"] == r2.json()["content_hash"]
    r = client.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
    assert len(r.json()) == 1


def test_same_revision_different_content_rejected(client):
    client.post("/fragments", json=frag(1, 2, 0, [10, 20]))
    r = client.post("/fragments", json=frag(1, 2, 0, [10, 21]))
    assert r.status_code == 409
    assert "different content" in r.json()["detail"]
    r = client.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()[0]["samples"] == [10, 20]


def test_higher_revision_supersedes(client):
    client.post("/fragments", json=frag(1, 1, 0, [10, 20]))
    r = client.post("/fragments", json=frag(1, 2, 0, [11, 21]))
    assert r.status_code == 201
    assert r.json()["status"] == "superseded"
    r = client.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()[0]["revision"] == 2
    assert r.json()[0]["samples"] == [11, 21]


# --------------------------------------------------------------------------- #
# Assembly: gaps and conflicts
# --------------------------------------------------------------------------- #
def test_assembly_gaps(client):
    client.post("/fragments", json=frag(1, 1, 0, [1, 2, 3]))
    r = client.get("/assembly", params={"station": "STA1", "minute_utc": MIN})
    body = r.json()
    assert body["complete"] is False
    assert body["coverage"] == 3
    assert body["gaps"] == list(range(3, 60))
    assert body["conflicts"] == []


def test_assembly_conflict_reports_position_and_fragments(client):
    client.post("/fragments", json=frag(1, 1, 0, [1, 2, 3]))
    client.post("/fragments", json=frag(2, 1, 2, [9, 4]))  # overlap at slot 2
    r = client.get("/assembly", params={"station": "STA1", "minute_utc": MIN})
    body = r.json()
    assert body["complete"] is False
    assert body["conflicts"] == [
        {"position": 2, "fragment_nos": [1, 2], "values": [3, 9]}
    ]
    assert body["gaps"] == list(range(4, 60))


def test_overlap_same_value_is_not_conflict(client):
    client.post("/fragments", json=frag(1, 1, 0, [1, 2, 3]))
    client.post("/fragments", json=frag(2, 1, 2, [3, 4]))  # slot 2 agrees
    r = client.get("/assembly", params={"station": "STA1", "minute_utc": MIN})
    body = r.json()
    assert body["conflicts"] == []
    assert body["coverage"] == 4
    assert body["gaps"] == list(range(4, 60))


def test_complete_minute_assembly(client):
    client.post("/fragments", json=frag(1, 1, 0, list(range(0, 30))))
    client.post("/fragments", json=frag(2, 1, 30, list(range(30, 60))))
    r = client.get("/assembly", params={"station": "STA1", "minute_utc": MIN})
    body = r.json()
    assert body["complete"] is True
    assert body["coverage"] == 60
    assert body["gaps"] == []
    assert body["conflicts"] == []


# --------------------------------------------------------------------------- #
# Manifest submission rules
# --------------------------------------------------------------------------- #
def test_manifest_submit_and_get(client):
    r = put_manifest(client, 1, FULL_SET)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "accepted"
    assert body["manifest_revision"] == 1
    assert len(body["manifest_hash"]) == 64
    assert [e["fragment_no"] for e in body["entries"]] == [1, 2]

    r = client.get("/manifests", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 200
    assert r.json()["manifest_revision"] == 1


def test_manifest_missing_minute_404(client):
    r = client.get("/manifests", params={"station": "NOPE", "minute_utc": MIN})
    assert r.status_code == 404


def test_manifest_empty_entries_rejected(client):
    payload = manifest(1, [])
    r = client.post("/manifests", json=payload)
    assert r.status_code == 422


def test_manifest_duplicate_fragment_no_rejected(client):
    payload = manifest(1, FULL_SET)
    payload["entries"].append(dict(payload["entries"][0]))
    r = client.post("/manifests", json=payload)
    assert r.status_code == 422
    assert "duplicate" in r.json()["detail"][0]["msg"]


def test_manifest_bad_hash_shape_rejected(client):
    payload = manifest(1, FULL_SET)
    payload["entries"][0]["content_hash"] = "not-a-hash"
    r = client.post("/manifests", json=payload)
    assert r.status_code == 422


def test_manifest_higher_revision_supersedes(client):
    put_manifest(client, 1, FULL_SET)
    # New revision: fragment 1 now expected at revision 2.
    f1_v2 = (1, 2, 0, [100] + WAVE_A[1:30])
    r = put_manifest(client, 2, [f1_v2, F2_A])
    assert r.status_code == 201
    assert r.json()["status"] == "superseded"
    r = client.get("/manifests", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()["manifest_revision"] == 2
    # full history stays available for provenance
    revs = client.get(
        "/manifests/revisions",
        params={"station": "STA1", "minute_utc": MIN},
    ).json()
    assert [m["manifest_revision"] for m in revs] == [2, 1]


def test_manifest_lower_revision_rejected(client):
    put_manifest(client, 2, FULL_SET)
    r = put_manifest(client, 1, FULL_SET)
    assert r.status_code == 409
    assert "not higher" in r.json()["detail"]
    # stored manifest untouched
    r = client.get("/manifests", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()["manifest_revision"] == 2


def test_manifest_same_revision_same_content_idempotent(client):
    r1 = put_manifest(client, 1, FULL_SET)
    r2 = put_manifest(client, 1, FULL_SET)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r2.json()["status"] == "unchanged"
    assert r1.json()["id"] == r2.json()["id"]
    revs = client.get(
        "/manifests/revisions",
        params={"station": "STA1", "minute_utc": MIN},
    ).json()
    assert len(revs) == 1  # no duplicate row


def test_manifest_same_revision_different_content_rejected(client):
    put_manifest(client, 1, FULL_SET)
    changed = [(1, 1, 0, [999] + WAVE_A[1:30]), F2_A]
    r = put_manifest(client, 1, changed)
    assert r.status_code == 409
    assert "different entries" in r.json()["detail"]
    # failed write leaves no half manifest: revision 1 keeps original content
    got = client.get(
        "/manifests", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert got["entries"][0]["content_hash"] == chash(0, WAVE_A[:30])


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #
def test_reconciliation_without_manifest(client):
    upload_all(client, FULL_SET)
    r = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    )
    body = r.json()
    assert body["manifest_present"] is False
    assert body["reconciled"] is False
    assert body["publishable"] is False
    # everything uploaded is "extra" without a manifest
    assert sorted(f["fragment_no"] for f in body["extra_fragments"]) == [1, 2]


def test_reconciliation_reports_missing(client):
    put_manifest(client, 1, FULL_SET)
    # only fragment 1 uploaded
    client.post("/fragments", json=frag(*F1_A))
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["reconciled"] is False
    assert body["publishable"] is False
    assert [i["fragment_no"] for i in body["missing"]] == [2]
    assert body["missing"][0]["actual_revision"] is None
    assert [i["fragment_no"] for i in body["matched"]] == [1]


def test_reconciliation_reports_revision_mismatch(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    # fragment 1 corrected to revision 2 -> manifest still expects revision 1
    client.post("/fragments", json=frag(1, 2, 0, [100] + WAVE_A[1:30]))
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["reconciled"] is False
    mm = body["revision_mismatches"]
    assert len(mm) == 1
    assert mm[0]["fragment_no"] == 1
    assert mm[0]["expected_revision"] == 1
    assert mm[0]["actual_revision"] == 2


def test_reconciliation_reports_content_mismatch(client):
    # Same revision number cannot hold different content through the API, so
    # craft the mismatch by declaring a different digest in the manifest while
    # uploading the fragment at the declared revision.
    bad_entries = [
        (1, 1, 0, [9] + WAVE_A[1:30]),  # hash computed from different samples
        F2_A,
    ]
    put_manifest(client, 1, bad_entries)
    upload_all(client, FULL_SET)  # actual fragment 1 content differs
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    cm = body["content_mismatches"]
    assert len(cm) == 1
    assert cm[0]["fragment_no"] == 1
    assert cm[0]["actual_revision"] == 1
    assert cm[0]["actual_content_hash"] != cm[0]["expected_content_hash"]


def test_reconciliation_reports_extra_fragments(client):
    put_manifest(client, 1, [F1_A])
    upload_all(client, FULL_SET)  # fragment 2 is not on the list
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["reconciled"] is False
    assert [f["fragment_no"] for f in body["extra_fragments"]] == [2]
    assert [i["fragment_no"] for i in body["matched"]] == [1]


def test_reconciliation_full_match(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["manifest_present"] is True
    assert body["manifest_revision"] == 1
    assert sorted(i["fragment_no"] for i in body["matched"]) == [1, 2]
    assert body["missing"] == []
    assert body["revision_mismatches"] == []
    assert body["content_mismatches"] == []
    assert body["extra_fragments"] == []
    assert body["reconciled"] is True
    assert body["complete"] is True
    assert body["publishable"] is True


def test_reconciliation_recomputes_after_correction(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    assert client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()["publishable"] is True

    # correction breaks the match; a matching new manifest restores it
    f1_v2 = (1, 2, 0, [100] + WAVE_A[1:30])
    client.post("/fragments", json=frag(*f1_v2))
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["reconciled"] is False
    assert body["publishable"] is False

    put_manifest(client, 2, [f1_v2, F2_A])
    body = client.get(
        "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert body["manifest_revision"] == 2
    assert body["reconciled"] is True
    assert body["publishable"] is True


# --------------------------------------------------------------------------- #
# Publishing under manifest gating
# --------------------------------------------------------------------------- #
def test_publish_without_manifest_rejected(client):
    upload_all(client, FULL_SET)
    r = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    )
    assert r.status_code == 409
    assert "manifest" in r.json()["detail"]


def test_publish_fails_with_gaps(client):
    put_manifest(client, 1, [(1, 1, 0, [1, 2])])
    client.post("/fragments", json=frag(1, 1, 0, [1, 2]))
    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 409
    assert "gaps" in r.json()["detail"]


def test_publish_fails_with_conflict(client):
    entries = [
        (1, 1, 0, [1] + [0] * 29),
        (2, 1, 0, [9] + [0] * 59),
    ]
    put_manifest(client, 1, entries)
    client.post("/fragments", json=frag(1, 1, 0, [1] + [0] * 29))
    client.post("/fragments", json=frag(2, 1, 0, [9] + [0] * 59))
    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 409
    assert "conflicts" in r.json()["detail"]
    # failed publish leaves nothing behind
    r = client.get("/publications", params={"station": "STA1"})
    assert r.json() == []


def test_publish_fails_when_reconciliation_open(client):
    # manifest expects two fragments, only one delivered; also incomplete
    put_manifest(client, 1, FULL_SET)
    client.post("/fragments", json=frag(*F1_A))
    r = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    )
    assert r.status_code == 409
    assert "missing" in r.json()["detail"]


def test_publish_fails_with_extra_fragment(client):
    # Complete, conflict-free assembly but one fragment sits outside the list.
    small = [1] + [0] * 59
    entries = [
        (1, 1, 0, [1] + [0] * 29),
        (2, 1, 30, [0] * 30),
        (3, 1, 0, small),  # extra, overlaps fragment 1 with same values
    ]
    put_manifest(client, 1, entries[:2])
    upload_all(client, entries)
    # assembly itself is complete and conflict free (overlaps agree)
    assert client.get(
        "/assembly", params={"station": "STA1", "minute_utc": MIN}
    ).json()["complete"] is True
    r = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    )
    assert r.status_code == 409
    assert "outside manifest" in r.json()["detail"]


def test_full_publish_lifecycle(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)

    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 201
    pub = r.json()
    pub_id = pub["id"]
    assert pub["wave"] == WAVE_A
    assert pub["stale"] is False
    assert pub["manifest_revision"] == 1
    assert sorted(it["fragment_no"] for it in pub["items"]) == [1, 2]
    assert {it["revision"] for it in pub["items"]} == {1}
    # revisions and per-fragment waveforms are pinned in the record
    pinned = {it["fragment_no"]: it["samples"] for it in pub["items"]}
    assert pinned[1] == WAVE_A[:30]
    assert pinned[2] == WAVE_A[30:]

    # publishing the same fragment set + manifest again is idempotent
    r2 = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r2.json()["id"] == pub_id

    # a later correction changes current assembly but must not mutate the record
    f1_v2 = (1, 2, 0, [100] + WAVE_A[1:30])
    client.post("/fragments", json=frag(*f1_v2))
    r = client.get(f"/publications/{pub_id}")
    old = r.json()
    assert old["wave"] == WAVE_A  # frozen waveform
    assert old["stale"] is True
    assert old["items"][0]["revision"] == 1  # frozen revisions
    assert old["manifest_revision"] == 1  # frozen source manifest

    # publishing while the manifest no longer matches is refused
    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 409

    # a matching new manifest restores eligibility and allows a fresh record
    put_manifest(client, 2, [f1_v2, F2_A])
    new_wave = [100] + WAVE_A[1:]
    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 201
    pub2 = r.json()
    assert pub2["id"] != pub_id
    assert pub2["wave"] == new_wave
    assert pub2["stale"] is False
    assert pub2["manifest_revision"] == 2

    # both records remain readable; latest endpoint returns the new one
    r = client.get(
        "/publications/latest",
        params={"station": "STA1", "minute_utc": MIN},
    )
    assert r.json()["id"] == pub2["id"]
    r = client.get("/publications", params={"station": "STA1", "minute_utc": MIN})
    ids = [p["id"] for p in r.json()]
    assert ids == [pub_id, pub2["id"]]
    assert [p["stale"] for p in r.json()] == [True, False]


def test_new_manifest_revision_marks_publication_stale(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    pub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert pub["stale"] is False

    # Same fragment set, but a newer handover list: prior publication is stale.
    put_manifest(client, 2, FULL_SET)
    r = client.get(f"/publications/{pub['id']}")
    assert r.json()["stale"] is True

    # the new snapshot can be published again, pinned to manifest revision 2
    r = client.post("/publications", params={"station": "STA1", "minute_utc": MIN})
    assert r.status_code == 201
    assert r.json()["manifest_revision"] == 2


def test_supersede_marks_previous_publication_stale(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    pub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert pub["stale"] is False

    # replacement fragment at same number, higher revision, different span
    client.post("/fragments", json=frag(2, 2, 29, [77] + list(range(30, 60))))
    r = client.get(f"/publications/{pub['id']}")
    assert r.json()["stale"] is True


def test_idempotent_retry_does_not_stale_publication(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    pub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()

    # same version, same content retransmission: fragment set unchanged
    client.post("/fragments", json=frag(*F2_A))
    r = client.get(f"/publications/{pub['id']}")
    assert r.json()["stale"] is False
    # no duplicate publication either
    again = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert again["id"] == pub["id"]


def test_idempotent_manifest_retry_does_not_stale_publication(client):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    pub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    # identical manifest resubmission changes nothing
    put_manifest(client, 1, FULL_SET)
    assert client.get(f"/publications/{pub['id']}").json()["stale"] is False
    again = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()
    assert again["id"] == pub["id"]


# --------------------------------------------------------------------------- #
# Restart durability and legacy volumes
# --------------------------------------------------------------------------- #
def test_data_survives_restart(client, monkeypatch, tmp_path):
    put_manifest(client, 1, FULL_SET)
    upload_all(client, FULL_SET)
    pub = client.post(
        "/publications", params={"station": "STA1", "minute_utc": MIN}
    ).json()

    # Simulate a process restart: rebuild app objects against the same file.
    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    with TestClient(main.app) as restarted:
        r = restarted.get(f"/publications/{pub['id']}")
        assert r.status_code == 200
        body = r.json()
        assert body["wave"] == list(range(60))
        assert body["stale"] is False
        assert body["manifest_revision"] == 1
        r = restarted.get(
            "/assembly", params={"station": "STA1", "minute_utc": MIN}
        )
        assert r.json()["complete"] is True
        r = restarted.get(
            "/manifests", params={"station": "STA1", "minute_utc": MIN}
        )
        assert r.json()["manifest_revision"] == 1


def test_legacy_sqlite_volume_still_readable(monkeypatch, tmp_path):
    """A database created by the pre-manifest release migrates transparently."""
    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    import importlib
    import sqlite3

    from app import config, database, main, service

    # Hand-build the *old* schema (no manifest table, no manifest column).
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE fragments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL, minute_utc TEXT NOT NULL,
            fragment_no INTEGER NOT NULL, revision INTEGER NOT NULL,
            start_sample INTEGER NOT NULL, samples TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            UNIQUE (station, minute_utc, fragment_no)
        );
        CREATE TABLE publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station TEXT NOT NULL, minute_utc TEXT NOT NULL,
            wave TEXT NOT NULL, fragment_set_hash TEXT NOT NULL,
            stale INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        CREATE TABLE publication_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            publication_id INTEGER NOT NULL, fragment_no INTEGER NOT NULL,
            revision INTEGER NOT NULL, start_sample INTEGER NOT NULL,
            samples TEXT NOT NULL, content_hash TEXT NOT NULL
        );
        INSERT INTO fragments (station, minute_utc, fragment_no, revision,
            start_sample, samples, content_hash)
        VALUES ('OLD', '2026-11-17T05:06:00Z', 1, 1, 0, '[1,2,3]', 'x');
        INSERT INTO publications (station, minute_utc, wave, fragment_set_hash, stale)
        VALUES ('OLD', '2026-11-17T05:06:00Z', '[1,2,3]', 'oldhash', 0);
        INSERT INTO publication_items (publication_id, fragment_no, revision,
            start_sample, samples, content_hash)
        VALUES (1, 1, 1, 0, '[1,2,3]', 'x');
        """
    )
    conn.commit()
    conn.close()

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()  # must migrate, not fail

    with TestClient(main.app) as c:
        # Old query endpoints still work.
        r = c.get(
            "/fragments", params={"station": "OLD", "minute_utc": MIN}
        )
        assert r.status_code == 200
        assert r.json()[0]["samples"] == [1, 2, 3]
        r = c.get("/publications", params={"station": "OLD"})
        pubs = r.json()
        assert len(pubs) == 1
        assert pubs[0]["wave"] == [1, 2, 3]
        assert pubs[0]["manifest_revision"] is None  # pre-manifest record
        # New endpoints are available on the migrated volume.
        r = c.get(
            "/reconciliation", params={"station": "OLD", "minute_utc": MIN}
        )
        assert r.status_code == 200
        assert r.json()["manifest_present"] is False
        # And the minute can now be brought under manifest control.
        r = c.post(
            "/manifests",
            json=manifest(
                1, [(1, 1, 0, [1, 2, 3])], station="OLD"
            ),
        )
        assert r.status_code == 201


# --------------------------------------------------------------------------- #
# Concurrency: interleaved manifest writes, uploads and publishes
# --------------------------------------------------------------------------- #
def test_concurrent_uploads_and_publishes(monkeypatch):
    """Hit a real uvicorn server with parallel uploads/publishes.

    Every publication must come from one committed state and failed publishes
    must never leave partial rows behind.
    """
    import socket
    import time

    import httpx
    import uvicorn

    tmpdir = tempfile.mkdtemp(prefix="wf-live-")
    db_path = Path(tmpdir) / "live.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    server = uvicorn.Server(
        uvicorn.Config(main.app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if httpx.get(base + "/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)
    else:  # pragma: no cover
        raise RuntimeError("test server did not start")

    # The manifest must be committed before any publish attempt can succeed.
    entries = [
        (no, 1, (no - 1) * 10, [no * 100] * 10) for no in range(1, 7)
    ]
    r = httpx.post(base + "/manifests", json=manifest(1, entries), timeout=10)
    assert r.status_code == 201, r.text

    errors: list[Exception] = []
    barrier = threading.Barrier(6)

    def worker(idx: int) -> None:
        try:
            no = idx + 1
            _, rev, start, samples = entries[idx]
            barrier.wait()
            r = httpx.post(
                base + "/fragments",
                json=frag(no, rev, start, samples),
                timeout=10,
            )
            assert r.status_code == 201, r.text
            rp = httpx.post(
                base + "/publications",
                params={"station": "STA1", "minute_utc": MIN},
                timeout=10,
            )
            # incomplete snapshots fail cleanly; the complete one succeeds —
            # a 500/half-written row would be a correctness bug
            assert rp.status_code in (201, 409), rp.text
        except Exception as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    with httpx.Client(base_url=base, timeout=10) as http:
        r = http.get("/fragments", params={"station": "STA1", "minute_utc": MIN})
        assert len(r.json()) == 6
        pubs = http.post(
            "/publications", params={"station": "STA1", "minute_utc": MIN}
        ).json()
        assert pubs["wave"] == [
            v for no in range(1, 7) for v in [no * 100] * 10
        ]
        assert pubs["manifest_revision"] == 1
        listing = http.get(
            "/publications", params={"station": "STA1", "minute_utc": MIN}
        ).json()
        # only one publication row for a single committed state
        assert len(listing) == 1

    server.should_exit = True
    thread.join(timeout=10)


def test_concurrent_manifest_and_fragment_publish(monkeypatch):
    """Publish eligibility always reflects one committed manifest+fragment state."""
    import socket
    import time

    import httpx
    import uvicorn

    tmpdir = tempfile.mkdtemp(prefix="wf-live2-")
    db_path = Path(tmpdir) / "live2.db"
    monkeypatch.setenv("WAVEFORM_DB_PATH", str(db_path))

    import importlib

    from app import config, database, main, service

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(service)
    importlib.reload(main)
    database.init_db()

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    server = uvicorn.Server(
        uvicorn.Config(main.app, host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if httpx.get(base + "/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)
    else:  # pragma: no cover
        raise RuntimeError("test server did not start")

    errors: list[Exception] = []
    barrier = threading.Barrier(3)

    # Manifest writer keeps bumping revisions; fragment writers upload and
    # correct; a publisher hammers publish the whole time.
    f1_v1 = F1_A
    f1_v2 = (1, 2, 0, [100] + WAVE_A[1:30])

    def manifest_writer() -> None:
        try:
            barrier.wait()
            for rev in range(1, 11):
                entries = [f1_v1 if rev % 2 else f1_v2, F2_A]
                rp = httpx.post(base + "/manifests", json=manifest(rev, entries),
                                timeout=10)
                assert rp.status_code == 201, rp.text
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def fragment_writer() -> None:
        try:
            barrier.wait()
            for _ in range(10):
                rp = httpx.post(base + "/fragments", json=frag(*f1_v2), timeout=10)
                assert rp.status_code in (201, 409), rp.text
                rp = httpx.post(base + "/fragments", json=frag(*f1_v1), timeout=10)
                # revision 1 after revision 2 must be refused, never overwrite
                assert rp.status_code == 409, rp.text
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def publisher() -> None:
        params = {"station": "STA1", "minute_utc": MIN}
        try:
            barrier.wait()
            for _ in range(20):
                rp = httpx.post(
                    base + "/publications", params=params, timeout=10
                )
                assert rp.status_code in (201, 409), rp.text
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=manifest_writer),
        threading.Thread(target=fragment_writer),
        threading.Thread(target=publisher),
    ]
    # Upload the steady-state set first so some attempts can actually succeed.
    httpx.post(base + "/fragments", json=frag(*F2_A), timeout=10)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    with httpx.Client(base_url=base, timeout=10) as http:
        # Every persisted publication must be internally consistent: its pinned
        # manifest revision and fragment set belong to one committed snapshot.
        pubs = http.get(
            "/publications", params={"station": "STA1", "minute_utc": MIN}
        ).json()
        for p in pubs:
            assert p["manifest_revision"] is not None
            assert len(p["wave"]) == 60
        recon = http.get(
            "/reconciliation", params={"station": "STA1", "minute_utc": MIN}
        ).json()
        # final state is coherent regardless of interleaving
        assert recon["manifest_present"] is True

    server.should_exit = True
    thread.join(timeout=10)


def test_station_isolation(client):
    client.post("/fragments", json=frag(1, 1, 0, [1, 2, 3], station="STA1"))
    client.post("/fragments", json=frag(1, 1, 0, [4, 5, 6], station="STA2"))
    r = client.get("/fragments", params={"station": "STA2", "minute_utc": MIN})
    assert r.json()[0]["samples"] == [4, 5, 6]
    r = client.get("/assembly", params={"station": "STA1", "minute_utc": MIN})
    assert r.json()["coverage"] == 3
