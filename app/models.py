"""Pydantic request/response models."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class FragmentIn(BaseModel):
    station: str = Field(min_length=1, max_length=64)
    minute_utc: datetime
    fragment_no: int = Field(ge=0, le=9999)
    revision: int = Field(ge=1)
    start_sample: int = Field(ge=0, le=59)
    samples: list[int] = Field(min_length=1, max_length=60)

    @field_validator("station")
    @classmethod
    def _station_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("station must not be blank")
        return v

    @field_validator("minute_utc")
    @classmethod
    def _whole_minute(cls, v: datetime) -> datetime:
        # Reject sub-minute precision: every fragment belongs to a whole minute.
        if v.second != 0 or v.microsecond != 0:
            raise ValueError("minute_utc must be a whole minute (seconds=0)")
        return v


class FragmentOut(BaseModel):
    station: str
    minute_utc: datetime
    fragment_no: int
    revision: int
    start_sample: int
    samples: list[int]
    content_hash: str


class ConflictOut(BaseModel):
    position: int
    fragment_nos: list[int]
    values: list[int]


class AssemblyOut(BaseModel):
    station: str
    minute_utc: datetime
    complete: bool
    coverage: int
    gaps: list[int]
    conflicts: list[ConflictOut]
    fragments: list[FragmentOut]


# --------------------------------------------------------------------------- #
# Handover manifests (upstream delivery lists)
# --------------------------------------------------------------------------- #
class ManifestEntryIn(BaseModel):
    """One expected fragment line on an upstream handover list."""

    fragment_no: int = Field(ge=0, le=9999)
    revision: int = Field(ge=1)
    content_hash: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")


class ManifestIn(BaseModel):
    station: str = Field(min_length=1, max_length=64)
    minute_utc: datetime
    manifest_revision: int = Field(ge=1)
    entries: list[ManifestEntryIn] = Field(min_length=1)

    @field_validator("station")
    @classmethod
    def _station_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("station must not be blank")
        return v

    @field_validator("minute_utc")
    @classmethod
    def _whole_minute(cls, v: datetime) -> datetime:
        if v.second != 0 or v.microsecond != 0:
            raise ValueError("minute_utc must be a whole minute (seconds=0)")
        return v

    @field_validator("entries")
    @classmethod
    def _no_duplicate_fragment_numbers(cls, v: list[ManifestEntryIn]) -> list[ManifestEntryIn]:
        seen: set[int] = set()
        for entry in v:
            if entry.fragment_no in seen:
                raise ValueError(
                    f"duplicate fragment_no {entry.fragment_no} in manifest entries"
                )
            seen.add(entry.fragment_no)
        return v


class ManifestEntryOut(BaseModel):
    fragment_no: int
    revision: int
    content_hash: str


class ManifestOut(BaseModel):
    id: int
    station: str
    minute_utc: datetime
    manifest_revision: int
    entries: list[ManifestEntryOut]
    manifest_hash: str
    created_at: str


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #
class ReconciliationItemOut(BaseModel):
    fragment_no: int
    expected_revision: int
    expected_content_hash: str
    actual_revision: int | None = None
    actual_content_hash: str | None = None


class ReconciliationOut(BaseModel):
    station: str
    minute_utc: datetime
    manifest_present: bool
    manifest_revision: int | None = None
    manifest_hash: str | None = None
    matched: list[ReconciliationItemOut]
    missing: list[ReconciliationItemOut]
    revision_mismatches: list[ReconciliationItemOut]
    content_mismatches: list[ReconciliationItemOut]
    # Fragments uploaded for this minute that no current manifest line covers.
    extra_fragments: list[FragmentOut]
    # True only when every manifest line matches an uploaded fragment exactly
    # and no fragment sits outside the manifest.
    reconciled: bool
    # Assembly state of the same committed snapshot.
    complete: bool
    coverage: int
    gaps: list[int]
    conflicts: list[ConflictOut]
    # A minute may be newly published only when reconciled AND complete with
    # no conflicts, evaluated against one committed state.
    publishable: bool


class PublicationItemOut(BaseModel):
    fragment_no: int
    revision: int
    start_sample: int
    samples: list[int]
    content_hash: str


class PublicationOut(BaseModel):
    id: int
    station: str
    minute_utc: datetime
    wave: list[int]
    stale: bool
    fragment_set_hash: str
    manifest_revision: int | None = None
    created_at: str
    items: list[PublicationItemOut]
