"""FastAPI application and HTTP routes."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import service
from .database import init_db
from .models import (
    AssemblyOut,
    FragmentIn,
    FragmentOut,
    ManifestIn,
    ManifestOut,
    PublicationOut,
    ReconciliationOut,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Seismic Waveform Assembly API",
    version="1.1.0",
    description=(
        "Ingest out-of-order waveform fragments against upstream handover "
        "manifests, reconcile them, assemble whole UTC-minute records and "
        "publish immutable minutes for phase pickers."
    ),
    lifespan=lifespan,
)


@app.exception_handler(service.ServiceError)
async def _service_error_handler(_: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": str(exc)},
    )


class UploadOut(FragmentOut):
    status: str


class ManifestSubmitOut(ManifestOut):
    status: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Handover manifests
# --------------------------------------------------------------------------- #
@app.post("/manifests", response_model=ManifestSubmitOut, status_code=201)
def submit_manifest(manifest: ManifestIn) -> dict[str, Any]:
    return service.submit_manifest(manifest.model_dump())


@app.get("/manifests", response_model=ManifestOut)
def get_manifest(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> JSONResponse | dict[str, Any]:
    result = service.get_manifest(station, minute_utc)
    if result is None:
        return JSONResponse(status_code=404, content={"detail": "manifest not found"})
    return result


@app.get("/manifests/revisions", response_model=list[ManifestOut])
def list_manifest_revisions(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> list[dict[str, Any]]:
    return service.list_manifest_revisions(station, minute_utc)


@app.get("/reconciliation", response_model=ReconciliationOut)
def get_reconciliation(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> dict[str, Any]:
    return service.get_reconciliation(station, minute_utc)


# --------------------------------------------------------------------------- #
# Fragments
# --------------------------------------------------------------------------- #
@app.post("/fragments", response_model=UploadOut, status_code=201)
def upload_fragment(frag: FragmentIn) -> dict[str, Any]:
    return service.upload_fragment(frag.model_dump())


@app.get("/fragments", response_model=list[FragmentOut])
def list_fragments(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> list[dict[str, Any]]:
    return service.list_fragments(station, minute_utc)


@app.get("/assembly", response_model=AssemblyOut)
def get_assembly(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> dict[str, Any]:
    return service.get_assembly(station, minute_utc)


# --------------------------------------------------------------------------- #
# Publications
# --------------------------------------------------------------------------- #
@app.post("/publications", response_model=PublicationOut, status_code=201)
def publish_minute(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(description="UTC minute, e.g. 2026-11-17T05:06:00Z"),
) -> dict[str, Any]:
    return service.publish_minute(station, minute_utc)


@app.get("/publications", response_model=list[PublicationOut])
def list_publications(
    station: str | None = Query(default=None),
    minute_utc: datetime | None = Query(default=None),
) -> list[dict[str, Any]]:
    return service.list_publications(station, minute_utc)


@app.get("/publications/latest", response_model=PublicationOut)
def get_latest_publication(
    station: str = Query(min_length=1),
    minute_utc: datetime = Query(...),
) -> JSONResponse | dict[str, Any]:
    result = service.get_latest_publication(station, minute_utc)
    if result is None:
        return JSONResponse(status_code=404, content={"detail": "publication not found"})
    return result


@app.get("/publications/{pub_id}", response_model=PublicationOut)
def get_publication(pub_id: int) -> JSONResponse | dict[str, Any]:
    result = service.get_publication(pub_id)
    if result is None:
        return JSONResponse(status_code=404, content={"detail": "publication not found"})
    return result
