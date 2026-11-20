"""Archive volume management and the seal ("封卷") state machine.

A *volume* is one SQLite archive file.  The collector always has exactly one
**current write volume**; sealing it produces a frozen, downloadable file and
moves all subsequent writes onto a freshly created volume.

Sealing rules implemented here together with :mod:`app.sampler`:

* When a seal request is accepted, every trace whose sampling window has
  already closed (the pending archive queue) keeps targeting the old volume.
* Traces whose window is still open, and traces admitted afterwards, target
  the new volume if they end up archived.
* ``/v1/traces`` keeps accepting spans throughout; the window and slot rules
  never change.
* The write path is switched only after the old volume has no pending traces
  left, its WAL has been folded into the single database file, every trace in
  that file has been re-read successfully, and the new volume has accepted a
  write probe.
* A failed seal leaves the old volume as the write volume; already received
  traces keep their normal processing.
* The manifest (``volumes.json``) makes the current write volume and the list
  of sealed files survive a process restart.
"""

from __future__ import annotations

import dataclasses
import hashlib
import heapq
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

from .models import SpanRecord
from .storage import ArchiveStore

log = logging.getLogger("tailsampler.volumes")

MANIFEST_NAME = "volumes.json"
MANIFEST_VERSION = 1
_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+\.db$")


class VolumeError(RuntimeError):
    """A sealing operation could not be completed."""


@dataclasses.dataclass
class VolumeRecord:
    """A sealed, read-only volume file."""

    file: str
    sealed_at: float
    trace_count: int
    span_count: int
    size_bytes: int
    sha256: str
    reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VolumeRecord":
        return cls(
            file=str(data["file"]),
            sealed_at=float(data["sealed_at"]),
            trace_count=int(data.get("trace_count", 0)),
            span_count=int(data.get("span_count", 0)),
            size_bytes=int(data.get("size_bytes", 0)),
            sha256=str(data.get("sha256", "")),
            reason=data.get("reason"),
        )


@dataclasses.dataclass
class SealState:
    """In-progress seal, persisted so a restart can resume or abort it."""

    old_file: str
    new_file: str
    started_at: float
    old_targets: list[str] = dataclasses.field(default_factory=list)
    last_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SealState":
        return cls(
            old_file=str(data["old_file"]),
            new_file=str(data["new_file"]),
            started_at=float(data["started_at"]),
            old_targets=list(data.get("old_targets", [])),
            last_error=data.get("last_error"),
        )


class VolumeManager:
    """Owns every archive file and routes reads/writes between volumes.

    Method-level locking makes the manager safe to call from the OTLP request
    thread, the sampler worker thread and page handlers alike.
    """

    def __init__(self, path: str) -> None:
        self._path = os.path.abspath(path)
        self._dir = os.path.dirname(self._path)
        os.makedirs(self._dir, exist_ok=True)
        self._manifest_path = os.path.join(self._dir, MANIFEST_NAME)
        self._lock = threading.RLock()

        self.current_file: str = self._path
        self._store = self._open_store(self._path)
        self.sealed: list[VolumeRecord] = []
        self.seal: Optional[SealState] = None
        self.last_error: Optional[str] = None
        # Read-only connections to sealed files, cached lazily.
        self._readonly: dict[str, sqlite3.Connection] = {}

        self._load_manifest()

    # ------------------------------------------------------------ lifecycle
    def _open_store(self, path: str) -> ArchiveStore:
        store = ArchiveStore(path)
        store.set_meta("path", os.path.abspath(path))
        return store

    def close(self) -> None:
        with self._lock:
            self._store.close()
            for conn in self._readonly.values():
                conn.close()
            self._readonly.clear()

    # ------------------------------------------------------------- manifest
    def _load_manifest(self) -> None:
        if not os.path.exists(self._manifest_path):
            self._persist_manifest()
            return
        try:
            with open(self._manifest_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise VolumeError(
                f"cannot read volume manifest {self._manifest_path}: {exc}"
            ) from exc

        current = os.path.abspath(str(data.get("current_file") or self._path))
        if not os.path.exists(current):
            raise VolumeError(
                f"current write volume is missing after restart: {current}"
            )
        self.current_file = current
        # Reopen the surviving write volume; the constructor store pointed at
        # the default path and is replaced before any request is served.
        if os.path.abspath(self._store.path) != current:
            self._store.close()
            self._store = self._open_store(current)

        self.sealed = [
            VolumeRecord.from_dict(item)
            for item in data.get("sealed", [])
            if os.path.exists(os.path.join(self._dir, item["file"]))
        ]
        seal_data = data.get("seal")
        if seal_data:
            self.seal = SealState.from_dict(seal_data)
            log.warning(
                "interrupted seal detected on startup: old=%s new=%s",
                self.seal.old_file,
                self.seal.new_file,
            )
        self.last_error = data.get("last_error")
        self._persist_manifest()

    def _persist_manifest(self) -> None:
        payload = {
            "version": MANIFEST_VERSION,
            "current_file": self.current_file,
            "sealed": [item.to_dict() for item in self.sealed],
            "seal": self.seal.to_dict() if self.seal else None,
            "last_error": self.last_error,
        }
        tmp = f"{self._manifest_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self._manifest_path)

    # --------------------------------------------------------------- writes
    @property
    def path(self) -> str:
        return self.current_file

    def write_trace(
        self,
        trace_id: str,
        spans: list[SpanRecord],
        target_file: Optional[str] = None,
    ) -> None:
        """Archive a trace, routing it to its assigned volume.

        Writes for a new volume are only possible after the switch; while a
        seal is in progress the sampler keeps those traces in its pending
        queue instead.
        """
        with self._lock:
            target = os.path.abspath(target_file) if target_file else self.current_file
            if target != os.path.abspath(self.current_file):
                raise VolumeError(
                    f"volume {os.path.basename(target)} is not open for writing"
                )
            self._store.write_trace(trace_id, spans)

    # ---------------------------------------------------------------- seal
    def begin_seal(self, old_targets: list[str]) -> SealState:
        """Accept a seal request and create the new (not yet active) volume."""
        with self._lock:
            if self.seal is not None:
                raise VolumeError("a seal is already in progress")
            old_file = self.current_file
            new_file = self._new_volume_path()
            try:
                ArchiveStore.probe_writable(new_file)
            except Exception as exc:
                self.last_error = f"new volume not writable: {type(exc).__name__}: {exc}"
                self._persist_manifest()
                raise VolumeError(self.last_error) from exc
            self.seal = SealState(
                old_file=old_file,
                new_file=new_file,
                started_at=time.time(),
                old_targets=list(old_targets),
            )
            self.last_error = None
            self._persist_manifest()
            log.info(
                "seal accepted: old=%s new=%s old_targets=%d",
                os.path.basename(old_file),
                os.path.basename(new_file),
                len(old_targets),
            )
            return self.seal

    def _new_volume_path(self) -> str:
        stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        suffix = uuid.uuid4().hex[:8]
        base = os.path.splitext(os.path.basename(self._path))[0]
        name = f"{base}-{stamp}-{suffix}.db"
        candidate = os.path.join(self._dir, name)
        # The uuid suffix makes a clash astronomically unlikely; guard anyway.
        while os.path.exists(candidate):
            suffix = uuid.uuid4().hex[:8]
            candidate = os.path.join(self._dir, f"{base}-{stamp}-{suffix}.db")
        return candidate

    def finish_seal(self, old_volume_drained: bool) -> Optional[VolumeRecord]:
        """Switch volumes once the old one is fully settled.

        Returns the sealed :class:`VolumeRecord` on success, ``None`` when the
        seal is still waiting for old-volume pending traces.  Raises
        :class:`VolumeError` (and aborts the seal) on any verification error.
        """
        with self._lock:
            if self.seal is None:
                return None
            if not old_volume_drained:
                return None

            seal = self.seal
            old_file = seal.old_file
            new_file = seal.new_file
            try:
                record = self._finalize_locked(seal)
            except Exception as exc:
                self._abort_seal_locked(seal, exc)
                raise
            log.info(
                "seal complete: %s (%d traces) frozen, writing now %s",
                os.path.basename(old_file),
                record.trace_count,
                os.path.basename(new_file),
            )
            return record

    def _finalize_locked(self, seal: SealState) -> VolumeRecord:
        old_file = seal.old_file
        new_file = seal.new_file

        if os.path.abspath(old_file) != os.path.abspath(self._store.path):
            raise VolumeError("write volume changed unexpectedly during seal")

        # 2. Prove every trace is readable via an independent read-only
        #    connection while the write connection is still open.
        old_store = self._store
        try:
            old_store.checkpoint_wal()
        except Exception as exc:
            raise VolumeError(
                f"old volume checkpoint failed: {type(exc).__name__}: {exc}"
            ) from exc
        if not os.path.exists(old_file):
            raise VolumeError(f"old volume disappeared: {old_file}")
        readonly = ArchiveStore.open_readonly(old_file)
        try:
            integrity = readonly.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise VolumeError(f"integrity check failed: {integrity}")
            verified = ArchiveStore.verify_readable(readonly)
        finally:
            readonly.close()

        # 3. Probe the replacement volume before disturbing the write path.
        try:
            ArchiveStore.probe_writable(new_file)
        except Exception as exc:
            raise VolumeError(
                f"new volume write probe failed: {type(exc).__name__}: {exc}"
            ) from exc

        # 4. Drop every other handle on the old file, then close the write
        #    connection and convert it out of WAL journaling.  A WAL-mode file
        #    recreates -wal/-shm even for read-only opens, whereas a file in
        #    DELETE (rollback) journal mode stays one self-contained .db file
        #    forever afterwards.
        cached = self._readonly.pop(old_file, None)
        if cached is not None:
            cached.close()
        old_store.close()
        try:
            cleanup = sqlite3.connect(old_file)
            try:
                cleanup.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                mode = cleanup.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
                if str(mode).lower() != "delete":
                    raise VolumeError(
                        f"could not convert sealed volume to rollback journal: {mode}"
                    )
            finally:
                cleanup.close()
            for side in (f"{old_file}-wal", f"{old_file}-shm"):
                if os.path.exists(side):
                    raise VolumeError(f"old volume side file still present: {side}")

            # 5. Everything proven; switch the write connection atomically.
            self._store = self._open_store(new_file)
            self.current_file = new_file
        except Exception:
            # Verification failed: reopen the old volume and keep writing it.
            # The write connection may already have been closed in step 4;
            # only replace it when that happened, otherwise it is still live.
            cached = self._readonly.pop(old_file, None)
            if cached is not None:
                try:
                    cached.close()
                except sqlite3.ProgrammingError:
                    pass
            try:
                self._store.count()
            except sqlite3.ProgrammingError:
                self._store = self._open_store(old_file)
            self.current_file = old_file
            raise

        digest, size = self._hash_file(old_file)
        record = VolumeRecord(
            file=os.path.basename(old_file),
            sealed_at=time.time(),
            trace_count=verified["traces"],
            span_count=verified["spans"],
            size_bytes=size,
            sha256=digest,
        )
        self.sealed.append(record)
        self.seal = None
        self.last_error = None
        self._persist_manifest()
        return record

    def _abort_seal_locked(self, seal: SealState, exc: Exception) -> None:
        reason = f"{type(exc).__name__}: {exc}"
        seal.last_error = reason
        self.last_error = reason
        # The old file remains the write volume.  Remove the unused new file
        # and its side files; nothing was ever routed to it by the sampler.
        for suffix in ("", "-wal", "-shm"):
            side = seal.new_file + suffix
            try:
                if os.path.exists(side):
                    os.remove(side)
            except OSError:
                log.warning("could not remove abandoned volume file %s", side)
        self.seal = None
        self._persist_manifest()
        log.error("seal failed, continuing on old volume: %s", reason)

    def recover_interrupted_seal(self) -> Optional[VolumeRecord]:
        """Resume a seal left over from a previous process.

        The in-memory pending queue is empty after a restart, so any seal that
        was in progress can immediately run its verification gate.  On failure
        the seal is aborted and the old volume stays active.
        """
        with self._lock:
            if self.seal is None:
                return None
            seal = self.seal
            if os.path.abspath(seal.old_file) != os.path.abspath(self.current_file):
                # The switch already happened before the crash; just tidy up.
                self.seal = None
                self._persist_manifest()
                return None
            try:
                return self._finalize_locked(seal)
            except Exception as exc:
                self._abort_seal_locked(seal, exc)
                return None

    @staticmethod
    def _hash_file(path: str) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
        return digest.hexdigest(), size

    # ---------------------------------------------------------------- reads
    def count(self) -> int:
        with self._lock:
            total = ArchiveStore.count_in(self._store.connection)
            for record in self.sealed:
                total += ArchiveStore.count_in(self._readonly_conn(record.file))
            return total

    def get_trace(self, trace_id: str) -> Optional[dict[str, Any]]:
        trace_id = trace_id.strip().lower()
        with self._lock:
            trace = ArchiveStore.read_trace(self._store.connection, trace_id)
            if trace is not None:
                trace["volume_file"] = os.path.basename(self.current_file)
                trace["volume_sealed"] = False
                return trace
            for record in self.sealed:
                conn = self._readonly_conn(record.file)
                trace = ArchiveStore.read_trace(conn, trace_id)
                if trace is not None:
                    trace["volume_file"] = record.file
                    trace["volume_sealed"] = True
                    return trace
        return None

    def list_traces(
        self, limit: int = 100, offset: int = 0
    ) -> tuple[list[dict], int]:
        """Merge every volume, newest archive first, with stable paging."""
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self._lock:
            sources: list[tuple[list[dict], str, bool]] = [
                (
                    ArchiveStore.all_trace_summaries(self._store.connection),
                    os.path.basename(self.current_file),
                    False,
                )
            ]
            for record in self.sealed:
                rows = ArchiveStore.all_trace_summaries(
                    self._readonly_conn(record.file)
                )
                sources.append((rows, record.file, True))

            # Annotate each row with its source volume before merging; each
            # source is already ordered by archived_at_ns DESC.
            labelled: list[list[dict]] = []
            for rows, file_name, is_sealed in sources:
                for row in rows:
                    row["volume_file"] = file_name
                    row["volume_sealed"] = is_sealed
                labelled.append(rows)

            merged = list(
                heapq.merge(
                    *labelled, key=lambda row: row["archived_at_ns"], reverse=True
                )
            )
            total = len(merged)
            page = merged[offset : offset + limit]
            return page, total

    def _readonly_conn(self, file_name: str) -> sqlite3.Connection:
        if not _SAFE_NAME.match(file_name):
            raise VolumeError(f"refusing to open unsafe volume name: {file_name}")
        path = os.path.join(self._dir, file_name)
        conn = self._readonly.get(path)
        if conn is None:
            conn = ArchiveStore.open_readonly(path)
            self._readonly[path] = conn
        return conn

    def download_path(self, file_name: str) -> Optional[str]:
        """Resolve a sealed file name to an absolute path, or None."""
        with self._lock:
            if not _SAFE_NAME.match(file_name):
                return None
            if not any(record.file == file_name for record in self.sealed):
                return None
            path = os.path.join(self._dir, file_name)
            return path if os.path.exists(path) else None

    # ---------------------------------------------------------------- state
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "currentFile": os.path.basename(self.current_file),
                "dataDir": self._dir,
                "sealing": self.seal is not None,
                "seal": self.seal.to_dict() if self.seal else None,
                "lastError": self.last_error,
                "sealed": [record.to_dict() for record in self.sealed],
            }
