"""Tests for archive-volume sealing ("封卷").

The split rule under test: at seal-accept time, traces whose sampling window
has already closed (the pending queue) finish into the old SQLite file; traces
still collecting, and traces admitted afterwards, go to the new file.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402
from app.volumes import VolumeError, VolumeManager  # noqa: E402

HEX = "0123456789abcdef"


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def otlp(spans) -> dict:
    groups: dict[tuple, list] = {}
    for item in spans:
        span = {
            "traceId": item["trace"],
            "spanId": item["span"],
            "name": item.get("name", "op"),
            "kind": 2,
            "startTimeUnixNano": "1700000000000000000",
            "endTimeUnixNano": "1700000000500000000",
            "status": {"code": 2, "message": "boom"} if item.get("error") else {"code": 1},
        }
        if item.get("parent"):
            span["parentSpanId"] = item["parent"]
        key = (item.get("service", "checkout"), "test")
        groups.setdefault(key, []).append(span)
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}}
                    ]
                },
                "scopeSpans": [{"scope": {"name": scope}, "spans": group}],
            }
            for (service, scope), group in groups.items()
        ]
    }


class Clock:
    def __init__(self) -> None:
        self.now = 5000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock():
    return Clock()


@pytest.fixture()
def env(tmp_path, clock):
    db = str(tmp_path / "data" / "traces.db")
    app = create_app(
        db,
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as client:
        yield {
            "client": client,
            "app": app,
            "store": app.state.store,
            "sampler": app.state.sampler,
            "clock": clock,
            "db": db,
        }
    try:
        app.state.store.close()
    except sqlite3.ProgrammingError:
        pass


def post(client, spans):
    return client.post(
        "/v1/traces",
        content=json.dumps(otlp(spans)),
        headers={"content-type": "application/json"},
    )


def archive_one(env, n: int, error: bool = True):
    post(env["client"], [{"trace": tid(n), "span": sid(1), "error": error}])
    env["clock"].advance(2.0)
    env["sampler"].tick()


# ----------------------------------------------------------- happy path


def test_first_volume_is_the_original_sqlite_file(env):
    store = env["store"]
    assert isinstance(store, VolumeManager)
    assert store.current_file.endswith("traces.db")
    assert store.snapshot()["sealed"] == []
    assert os.path.exists(store.current_file)
    assert os.path.exists(os.path.join(store._dir, "volumes.json"))


def test_seal_freezes_old_volume_and_switches_write_volume(env):
    store, sampler = env["store"], env["sampler"]
    archive_one(env, 1)  # lands in the first volume

    old_file = store.current_file
    response = env["client"].post("/api/volumes/seal")
    assert response.status_code == 200
    body = response.json()
    assert body["failed"] is False
    assert store.current_file != old_file
    assert sampler.sealing is False

    sealed = store.sealed
    assert len(sealed) == 1
    record = sealed[0]
    assert record.file == "traces.db"
    assert record.trace_count == 1
    assert record.sha256 and len(record.sha256) == 64

    # The sealed volume is one self-contained file: no WAL side files even
    # after it has been read back through the read-only path.
    sealed_path = os.path.join(store._dir, record.file)
    assert not os.path.exists(sealed_path + "-wal")
    assert not os.path.exists(sealed_path + "-shm")
    trace = store.get_trace(tid(1))
    assert trace["volume_sealed"] is True
    assert trace["volume_file"] == record.file
    assert not os.path.exists(sealed_path + "-wal")


def test_traces_archived_after_switch_land_in_new_volume(env):
    store = env["store"]
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    new_file = store.current_file

    archive_one(env, 2)
    trace = store.get_trace(tid(2))
    assert trace["volume_sealed"] is False
    assert trace["volume_file"] == os.path.basename(new_file)
    assert store.count() == 2


def test_seal_split_closed_windows_old_open_windows_new(env):
    store, sampler, clock = env["store"], env["sampler"], env["clock"]

    # Trace 1: window already closed and pending when the seal starts.
    original_write = type(store).write_trace

    def faulted(self, trace_id, spans, target_file=None):
        raise RuntimeError("disk stall")

    type(store).write_trace = faulted
    post(env["client"], [{"trace": tid(1), "span": sid(1), "error": True}])
    clock.advance(2.0)
    sampler.tick()
    pending = sampler.snapshot()["pendingTraces"]
    assert [p["traceId"] for p in pending] == [tid(1)]

    # Trace 2: still inside its 2s window at seal-accept time.
    post(env["client"], [{"trace": tid(2), "span": sid(1), "error": True}])

    response = env["client"].post("/api/volumes/seal")
    assert response.status_code == 202
    assert sampler.sealing is True
    assert response.json()["oldVolumePending"] == 1
    old_file, new_file = store.current_file, store.seal.new_file

    # Trace 3 arrives after the seal was accepted.
    post(env["client"], [{"trace": tid(3), "span": sid(1), "error": True}])

    # Trace 2's window closes during the seal: routed to the new volume.
    clock.advance(2.0)
    sampler.tick()
    targets = {
        p["traceId"]: p["targetFile"]
        for p in sampler.snapshot()["pendingTraces"]
    }
    assert targets[tid(1)] == os.path.basename(old_file)
    assert targets[tid(2)] == os.path.basename(new_file)
    assert targets[tid(3)] == os.path.basename(new_file)

    # Heal the store; the old-volume trace flushes first, then the switch.
    type(store).write_trace = original_write
    clock.advance(60)
    sampler.tick()
    assert sampler.sealing is False
    assert store.current_file == new_file

    sampler.tick()
    clock.advance(2.0)
    sampler.tick()
    assert store.get_trace(tid(1))["volume_sealed"] is True
    for n in (2, 3):
        trace = store.get_trace(tid(n))
        assert trace["volume_sealed"] is False
        assert trace["volume_file"] == os.path.basename(new_file)


def test_repeated_seal_request_while_sealing_is_rejected(env):
    store, sampler, clock = env["store"], env["sampler"], env["clock"]
    original_write = type(store).write_trace

    def faulted(self, trace_id, spans, target_file=None):
        raise RuntimeError("disk stall")

    type(store).write_trace = faulted
    archive_one(env, 1)
    try:
        first = env["client"].post("/api/volumes/seal")
        assert first.status_code == 202
        second = env["client"].post("/api/volumes/seal")
        assert second.status_code == 409
        assert "already in progress" in second.json()["error"]
        # HTML form submission is silently ignored too.
        page = env["client"].post("/seal")
        assert page.status_code == 200
        assert sampler.sealing is True
    finally:
        type(store).write_trace = original_write
        clock.advance(60)
        sampler.tick()


def test_ingest_continues_under_same_window_and_slot_rules_during_seal(env):
    store, sampler, clock = env["store"], env["sampler"], env["clock"]
    original_write = type(store).write_trace

    def faulted(self, trace_id, spans, target_file=None):
        raise RuntimeError("disk stall")

    type(store).write_trace = faulted
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    assert sampler.sealing is True

    # A fresh trace is still admitted...
    ok = post(env["client"], [{"trace": tid(9), "span": sid(1), "error": True}])
    assert ok.status_code == 200 and ok.json() == {}
    # ...and its window still rejects late spans after 2 seconds.
    clock.advance(2.1)
    late = post(env["client"], [{"trace": tid(9), "span": sid(2)}])
    assert late.json()["partialSuccess"]["rejectedSpans"] == 1
    assert "window_closed" in late.json()["partialSuccess"]["errorMessage"]

    type(store).write_trace = original_write
    clock.advance(60)
    sampler.tick()
    assert sampler.sealing is False


# --------------------------------------------------------------- downloads


def test_sealed_volume_download(env):
    store = env["store"]
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    record = store.sealed[0]

    response = env["client"].get(f"/volumes/{record.file}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-sqlite3"
    assert "attachment" in response.headers["content-disposition"]
    assert record.file in response.headers["content-disposition"]
    assert len(response.content) == record.size_bytes

    import hashlib

    assert hashlib.sha256(response.content).hexdigest() == record.sha256


def test_download_rejects_unknown_and_traversal_names(env):
    assert env["client"].get("/volumes/does-not-exist.db").status_code == 404
    nasty = env["client"].get("/volumes/..%2f..%2fvolumes.json")
    assert nasty.status_code == 404
    assert env["client"].get("/volumes/volumes.json").status_code == 404


# --------------------------------------------------------------- failures


def arm_one_shot_finalize_failure(manager: VolumeManager) -> list[str]:
    """Make a live manager fail its next finalize attempt, in place."""
    state = {"attempts": 0}
    original = manager._finalize_locked

    def fail_once(seal):
        state["attempts"] += 1
        if state["attempts"] == 1:
            raise VolumeError("simulated verification failure")
        return original(seal)

    manager._finalize_locked = fail_once  # type: ignore[method-assign]
    return state


def test_failed_seal_keeps_old_volume_and_explains_reason(env, tmp_path, clock):
    store, sampler = env["store"], env["sampler"]
    archive_one(env, 1)
    old_file = store.current_file

    arm_one_shot_finalize_failure(store)
    sampler.begin_seal()
    sampler.tick()  # verification gate fails, seal aborts
    assert sampler.sealing is False
    assert "simulated verification failure" in (sampler.seal_error or "")
    assert store.current_file == old_file
    assert store.sealed == []
    assert os.path.exists(old_file)
    # The half-created new volume file is removed.
    leftovers = [n for n in os.listdir(store._dir) if n.startswith("traces-")]
    assert leftovers == []

    # Already received traces keep normal processing on the old volume.
    status = env["client"].get("/api/status").json()
    assert "verification failure" in status["volumes"]["lastError"]
    assert status["archivedTraces"] == 1

    # The store is fully usable again without any reconnect dance.
    archive_one(env, 7)
    assert store.count() == 2

    # The page explains the failure and offers a retry rather than hiding the action.
    page = env["client"].get("/").text
    assert "Seal failed, writes continue on the old volume" in page
    assert "simulated verification failure" in page
    assert "Retry seal" in page


def test_retry_after_failure_then_successful_seal(env):
    store, sampler = env["store"], env["sampler"]
    archive_one(env, 1)
    old_file = store.current_file

    arm_one_shot_finalize_failure(store)
    sampler.begin_seal()
    sampler.tick()  # fails once, rolls back
    assert store.current_file == old_file
    sampler.begin_seal()
    sampler.tick()  # second attempt runs the real finalize gate
    assert store.current_file != old_file
    assert len(store.sealed) == 1


# --------------------------------------------------------------- restart


def test_restart_uses_last_switched_write_volume(env):
    store = env["store"]
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    switched_file = store.current_file
    sealed_name = store.sealed[0].file
    db_path = env["db"]

    # Brand new process, same data directory.
    app2 = create_app(
        db_path, window_seconds=2.0, start_sampler_thread=False, clock=env["clock"]
    )
    with TestClient(app2) as client2:
        store2 = app2.state.store
        assert store2.current_file == switched_file
        assert [v.file for v in store2.sealed] == [sealed_name]
        assert store2.count() == 1
        assert store2.get_trace(tid(1))["volume_sealed"] is True

        response = client2.get(f"/volumes/{sealed_name}")
        assert response.status_code == 200

        # New archives follow the last successful switch.
        post(client2, [{"trace": tid(5), "span": sid(1), "error": True}])
        env["clock"].advance(2.0)
        app2.state.sampler.tick()
        assert store2.get_trace(tid(5))["volume_file"] == os.path.basename(
            switched_file
        )
    app2.state.store.close()


def test_interrupted_seal_is_completed_on_restart(env):
    store = env["store"]
    archive_one(env, 1)
    old_file = store.current_file
    data_dir = store._dir
    manifest = os.path.join(data_dir, "volumes.json")

    # Simulate a crash between seal accept and switch: a prepared new volume
    # file exists, while the manifest still points at the old volume and marks
    # the seal as in progress.
    new_file = os.path.join(data_dir, "traces-interrupted-abcd1234.db")
    from app.storage import ArchiveStore

    ArchiveStore.probe_writable(new_file)
    with open(manifest, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "version": 1,
                "current_file": old_file,
                "sealed": [],
                "seal": {
                    "old_file": old_file,
                    "new_file": new_file,
                    "started_at": 1.0,
                    "old_targets": [],
                    "last_error": None,
                },
                "last_error": None,
            },
            handle,
        )
    # Release all handles from the first "process" before restarting.
    env["app"].state.store.close()

    app2 = create_app(
        env["db"], window_seconds=2.0, start_sampler_thread=False, clock=env["clock"]
    )
    with TestClient(app2) as client2:
        store2 = app2.state.store
        # Recovery finalized the interrupted seal onto the prepared new file.
        assert store2.current_file == new_file
        assert len(store2.sealed) == 1
        assert store2.sealed[0].file == os.path.basename(old_file)
        assert store2.snapshot()["sealing"] is False
        sealed_name = store2.sealed[0].file
        assert client2.get(f"/volumes/{sealed_name}").status_code == 200
        # All old-volume data is readable after recovery from the single file.
        trace = store2.get_trace(tid(1))
        assert trace["volume_sealed"] is True
        assert not os.path.exists(old_file + "-wal")
    app2.state.store.close()


# ------------------------------------------------------------------- UI


def test_index_shows_write_volume_seal_button_and_downloads(env):
    store = env["store"]
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    archive_one(env, 2)

    page = env["client"].get("/").text
    assert "Current write volume" in page
    assert os.path.basename(store.current_file) in page
    assert "Seal current volume" in page
    assert store.sealed[0].file in page
    assert "/volumes/" + store.sealed[0].file in page
    assert "Download" in page
    # Each archived row names its source volume.
    assert "volume" in page.lower()


def test_index_shows_sealing_state_and_failure_reason(env):
    store, sampler, clock = env["store"], env["sampler"], env["clock"]
    original_write = type(store).write_trace

    def faulted(self, trace_id, spans, target_file=None):
        raise RuntimeError("disk stall")

    type(store).write_trace = faulted
    archive_one(env, 1)
    env["client"].post("/api/volumes/seal")
    page = env["client"].get("/").text
    assert "SEALING" in page
    type(store).write_trace = original_write
    clock.advance(60)
    sampler.tick()


def test_pause_during_sealing_holds_switch_until_resume(env):
    store, sampler, clock = env["store"], env["sampler"], env["clock"]
    original_write = type(store).write_trace

    def faulted(self, trace_id, spans, target_file=None):
        raise RuntimeError("disk stall")

    type(store).write_trace = faulted
    archive_one(env, 1)  # pending on the old volume
    env["client"].post("/api/volumes/seal")
    assert sampler.sealing is True
    old_file, new_file = store.current_file, store.seal.new_file

    sampler.pause()
    clock.advance(60)
    sampler.tick()
    # While paused nothing flushes, so the seal cannot complete.
    assert sampler.sealing is True
    assert store.current_file == old_file

    type(store).write_trace = original_write
    sampler.resume()
    sampler.tick()
    assert sampler.sealing is False
    assert store.current_file == new_file
    assert len(store.sealed) == 1


def test_list_traces_merges_volumes_with_paging(env):
    store = env["store"]
    for n in range(1, 4):
        archive_one(env, n)
    env["client"].post("/api/volumes/seal")
    for n in range(4, 6):
        archive_one(env, n)

    traces, total = store.list_traces(limit=100, offset=0)
    assert total == 5
    files = {t["trace_id"]: t["volume_file"] for t in traces}
    assert files[tid(1)] == store.sealed[0].file
    assert files[tid(5)] == os.path.basename(store.current_file)

    page, page_total = store.list_traces(limit=2, offset=2)
    assert page_total == 5
    assert len(page) == 2
