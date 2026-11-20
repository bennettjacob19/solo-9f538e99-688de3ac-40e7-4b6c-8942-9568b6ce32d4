# OTLP Tail Sampler

A FastAPI + SQLite tail-sampling collector for OTLP/HTTP JSON traces. It keeps a
trace only when an ERROR span arrives during that trace's two-second collection
window, then archives the whole trace and exposes it on a small web UI.

## The rule that matters

A successful root span can be exported long before the failing child span shows
up. This service therefore cannot decide on the first span:

1. The first span of a trace opens a **2 second window** and takes one of 16
   trace slots.
2. Spans are grouped by `traceId`, de-duplicated by `spanId`, and capped at 64
   distinct spans per trace.
3. When the window closes the trace is decided:
   - **no ERROR span → dropped**, the slot is released immediately;
   - **at least one ERROR span → the entire trace is written to SQLite** in one
     transaction, and the slot is released only after the commit succeeds.
4. Spans for a trace whose window has closed are rejected, even if the trace is
   still waiting to be written.
5. At 16 occupied slots only *new* trace ids are refused; traces already
   admitted keep collecting until their own window closes.

Slots count both open traces and error traces that have expired but not yet
been committed, so the number of traces occupying the budget never exceeds the
configured capacity.

### Partial success

A rejected span never poisons the rest of the batch. Valid, in-window spans are
stored normally and the OTLP response reports the rejection count:

```json
{
  "partialSuccess": {
    "rejectedSpans": 2,
    "acceptedSpans": 5,
    "errorMessage": "rejected 2 span(s): capacity=1, window_closed=1"
  }
}
```

Rejection reasons: `capacity`, `window_closed`, `span_limit`, `invalid_span`.
A fully accepted export returns `{}` per the OTLP spec.

### Pause / resume

Archiving can be paused from the page or via `POST /api/archive/pause`. Traces
that expire while paused keep exactly the spans they held at expiry (later spans
are still refused), stay in memory, and stay invisible on the page. When
archiving resumes — or when a failed SQLite write later succeeds — every pending
trace is written automatically. Write failures use exponential backoff (max
30s) and never lose the expired content.

## Volume sealing ("封卷")

At shift handover the on-call engineer can take away one complete file of the
error traces collected so far, while the collector keeps receiving spans. Every
archive is a single SQLite **volume**; the overview page shows the current
write volume and offers *Seal current volume*.

The split rule, relative to the moment a seal request is **accepted**:

1. Traces whose sampling window has already closed (the pending archive queue)
   keep being written into the **old volume**, even if their write is still
   retrying.
2. Traces whose window is still open, and traces admitted afterwards, are
   routed to the **new volume** if they end up archived.
3. `POST /v1/traces` keeps accepting spans throughout with the unchanged
   2-second window and 16-slot rules; the overview keeps showing already
   committed traces normally.
4. A second seal request while one is running is refused (`409` on the API,
   silently ignored on the HTML form) — the page simply shows *sealing…*.
5. The write path switches **only** after all of these hold:
   - every old-volume pending trace has been committed;
   - the old WAL is folded into the database and the file is converted to
     rollback journal mode, so the sealed volume is one self-contained `.db`
     with no `-wal`/`-shm` side files;
   - an independent read-only connection re-reads **every** trace and passes
     `PRAGMA integrity_check`;
   - the replacement volume accepts a committed write probe.
6. If any gate fails the seal is aborted, the old volume stays the write
   volume, already received traces follow the normal write/retry rules, and
   the overview shows the failure reason. A fresh seal can be requested later.
7. A `volumes.json` manifest next to the databases records the current write
   volume and every sealed file. After a restart the service reopens the last
   successfully switched write volume, resumes (or aborts) an interrupted
   seal, and keeps every sealed volume downloadable.

Sealed volumes are listed on the overview with trace/span counts, size and a
SHA-256, and downloaded through `GET /volumes/{file}` (path traversal is
rejected). Trace detail pages and the archived table label each trace with its
source volume.

## Run with Docker Compose

```bash
docker compose up --build
# open http://localhost:8000
```

Data is persisted in the `sampler-data` volume. Environment overrides can be
set in a `.env` file next to `docker-compose.yml`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8000` | host port |
| `SAMPLE_WINDOW_SECONDS` | `2` | collection window per trace |
| `MAX_ACTIVE_TRACES` | `16` | traces occupying slots |
| `MAX_SPANS_PER_TRACE` | `64` | distinct spans kept per trace |
| `DB_PATH` | `/data/traces.db` | first/current SQLite volume (container) |
| `LOG_LEVEL` | `INFO` | log level |

Sealed volumes and the `volumes.json` manifest are written next to `DB_PATH`,
so mounting the `/data` volume preserves them across container restarts.

## Run locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
pytest -q
```

## Sending traces

```bash
curl -s http://localhost:8000/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceSpans": [{
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]},
      "scopeSpans": [{"scope": {"name": "demo"}, "spans": [{
        "traceId": "0123456789abcdef0123456789abcdef",
        "spanId": "0123456789abcdef",
        "name": "POST /checkout",
        "kind": 2,
        "startTimeUnixNano": "1700000000000000000",
        "endTimeUnixNano": "1700000000500000000",
        "status": {"code": 2, "message": "payment declined"}
      }]}]
    }]
  }'
```

Only OTLP/HTTP JSON is decoded; protobuf exports receive HTTP 415.

## HTTP endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/traces` | OTLP/HTTP JSON export |
| `GET` | `/` | overview: status, volumes, archived traces, pause/resume |
| `GET` | `/traces/{traceId}` | every archived span of one trace |
| `POST` | `/seal` | seal the current write volume (HTML form) |
| `GET` | `/volumes/{file}` | download a sealed SQLite volume |
| `GET` | `/api/status` | sampler state and counters |
| `GET` | `/api/volumes` | current write volume and sealed volume list |
| `POST` | `/api/volumes/seal` | seal the current write volume (JSON) |
| `GET` | `/api/traces` | archived traces across all volumes (`limit`, `offset`) |
| `GET` | `/api/traces/{traceId}` | archived trace as JSON |
| `POST` | `/api/archive/pause` · `/api/archive/resume` | pause / resume archiving |
| `GET` | `/healthz` | liveness |

## Layout

```
app/otlp.py      OTLP/HTTP JSON parsing and response shaping
app/sampler.py   window/slot state machine, dedupe, cap, pause, retry, seal routing
app/storage.py   transactional SQLite archive and read-only verification helpers
app/volumes.py   volume manager, seal state machine, manifest, cross-volume reads
app/main.py      FastAPI app factory, routes, templates
tests/           sampler, HTTP and volume-sealing tests
```
