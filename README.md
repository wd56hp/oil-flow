# oil-flows

Backend-first data platform for historical global oil trade flows: normalize connector output, ingest into PostgreSQL, and verify results **as tables** (SQL or HTTP) before any visualization.

---

## Stack

- **Python 3.12+**, **FastAPI**, **Uvicorn**
- **PostgreSQL** + **SQLAlchemy 2** + **Alembic**
- **Docker Compose** for local Postgres (optional)
- **pytest** (always use the project venv: `.venv/bin/pytest` or `make test`)

---

## Quick start

### 1. Clone and virtualenv

```bash
cd oil-flows
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Do **not** rely on system `pytest` / system SQLAlchemy (they are often too old). Use `.venv/bin/...` or `make test`.

### 2. Environment

Copy the example env and set at least `DATABASE_URL` (and connector keys when you use them):

```bash
cp .env.example .env
# Edit .env — e.g. DATABASE_URL=postgresql://oiluser:oilpass@localhost:5432/oilflows
```

### 3. Database

**Option A — Docker Compose (Postgres only)**

```bash
docker compose up -d db
```

**Option B — Your own Postgres** — create a database and user, then set `DATABASE_URL` accordingly.

### 4. Migrations

From the project root (with `.env` loaded or `DATABASE_URL` exported):

```bash
.venv/bin/alembic upgrade head
```

This applies all revisions under `alembic/versions/`.

### 5. Run the API

```bash
.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- Health: `GET http://localhost:8000/health`
- OpenAPI docs: `http://localhost:8000/docs`

**Docker (API + DB)** — `docker compose up --build` starts the `api` service and Postgres; the API image runs Uvicorn on port **8000**.

### 6. Tests

```bash
make test
# or
.venv/bin/pytest tests/ -v
```

---

## Ingestion philosophy (read this before changing behavior)

### Business key

A trade flow row is keyed by:

`source`, `dataset`, `period_date`, `reporter_country`, `partner_country`, `commodity`, `flow_direction`

All ingestion and deduplication logic uses this key.

### What “unchanged” means

Change detection uses **measures only**: `quantity` and `quantity_unit`.  
Connector-specific **lineage** fields (e.g. EIA `eia_origin_id`, `eia_destination_id`, `eia_grade_id`) are **not** used to decide insert vs skip vs revision.

### Revision model: canonical row + history (not “append-only facts”)

This codebase uses:

- **`trade_flows`** — **one current row per business key** (the canonical snapshot you query for “what we believe now”).
- **`trade_flow_revisions`** — **history of measure changes**: when measures change, we store the **previous** measure snapshot and **update** the canonical `trade_flows` row to the new measures.

So it is **not** “only append rows and never update” for the canonical table. The **revision** table is append-only audit of *changes*; the **canonical** table is **updated** when measures change.

If you need a pure append-only fact table with `observed_at` and “latest wins” in a view, that would be a different design; the implemented choice is **canonical + revision history** for simpler table-first reads.

### Lineage when measures are unchanged

If upstream sends the **same** quantity/unit but **different** lineage (e.g. port of entry), the engine **skips** the row (counts as **unchanged**) and **does not** refresh lineage on the canonical row. So the canonical row **may retain older lineage** until a measure change triggers an update. That is **intentional** to keep revision noise tied to measure changes; document upstream quirks in ops runbooks if needed.

### Runs

Each batch records an **`ingestion_runs`** row with counts (`inserted`, `revised`, `unchanged`, `failed`, etc.). On success, `failed_count` is currently **0**; full-batch failures roll back the transaction (no persisted run unless you add different error handling later).

---

## Verification API (phase 5 — read-only)

JSON and CSV endpoints for table-first checks (same idea as querying in `psql`). All synchronous.

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/trade-flows` | Paginated canonical rows |
| `GET` | `/trade-flows/export.csv` | CSV export (same filters as `/trade-flows`; uses `limit` / `offset`, max 10k rows per request) |
| `GET` | `/revisions` | Measure-change audit rows (`?trade_flow_id=` optional) |
| `GET` | `/ingestion-runs` | Ingestion batches |

**Pagination:** `limit` (default 100 for JSON lists), `offset`.

**Filters for `/trade-flows` and `/trade-flows/export.csv`:** `source`, `dataset`, `period_from`, `period_to` (inclusive date range on `period_date`), `reporter_country`, `partner_country`, `commodity`, `flow_direction`. Omit a parameter to leave it unfiltered.

Examples:

```bash
curl -s "http://localhost:8000/trade-flows?source=eia&limit=10" | jq .
curl -s "http://localhost:8000/trade-flows/export.csv?source=eia&period_from=2024-01-01&period_to=2024-12-31" -o trade_flows.csv
```

OpenAPI: `http://localhost:8000/docs`

---

## Project layout (high level)

```
app/
  api/           # HTTP routes (health, verification API)
  connectors/    # Source-specific fetch + normalize (e.g. EIA)
  core/          # Settings, DB session
  models/        # SQLAlchemy ORM
  schemas/       # Pydantic: TradeFlowRecord, inspection DTOs, …
  services/      # Ingestion engine
  jobs/          # CLI / cron entrypoints (future)
alembic/         # Migrations
tests/           # pytest
```

---

## Makefile

| Target | Command |
|--------|---------|
| `make install` | Create `.venv` and `pip install -r requirements.txt` |
| `make test` | Run pytest via `.venv/bin/pytest` |

---

## License / data

EIA and other sources have their own terms of use; respect API keys and rate limits. This README does not replace vendor documentation.
