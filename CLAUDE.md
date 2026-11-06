# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

The only code lives in `rtdp-demo/`, a runnable demo of a real-time data pipeline (RTDP) for bank transactions,
built for a university project. The `.pptx`/`.docx` files at the root are the presentation and its speaker script.
All docs, code comments, CLI output and test names are in **Vietnamese**; keep new text consistent with that.
`rtdp-demo/README.md` is the full operator guide (ports, credentials, demo script, test cases TC01–TC07, troubleshooting).

## Commands

Run everything from `rtdp-demo/`.

```bash
pip install -r requirements-dev.txt
python -m pyflakes generator scripts tests          # lint (the only static check; CI runs it)
python -m pytest -m "not e2e"                        # unit tests, no DB needed
python -m pytest tests/test_generator.py::test_money_within_bounds_and_rounded_to_thousand   # single test
RTDP_E2E=1 python -m pytest -m e2e                   # e2e: needs the stack up, seeded, and a CDC source running

docker compose up -d                                 # Oracle, Kafka (+kafka-init), ClickHouse, Grafana
docker compose -f docker-compose.yml -f docker-compose.goldengate.yml up -d --build   # + real GoldenGate (needs installer zips outside repo)
scripts/setup-goldengate.sh                          # create/repair Extract EXTCORE + Replicat RKAFKA (idempotent)
python scripts/gg_simulator.py                       # fake GoldenGate (polls Oracle -> Kafka) when GG isn't available
scripts/check.sh                                     # per-stage health: Oracle rows, Kafka offsets, ClickHouse consumers/rows

set -a; . ./.env; set +a                             # Python scripts don't read .env themselves
python generator/generator.py seed|run|once|anomaly|update|delete|resync|stats
python generator/reconcile.py                        # Oracle vs ClickHouse row/amount reconciliation
```

CI (`.github/workflows/rtdp-ci.yml`) runs pyflakes + unit tests, then an e2e job that starts Oracle/Kafka/ClickHouse
(no Grafana, no GoldenGate), seeds, runs `gg_simulator.py` in the background, and runs `pytest -m e2e`.

## Architecture

```
generator.py -> Oracle 23ai Free (schema CORE) -> GoldenGate Extract (trail ct) -> GoldenGate DAA Replicat (Kafka Handler)
             -> Kafka topics CORE.BRANCH/CUSTOMER/ACCOUNT/TXN -> ClickHouse (Kafka engine + MV) -> Grafana dashboard + alerts
```

Key cross-file contracts — changing one side requires changing the other:

- **Message format.** ClickHouse expects GoldenGate Kafka Handler JSON, `format=json`, `mode=op`:
  `{"table","op_type","op_ts","current_ts","pos","before","after"}` with upper-case Oracle column names.
  GoldenGate 26ai only emits `table/op_type/pos` because of `format.metaColumnsTemplate` in `goldengate/daa/kafka.props`.
  `scripts/gg_simulator.py` must produce the identical shape (it cannot capture DELETEs; use `--delete-txn <id>`).
- **ClickHouse ingestion** (`clickhouse/init/01_schema.sql`): per-topic `q_*` Kafka-engine tables (single partition,
  `kafka_flush_interval_ms=1000`) → materialized views parse JSON with helper UDFs `gg_col`/`gg_val`/`gg_ts`/`gg_version`
  (insert/update from `after`, delete from `before`) → `ReplacingMergeTree(version, is_deleted)` tables keyed by PK.
  Everything downstream (dashboard, alerts, reconcile, tests) must read the `v_*` views (`FINAL`, deleted rows dropped),
  not the raw tables, otherwise duplicates/updates/deletes skew numbers. `rtdp.cdc_events` holds raw messages (3-day TTL).
  End-to-end latency = `ingested_at − CREATED_AT`; Oracle runs with `TZ=UTC` and all timestamps are UTC.
- **Alert thresholds** live in ClickHouse table `rtdp.alert_config` (not in files). They are read by
  `grafana/provisioning/alerting/rules.yml`, by `generator.py anomaly` (so the anomaly burst always exceeds them, with
  `DEFAULT_THRESHOLDS` as fallback), and changed via `scripts/set-threshold.sh`.
- **Domain rule:** `WITHDRAW` transactions only occur on `ATM` or `COUNTER` channels (`random_txn`, covered by a unit test).
- **Oracle init** (`oracle/setup/*.sh`) runs only on first container start: enables ARCHIVELOG/supplemental logging,
  creates `ggadmin` (PDB) / `c##ggadmin` (CDB) and the `CORE` tables. Changes require `docker compose down -v`.
- **GoldenGate in Docker:** `ogg-oracle` (Extract) and `ogg-daa` (Replicat) share the trail via volume `ogg-trail`;
  `goldengate/` is mounted read-only and `setup-goldengate.sh` copies/templating-substitutes the `<...>` placeholders in
  the `.prm`/`.props` files into each deployment. In the Extract setup, `ADD EXTRACT` must precede `REGISTER EXTRACT`.
  After `down -v`, GoldenGate checkpoints are stale — rerun `setup-goldengate.sh` (it also drops the leftover Oracle
  capture registration). Don't run `gg_simulator.py` and real GoldenGate at the same time (duplicate topic sources).
- `generator.py seed` should run after Extract is capturing; otherwise use `resync` to replay base data through CDC.

## Python layout

`generator/common.py` holds shared connections (python-oracledb thin mode; ClickHouse over plain HTTP via `ch_query`,
bypassing system proxies), configured only through env vars (`ORACLE_DSN`, `CORE_PASSWORD`, `CLICKHOUSE_URL`, …).
Scripts import each other via `sys.path` (no package); `tests/conftest.py` adds `generator/` and `scripts/` to the path,
so tests import modules as `import generator as g`. Unit tests monkeypatch `ch_query` rather than hitting ClickHouse.
