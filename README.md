# Vera Deterministic Bot

Deterministic, stateful bot implementation for the magicpin Vera challenge.

## What this repo now includes

- `bot.py`: FastAPI + Pydantic server exposing:
  - `GET /v1/healthz`
  - `GET /v1/metadata`
  - `POST /v1/context`
  - `POST /v1/tick`
  - `POST /v1/reply`
- `composer.py`: deterministic `compose(category, merchant, trigger, customer?)` logic.
- `generate_submission.py`: creates `submission.jsonl` from canonical `test_pairs.json`.

## Local run

Install dependencies:

```bash
pip install fastapi uvicorn pydantic
```

1. Start bot:

```bash
python bot.py
```

2. In another terminal, run judge simulator (after setting provider key in `judge_simulator.py`):

```bash
python judge_simulator.py
```

Default bot URL in simulator is already `http://localhost:8080`.

## Dataset expansion + submission file

Generate expanded dataset:

```bash
python dataset/generate_dataset.py --seed-dir dataset --out expanded
```

Generate JSONL for the 30 canonical pairs:

```bash
python generate_submission.py --expanded-dir expanded --out submission.jsonl
```

## Determinism + state behavior

- Context updates are idempotent by `(scope, context_id, version)`.
- Stale/same versions return `409 stale_version`.
- Trigger suppressions prevent repeat sends for the same `suppression_key`.
- `/reply` handles:
  - auto-reply loops (`send` -> `wait` -> `end`)
  - hostile/opt-out (`end` + merchant snooze)
  - intent transition (`let's do it` => execution mode)

## Optional metadata env vars

- `TEAM_NAME`
- `TEAM_MEMBERS` (comma-separated)
- `CONTACT_EMAIL`
- `BOT_VERSION`
- `SUBMITTED_AT`
- `HOST`
- `PORT`
