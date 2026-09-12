# Event Normalizer

## Contents

- [Overview](#overview)
- [Demo](#demo)
- [Project structure](#project-structure)
- [Processing flow](#processing-flow)
- [API documentation](#api-documentation)
- [Run locally](#run-locally)
- [MVP scope and limitations](#mvp-scope-and-limitations)
- [Security](#security)
- [Future improvements](#future-improvements)

## Overview

Event Normalizer provides a unified interface for ingesting and processing enterprise cost and usage events from different systems, including AI providers, cloud platforms, SaaS products, and internal services. Each source may represent usage differently. One provider may report `input_tokens`, another may use `prompt_tokens`, while a cloud platform may report compute time, storage, or network transfer.

The main goal is to prevent upstream schema changes from causing failures across the cost-processing pipeline. When a known mapping exists, the event is processed using the deterministic mapping engine to achieve high throughput. When the event structure changes and no valid mapping exists, the event is sent to a schema-drift topic, where AI can propose a new mapping. AI-generated mappings are validated by the application before they are saved and used.

## Demo

See the [Event Normalizer demo](example_input/demo/demo.md) for the predefined-mapping and AI-assisted schema-drift workflows.

## Project structure

```text
Event Normalizer/
├── app/
│   ├── main.py                         # FastAPI application and shared resources
│   ├── config.py                       # Environment and JSON configuration
│   ├── api/
│   │   └── routes.py                   # Event, mapping, health, and test endpoints
│   ├── schemas/
│   │   ├── requests.py                 # API and Kafka message models
│   │   ├── mappings.py                 # Predefined and runtime mapping models
│   │   └── operations.py               # Copy, cast, and multiply models
│   ├── messaging/
│   │   ├── kafka_client.py             # Kafka clients, topics, and serialization
│   │   ├── raw_event_consumer.py       # Normal event-processing worker
│   │   └── drift_consumer.py           # AI mapping-generation worker
│   ├── normalization/
│   │   ├── fingerprint.py              # Schema fingerprints and case IDs
│   │   ├── predefined_loader.py        # YAML mapping loader and conversion
│   │   ├── mapping_store.py            # Memory cache and JSON persistence
│   │   ├── mapping_engine.py           # Mapping pipeline execution
│   │   └── casting.py                  # Scalar cast operations
│   ├── ai/
│   │   ├── client.py                   # OpenRouter client and prompt building
│   │   └── security.py                 # AI response and injection checks
│   └── db/
│       ├── database.py                 # PostgreSQL engine and sessions
│       ├── models.py                   # Normalized-event database model
│       └── repository.py               # Normalized-event persistence
├── config/
│   ├── normalized_event_schema.json    # Normalized event definition
│   ├── user_config.json                # Kafka, AI, database, and source settings
│   └── mappings/
│       ├── predefined/
│       │   ├── ec2.yaml                # Initial EC2 mapping
│       │   └── openai.yaml             # Initial OpenAI mapping
│       └── generated/                  # Runtime mapping JSON files
├── example_input/
│   ├── expected_ec2_event.json
│   ├── expected_openai_event.json
│   ├── schema_drift_openai.json
│   └── demo/                           # Demo guide and expected results
├── tests/
│   ├── conftest.py
│   ├── test_ai.py
│   ├── test_api.py
│   ├── test_casting.py
│   ├── test_db_repository.py
│   ├── test_drift_consumer.py
│   ├── test_fingerprint.py
│   ├── test_kafka_client.py
│   ├── test_mapping_engine.py
│   ├── test_mapping_store.py
│   └── test_raw_event_consumer.py
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

## Processing flow

```text
Client or source system
→ POST /events
→ Calculate schema_fingerprint from sorted payload paths and JSON types
→ Calculate case_id from source + schema_fingerprint
→ Publish to Kafka raw-events
→ Find the mapping by case_id
  → Check the in-memory cache
  → Check the exact generated/{case_id}.json file
```

### Happy path

When a valid mapping exists:

```text
Apply mapping
→ Validate normalized event
→ Store normalized columns and the raw payload in PostgreSQL
```

### Schema-drift path

When no mapping exists:

```text
Publish to schema-drift using case_id as the message key
→ Schema-drift consumer checks for the mapping again
```

If another consumer has already created the mapping, the event is republished to `raw-events`. Otherwise:

```text
AI proposes a mapping
→ Application validates the proposal
→ Application applies the mapping to the event as a final check
→ Save the mapping as generated/{case_id}.json
→ Republish the event to raw-events
→ Normalize and store the event
```

If the AI cannot produce a valid proposal within the configured maximum attempts, the event is published to `failed-events`.

### Failure path

If a mapping exists but cannot be applied, the event is published to `failed-events`. The AI path is not used because schema drift is only triggered when no mapping can be found.

All schema-drift consumers use the same Kafka consumer group. This prevents multiple consumers from processing the same partition concurrently.

For the MVP, validated AI-generated mappings are activated automatically. Human approval can be added in a future version.

## API documentation

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/events` | Accepts a source usage event, assigns its event and case IDs, and publishes it for processing. |
| `POST` | `/mappings/resolve` | Checks whether a mapping exists for the supplied source and payload structure. |
| `GET` | `/health` | Reports PostgreSQL and Kafka connectivity. |
| `GET` | `/test/get-all-events` | Returns all normalized events currently stored in PostgreSQL. |

Interactive API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs) while the application is running.

## Run locally

You’ll need Python 3.11 or newer and Docker.

Create a virtual environment and install the project:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Create your local environment file:

```bash
cp .env.example .env
```

Add a PostgreSQL password and your OpenRouter API key to `.env`:

```dotenv
POSTGRES_PASSWORD=your-password
OPENROUTER_API_KEY=your-openrouter-api-key
```

Start PostgreSQL and Kafka:

```bash
docker compose up -d
docker compose ps
```

Wait until both services are healthy. Then start each Event Normalizer process in a separate terminal:

```bash
uvicorn app.main:app --reload
```

```bash
python -m app.messaging.raw_event_consumer
```

```bash
python -m app.messaging.drift_consumer
```

The API is available at [http://localhost:8000](http://localhost:8000), with interactive documentation at [http://localhost:8000/docs](http://localhost:8000/docs).

When you’re done, stop the Docker services:

```bash
docker compose down
```

## MVP scope and limitations

### Supported events

The MVP focuses on JSON usage events from two categories:

- AI usage, such as input tokens, output tokens, generated images, audio minutes, or model requests
- Cloud usage, such as compute time, storage, or network transfer

Support for SaaS platforms and internal services can be added through the same mapping system and source configuration.

### Schema drift

Valid payloads that do not match an existing mapping are treated as schema drift and sent for AI-assisted normalization.

Malformed JSON, missing required request values, unsupported sources, and invalid request data types are rejected by the API. Events that are incomplete, corrupt, or cannot produce a valid normalized event are published to `failed-events` rather than stored.

### Deferred features

The following features are intentionally excluded from the MVP:

- Switching between human and automatic mapping approval
- Reviewing and approving mappings through the API
- Rejecting an AI-generated mapping and submitting a manual replacement
- Requesting a new AI-generated mapping proposal
- Sending push notifications when a drift case requires human attention

## Security

Prompt-injection protection combines input keyword screening with LLM-based output review. Generated mappings are also validated against the allowed fields, payload paths, and mapping operations before use.

## Future improvements

- Support additional mapping operations, including joining and splitting fields.
- Store a small sample of schema-drift events in Redis to give AI mapping generation more context.
- Store generated mappings in a shared database instead of local JSON files, allowing consumers to run on different machines.
- Batch database writes to improve throughput.
- Add database migrations with Alembic.
- Expand edge-case, failure-path, and performance testing.
- Add API-key authentication and access control.
