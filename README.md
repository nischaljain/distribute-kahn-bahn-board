# Real-Time Distributed Kanban Board

A Trello-style task board where changes appear instantly — across every open browser tab, **and
across independent server processes that share no memory**.

> **Live demo:** _(link to be added after deployment)_
>
> The hosted version runs as a **single node**, so Kafka is inactive there. To see the
> distributed behaviour — the actual point of this project — run it locally with two nodes
> using the [two-node demo](#the-two-node-demo) below.

---

## The point of this project

The board itself is ordinary. The interesting part is the problem underneath it:

**How do you keep state in sync when the thing holding your users' connections isn't a single
process?**

That splits into two genuinely different problems, solved by two different technologies:

| Problem | Why it's hard | Solution |
|---|---|---|
| A change in one tab must appear in **other tabs** | HTTP is request/response — a server cannot push to a browser that didn't ask | **WebSocket** (via Socket.IO) |
| A change on **server A** must appear on tabs held by **server B** | Two Flask processes share no memory, no threads, no sockets. Server B has no idea server A exists | **Apache Kafka** |

A common misconception this project clears up: the database can't solve the second problem.
It holds the truth, but it's **passive** — it can't tell anyone the truth just changed. Kafka is
the channel that carries that notification between servers.

---

## Tech stack — *what* each thing is

| Thing | What it **is** | Its job here |
|---|---|---|
| HTML5 | markup language | structure of the board (columns, cards) |
| Tailwind CSS | CSS framework | styling the UI |
| Vanilla JavaScript (ES6+) | language running in the browser | drag-and-drop, live updates, DOM rendering |
| Python 3.9+ | language running on the server | backend logic |
| Flask | Python web framework | serves pages, handles the REST API |
| Flask-SocketIO | Python library | adds WebSocket support + broadcasting to Flask |
| WebSocket | a network **protocol** | persistent two-way link between browser and server |
| PostgreSQL | a standalone **client-server database** (separate program) | permanent storage, shared by every node |
| SQLAlchemy | Python **ORM** (object-relational mapper) | work in Python objects instead of raw SQL |
| Apache Kafka | a standalone **event-streaming platform** (separate program) | the ordered, durable log servers publish to and read from |
| Docker / Docker Compose | containerization tooling | runs Kafka and Postgres locally without manual installs |

**Producer** and **consumer** are *roles the Flask app plays*, not separate programs. The app
*acts as* a producer when it publishes a change, and *acts as* a consumer when it reads the
stream back.

---

## How it fits together

```
   BROWSERS (tabs)
   ┌────┐ ┌────┐              ┌────┐ ┌────┐
   │Tab1│ │Tab2│              │Tab3│ │Tab4│
   └─┬──┘ └─┬──┘              └─┬──┘ └─┬──┘
     │ WebSocket                │ WebSocket        ← browser ↔ server
     ▼      ▼                   ▼      ▼
  ┌──────────────┐          ┌──────────────┐
  │  FLASK node  │          │  FLASK node  │       ← our Python processes
  │  port 5001   │          │  port 5002   │         (Kafka producer + consumer)
  │ group=       │          │ group=       │
  │  node-5001   │          │  node-5002   │
  └──┬────────┬──┘          └──┬────────┬──┘
     │        │  produce /     │        │
     │        └─── consume ────┼────────┘          ← server ↔ server
     │                 ┌───────▼────────┐
     │                 │  KAFKA broker  │
     │                 │  board-events  │  ordered, durable log
     │                 └────────────────┘
     │                          │
     └──────────┬───────────────┘
                ▼                                  ← server ↔ database
       ┌──────────────────┐
       │   PostgreSQL     │  source of truth
       └──────────────────┘
```

**Three links, three jobs:** WebSocket reaches browsers. Kafka reaches sibling servers.
SQLAlchemy reaches the truth.

### End-to-end: what happens when you drag a card

1. Tab 1 drags a card → `PATCH /api/tasks/<id>/move` to **node 5001**
2. Node 5001 writes the change to **Postgres** and commits — *state changes here*
3. Node 5001 **publishes** `{"type": "board_changed"}` to the Kafka topic — *just a nudge, no data*
4. Kafka appends it to the log at the next offset
5. **Both** nodes consume that event — each has its own consumer group
6. Each node **emits** a Socket.IO event over its own WebSockets
7. Every tab re-fetches `GET /api/boards/1` and re-renders

Note step 5: a node broadcasts **only** in response to reading from Kafka — including events it
published itself. There is no "broadcast my own change directly" shortcut.

---

## Architecture invariants

Four rules the code is built to enforce. Each exists because breaking it causes a *silent* bug.

1. **Unique Kafka consumer group per node.**
   Kafka delivers each message to every *group*, but only one consumer *within* a group. A
   shared group would **load-balance** events between nodes — node A gets event 1, node B gets
   event 2 — so half the tabs silently go stale. Each process uses `node-<PORT>` so every node
   receives every event (fan-out).

2. **DB write happens before the Kafka publish.**
   Persist and commit, *then* publish. The database is the source of truth; never announce a
   change that failed to save.

3. **Single broadcast path.**
   A server broadcasts to its tabs *only* when reading a Kafka message. This keeps every node
   running identical code and applying events in the same Kafka-defined order. Enforced
   structurally: request handlers call `publish_board_changed()`; only the consumer calls
   `broadcast_board_changed()`.

4. **`User` stays in the schema, but auth is out of scope.**
   A deliberate boundary to keep the focus on the distributed-systems problem.

---

## Running it locally

### Prerequisites

- Python 3.9+
- Docker Desktop (for Kafka and Postgres)

### Setup

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# Start Kafka (port 9092) and Postgres (port 5432)
docker compose up -d

# Create the Kafka topic. The broker has no named volume, so `docker compose down`
# deletes it — recreate it after each `up`.
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic board-events --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Create the database schema and a starter board (safe to re-run)
./venv/bin/python seed.py
```

### Single node

```bash
./venv/bin/python app.py     # http://127.0.0.1:5001
```

### The two-node demo

This is the part worth seeing. Two independent Flask processes, kept in sync only by Kafka.

```bash
./venv/bin/python app.py              # node A -> http://127.0.0.1:5001
PORT=5002 ./venv/bin/python app.py    # node B -> http://127.0.0.1:5002
```

`PORT` sets both the listen port **and** the Kafka consumer group (`node-5001`, `node-5002`),
so each process forms its own group and receives every event.

Open a browser window on **each** port and drag a card in one — it moves in the other.

To prove it's really going through Kafka, watch both server logs. Both nodes will report
consuming the **same offset**:

```
Consumed offset 3: {'type': 'board_changed'}
```

And confirm both groups registered with the broker:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --list --bootstrap-server localhost:9092     # expect node-5001 and node-5002
```

### Useful commands

```bash
# Stop a node by PORT, not by name (`pkill -f "python app.py"` misses the macOS
# framework Python, whose process is "Python app.py" with a capital P).
lsof -tiTCP:5001 -sTCP:LISTEN | xargs kill -9

# Inspect the database (psql is Postgres' CLI client)
docker exec postgres psql -U kanban -d kanban -c '\dt'
docker exec postgres psql -U kanban -d kanban -c 'SELECT * FROM task;'

# Read the raw Kafka topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic board-events --from-beginning --bootstrap-server localhost:9092

docker compose down       # stop services (Postgres data survives)
docker compose down -v    # stop and wipe Postgres data too
```

### Configuration

| Env var | Default | Purpose |
|---|---|---|
| `PORT` | `5001` | listen port **and** consumer group id (`node-<PORT>`) |
| `HOST` | `127.0.0.1` | bind address; set `0.0.0.0` to accept external connections |
| `DATABASE_URL` | `postgresql+psycopg2://kanban:kanban@localhost:5432/kanban` | Postgres connection |
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |

If no broker is reachable at startup, the app logs `running in single-node mode` and falls back
to broadcasting directly. Tabs on that one node still sync; multi-node sync requires Kafka.

---

## Project structure

```
app.py               Flask app: REST API, WebSocket handlers, Kafka producer + consumer
models.py            SQLAlchemy models — Board -> Column -> Task
seed.py              Creates the schema and a starter board
docker-compose.yml   Kafka (KRaft mode) and PostgreSQL
static/board.js      Rendering, drag-and-drop, Socket.IO client
templates/index.html Page shell
docs/learnings.md    Design decisions and trade-offs, written as we went
```

---

## How it was built

| Phase | What it added |
|---|---|
| **1 — Monolith** | Flask, SQLAlchemy schema, REST CRUD, drag-and-drop UI. No real-time. |
| **2 — Real-time, one node** | Flask-SocketIO + browser WebSocket client. Tabs sync on a single server. |
| **3 — Distributed** | Kafka: publish on state change, background consumer thread, unique group per node. Tabs sync across *processes*. |
| **4 — Shared database** | SQLite → PostgreSQL, so nodes aren't bound to one machine's filesystem. |

[`docs/learnings.md`](docs/learnings.md) records the design decisions in detail — why the
broadcast is a signal rather than the data, the thundering-herd problem left unsolved, why
each node needs its own consumer group, and why SQLite had to go.

---

## Current limitations

Deliberate boundaries, not oversights.

### It will not run across two separate laptops as-is

The distributed design is real, but three things still assume everything is on one host:

1. **Kafka advertises `localhost:9092`.** `KAFKA_ADVERTISED_LISTENERS` is the address the broker
   tells clients to use. A node on another machine would connect once, be told "reach me at
   localhost," and then try to connect to *itself*. It needs the host's real LAN IP.
2. **Flask binds `127.0.0.1` by default.** Loopback only — unreachable from another machine.
   `HOST=0.0.0.0` fixes this, but it isn't the default.
3. **Socket.IO needs sticky sessions behind a load balancer.** The handshake begins as HTTP
   long-polling; if the first request lands on node A and the next on node B, it fails. A real
   multi-machine deployment needs session affinity at the proxy, or websocket-only transport.

Postgres was the prerequisite for going multi-machine — an embedded SQLite file cannot be shared
across hosts at all — but it isn't sufficient on its own.

### The hosted demo runs Kafka-free

The live deployment is a single node with no broker, so it exercises the Phase-2 architecture:
tabs sync, but through a direct broadcast rather than the Kafka loop. Managed Kafka is priced
well beyond a hobby project's budget. **The distributed behaviour is only observable by running
the two-node demo locally.**

### Other known gaps

- **Only task changes broadcast.** Create, delete, and move publish events; board and column
  changes don't yet.
- **No authentication.** A single hardcoded user, by design (invariant 4).
- **The thundering herd.** Every connected tab re-fetches on every change: N tabs → N database
  reads per change. Fine at this scale, documented with fixes in `docs/learnings.md`.
- **Development server.** Runs on Werkzeug, not a production WSGI server.
- **Dev credentials in `docker-compose.yml`.** Local-only, not reused anywhere.
