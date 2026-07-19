# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this project is

A **real-time, horizontally-scalable Kanban task board** (Trello-style: columns like To Do / In
Progress / Code Review / Done, with cards dragged between them).

The point of the board is not the board — it's the **engineering challenge underneath it**: keeping
state synced instantly across (a) multiple open browser tabs and (b) multiple independent Flask
server processes that don't know about each other. Everything in the stack exists to solve that.

This repo is built as a **learning project**. The owner is learning every technology here from
scratch. See "How to work in this repo" below — it is as important as the architecture.

## Tech stack (and *what* each thing is)

| Thing | What it is | Job in this project |
|-------|-----------|---------------------|
| HTML5 | markup language | structure of the board (columns, cards) |
| Tailwind CSS | CSS framework | styling the UI |
| Vanilla JavaScript (ES6+) | language running in the browser | drag-and-drop, listens for live updates, redraws the DOM |
| Python 3.11+ | language running on the server | backend logic |
| Flask | Python web framework | serves pages, handles REST API requests |
| Flask-SocketIO | Python library | gives Flask WebSocket powers + easy broadcasting |
| WebSocket | a network protocol | persistent two-way (full-duplex) link between browser and server |
| SQLite → PostgreSQL | databases | permanent storage of boards/columns/tasks (start SQLite) |
| SQLAlchemy | Python ORM (object-relational mapper) | lets us work in Python objects instead of raw SQL |
| Apache Kafka | a standalone event-streaming platform (separate program) | the ordered "news wire" servers publish to / read from, to sync across nodes |
| Docker / Docker Compose | containerization tooling | runs Kafka (and friends) locally without manual install |

Producer / Consumer are **roles the Flask app plays**, not separate programs: the app *acts as* a
Kafka producer when it publishes a change, and *acts as* a consumer when it reads the stream.

## How the system fits together (end-to-end flow)

```
Tab 1 ──WebSocket──► Server A          (browser ↔ server link = WebSocket)
                        │
                  save to DB (SQLAlchemy → SQLite/Postgres)
                        │
                  Server A publishes "card moved" to a Kafka topic   (acts as PRODUCER)
                        │
            ┌────── Kafka topic (ordered, durable log) ──────┐       (server ↔ server link = Kafka)
            ▼                                                ▼
   Server A reads it back (CONSUMER)              Server B reads it (CONSUMER)
            │                                                │
   WebSocket-broadcast to A's tabs                WebSocket-broadcast to B's tabs
            ▼                                                ▼
        Tab 1                                            Tab 2  ← updates, no refresh
```

- **WebSocket** links a browser to a server.
- **Kafka** links servers to each other. They are different connections solving different problems.

## Architecture invariants (do not violate without discussion)

1. **Unique Kafka consumer group per server node.** We want every node to receive *every* event so
   it can fan out to its own tabs. A shared consumer group would load-balance messages (only one
   node gets each) and silently break real-time. Each process gets its own group id.
2. **DB write happens before Kafka publish.** Persist (and commit) the change, *then* publish the
   event. The database is the source of truth; never broadcast a change that failed to save.
3. **Single broadcast path.** A server only ever broadcasts to its tabs in response to *reading a
   Kafka message* — including messages it published itself. No "broadcast my own change directly"
   shortcut. This keeps every node running identical code and makes all nodes apply events in the
   same Kafka-defined order (consistency).
4. **Keep `User` in the schema, but auth is out of scope for now.** Stub a single hardcoded user
   through Phases 1–2 unless we explicitly decide to add real auth.

## Build phases

- **Phase 1 — Monolith foundations:** Flask server, SQLAlchemy schema (`User → Board → Column →
  Task`), static UI layout, REST CRUD endpoints. No real-time yet.
- **Phase 2 — Real-time on one node:** add Flask-SocketIO + browser WebSocket listeners; card
  drops/sorts sync live across tabs on a single server.
- **Phase 3 — Distributed scale:** run Kafka via Docker; Flask publishes events on state change; a
  background consumer thread broadcasts events so multiple server processes stay in sync.

## How to work in this repo (IMPORTANT — this is a teaching project)

- The owner is learning every technology from scratch. **Always name *what* a thing is** (protocol,
  library, framework, ORM, platform…) before explaining what it does.
- **Go deep commit by commit, code by code.** Small, focused changes. Explain the concept behind a
  change *as* we make it, not in a big upfront dump. Prefer one new idea at a time.
- Favor clarity over cleverness in the code itself — it should read like a teaching example.
- Check understanding before moving to the next concept; don't rush ahead phases.

## Behavioral guidelines

- **Simplicity first:** Always choose the simplest solution that works. Don't reach for advanced
  design patterns or over-engineer unless a concrete need makes it necessary.
- **Surgical changes only:** Touch only the lines and files needed for the task at hand. No
  speculative refactors or unrelated edits.
- **Think before coding:** Understand the current architecture and existing abstractions before
  writing new code.

## Commands

The dev server runs on **port 5001**, not 5000: on macOS, port 5000 is taken by the AirPlay
Receiver (ControlCenter), which causes confusing "Address already in use" / phantom-server bugs.

```bash
# One-time setup
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# Run the server (http://127.0.0.1:5001)
./venv/bin/python app.py

# Stop it — kill by PORT, not by name. `pkill -f "python app.py"` misses the
# macOS framework Python (its process is "Python app.py" with a capital P).
lsof -tiTCP:5001 -sTCP:LISTEN | xargs kill -9

# Quick API smoke test
curl -s -X POST http://127.0.0.1:5001/api/boards \
  -H "Content-Type: application/json" -d '{"name":"Test Board"}'
```

```bash
# Kafka (Phase 3) — needs Docker Desktop running
docker compose up -d      # start the single Kafka broker (KRaft mode, port 9092)
docker compose ps         # check it's Up
docker logs kafka         # broker logs; look for "Kafka Server started"
docker compose down       # stop and remove it

# Create the event topic (no named volume, so `down` deletes it — recreate after each up).
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic board-events --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

```bash
# Run two server nodes (Kafka must be up and the topic created first).
# PORT sets both the listen port and the Kafka consumer group (node-<PORT>),
# so each process gets its own group and receives every event.
./venv/bin/python app.py              # node A -> http://127.0.0.1:5001
PORT=5002 ./venv/bin/python app.py    # node B -> http://127.0.0.1:5002

# Stop both.
lsof -tiTCP:5001 -sTCP:LISTEN | xargs kill -9
lsof -tiTCP:5002 -sTCP:LISTEN | xargs kill -9

# Prove cross-node sync: open a tab on each port, drag a card in one, watch
# the other update. Server logs should show BOTH nodes consuming the same offset.
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --list --bootstrap-server localhost:9092   # expect node-5001 and node-5002
```
