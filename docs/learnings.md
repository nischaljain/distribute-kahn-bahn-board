# Learnings & Design Decisions

A running log of the non-obvious choices we made building this board, why we made
them, and what we deliberately gave up. Written as we go, so future-us (and anyone
reading on GitHub) can see the *reasoning*, not just the code.

---

## How does a server tell browsers "the board changed"? (Phase 3)

When a server consumes a `board_changed` event from Kafka, it has to update the tabs
connected to it. There are three ways to do that, and they trade **speed** against
**complexity**.

### The options

| Pattern | DB reads per update | Complexity | The client is… |
|---|---|---|---|
| **1. Signal + re-fetch** *(chosen)* | N — one per connected tab | lowest | a mirror of the DB |
| **2. Push full state** | 1 — server reads once, broadcasts snapshot | medium | a renderer of pushed snapshots |
| **3. Delta in the event** | 0 (steady state) | highest | a state machine replaying a log |

- **Signal + re-fetch** — server emits a bare `"board_changed"` (no data); each browser
  calls `loadBoard()` → `GET /api/boards/1` → re-render. This is *cache invalidation*:
  don't send the new value, just say "your copy is stale."
- **Push full state** — server reads the DB once and pushes the whole board down the
  WebSocket. Known as *event-carried state transfer*. Saves a round-trip and collapses
  N reads into 1.
- **Delta in the event** — put the actual change in the Kafka event
  (`{type: "task_moved", task_id: 5, to_column: 2, position: 3}`); server relays it to
  browsers, which patch their own state. Zero DB reads on the hot path. This is
  *event sourcing / event-carried state transfer at delta granularity* — the family that
  powers Google Docs, Figma, Linear (CRDTs / operational transforms).

### Why we chose #1 (signal + re-fetch) for now

- **Reuse / one render path.** The browser already has `loadBoard()` from Phase 1, fed by
  one canonical API response. The signal path adds *zero* new code and no second payload
  shape that could drift from the REST one.
- **Handles per-user views for free.** The moment different users see different data
  (their own boards, permissions, filters), "broadcast one blob to everyone" breaks —
  each client must fetch *its own* view. Re-fetch does this naturally.
- **Always converges to the source of truth.** Clients render what's *in the DB*, never a
  replayed log that could disagree with it. Matches our invariant: the DB is truth.
- **Simplicity-first.** With zero users and one shared board, building machinery to save an
  unmeasurable DB read would be over-engineering — and it would bury the thing Step 3 is
  actually here to teach: how a Kafka consumer works.

### What we gave up (and when we'd revisit)

- **The thundering herd.** One change makes *all* connected tabs stampede the server with
  simultaneous `GET`s → N DB reads at once. At real load, switch to **#2 (push full state)**
  to collapse that into a single read.
- **Steady-state DB reads entirely.** Only **#3 (delta)** removes them — but it costs a lot:
  mutation logic implemented *twice* (server DB write + client patch) that must agree
  forever; **sequence numbers + gap detection + resync** so a tab that missed events
  doesn't corrupt its state; and a full fetch *anyway* for initial page load and every
  resync. It's the right endpoint for a high-scale collaborative editor, not for here yet.

**Decision:** start with the signal; treat push-state and delta as documented future
upgrades we now understand the cost of.

### Known unsolved problem: the thundering herd

Our chosen pattern (#1) has a failure mode we are knowingly *not* solving yet, called the
**thundering herd**: one change causes *every* connected browser to fire a `GET` at the
same instant, so the server takes **N simultaneous requests → N database reads** for a
single logical change. With a handful of tabs it's invisible; with thousands it's a
self-inflicted traffic spike — each edit stampedes the DB, and a burst of edits can pile
spikes on top of each other until the database is the bottleneck.

Why we're leaving it for now: with one shared board and ~no concurrent users, the cost is
unmeasurable, and the fix would add real machinery for no observable gain (over-engineering).
Being explicit about it matters more than fixing it today — you should *know* the sharp edge
is there.

When it becomes real, the fixes (roughly in order of reach-for):

- **Push full state (#2 above).** Server reads the DB *once* per change and broadcasts the
  snapshot. Collapses N reads into 1 — directly removes the herd. This is the natural next
  step.
- **Debounce / coalesce.** If many changes land in a short window, emit one signal for the
  batch instead of one per change, so clients re-fetch once. Smooths bursts.
- **Cache the read.** Serve `GET /api/boards/1` from an in-memory/Redis cache that the write
  path invalidates, so the N reads hit cache, not the DB.
- **Delta in the event (#3 above).** The endgame — no steady-state reads at all — at the
  cost of the duplicate-logic / resync machinery documented above.

**Decision:** accept the thundering herd at current scale; reach for push-full-state first
if/when load makes it measurable.

---

## Consumer groups: why each server node needs its own (Phase 3)

The single most important line of Kafka config in this project, and the one that fails
*silently* if you get it wrong.

### The rule

> Kafka delivers each message to **every consumer group**, but to only **one consumer
> *within* a group.**

The group (`group_id`, just a string) is Kafka's unit of **load-balancing** — the mechanism
that lets you scale consumption by splitting work across a team of consumers.

### Why load-balancing is the wrong default *here*

Our goal is **fan-out**, not work-splitting. Every Flask process must receive *every* event,
because each one holds a different set of browser WebSockets and must push to its own tabs.

- **Shared group** (`group_id="kanban"` on both nodes) → Kafka sees one team and splits the
  stream: event 0 to node A, event 1 to node B. A change reaches only one node's tabs; the
  other node's tabs go stale. **Nothing errors** — it just quietly half-works.
- **Unique group per node** (`node-5001`, `node-5002`) → Kafka sees two independent teams and
  sends the full stream to both. Each node fans out to its own tabs. Correct.

We derive it from the port, which is already unique per process:

```python
PORT = int(os.environ.get("PORT", "5001"))
CONSUMER_GROUP = f"node-{PORT}"
```

Verified: with both nodes running, `kafka-consumer-groups.sh --list` shows `node-5001` and
`node-5002`, and both server logs consume the **same** offsets (0,1,2,3) — proof of fan-out
rather than splitting.

### Terminology that caused confusion

"Server" means two different things in this project. Worth stating plainly:

- **Kafka broker** — the Kafka program in Docker. We run exactly **one**. It stores the topic.
  With brokers/controllers it forms the **Kafka cluster**.
- **Flask app node** — our Python process (`app.py`). We run **two** (5001, 5002).

Producers and consumers are **clients** of the cluster, never part of it. So `group_id` is a
label worn by our *Flask processes*, not by the broker. One broker, two group ids.

### `auto_offset_reset="latest"`, not `"earliest"`

Answers "where do I start when this group has no saved bookmark?" — which is every fresh
start, since each node's group is new. `"earliest"` would replay the whole topic on boot and
re-broadcast changes already applied, making every tab re-fetch repeatedly for ancient
events. `"latest"` starts at the end: only events published while we're running. Live sync
cares about *now*.

### Gotcha: the Flask reloader forks a second process

The consumer loop (`for message in consumer`) **blocks forever**, so it runs via
`socketio.start_background_task(...)`. Calling it directly would hang startup and the web
server would never begin serving.

But with `debug=True`, Flask's **auto-reloader** runs the app as **two** processes: a parent
that watches files and a child that actually serves. Our script starts the consumer at module
level, so *both* would start one — and the parent, which holds **zero WebSocket
connections**, could win events from the group and emit into the void. Real-time would look
randomly broken with no error anywhere.

Fix: `use_reloader=False`. Costs auto-restart on file edits; buys one process and correct
behavior. (The alternative — guarding on `WERKZEUG_RUN_MAIN` — is more machinery than a
teaching project needs.)

---

## SQLite → PostgreSQL: why we switched (Phase 4)

Phase 3 made the *event* layer distributed, but the storage layer still quietly assumed one
machine. This closes that gap.

### The real reason isn't performance — it's embedded vs client-server

- **SQLite** is an **embedded** database: a *library* running inside your process, reading and
  writing a **file on local disk**. There is no database program. "Connecting" is opening a file.
- **PostgreSQL** is a **client-server** database: a **separate program** you reach **over the
  network** (port 5432) — architecturally the same shape as Kafka.

Our two nodes worked on SQLite only because they happened to run on the **same laptop** and
open the same file path. The moment node A and node B live on different machines — the entire
point of this project — they cannot share a SQLite file at all. Not slowly; not at all. (Putting
SQLite on a network filesystem is a well-known route to corruption; its locking isn't reliable
over NFS.) So the switch buys **possibility**, not speed.

Secondary win, concurrency: SQLite takes a write lock on the **whole database file**, so writes
serialize globally and you eventually see `database is locked`. Postgres uses **row-level
locking** and **MVCC** (readers see a consistent snapshot instead of blocking), so writes to
different rows proceed in parallel.

### The ORM earned its keep

Swapping engines took **one line** — `SQLALCHEMY_DATABASE_URI` from `sqlite:///kanban.db` to
`postgresql+psycopg2://kanban:kanban@localhost:5432/kanban`. Not a single query or model
changed, because we wrote SQLAlchemy models instead of raw SQL. The URL shape itself shows the
shift: the SQLite URL has no host, port, or credentials — because there was no server.

`psycopg2-binary` is the **driver** (database adapter) that speaks Postgres' wire protocol —
the same role `kafka-python` plays for Kafka. The `-binary` build ships precompiled, so there's
no C toolchain needed.

### Things Postgres surfaced that SQLite hid

- **`column` is a reserved SQL keyword.** Our `Column` model maps to a table literally named
  `column`, which is a syntax error unquoted. SQLAlchemy quotes identifiers automatically
  (`REFERENCES "column"(id)`), so it just worked — but raw SQL would have broken.
- **Auto-increment is a `SEQUENCE`** (`nextval('task_id_seq')`), a different mechanism from
  SQLite's implicit ROWID. Same model definition, different DDL per engine.
- **Types are enforced.** `character varying(200)` really rejects a longer title; SQLite's
  flexible typing would have stored it happily.

### Operational notes

- Postgres gets a **named volume** (`postgres-data`), so `docker compose down` no longer
  destroys data — unlike the Kafka topic, which has none and must be recreated each session.
  Use `docker compose down -v` to wipe deliberately.
- Postgres starts **empty**, so `seed.py` creates the schema and the starter board. Existing
  SQLite data did **not** migrate; for a real system that would need a migration step.
- `DATABASE_URL` is env-overridable (like `PORT`) so a node on another host can point at
  whichever machine runs the database.

### Still to do for a true multi-machine setup

Postgres alone isn't sufficient. Two things still bind us to one host:
- **Kafka advertises `localhost:9092`**, so a remote node would be told to connect to *itself*.
- **Flask binds `127.0.0.1`**, reachable only from its own machine.

Both need real LAN addresses before the two-laptop demo works.

---

## The N+1 query problem: a bug that only appeared once deployed

After deploying, the UI took seconds to update after every card move. It looked like slow
hosting. It wasn't — it was our code, and the bug had been there since Phase 1.

### Measuring first

The deployed numbers pointed away from the host immediately:

| Request | Time |
|---|---|
| static page (no database) | 0.098s |
| `GET /api/boards/1` | 1.9s |
| `POST` a card | 1.9s |

TCP connect was 0.014s, and a page with no database access returned in under 100ms. So the
server and network were fine; ~1.8s was being spent talking to the database. Instrumenting
SQLAlchemy showed why — **one board fetch issued six separate queries**:

```
1. 524 ms  SELECT board...      <- the board
2. 262 ms  SELECT "column"...   <- its columns
3. 262 ms  SELECT task...       <- tasks for column 1
4. 262 ms  SELECT task...       <- tasks for column 2
5. 262 ms  SELECT task...       <- tasks for column 3
6. 327 ms  SELECT task...       <- tasks for column 4
```

### What N+1 is

Fetch **1** parent, then loop over its children, and the ORM silently issues **N** more
queries — one per child. Ours came from this line:

```python
board_data["columns"] = [
    {**column.to_dict(), "tasks": [task.to_dict() for task in column.tasks]}
    for column in board.columns
]
```

`column.tasks` is a **lazy relationship**: SQLAlchemy doesn't load it until touched, and we
touch it once per column inside a loop. Comfortable Python, six network round trips.

**Why it stayed hidden for three phases:** against a local database each query costs ~0.5ms,
so all six totalled ~3ms — invisible. Move the database to another datacenter and each costs
~262ms. The bug never changed; the latency just made it legible. This is the most common way
N+1 gets discovered — in production, not in development.

### Choosing the fix with numbers, not instinct

Eager loading tells SQLAlchemy to fetch the tree up front. Two strategies, measured against
the real (remote) database:

| Strategy | Queries | Time |
|---|---|---|
| lazy (before) | 6 | 6.79s |
| `selectinload` | 3 | 1.23s |
| **`joinedload`** (chosen) | **1** | **0.58s** |

`selectinload` issues a second `SELECT ... WHERE id IN (...)` per level; `joinedload` pulls
everything in one `LEFT JOIN`. Conventional advice favours `selectinload` for collections
because `joinedload` multiplies rows (a cartesian product across levels), but here **round
trips dominate everything** — 1 trip beat 3 by 2x. With a board this small, the row
duplication costs nothing.

The lesson isn't "always use joinedload." It's that the right answer depends on whether your
bottleneck is **latency** or **data volume**, and you find out by measuring, not by recalling
a rule.

Verified after the change: columns and tasks still come back in correct `position` order with
no duplicate rows leaking through the de-duplication.

### What this doesn't fix

Moving a card is still **two** HTTP round trips — the mutation, then the re-fetch that the
signal pattern requires — and there's no optimistic UI, so nothing moves on screen until both
finish. Remaining improvements, in order of value:

- **Optimistic UI** — move the card in the DOM immediately, reconcile when the response
  arrives. Removes *perceived* latency entirely; the largest UX win available. **Done — see
  below.**
- **Co-locate the app and database** in the same region. Pure configuration.
- **Push full state** (documented above) — removes the second round trip.

---

## Optimistic UI: stop making the user wait for the network

Even after the N+1 fix, a card move cost roughly `PATCH (~1.8s) + GET (~0.75s)`. The card
didn't visibly move until both finished, so the board felt broken regardless of how fast the
backend got.

### The change

Previously every action was *server-first*: send the request, await it, re-fetch, re-render.
The UI was a pure mirror of the server, which is simple but means **every interaction costs a
round trip before anything happens on screen**.

Now the client keeps the board in a local variable and applies the edit **immediately**:

```js
async function optimistically(applyLocally, persist) {
  applyLocally();            // mutate local state
  renderBoard(board);        // paint it right now
  try {
    const response = await persist();          // then tell the server
    if (!response.ok) throw new Error(response.status);
  } catch (error) {
    await loadBoard();       // we guessed wrong — fall back to the server's truth
  }
}
```

Three things make this safe rather than reckless:

1. **The server stays authoritative.** The optimistic edit is a *prediction*. The existing
   `board_changed` → `loadBoard()` path already re-fetches after every change, so the server's
   version overwrites the prediction a second later. Reconciliation was free — we'd already
   built it.
2. **Failures self-heal.** Any non-OK response or network error triggers `loadBoard()`, which
   discards the bad prediction and restores the truth.
3. **Placeholder ids.** A newly created card has no server id yet, so it gets `id: null` and is
   rendered non-draggable with its delete disabled — it can't be addressed by an API that
   doesn't know about it. The reconcile replaces it with the real row within ~a second.

`renumber()` recalculates contiguous `position` values locally, mirroring what the server does,
so the prediction matches the eventual truth instead of flickering into place.

### Measured in a real browser

Both paths update the DOM **synchronously**, before any network call resolves:

| Action | DOM updated in |
|---|---|
| Add a card | **0.8 ms** |
| Move a card between columns | **1.2 ms** |

Verified afterwards that the server state and DOM state matched exactly, that no placeholder
ids survived reconciliation, and that the console was clean.

Perceived latency went from ~2000ms to under 2ms — roughly a thousandfold — **without the
backend getting any faster**. Worth sitting with: the biggest performance win in this project
came from changing *when* we render, not from optimising a query.

### The trade-off

The client now duplicates a little server logic (position renumbering), which is exactly the
coupling we rejected when we chose signal-over-delta broadcasting. The difference is scope:
this duplication is a *disposable prediction* that gets overwritten a second later, not a
source of truth the client must maintain correctly forever. If the prediction is wrong, the
reconcile silently fixes it.
