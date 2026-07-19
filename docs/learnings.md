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
