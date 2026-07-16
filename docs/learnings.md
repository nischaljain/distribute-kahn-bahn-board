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
