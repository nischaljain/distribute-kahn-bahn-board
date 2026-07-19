"""Application entry point for the Kanban board server."""

import json
import os

from flask import Flask, abort, jsonify, render_template, request
from flask_socketio import SocketIO
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy.orm import joinedload

from models import Board, Column, Task, db

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
BOARD_EVENTS_TOPIC = "board-events"

# Postgres runs as a service (see docker-compose.yml), so nodes on other hosts can
# share it — override DATABASE_URL to point at whichever machine runs the database.
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg2://kanban:kanban@localhost:5432/kanban"
)

# One process per server node; the port makes each node's identity unique.
PORT = int(os.environ.get("PORT", "5001"))
# Unique consumer group per node so every node receives EVERY event (fan-out,
# not load-balancing). A shared group would split events and break real-time.
CONSUMER_GROUP = f"node-{PORT}"

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL

db.init_app(app)

# Adds a WebSocket layer for broadcasting to connected browsers; REST routes unchanged.
socketio = SocketIO(app)

def create_producer():
    """One producer for the app's lifetime — connections are expensive per request.
    value_serializer turns each event into the JSON bytes Kafka stores on the wire.

    Returns None when no broker is reachable, which puts the app in single-node
    mode (see publish_board_changed). Multi-node sync requires Kafka.
    """
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        )
    except NoBrokersAvailable:
        print(f"No Kafka broker at {KAFKA_BROKER}; running in single-node mode", flush=True)
        return None


producer = create_producer()


@app.route("/")
def home():
    return render_template("index.html")


@socketio.on("connect")
def on_connect():
    print("WebSocket client connected", flush=True)


@socketio.on("disconnect")
def on_disconnect():
    print("WebSocket client disconnected", flush=True)


def publish_board_changed():
    """Producer role: append a change event to the topic. Called after a DB commit.

    Without a broker this node has no siblings to stay in sync with, so it
    broadcasts directly. That shortcut is only safe while running alone.
    """
    if producer is None:
        broadcast_board_changed()
        return
    producer.send(BOARD_EVENTS_TOPIC, {"type": "board_changed"})


def broadcast_board_changed():
    """Push a change signal to every connected browser so each re-fetches.
    Only the Kafka consumer calls this — keeps a single broadcast path."""
    socketio.emit("board_changed")


def consume_board_events():
    """Consumer role: fan every event out to this node's browsers, including the
    ones this node published. Blocks forever, so it runs as a background task.

    auto_offset_reset="latest": a new group starts at the end of the log rather
    than replaying history, which would re-broadcast changes already applied.
    """
    consumer = KafkaConsumer(
        BOARD_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    print(f"Kafka consumer listening as {CONSUMER_GROUP}", flush=True)

    for message in consumer:
        print(f"Consumed offset {message.offset}: {message.value}", flush=True)
        broadcast_board_changed()


@app.route("/api/boards", methods=["GET", "POST"])
def boards():
    if request.method == "POST":
        data = request.get_json()
        if not data or not data.get("name"):
            abort(400, "name is required")
        board = Board(name=data["name"])
        db.session.add(board)
        db.session.commit()
        return jsonify(board.to_dict()), 201

    return jsonify([board.to_dict() for board in Board.query.all()])


@app.route("/api/boards/<int:board_id>", methods=["GET", "PATCH", "DELETE"])
def board(board_id):
    # Eager-load the tree: the nested loop below reads every column's tasks, which
    # lazy loading would satisfy with one query per column (N+1). joinedload pulls
    # the whole board in a single round trip, which dominates cost over a network.
    board = db.session.get(
        Board,
        board_id,
        options=[joinedload(Board.columns).joinedload(Column.tasks)],
    ) or abort(404)

    if request.method == "PATCH":
        data = request.get_json() or {}
        if "name" in data:
            board.name = data["name"]
        db.session.commit()
        return jsonify(board.to_dict())

    if request.method == "DELETE":
        db.session.delete(board)
        db.session.commit()
        return "", 204

    # A single-board GET returns the full nested tree so the UI can render
    # the whole board from one request.
    board_data = board.to_dict()
    board_data["columns"] = [
        {**column.to_dict(), "tasks": [task.to_dict() for task in column.tasks]}
        for column in board.columns
    ]
    return jsonify(board_data)


@app.route("/api/boards/<int:board_id>/columns", methods=["GET", "POST"])
def columns(board_id):
    board = db.session.get(Board, board_id) or abort(404)

    if request.method == "POST":
        data = request.get_json()
        if not data or not data.get("name"):
            abort(400, "name is required")
        column = Column(name=data["name"], board=board, position=len(board.columns))
        db.session.add(column)
        db.session.commit()
        return jsonify(column.to_dict()), 201

    return jsonify([column.to_dict() for column in board.columns])


@app.route("/api/columns/<int:column_id>", methods=["GET", "PATCH", "DELETE"])
def column(column_id):
    column = db.session.get(Column, column_id) or abort(404)

    if request.method == "PATCH":
        data = request.get_json() or {}
        if "name" in data:
            column.name = data["name"]
        if "position" in data:
            column.position = data["position"]
        db.session.commit()
        return jsonify(column.to_dict())

    if request.method == "DELETE":
        db.session.delete(column)
        db.session.commit()
        return "", 204

    return jsonify(column.to_dict())


@app.route("/api/columns/<int:column_id>/tasks", methods=["GET", "POST"])
def tasks(column_id):
    column = db.session.get(Column, column_id) or abort(404)

    if request.method == "POST":
        data = request.get_json()
        if not data or not data.get("title"):
            abort(400, "title is required")
        task = Task(
            title=data["title"],
            description=data.get("description", ""),
            column=column,
            position=len(column.tasks),
        )
        db.session.add(task)
        db.session.commit()
        publish_board_changed()
        return jsonify(task.to_dict()), 201

    return jsonify([task.to_dict() for task in column.tasks])


@app.route("/api/tasks/<int:task_id>", methods=["GET", "PATCH", "DELETE"])
def task(task_id):
    task = db.session.get(Task, task_id) or abort(404)

    if request.method == "PATCH":
        data = request.get_json() or {}
        if "title" in data:
            task.title = data["title"]
        if "description" in data:
            task.description = data["description"]
        db.session.commit()
        return jsonify(task.to_dict())

    if request.method == "DELETE":
        db.session.delete(task)
        db.session.commit()
        publish_board_changed()
        return "", 204

    return jsonify(task.to_dict())


@app.route("/api/tasks/<int:task_id>/move", methods=["PATCH"])
def move_task(task_id):
    task = db.session.get(Task, task_id) or abort(404)
    data = request.get_json() or {}
    if "column_id" not in data or "position" not in data:
        abort(400, "column_id and position are required")
    target_column = db.session.get(Column, data["column_id"]) or abort(404)
    target_position = data["position"]

    # Close the gap the task leaves behind in its current column.
    for sibling in task.column.tasks:
        if sibling.position > task.position:
            sibling.position -= 1

    # Open a gap in the target column at the requested position.
    task.column = target_column
    for sibling in target_column.tasks:
        if sibling is not task and sibling.position >= target_position:
            sibling.position += 1

    task.position = target_position
    db.session.commit()
    publish_board_changed()
    return jsonify(task.to_dict())


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    if producer is not None:
        # Runs alongside the server; calling it directly would block startup forever.
        socketio.start_background_task(consume_board_events)
    # socketio.run replaces app.run to handle the WebSocket upgrade handshake.
    # allow_unsafe_werkzeug: opt in to the dev server for WebSockets (local only).
    # use_reloader: the reloader forks a second process, which would start a rival
    # consumer that steals events while holding no WebSocket connections.
    socketio.run(
        app,
        # Off unless opted in: debug mode exposes the Werkzeug console, which is
        # remote code execution on a public host. Set DEBUG=1 for local work.
        debug=os.environ.get("DEBUG") == "1",
        # Bind all interfaces by default: hosting platforms route to 0.0.0.0 and
        # never see a loopback-only listener. Set HOST=127.0.0.1 to stay local.
        host=os.environ.get("HOST", "0.0.0.0"),
        port=PORT,
        allow_unsafe_werkzeug=True,
        use_reloader=False,
    )
