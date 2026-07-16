"""Application entry point for the Kanban board server."""

import json

from flask import Flask, abort, jsonify, render_template, request
from flask_socketio import SocketIO
from kafka import KafkaProducer

from models import Board, Column, Task, db

KAFKA_BROKER = "localhost:9092"
BOARD_EVENTS_TOPIC = "board-events"

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///kanban.db"

db.init_app(app)

# Adds a WebSocket layer for broadcasting to connected browsers; REST routes unchanged.
socketio = SocketIO(app)

# One producer for the app's lifetime — connections are expensive to open per request.
# value_serializer turns each Python event into the JSON bytes Kafka stores on the wire.
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda event: json.dumps(event).encode("utf-8"),
)


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
    """Producer role: append a change event to the topic. Called after a DB commit."""
    producer.send(BOARD_EVENTS_TOPIC, {"type": "board_changed"})


def broadcast_board_changed():
    """Push a change signal to every connected browser so each re-fetches.
    Only the Kafka consumer calls this — keeps a single broadcast path."""
    socketio.emit("board_changed")


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
    board = db.session.get(Board, board_id) or abort(404)

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
    # socketio.run replaces app.run to handle the WebSocket upgrade handshake.
    # allow_unsafe_werkzeug: opt in to the dev server for WebSockets (local only).
    socketio.run(app, debug=True, port=5001, allow_unsafe_werkzeug=True)
