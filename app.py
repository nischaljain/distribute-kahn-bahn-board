"""Application entry point for the Kanban board server."""

from flask import Flask, abort, jsonify, request

from models import Board, Column, Task, db

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///kanban.db"

db.init_app(app)


@app.route("/")
def home():
    return "Kanban board server is running."


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

    return jsonify(board.to_dict())


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
        if "position" in data:
            task.position = data["position"]
        db.session.commit()
        return jsonify(task.to_dict())

    if request.method == "DELETE":
        db.session.delete(task)
        db.session.commit()
        return "", 204

    return jsonify(task.to_dict())


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5001)
