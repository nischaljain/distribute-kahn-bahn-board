"""Application entry point for the Kanban board server."""

from flask import Flask, abort, jsonify, request

from models import Board, db

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


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5000)
