"""Application entry point for the Kanban board server."""

from flask import Flask

from models import db

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///kanban.db"

db.init_app(app)


@app.route("/")
def home():
    return "Kanban board server is running."


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5000)
