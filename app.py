"""Application entry point for the Kanban board server."""

from flask import Flask

app = Flask(__name__)


@app.route("/")
def home():
    return "Kanban board server is running."


if __name__ == "__main__":
    app.run(debug=True, port=5000)
