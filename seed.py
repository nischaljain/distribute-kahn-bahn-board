"""Create the schema and a starter board.

Postgres starts empty, so run this once after `docker compose up -d`:
    ./venv/bin/python seed.py
"""

from app import app
from models import Board, Column, db

COLUMN_NAMES = ["To Do", "In Progress", "Code Review", "Done"]

with app.app_context():
    db.create_all()

    if Board.query.first():
        print("Board already exists; nothing to seed.")
    else:
        board = Board(name="My Board")
        board.columns = [
            Column(name=name, position=index)
            for index, name in enumerate(COLUMN_NAMES)
        ]
        db.session.add(board)
        db.session.commit()
        print(f"Seeded board {board.id} with columns: {', '.join(COLUMN_NAMES)}")
