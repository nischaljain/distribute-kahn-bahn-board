"""Database models: the Board -> Column -> Task hierarchy."""

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Board(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)

    # Deleting a board deletes its columns (and, in turn, their tasks).
    columns = db.relationship(
        "Column",
        back_populates="board",
        cascade="all, delete-orphan",
        order_by="Column.position",
    )

    def to_dict(self):
        return {"id": self.id, "name": self.name}

    def __repr__(self):
        return f"<Board {self.id} {self.name!r}>"


class Column(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), nullable=False)
    position = db.Column(db.Integer, nullable=False, default=0)  # left-to-right order
    board_id = db.Column(db.Integer, db.ForeignKey("board.id"), nullable=False)

    board = db.relationship("Board", back_populates="columns")
    tasks = db.relationship(
        "Task",
        back_populates="column",
        cascade="all, delete-orphan",
        order_by="Task.position",
    )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "position": self.position,
            "board_id": self.board_id,
        }

    def __repr__(self):
        return f"<Column {self.id} {self.name!r}>"


class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text, default="")
    position = db.Column(db.Integer, nullable=False, default=0)  # order within its column
    column_id = db.Column(db.Integer, db.ForeignKey("column.id"), nullable=False)

    column = db.relationship("Column", back_populates="tasks")

    def __repr__(self):
        return f"<Task {self.id} {self.title!r}>"
