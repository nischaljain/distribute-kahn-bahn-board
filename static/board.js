const BOARD_ID = 1;

let draggedTaskId = null;

// Open a persistent WebSocket to this server (via the Socket.IO client).
const socket = io();

// The server broadcasts this when any tab changes the board; re-fetch to sync.
socket.on("board_changed", () => {
  loadBoard();
});

async function loadBoard() {
  const response = await fetch(`/api/boards/${BOARD_ID}`);
  const board = await response.json();
  renderBoard(board);
}

async function createTask(columnId, title) {
  await fetch(`/api/columns/${columnId}/tasks`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title }),
  });
}

async function deleteTask(taskId) {
  await fetch(`/api/tasks/${taskId}`, { method: "DELETE" });
}

async function moveTask(taskId, columnId, position) {
  await fetch(`/api/tasks/${taskId}/move`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ column_id: columnId, position }),
  });
}

function renderBoard(board) {
  const container = document.getElementById("board");
  container.innerHTML = "";
  for (const column of board.columns) {
    container.appendChild(renderColumn(column));
  }
}

function renderColumn(column) {
  const section = document.createElement("section");
  section.className = "w-72 shrink-0 bg-slate-200 rounded-lg p-3";

  const heading = document.createElement("h2");
  heading.className = "text-sm font-semibold text-slate-600 uppercase tracking-wide mb-3";
  heading.textContent = column.name;
  section.appendChild(heading);

  const list = document.createElement("div");
  list.className = "space-y-2";
  for (const task of column.tasks) {
    list.appendChild(renderCard(task));
  }
  section.appendChild(list);

  section.appendChild(renderAddForm(column));

  // A column is a drop zone: allow the drop, then move the card to its end.
  section.addEventListener("dragover", (event) => {
    event.preventDefault();
  });
  section.addEventListener("drop", async (event) => {
    event.preventDefault();
    if (draggedTaskId === null) return;
    const position = column.tasks.filter((task) => task.id !== draggedTaskId).length;
    await moveTask(draggedTaskId, column.id, position);
    draggedTaskId = null;
    await loadBoard();
  });

  return section;
}

function renderCard(task) {
  const card = document.createElement("article");
  card.className =
    "bg-white rounded-md shadow-sm p-3 text-sm text-slate-700 flex justify-between items-start gap-2 cursor-grab";
  card.draggable = true;
  card.addEventListener("dragstart", () => {
    draggedTaskId = task.id;
  });

  const title = document.createElement("span");
  title.textContent = task.title;
  card.appendChild(title);

  const remove = document.createElement("button");
  remove.textContent = "×";
  remove.className = "text-slate-400 hover:text-red-500 leading-none";
  remove.addEventListener("click", async () => {
    await deleteTask(task.id);
    await loadBoard();
  });
  card.appendChild(remove);

  return card;
}

function renderAddForm(column) {
  const form = document.createElement("form");
  form.className = "mt-2";

  const input = document.createElement("input");
  input.className = "w-full rounded-md border border-slate-300 px-2 py-1 text-sm";
  input.placeholder = "+ Add a card";
  form.appendChild(input);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const title = input.value.trim();
    if (!title) return;
    await createTask(column.id, title);
    await loadBoard();
  });

  return form;
}

loadBoard();
