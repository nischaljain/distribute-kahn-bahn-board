const BOARD_ID = 1;

let draggedTaskId = null;

// The board we last rendered. Edits apply here first so the UI responds
// immediately, then the server's version replaces it once it arrives.
let board = null;

// Open a persistent WebSocket to this server (via the Socket.IO client).
const socket = io();

// The server broadcasts this when any tab changes the board; re-fetch to sync.
socket.on("board_changed", () => {
  loadBoard();
});

async function loadBoard() {
  const response = await fetch(`/api/boards/${BOARD_ID}`);
  board = await response.json();
  renderBoard(board);
}

/**
 * Render a local edit right away, then persist it. A round trip takes long
 * enough to feel broken, so the UI never waits for one. If the request fails
 * the optimistic edit was wrong, so re-fetch to fall back to the server.
 */
async function optimistically(applyLocally, persist) {
  applyLocally();
  renderBoard(board);
  try {
    const response = await persist();
    if (!response.ok) throw new Error(`request failed: ${response.status}`);
  } catch (error) {
    console.error("Reverting optimistic update:", error);
    await loadBoard();
  }
}

function columnById(columnId) {
  return board.columns.find((column) => column.id === columnId);
}

function removeTask(taskId) {
  for (const column of board.columns) {
    const index = column.tasks.findIndex((task) => task.id === taskId);
    if (index !== -1) return column.tasks.splice(index, 1)[0];
  }
  return null;
}

// Positions must stay contiguous, matching what the server recalculates.
function renumber() {
  for (const column of board.columns) {
    column.tasks.forEach((task, index) => {
      task.position = index;
      task.column_id = column.id;
    });
  }
}

function createTask(columnId, title) {
  return optimistically(
    () => {
      // Placeholder id until the server's version arrives and replaces it.
      columnById(columnId).tasks.push({ id: null, title, description: "" });
      renumber();
    },
    () =>
      fetch(`/api/columns/${columnId}/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title }),
      })
  );
}

function deleteTask(taskId) {
  return optimistically(
    () => {
      removeTask(taskId);
      renumber();
    },
    () => fetch(`/api/tasks/${taskId}`, { method: "DELETE" })
  );
}

function moveTask(taskId, columnId, position) {
  return optimistically(
    () => {
      const task = removeTask(taskId);
      if (task) columnById(columnId).tasks.splice(position, 0, task);
      renumber();
    },
    () =>
      fetch(`/api/tasks/${taskId}/move`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ column_id: columnId, position }),
      })
  );
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
  section.addEventListener("drop", (event) => {
    event.preventDefault();
    if (draggedTaskId === null) return;
    const position = column.tasks.filter((task) => task.id !== draggedTaskId).length;
    const taskId = draggedTaskId;
    draggedTaskId = null;
    moveTask(taskId, column.id, position);
  });

  return section;
}

function renderCard(task) {
  const card = document.createElement("article");
  card.className =
    "bg-white rounded-md shadow-sm p-3 text-sm text-slate-700 flex justify-between items-start gap-2 cursor-grab";
  // A task still awaiting its server id can't be addressed by the API yet.
  card.draggable = task.id !== null;
  card.addEventListener("dragstart", () => {
    draggedTaskId = task.id;
  });

  const title = document.createElement("span");
  title.textContent = task.title;
  card.appendChild(title);

  const remove = document.createElement("button");
  remove.textContent = "×";
  remove.className = "text-slate-400 hover:text-red-500 leading-none";
  remove.addEventListener("click", () => {
    if (task.id !== null) deleteTask(task.id);
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

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const title = input.value.trim();
    if (!title) return;
    input.value = "";
    createTask(column.id, title);
  });

  return form;
}

loadBoard();
