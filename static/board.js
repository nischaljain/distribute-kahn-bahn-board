const BOARD_ID = 1;

async function loadBoard() {
  const response = await fetch(`/api/boards/${BOARD_ID}`);
  const board = await response.json();
  renderBoard(board);
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

  return section;
}

function renderCard(task) {
  const card = document.createElement("article");
  card.className = "bg-white rounded-md shadow-sm p-3 text-sm text-slate-700";
  card.textContent = task.title;
  return card;
}

loadBoard();
