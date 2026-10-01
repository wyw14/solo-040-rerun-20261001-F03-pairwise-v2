const api = async (path, options = {}) => {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
};

const text = (value) => value === undefined || value === null || value === "" ? "-" : String(value);
const shortID = (value) => value ? String(value).slice(0, 8) : "-";
const setMessage = (element, value) => { if (element) element.textContent = value || ""; };
const withBusy = async (button, action) => {
  if (button) button.disabled = true;
  try { return await action(); } finally { if (button) button.disabled = false; }
};

const STATE_LABELS = {
  queued: "Queued", accepted: "Device received", executing: "Executing",
  settled: "Settled", failed: "Failed", timed_out: "Timed out", cancelled: "Cancelled",
};

const stateBadge = (state) => {
  const label = STATE_LABELS[state] || state;
  const tone = state === "settled" ? "ok"
    : state === "failed" || state === "timed_out" ? "bad"
    : state === "cancelled" ? "muted-badge" : "wait";
  return `<span class="badge ${tone}">${label}</span>`;
};

const formatAt = (value) => value ? new Date(value).toLocaleTimeString() : "-";

// renderTimeline draws the real queued → received → executing → settled path.
const renderTimeline = (entries) => {
  if (!entries || !entries.length) return "";
  return `<ol class="timeline">${entries.map((entry) => {
    const note = entry.note ? `<em>${escapeHTML(entry.note)}</em>` : "";
    return `<li class="tl-${entry.state}"><span>${STATE_LABELS[entry.state] || entry.state}</span><small>${formatAt(entry.at)}</small>${note}</li>`;
  }).join("")}</ol>`;
};

const escapeHTML = (value) => String(value)
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;");

window.LockFlow = {
  api, text, shortID, setMessage, withBusy,
  stateBadge, renderTimeline, formatAt, escapeHTML,
};
