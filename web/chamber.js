const { api, shortID, setMessage, withBusy, stateBadge, renderTimeline, formatAt, escapeHTML } = window.LockFlow;
const host = document.querySelector("#chambers");
const message = document.querySelector("#message");

async function sendFeedback(chamberID, commandID, kind, reason) {
  return api(`/api/chambers/${chamberID}/commands/${commandID}/feedback`, {
    method: "POST",
    body: JSON.stringify(reason ? { kind, reason } : { kind }),
  });
}

function commandRow(chamberID, command) {
  const failed = command.state === "failed" || command.state === "timed_out";
  const inProgress = ["queued", "accepted", "executing"].includes(command.state);
  const reason = failed && command.reason ? `<div class="fail-reason">原因：${escapeHTML(command.reason)}</div>` : "";
  const attempt = command.attempt > 1 ? `<span class="attempt">第 ${command.attempt} 次尝试 · 源自 ${shortID(command.attempt_of)}</span>` : "";

  let actions = "";
  if (inProgress) {
    if (command.state === "queued") {
      actions += `<button data-fb="received">设备接收</button>`;
    }
    if (command.state === "queued" || command.state === "accepted") {
      actions += `<button data-fb="executing">开始执行</button>`;
    }
    actions += `<button data-fb="settled" class="ok">完成(settled)</button>`;
    actions += `<button data-fb="failed" class="danger">上报失败</button>`;
  }
  if (failed) {
    actions += `<button data-retry class="warn">按安全规则重发</button>`;
  }

  return `<tr>
    <td>${escapeHTML(command.device)}<div class="muted">${escapeHTML(command.action)}${command.target ? " → " + command.target : ""}</div></td>
    <td>${shortID(command.id)}${attempt}</td>
    <td>${stateBadge(command.state)}${reason}</td>
    <td>${renderTimeline(command.timeline)}</td>
    <td class="actions" data-chamber="${chamberID}" data-command="${command.id}">${actions}</td>
  </tr>`;
}

function chamberSection(item, commandData) {
  const pending = commandData?.pending || [];
  const blocking = commandData?.blocking || [];
  const commands = commandData?.commands || [];
  const waiting = pending.length + blocking.length;
  const waitingNote = waiting
    ? `<div class="wait-note">闸次保持等待：${pending.length} 个执行中，${blocking.length} 个待重发</div>`
    : "";

  const rows = commands.length
    ? commands.map((command) => commandRow(item.chamber_id, command)).join("")
    : '<tr><td colspan="5" class="muted">本闸次暂无设备命令</td></tr>';

  return `<section class="panel wide">
    <h2>${item.chamber_id} <span class="muted">· stage: ${item.cycle?.stage || "idle"} · cycle: ${shortID(item.cycle?.id)}</span></h2>
    <div class="metrics">
      <div class="metric">上游闸<strong>${item.gates?.upstream || "-"}</strong></div>
      <div class="metric">下游闸<strong>${item.gates?.downstream || "-"}</strong></div>
      <div class="metric">队列<strong>${item.queue?.length || 0} 艘</strong></div>
      <div class="metric">紧急状态<strong>${item.emergency?.status || "idle"}</strong></div>
    </div>
    ${waitingNote}
    <div class="dispatch" data-chamber="${item.chamber_id}">
      <label>方向
        <select data-dir><option value="upstream">上游闸</option><option value="downstream">下游闸</option></select>
      </label>
      <button data-cmd="open">发令：开闸</button>
      <button data-cmd="close">发令：关闸</button>
    </div>
    <table class="commands">
      <thead><tr><th>设备</th><th>命令</th><th>状态</th><th>时间线</th><th>操作</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </section>`;
}

async function load() {
  const chambers = await api("/api/chambers");
  const byChamber = {};
  await Promise.all(chambers.map(async (item) => {
    byChamber[item.chamber_id] = await api(`/api/chambers/${item.chamber_id}/commands`).catch(() => null);
  }));
  host.innerHTML = chambers.map((item) => chamberSection(item, byChamber[item.chamber_id])).join("");
  bindActions();
  setMessage(message, "");
}

function bindActions() {
  host.querySelectorAll(".dispatch").forEach((bar) => {
    const chamberID = bar.dataset.chamber;
    bar.querySelectorAll("[data-cmd]").forEach((button) => {
      button.addEventListener("click", () => withBusy(button, async () => {
        try {
          const direction = bar.querySelector("[data-dir]").value;
          const path = `/api/chambers/${chamberID}/gates/${direction}/${button.dataset.cmd}`;
          await api(path, { method: "POST" });
          await load();
        } catch (error) { setMessage(message, error.message); }
      }));
    });
  });
  host.querySelectorAll(".actions").forEach((cell) => {
    const chamberID = cell.dataset.chamber;
    const commandID = cell.dataset.command;
    cell.querySelectorAll("[data-fb]").forEach((button) => {
      button.addEventListener("click", () => withBusy(button, async () => {
        try {
          let reason = "";
          if (button.dataset.fb === "failed") {
            reason = window.prompt("设备上报的失败原因", "液压超压，阀门拒动") || "";
            if (!reason) return;
          }
          await sendFeedback(chamberID, commandID, button.dataset.fb, reason);
          await load();
        } catch (error) { setMessage(message, error.message); }
      }));
    });
    const retryButton = cell.querySelector("[data-retry]");
    if (retryButton) {
      retryButton.addEventListener("click", () => withBusy(retryButton, async () => {
        try {
          await api(`/api/chambers/${chamberID}/commands/${commandID}/retry`, { method: "POST" });
          await load();
        } catch (error) { setMessage(message, error.message); }
      }));
    }
  });
}

document.querySelector("#refresh").addEventListener("click", () => load().catch((error) => setMessage(message, error.message)));
load().catch((error) => setMessage(message, error.message));
setInterval(() => load().catch(() => {}), 5000);
