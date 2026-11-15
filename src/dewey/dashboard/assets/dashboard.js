/* Portable UI: host owns JSON routes/auth. No framework, build step or CDN. */
import Chart from "./chart.js";

// Recorded event counts, not sampled worker state or inferred legacy history.
export function outcomeChartData(history, {mode = "cumulative", observedAt, hours = 24} = {}) {
  const end = new Date(observedAt).getTime();
  const windowStart = end - hours * 3600000;
  const labels = [], coverage = [], completed = [], dead = [];
  let done = 0, dlq = 0, baseline = false;
  for (const bucket of history.buckets) {
    const known = bucket.completed !== null && bucket.dead !== null;
    if (mode === "cumulative" && known && !baseline) {
      const start = Math.max(windowStart, new Date(history.recorded_from).getTime());
      labels.push(new Date(start).toISOString()); coverage.push("recording baseline");
      completed.push(0); dead.push(0); baseline = true;
    }
    labels.push(new Date(Math.min(end, new Date(bucket.at).getTime() + history.bucket_seconds * 1000)).toISOString());
    coverage.push(bucket.coverage);
    if (!known) { completed.push(null); dead.push(null); continue; }
    done += bucket.completed; dlq += bucket.dead;
    completed.push(mode === "cumulative" ? done : bucket.completed);
    dead.push(mode === "cumulative" ? dlq : bucket.dead);
  }
  return {labels, coverage, datasets: [
    {label: "Completed", data: completed},
    {label: "DLQ", data: dead},
  ]};
}

const statuses = ["pending", "dispatching", "processing", "completed", "failed", "dead", "expired"];
function el(tag, text, cls) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = String(text);
  if (cls) node.className = cls;
  return node;
}
function button(text, action, cls = "dw-button") {
  const node = el("button", text, cls); node.type = "button";
  node.addEventListener("click", action); return node;
}
function time(value) { return value ? new Date(value).toLocaleString() : "—"; }
function badge(status) { return el("span", status, `dw-badge dw-${statuses.includes(status) ? status : "unknown"}`); }
function field(label, value) { const row = el("div", undefined, "dw-field"); row.append(el("dt", label), el("dd", value ?? "—")); return row; }

export function mountDashboard(root, options = {}) {
  const base = new URL(options.apiBase || root.dataset.apiBase, location.href);
  if (base.origin !== location.origin) throw new Error("Dashboard API must be same-origin");
  if (!base.pathname.endsWith("/")) base.pathname += "/";
  const readOnly = options.readOnly ?? (root.dataset.readOnly === "true");
  const navigation = options.navigation ?? (root.dataset.navigation === "true");
  const retentionEnabled = !readOnly && (options.retention ?? (root.dataset.retention === "true"));
  const initialTab = (options.initialTab || root.dataset.initialTab) === "tasks" ? "tasks" : "overview";
  const state = {tab: initialTab, hours: "24", queue: "", status: "", search: "", cursor: "", cursors: [], detail: null, chartMode: "cumulative"};
  function readLocation() {
    if (!navigation) return;
    const params = new URL(location.href).searchParams, id = params.get("dw_task");
    state.detail = id && /^[a-zA-Z0-9-]{1,36}$/.test(id) ? id : null;
    state.tab = state.detail || params.get("dw_tab") === "tasks" ? "tasks" : initialTab;
    if (params.get("dw_tab") === "overview" && !state.detail) state.tab = "overview";
    if (params.get("dw_tab") === "retention" && retentionEnabled && !state.detail) state.tab = "retention";
  }
  function writeLocation() {
    if (!navigation) return;
    const url = new URL(location.href);
    url.searchParams.set("dw_tab", state.tab);
    if (state.detail) url.searchParams.set("dw_task", state.detail); else url.searchParams.delete("dw_task");
    if (url.href !== location.href) history.pushState(null, "", url);
  }
  readLocation();
  let outcomeChart = null;
  function chartTheme() {
    if (!outcomeChart) return;
    const css = getComputedStyle(root), color = name => css.getPropertyValue(name).trim();
    outcomeChart.data.datasets.forEach((dataset, i) => {dataset.borderColor = color(i ? "--dw-danger" : "--dw-good"); dataset.backgroundColor = dataset.pointBackgroundColor = dataset.pointBorderColor = dataset.borderColor;});
    for (const axis of Object.values(outcomeChart.options.scales)) {axis.ticks.color = color("--dw-muted"); axis.grid.color = color("--dw-line"); axis.title.color = color("--dw-muted");}
    outcomeChart.options.plugins.legend.labels.color = color("--dw-text");
    outcomeChart.options.plugins.tooltip.backgroundColor = color("--dw-text");
    outcomeChart.options.plugins.tooltip.titleColor = outcomeChart.options.plugins.tooltip.bodyColor = color("--dw-panel");
    outcomeChart.update("none");
  }
  function clearChart() {if (outcomeChart) {outcomeChart.destroy(); outcomeChart = null;}}
  let generation = 0, detailGeneration = 0, destroyed = false;
  const themeMode = options.theme || root.dataset.theme || "host";
  const media = matchMedia("(prefers-color-scheme: dark)");
  function theme() {
    const html = document.documentElement;
    const hostMode = html.dataset.theme;
    root.dataset.resolvedTheme = themeMode === "class" ? (html.classList.contains("dark") ? "dark" : "light") : themeMode === "dark" ? "dark" : themeMode === "light" ? "light" :
      html.classList.contains("dark") || hostMode === "dark" ? "dark" :
      hostMode === "light" || html.classList.contains("light") ? "light" : media.matches ? "dark" : "light";
    chartTheme();
  }
  const observer = new MutationObserver(theme);
  observer.observe(document.documentElement, {attributes: true, attributeFilter: ["class", "data-theme"]});
  media.addEventListener("change", theme); theme();
  root.classList.add("dw-root"); root.replaceChildren();
  const header = el("header", undefined, "dw-header");
  header.append(el("h1", "Dewey"));
  if (readOnly) header.append(el("span", "Read-only · task operations", "dw-badge"));
  const nav = el("nav", undefined, "dw-tabs"); nav.setAttribute("aria-label", "Dashboard sections");
  const overviewTab = button("Overview", () => switchTab("overview"));
  const tasksTab = button("Tasks", () => switchTab("tasks")); nav.append(overviewTab, tasksTab);
  const retentionTab = retentionEnabled ? button("Retention", () => switchTab("retention")) : null;
  if (retentionTab) nav.append(retentionTab);
  let retentionMessage = "";
  const controls = el("div", undefined, "dw-controls");
  const windowLabel = el("label", "Observation window "); const windowSelect = el("select");
  for (const [value, label] of [["1", "Last hour"], ["24", "Last 24 hours"], ["168", "Last 7 days"]]) {
    const option = el("option", label); option.value = value; windowSelect.append(option);
  }
  windowSelect.value = state.hours; windowLabel.append(windowSelect);
  windowSelect.addEventListener("change", () => {state.hours = windowSelect.value; resetPage(); load();});
  controls.append(windowLabel, button("Refresh", () => load()));
  const notice = el("p", "", "dw-notice"); notice.setAttribute("role", "status"); notice.setAttribute("aria-live", "polite");
  const content = el("section"); root.append(header, nav, controls, notice, content);
  function resetPage() { state.cursor = ""; state.cursors = []; state.detail = null; detailGeneration++; }
  function clearSelectionLocation() { if (navigation) {const url = new URL(location.href); url.searchParams.delete("dw_task"); history.replaceState(null, "", url);} }
  function switchTab(tab) { state.tab = tab; resetPage(); writeLocation(); load(); }
  function onPopState() { resetPage(); readLocation(); load(); }
  if (navigation) window.addEventListener("popstate", onPopState);
  async function request(path, params = {}) {
    const url = new URL(path, base);
    for (const [key, value] of Object.entries(params)) if (value !== "") url.searchParams.set(key, value);
    const response = await fetch(url, {credentials: "same-origin", headers: {Accept: "application/json"}, cache: "no-store"});
    if (response.redirected && new URL(response.url).pathname.endsWith("/login/")) throw new Error("Your session has ended. Sign in again, then refresh this page.");
    if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) throw new Error("Could not load data. Check your session and view permission, then refresh.");
    const data = await response.json(); if (data.version !== 1) throw new Error("Unsupported dashboard data version"); return data;
  }
  function panel(titleText) { const p = el("section", undefined, "dw-panel"); p.append(el("h2", titleText)); return p; }
  function table(headers, rows) {
    const wrap = el("div", undefined, "dw-table-wrap"), t = el("table"), head = el("thead"), tr = el("tr");
    for (const h of headers) {const th = el("th", h); th.scope = "col"; tr.append(th);} head.append(tr); t.append(head);
    const body = el("tbody");
    for (const cells of rows) {const row = el("tr"); for (const cell of cells) {const td = el("td"); td.append(cell instanceof Node ? cell : el("span", cell ?? "—")); row.append(td);} body.append(row);}
    t.append(body); wrap.append(t); return wrap;
  }
  function safeAnchor(label, value, business = false) {
    try {
      const url = new URL(value, location.href);
      if (!value || /[\s\\%]/.test(value) || url.username || url.password || url.search || url.hash) return el("span", label);
      if (business ? !value.startsWith("/") || value.startsWith("//") || url.origin !== location.origin : url.protocol !== "https:") return el("span", label);
      const a = el("a", label, "dw-link"); a.href = url.href;
      if (!business) {a.target = "_blank"; a.rel = "noopener noreferrer";}
      return a;
    } catch { return el("span", label); }
  }
  function throughput(data, observedAt) {
    const p = panel("Outcomes over time"); p.classList.add("dw-outcomes");
    const form = el("form", undefined, "dw-filters"), label = el("label", "Exact queue (blank = all)"), input = el("input");
    input.value = state.queue; input.maxLength = 50; label.append(input);
    const apply = el("button", "Apply queue", "dw-button"); apply.type = "submit"; form.append(label, apply);
    const modeLabel = el("label", "Chart view"), mode = el("select");
    for (const [value, text] of [["cumulative", "Cumulative outcomes"], ["bucket", "Per-bucket counts"]]) {const option = el("option", text); option.value = value; mode.append(option);}
    mode.value = state.chartMode; modeLabel.append(mode); form.append(modeLabel);
    form.addEventListener("submit", e => {e.preventDefault(); state.queue = input.value.trim(); resetPage(); load();}); p.append(form);
    if (!data?.recorded_from) {p.append(el("p", "Transition history unavailable. No legacy timestamps are inferred.", "dw-muted")); content.append(p); return;}
    const totals = el("div", undefined, "dw-outcome-totals");
    for (const [key, labelText] of [["completed", "Completed"], ["dead", "DLQ"]]) {
      const total = el("div", undefined, `dw-outcome-total dw-outcome-${key}`); total.append(el("span", labelText), el("strong", data.buckets.reduce((n, b) => n + (b[key] || 0), 0))); totals.append(total);
    }
    p.append(totals);
    const description = el("p", "", "dw-muted"); p.append(description);
    const card = el("div", undefined, "dw-chart-card"), scroll = el("div", undefined, "dw-chart-scroll"), inner = el("div", undefined, "dw-chart-inner"), canvas = el("canvas");
    canvas.setAttribute("role", "img"); canvas.setAttribute("aria-label", "Completed and DLQ recorded task events; exact values in the table below");
    inner.append(canvas); scroll.append(inner); card.append(scroll);
    const expand = button("⛶ Fullscreen", () => {
      if (document.fullscreenElement === card) document.exitFullscreen();
      else if (card.requestFullscreen) card.requestFullscreen().catch(() => {description.textContent = "Fullscreen is not available in this host browser.";});
    }); expand.classList.add("dw-chart-expand");
    card.addEventListener("fullscreenchange", () => {expand.textContent = document.fullscreenElement === card ? "✕ Exit fullscreen" : "⛶ Fullscreen";});
    card.append(expand); p.append(card);
    const details = el("details"); details.append(el("summary", "Time buckets and accessible values"));
    details.append(el("p", `History available since ${time(data.recorded_from)}. Earlier data is unavailable; edge buckets are partial.`, "dw-muted"));
    details.append(table(["Bucket start", "Completed", "DLQ", "Coverage"], data.buckets.map(b => [time(b.at), b.completed ?? "Unavailable", b.dead ?? "Unavailable", b.coverage]))); p.append(details); content.append(p);
    function draw() {
      clearChart();
      const series = outcomeChartData(data, {mode: state.chartMode, observedAt, hours: Number(state.hours)});
      description.textContent = `${state.chartMode === "cumulative" ? "Cumulative recorded task events" : "Recorded task events per bucket"} · ${data.bucket_seconds / 60}-minute buckets`;
      inner.style.minWidth = `${Math.max(560, series.labels.length * 24)}px`;
      outcomeChart = new Chart(canvas, {
        type: "line",
        data: {labels: series.labels, datasets: series.datasets.map(dataset => ({...dataset, borderWidth: 2, tension: .25, cubicInterpolationMode: "monotone", pointRadius: 2, pointHoverRadius: 5, spanGaps: false}))},
        options: {animation: false, responsive: true, maintainAspectRatio: false, interaction: {mode: "index", intersect: false},
          plugins: {legend: {position: "top", align: "start", labels: {usePointStyle: true, pointStyle: "circle", pointStyleWidth: 8, boxWidth: 8, boxHeight: 8, padding: 14}}, tooltip: {callbacks: {
            title: items => time(series.labels[items[0].dataIndex]),
            afterBody: items => series.coverage[items[0].dataIndex] === "partial" ? "Partial bucket" : series.coverage[items[0].dataIndex] === "recording baseline" ? "Start of available history in this window" : "",
          }}},
          scales: {x: {ticks: {maxTicksLimit: 8, maxRotation: 0, callback(value) {return new Date(this.getLabelForValue(value)).toLocaleString(undefined, {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit"});}}, grid: {}, title: {display: true, text: "Time (local)"}},
            y: {beginAtZero: true, ticks: {precision: 0}, grid: {}, title: {display: true, text: state.chartMode === "cumulative" ? "Cumulative transitions" : "Transitions per bucket"}}},
        },
      });
      chartTheme();
    }
    mode.addEventListener("change", () => {state.chartMode = mode.value; draw();}); draw();
  }
  function overview(data) {
    const cards = el("div", undefined, "dw-cards");
    for (const [key, label] of [["ready", "Ready tasks"], ["processing", "Processing"], ["completed", "Completed"], ["dead", "Dead letters"]]) {
      const card = el("section", undefined, `dw-card ${key === "dead" ? "dw-danger" : ""}`);
      card.append(el("h2", label), el("strong", data.summary[key]), el("small", "Tasks created in selected window")); cards.append(card);
    }
    content.append(cards); throughput(data.throughput, data.observed_at);
    const grid = el("div", undefined, "dw-grid"); const queues = panel("Queues");
    queues.append(table(["Queue", "Catalog", "Ready", "Scheduled", "Admitting", "Processing", "Done", "Dead"], data.queues.map(q => [
      button(q.queue, () => {state.queue = q.queue; switchTab("tasks");}, "dw-link"), q.configured ? "Configured" : "Observed only", q.ready, q.scheduled, q.dispatching, q.processing, q.completed, q.dead
    ])));
    if (!data.queues.length) queues.append(el("p", "No tasks in this window.", "dw-muted"));
    if (data.queues_truncated) queues.append(el("p", "First 50 queues shown.", "dw-muted"));
    const owners = panel("Active task claims");
    for (const owner of data.owners) {
      const entry = el("div", undefined, "dw-owner"); entry.append(el("code", owner.worker_id), el("p", `${owner.active} active claims · ${owner.expired_leases} elapsed leases`)); owners.append(entry);
    }
    if (!data.owners.length) owners.append(el("p", "No active task claims.", "dw-muted"));
    if (data.owners_truncated) owners.append(el("p", "First 20 owners shown.", "dw-muted"));
    grid.append(queues, owners); content.append(grid);
    const notes = el("details", undefined, "dw-metric-notes"); notes.append(el("summary", "About these metrics"));
    for (const text of data.limitations) notes.append(el("p", text, "dw-muted dw-note")); content.append(notes);
    notice.textContent = `Observed ${time(data.observed_at)} · refresh manually`;
  }
  function tasks(data) {
    const form = el("form", undefined, "dw-filters");
    function input(labelText, value, name) {const label = el("label", labelText); const i = el("input"); i.value = value; i.name = name; i.maxLength = name === "queue" ? 50 : 100; label.append(i); form.append(label); return i;}
    const search = input("Task ID/type prefix", state.search, "search"), queue = input("Queue", state.queue, "queue");
    const slabel = el("label", "Status"), select = el("select");
    for (const s of ["", ...statuses]) {const option = el("option", s || "All statuses"); option.value = s; select.append(option);} select.value = state.status; slabel.append(select); form.append(slabel);
    const apply = el("button", "Apply", "dw-button"); apply.type = "submit";
    form.append(apply, button("Dead letters", () => {state.status = "dead"; resetPage(); load();}), button("Clear filters", () => {state.queue = state.status = state.search = ""; resetPage(); load();}));
    form.addEventListener("submit", e => {e.preventDefault(); state.search = search.value.trim(); state.queue = queue.value.trim(); state.status = select.value; resetPage(); load();});
    content.append(form);
    const grid = el("div", undefined, "dw-grid dw-task-grid"), list = panel(state.status === "dead" ? "Dead letters" : "Task ledger"), detail = panel("Task details");
    list.append(table(["Task", "Queue", "Priority", "Status", "Attempts", "Created"], data.items.map(item => {
      const task = el("div");
      const selectTask = button(item.task_type, () => {state.detail = item.id; writeLocation(); showDetail(item.id, detail, true);}, "dw-link");
      selectTask.dataset.taskId = item.id;
      task.append(selectTask, el("code", item.id));
      return [task, item.queue, item.priority, badge(item.status), `${item.attempts} / ${item.max_attempts}`, time(item.created_at)];
    })));
    if (!data.items.length) list.append(el("p", "No matching tasks in this window.", "dw-muted"));
    const paging = el("div", undefined, "dw-paging");
    const previous = button("Previous", () => {state.cursor = state.cursors.pop() || ""; state.detail = null; load();}); previous.disabled = !state.cursors.length;
    const next = button("Next", () => {state.cursors.push(state.cursor); state.cursor = data.next_cursor; state.detail = null; load();}); next.disabled = !data.next_cursor;
    paging.append(previous, next); list.append(paging); detail.append(el("p", "Select a task to inspect its latest state.", "dw-muted"));
    grid.append(list, detail); content.append(grid);
    markSelection();
    if (state.detail) showDetail(state.detail, detail);
    notice.textContent = `${data.items.length} tasks on this page · created in selected window`;
  }
  function retention(data) {
    const p = panel("Completed-task retention"), grid = el("div", undefined, "dw-grid");
    p.append(el("p", `Saved policy: ${data.config.days} days · cleanup ${data.config.enabled ? "on" : "off"} · revision ${data.config.revision}`));
    const preview = panel("Deletion preview"), previewNotice = el("p", "", "dw-muted"); previewNotice.setAttribute("role", "status");
    function paintPreview(info) {
      const values = el("dl"); values.append(field("Retention", `${info.preview_days} days`), field("Cutoff", time(info.cutoff_at)), field("Eligible tasks", `${info.eligible_count}${info.eligible_truncated ? "+ (bounded preview)" : ""}`));
      preview.replaceChildren(el("h2", "Deletion preview"), values, el("p", "Only old, completed, unowned tasks without recent events are candidates. Previewing does not save settings or delete anything.", "dw-muted"), el("p", info.safety.explanation, info.safety.allowed ? "dw-muted" : "dw-error"), previewNotice);
    }
    paintPreview(data);
    const form = el("form", undefined, "dw-retention"), daysLabel = el("label", "Completed retention (days)"), days = el("input");
    days.type = "number"; days.min = "1"; days.max = "3650"; days.required = true; days.value = String(data.config.days); days.disabled = !data.can_manage; daysLabel.append(days);
    const enabledLabel = el("label", "Allow scheduled cleanup"), enabled = el("input"); enabled.type = "checkbox"; enabled.checked = data.config.enabled; enabled.disabled = !data.can_manage; enabledLabel.prepend(enabled);
    const previewButton = button("Preview selected days", async () => {
      if (!days.checkValidity()) {days.reportValidity(); return;}
      const serial = generation; previewButton.disabled = true; previewNotice.textContent = "Loading preview…";
      try {
        const answer = await request("retention/", {days: days.value});
        if (!destroyed && serial === generation && form.isConnected) {previewNotice.textContent = "Selected-days preview only; saved settings unchanged."; paintPreview(answer);}
      } catch (error) {if (!destroyed && serial === generation && form.isConnected) previewNotice.textContent = error.message;}
      finally {previewButton.disabled = false;}
    });
    form.append(daysLabel, enabledLabel, previewButton, el("p", "Default: 30 days, cleanup off. Enabling permits a host-scheduled job; it does not install cron. Saving settings never deletes tasks.", "dw-muted"));
    if (data.can_manage) {
      const reasonLabel = el("label", "Safe settings-change reason (no secrets)"), reason = el("input"); reason.required = true; reason.maxLength = 500; reasonLabel.append(reason);
      const confirmLabel = el("label", "I understand cleanup removes task rows, creation-deduplication keys and event history; host safety approval is required."), confirm = el("input"); confirm.type = "checkbox"; confirm.required = true; confirmLabel.prepend(confirm);
      const submit = el("button", "Save retention settings", "dw-button"); submit.type = "submit";
      const result = el("p", retentionMessage, "dw-muted"); result.setAttribute("role", "status"); form.append(reasonLabel, confirmLabel, submit, result);
      form.addEventListener("submit", async e => {
        e.preventDefault(); if (!confirm.checked || !reason.value.trim() || submit.disabled) return;
        const serial = generation; submit.disabled = true;
        try {
          const body = new URLSearchParams({days: days.value, enabled: enabled.checked ? "yes" : "no", revision: String(data.config.revision), reason: reason.value.trim(), confirm: "yes"});
          const response = await fetch(new URL("retention/configure/", base), {method: "POST", credentials: "same-origin", cache: "no-store", headers: {Accept: "application/json", "X-CSRFToken": options.csrfToken || root.dataset.csrfToken || ""}, body});
          if (!response.headers.get("content-type")?.includes("application/json")) throw new Error("Settings could not be saved. Check session, CSRF and retention permission.");
          const answer = await response.json(); if (!response.ok) throw new Error(answer.error || "Settings rejected; refresh before saving.");
          if (!destroyed && serial === generation) {retentionMessage = answer.changed ? "Retention settings saved and audited. No tasks deleted." : "Settings unchanged. No tasks deleted."; load();}
        } catch (error) {if (!destroyed && serial === generation && form.isConnected) {result.textContent = error.message; result.className = "dw-error"; result.setAttribute("role", "alert"); result.tabIndex = -1; result.focus(); result.scrollIntoView({block: "nearest"}); submit.disabled = false;}}
      });
    } else form.append(el("p", "Separate retention configuration permission is required to change this policy.", "dw-muted"));
    p.append(form); grid.append(p, preview); content.append(grid);
    const last = panel("Last cleanup"), run = data.last_run;
    if (run) {
      const fields = el("dl"); for (const [label, value] of [["Status", run.status], ["Started", time(run.started_at)], ["Finished", time(run.finished_at)], ["Cutoff", time(run.cutoff_at)], ["Tasks deleted", run.deleted_count], ["Policy revision", run.revision]]) fields.append(field(label, value)); last.append(fields, el("code", `Run ${run.id}`));
      if (run.error) last.append(el("p", run.error, "dw-error"));
    } else last.append(el("p", "No cleanup has run.", "dw-muted"));
    content.append(last);
    const audit = panel("Configuration audit");
    const configLabel = config => config.days === null ? "Unavailable policy" : `${config.days} days / ${config.enabled ? "on" : "off"}`;
    audit.append(table(["When", "Revision", "Actor / reason", "Change"], data.audits.map(a => [time(a.at), a.revision, `Actor ${a.actor}: ${a.reason}`, `${configLabel(a.old_config)} → ${configLabel(a.new_config)}`])));
    if (!data.audits.length) audit.append(el("p", "No settings changes recorded.", "dw-muted"));
    if (data.audits_truncated) audit.append(el("p", "Latest 20 changes shown.", "dw-muted")); content.append(audit);
    for (const text of data.limitations) content.append(el("p", text, "dw-muted dw-note"));
    notice.textContent = `Observed ${time(data.observed_at)} · configuration revision ${data.config.revision} · refresh manually`;
  }
  function markSelection() {
    for (const node of content.querySelectorAll("button[data-task-id]")) {
      const selected = node.dataset.taskId === state.detail;
      node.setAttribute("aria-current", selected ? "true" : "false");
      node.closest("tr").classList.toggle("dw-selected", selected);
    }
  }
  async function showDetail(id, target, focus = false) {
    state.detail = id; markSelection(); const serial = ++detailGeneration; target.replaceChildren(el("h2", "Task details"), el("p", "Loading…", "dw-muted"));
    try {
      const data = await request(`task/${encodeURIComponent(id)}/`);
      if (destroyed || serial !== detailGeneration || !target.isConnected) return;
      const task = data.task; target.replaceChildren(el("h2", task.task_type), badge(task.status), el("code", task.id));
      const recovery = task.recovery;
      const story = el("section", undefined, `dw-recovery dw-recovery-${["good", "waiting", "attention"].includes(recovery?.tone) ? recovery.tone : "neutral"}`);
      story.append(el("h3", recovery?.title || "Task state recorded"), el("p", recovery?.explanation || "This host does not provide a recovery explanation."), el("h4", "What happens next"), el("p", recovery?.next_step || "Inspect the recorded state; automatic recovery and worker availability are unknown."));
      story.append(el("p", `Attempt ${task.attempts} of ${task.max_attempts} · queue ${task.queue}`, "dw-muted"));
      if (task.scheduled_for) story.append(el("p", `Recorded schedule: ${time(task.scheduled_for)}`, "dw-muted"));
      story.append(el("p", data.observed_at ? `Observed ${time(data.observed_at)} · refresh manually` : "Observation time unavailable · refresh manually", "dw-muted"));
      target.append(story);
      const technical = el("details", undefined, "dw-technical"); technical.append(el("summary", "Technical details & diagnostics"));
      const fields = el("dl");
      for (const [label, value] of [["Queue", task.queue], ["Attempts", `${task.attempts} / ${task.max_attempts}`], ["Created", time(task.created_at)], ["Scheduled", time(task.scheduled_for)], ["Started", time(task.started_at)], ["Completed", time(task.completed_at)], ["Task expiry", time(task.expires_at)], ["Owner", task.worker_id], ["Lease expiry", time(task.lease_expires_at)], ["Deadline", time(task.execution_deadline_at)], ["Latest outcome", task.latest_outcome]]) fields.append(field(label, value));
      technical.append(fields);
      if (task.error_present) technical.append(el("p", task.error_summary || "An error was recorded. Its content is hidden by the host's privacy policy.", "dw-error"));
      const diagnostic = task.diagnostics;
      technical.append(el("h2", "Failure diagnostics"));
      if (diagnostic) {
        technical.append(el("p", `${diagnostic.category}: ${diagnostic.summary || "No safe summary supplied."}`));
        if (diagnostic.business_label || diagnostic.business_url) technical.append(safeAnchor(diagnostic.business_label || "Business record", diagnostic.business_url, true));
        if (diagnostic.reference) technical.append(el("p", `${diagnostic.provider}: ${diagnostic.reference}`));
        if (diagnostic.reference_url) technical.append(safeAnchor("Open failing-attempt reference", diagnostic.reference_url));
      } else technical.append(el("p", "Reviewed diagnostics unavailable for this current attempt.", "dw-muted"));
      technical.append(el("code", `Local correlation: ${task.id} · attempt ${task.attempts} · event ${task.diagnostic_event_id || "unavailable"}`));
      technical.append(el("p", task.retry?.explanation || "Retry not available.", "dw-muted"));
      if (!readOnly && task.retry?.allowed) {
        const form = el("form", undefined, "dw-retry"), label = el("label", "Safe audit reason (no secrets)"), reason = el("input");
        reason.required = true; reason.maxLength = 500; label.append(reason);
        const confirmLabel = el("label", "I reviewed diagnostics, business eligibility and effect replay safety."), confirm = el("input"); confirm.type = "checkbox"; confirm.required = true; confirmLabel.prepend(confirm);
        const submit = el("button", "Retry once", "dw-button"); submit.type = "submit";
        const result = el("p", "Same task/operation keys; expiry and attempts preserved. Retry is not undo or exactly-once.", "dw-muted"); result.setAttribute("role", "status");
        form.append(label, confirmLabel, submit, result); target.append(form);
        form.addEventListener("submit", async e => {
          e.preventDefault(); if (!confirm.checked || !reason.value.trim() || submit.disabled) return;
          submit.disabled = true;
          try {
            const body = new URLSearchParams({event_id: task.diagnostic_event_id, attempt: String(task.attempts), reason: reason.value.trim(), confirm: "yes"});
            const response = await fetch(new URL(`task/${encodeURIComponent(id)}/retry/`, base), {method: "POST", credentials: "same-origin", cache: "no-store", headers: {Accept: "application/json", "X-CSRFToken": options.csrfToken || root.dataset.csrfToken || ""}, body});
            if (!response.headers.get("content-type")?.includes("application/json")) throw new Error("Retry denied. Check session, CSRF and retry permission.");
            const answer = await response.json(); if (!response.ok) throw new Error(answer.error || "Retry denied; refresh diagnostics.");
            if (!destroyed && serial === detailGeneration) load();
          } catch (error) {if (!destroyed && serial === detailGeneration) {result.textContent = error.message; submit.disabled = false;}}
        });
      }
      target.append(el("h3", "Attempt timeline"));
      target.append(el("p", "Recorded outcomes, oldest first. Attempt starts and crash causes are not recorded here; missing events are not reconstructed.", "dw-muted"));
      const timeline = el("ol", undefined, "dw-timeline");
      const labels = {failed: "Attempt failed", completed: "Task completed", dead: "Automatic attempts stopped", expired: "Task expired", retry_requested: "Manual retry requested"};
      for (const event of [...(task.events || [])].reverse()) {
        const item = el("li");
        item.append(el("strong", `${labels[event.kind] || "Recorded event"} · attempt ${event.attempt}`), el("time", time(event.occurred_at)), el("p", event.kind === "retry_requested" ? `Actor ${event.audit?.actor || "unavailable"}: ${event.audit?.reason || "unavailable"}` : event.diagnostics?.summary || "No reviewed explanation attached.", "dw-muted"));
        timeline.append(item);
      }
      target.append(timeline);
      if (!(task.events || []).length) target.append(el("p", "No recorded outcome events are available yet.", "dw-muted"));
      if (task.events_truncated) target.append(el("p", "Latest 25 events only; earlier history is omitted.", "dw-muted"));
      target.append(technical);
      target.append(el("p", "Arguments, metadata, fencing tokens and effect keys are not exposed. Task completion is not proof of business completion.", "dw-muted"));
      if (focus) {target.tabIndex = -1; target.focus({preventScroll: true}); target.scrollIntoView({block: "nearest"});}
    } catch (error) {if (serial === detailGeneration && target.isConnected) target.replaceChildren(el("h2", "Task details"), el("p", error.message, "dw-error"));}
  }
  async function load() {
    const serial = ++generation; ++detailGeneration;
    if (!state.detail) clearSelectionLocation();
    root.setAttribute("aria-busy", "true"); notice.textContent = "Loading…";
    clearChart(); content.replaceChildren();
    overviewTab.setAttribute("aria-current", state.tab === "overview" ? "page" : "false"); tasksTab.setAttribute("aria-current", state.tab === "tasks" ? "page" : "false");
    if (retentionTab) retentionTab.setAttribute("aria-current", state.tab === "retention" ? "page" : "false");
    windowLabel.hidden = state.tab === "retention";
    try {
      const data = await request(`${state.tab}/`, state.tab === "retention" ? {} : {hours: state.hours, queue: state.queue, status: state.status, search: state.search, cursor: state.cursor});
      if (destroyed || serial !== generation) return;
      clearChart(); content.replaceChildren(); if (state.tab === "overview") overview(data); else if (state.tab === "retention") retention(data); else tasks(data);
    } catch (error) {if (!destroyed && serial === generation) {clearChart(); content.replaceChildren(); notice.textContent = error.message;}}
    finally {if (serial === generation) root.setAttribute("aria-busy", "false");}
  }
  load();
  return {refresh: load, destroy() {destroyed = true; generation++; detailGeneration++; clearChart(); observer.disconnect(); media.removeEventListener("change", theme); window.removeEventListener("popstate", onPopState); root.replaceChildren();}};
}
for (const root of document.querySelectorAll("[data-dewey-dashboard]")) mountDashboard(root);
