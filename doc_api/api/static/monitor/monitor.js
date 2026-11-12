'use strict';
const byId = id => document.getElementById(id);
const apiBase = new URL('./v1/monitor/', window.location.href);
const keyStorage = 'docapi.monitor.apiKey';
const sectionOrderStorage = 'docapi.monitor.sectionOrder';
let key = '', timer, controller, generation = 0, interval = 5000, stats;
let engines = [];
let jobOrder = 'desc', jobPage = 0, jobsHasMore = false;
const logTabs = [['log_user', 'User processing'], ['log', 'Processing'],
  ['final_log_user', 'User final'], ['final_log', 'Final']];
const logTabStorage = 'docapi.monitor.logTab';
let selectedLogTab = 'final_log';
try {
  const saved = localStorage.getItem(logTabStorage);
  if (logTabs.some(([field]) => field === saved)) selectedLogTab = saved;
} catch {}
const openLogs = new Map();

function closeLogs(id) {
  const viewer = openLogs.get(id);
  viewer?.controller.abort();
  viewer?.row.remove();
  openLogs.delete(id);
}
async function refreshLogs(id, viewer) {
  if (viewer.loading || openLogs.get(id) !== viewer) return;
  viewer.loading = true;
  try {
    const data = await get(`../jobs/${viewer.jobId}`, viewer.controller.signal);
    if (openLogs.get(id) !== viewer) return;
    viewer.data = data;
    viewer.select(viewer.tab);
    viewer.status.textContent = '';
  } catch (error) {
    if (error.name === 'AbortError' || openLogs.get(id) !== viewer) return;
    if ([401, 403].includes(error.status)) {
      signOut(); byId('message').textContent = error.message; return;
    }
    if (!viewer.data) viewer.empty.textContent = 'Logs unavailable.';
    viewer.status.textContent = `Could not refresh logs: ${error.message}. Retrying on the next refresh.`;
  } finally {
    viewer.loading = false;
  }
}
function logButton(tableId, jobId) {
  const id = `${tableId}-${jobId}`, button = node('button');
  button.type = 'button';
  button.className = 'job-logs-button icon-button';
  button.title = 'Show or hide job logs';
  button.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8zM14 2v6h6M8 13h8M8 17h8"/></svg>';
  button.setAttribute('aria-controls', `${id}-logs`);
  button.setAttribute('aria-expanded', String(openLogs.has(id)));
  button.setAttribute('aria-label', `Toggle logs for job ${jobId}`);
  button.addEventListener('click', () => {
    if (openLogs.has(id)) closeLogs(id);
    else {
      const row = node('tr', undefined, 'job-log-row'), cell = node('td');
      cell.colSpan = 10;
      const viewer = {row, jobId, controller: new AbortController(), tab: selectedLogTab};
      openLogs.set(id, viewer);
      const tabs = node('div', undefined, 'log-tabs'), content = node('pre', '', 'log-content');
      const panel = node('div'), empty = node('div', 'Loading logs…', 'muted');
      content.hidden = true;
      const status = node('div', '', 'muted');
      status.setAttribute('role', 'status');
      viewer.content = content; viewer.status = status; viewer.empty = empty;
      tabs.setAttribute('role', 'tablist');
      tabs.setAttribute('aria-label', `Logs for job ${jobId}`);
      panel.id = `${id}-log-content`;
      panel.setAttribute('role', 'tabpanel');
      panel.setAttribute('aria-live', 'polite');
      content.tabIndex = 0;
      function select(field, remember = false) {
        viewer.tab = field;
        for (const tab of tabs.children) {
          const active = tab.dataset.field === field;
          tab.setAttribute('aria-selected', String(active));
          tab.tabIndex = active ? 0 : -1;
        }
        panel.setAttribute('aria-labelledby', `${id}-tab-${field}`);
        if (viewer.data) {
          const text = viewer.data[field] || '';
          content.hidden = !text;
          empty.hidden = Boolean(text);
          empty.textContent = 'No log available.';
          if (content.textContent !== text) {
            const scrollTop = content.scrollTop;
            content.textContent = text;
            content.scrollTop = scrollTop;
          }
        }
        if (remember) {
          selectedLogTab = field;
          try { localStorage.setItem(logTabStorage, field); } catch {}
        }
      }
      viewer.select = select;
      for (const [field, label] of logTabs) {
        const tab = node('button', label);
        tab.type = 'button'; tab.dataset.field = field; tab.id = `${id}-tab-${field}`;
        tab.setAttribute('role', 'tab');
        tab.setAttribute('aria-controls', panel.id);
        tab.addEventListener('click', () => select(field, true));
        tab.addEventListener('keydown', event => {
          const index = logTabs.findIndex(([name]) => name === viewer.tab);
          const next = {ArrowRight: (index + 1) % 4, ArrowLeft: (index + 3) % 4, Home: 0, End: 3}[event.key];
          if (next === undefined) return;
          event.preventDefault(); select(logTabs[next][0], true); tabs.children[next].focus();
        });
        tabs.append(tab);
      }
      cell.id = `${id}-logs`;
      panel.append(content, empty);
      cell.append(tabs, panel, status); row.append(cell);
      button.closest('tr').after(row);
      select(viewer.tab);
      refreshLogs(id, viewer);
    }
    button.setAttribute('aria-expanded', String(openLogs.has(id)));
  });
  return button;
}
function updateJobOrder() {
  const button = byId('job-order');
  const label = jobOrder === 'desc' ? 'Descending' : 'Ascending';
  button.querySelector('.sort-arrow').setAttribute('d', jobOrder === 'desc' ?
    'M6 4v16m-4-4 4 4 4-4' : 'M6 20V4m-4 4 4-4 4 4');
  button.querySelector('.sort-bars').setAttribute('d', jobOrder === 'desc' ?
    'M13 5h9M13 12h6M13 19h3' : 'M13 5h3M13 12h6M13 19h9');
  button.title = `${label}; click to reverse`;
  button.setAttribute('aria-label',
    `${label} order; switch to ${jobOrder === 'desc' ? 'ascending' : 'descending'}`);
  byId('job-order-note').textContent =
    `${label} by ${byId('job-mode').selectedOptions[0].text.toLowerCase()} time. Missing timestamps appear last.`;
}
function savedKey() {
  try { return localStorage.getItem(keyStorage) || ''; }
  catch { return ''; }
}
function storeKey(value) {
  try {
    if (value) localStorage.setItem(keyStorage, value);
    else localStorage.removeItem(keyStorage);
    return true;
  } catch { return false; }
}
const date = value => {
  if (!value) return 'Never';
  const instant = new Date(value), pad = number => String(number).padStart(2, '0');
  return `${instant.getFullYear()}/${pad(instant.getMonth() + 1)}/${pad(instant.getDate())} ` +
    `${pad(instant.getHours())}:${pad(instant.getMinutes())}:${pad(instant.getSeconds())}`;
};
const duration = value => {
  if (value === null || value === undefined) return 'Unavailable';
  const seconds = Math.max(0, Math.floor(value));
  return `${Math.floor(seconds / 86400)}d ${Math.floor(seconds % 86400 / 3600)}h ${Math.floor(seconds % 3600 / 60)}m ${seconds % 60}s`;
};
const bytes = value => {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const unit = value > 0 ? Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1) : 0;
  return `${(value / 1024 ** unit).toLocaleString(undefined, {maximumFractionDigits: 1})} ${units[unit]}`;
};
function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}
function summary(section, values) {
  byId(`${section}-summary`).replaceChildren(...values.map(value => value instanceof Node ? value : node('span', value)));
}
function setupHeaderDrawer() {
  const drawer = byId('mobile-drawer'), opener = byId('drawer-open');
  const links = byId('header-links');
  const narrow = window.matchMedia('(max-width: 700px)');
  function update() {
    const mobile = document.body.dataset.layout === 'mobile' ||
      (document.body.dataset.layout === 'auto' && narrow.matches);
    document.body.classList.toggle('mobile-header', mobile);
    opener.hidden = !mobile;
    if (mobile) byId('drawer-actions').append(links);
    else {
      if (drawer.open) drawer.close();
      opener.before(links);
    }
  }
  opener.addEventListener('click', () => {
    drawer.showModal();
    opener.setAttribute('aria-expanded', 'true');
  });
  byId('drawer-close').addEventListener('click', () => drawer.close());
  drawer.addEventListener('close', () => { opener.setAttribute('aria-expanded', 'false'); });
  drawer.addEventListener('click', event => {
    const bounds = drawer.getBoundingClientRect();
    if (event.target === drawer && (event.clientX < bounds.left || event.clientX > bounds.right ||
        event.clientY < bounds.top || event.clientY > bounds.bottom)) drawer.close();
  });
  links.addEventListener('click', event => {
    if (drawer.open && event.target.closest('a, button')) drawer.close();
  });
  byId('layout').addEventListener('change', update);
  narrow.addEventListener('change', update);
  update();
}
async function get(path, signal, paginated = false) {
  const response = await fetch(new URL(path, apiBase), {headers: {'X-API-Key': key}, signal, cache: 'no-store'});
  const payload = await response.json();
  if (!response.ok) throw Object.assign(new Error(payload.detail || `Request failed: HTTP ${response.status}`), {status: response.status});
  return paginated ? {items: payload.data, total: Number(response.headers.get('X-Total-Count')), hasMore: response.headers.get('X-Has-More') === 'true'} : payload.data;
}
function options(id, rows, all) {
  const select = byId(id), selected = select.value;
  select.replaceChildren(new Option(all, ''), ...rows.map(row => new Option(row.label, row.id)));
  if ([...select.options].some(option => option.value === selected)) select.value = selected;
}
function setupSectionOrder() {
  const dashboard = byId('dashboard');
  const sections = [...dashboard.querySelectorAll(':scope > section[data-section]')];
  const known = new Map(sections.map(section => [section.dataset.section, section]));
  const hiddenStorage = 'docapi.monitor.hiddenSections';
  function savedList(storageKey) {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) || '[]');
      return Array.isArray(saved) ? saved.filter(id => known.has(id)) : [];
    } catch { return []; }
  }
  const savedOrder = [...new Set(savedList(sectionOrderStorage))];
  // Upgrade the previous default while preserving custom arrangements.
  if (savedOrder.join(',') === 'server,workers,recent-jobs,jobs,job-statistics,database') {
    savedOrder.splice(savedOrder.indexOf('jobs'), 1);
    savedOrder.splice(savedOrder.indexOf('job-statistics') + 1, 0, 'jobs');
  }
  if (savedOrder.includes('job-statistics') && !savedOrder.includes('jobs'))
    savedOrder.splice(savedOrder.indexOf('job-statistics') + 1, 0, 'jobs');
  if (savedOrder.join(',') === 'server,workers,recent-jobs,job-statistics,jobs,database') {
    savedOrder.splice(1, 2, 'recent-jobs', 'workers');
  }
  const order = [...new Set([...savedOrder, ...known.keys()])];
  dashboard.append(...order.map(id => known.get(id)));
  const collapsed = new Set(savedList(hiddenStorage));
  const currentSections = () => [...dashboard.querySelectorAll(':scope > section[data-section]')];
  function save() {
    try {
      localStorage.setItem(sectionOrderStorage, JSON.stringify(currentSections().map(section => section.dataset.section)));
      localStorage.setItem(hiddenStorage, JSON.stringify(currentSections().filter(section => section.querySelector('.section-body').hidden).map(section => section.dataset.section)));
    } catch { /* Preferences still work for this page when storage is blocked. */ }
  }
  const eye = 'M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12Zm13 0a3 3 0 1 1-6 0 3 3 0 0 1 6 0';
  const eyeOff = 'M3 3l18 18M10.6 10.6a2 2 0 0 0 2.8 2.8M9.9 5.2A10.7 10.7 0 0 1 12 5c6 0 10 7 10 7a16.5 16.5 0 0 1-3 3.7M6.2 6.2A17 17 0 0 0 2 12s4 7 10 7a11.7 11.7 0 0 0 5.8-1.8';
  function updateButtons() {
    const current = currentSections();
    current.forEach((section, index) => {
      section.querySelector('.section-up').disabled = index === 0;
      section.querySelector('.section-down').disabled = index === current.length - 1;
      const body = section.querySelector('.section-body');
      const button = section.querySelector('.section-hide');
      button.title = `${body.hidden ? 'Expand' : 'Collapse'} ${section.querySelector('h2').textContent}`;
      button.setAttribute('aria-label', button.title);
      button.setAttribute('aria-expanded', String(!body.hidden));
      button.querySelector('path').setAttribute('d', body.hidden ? eye : eyeOff);
      section.classList.toggle('collapsed', body.hidden);
    });
    const allCollapsed = current.every(section => section.querySelector('.section-body').hidden);
    const toggle = byId('sections-toggle');
    toggle.title = allCollapsed ? 'Expand all' : 'Collapse all';
    toggle.setAttribute('aria-label', toggle.title);
    toggle.querySelector('path').setAttribute('d', allCollapsed ?
      'm6 9 6-5 6 5M6 15l6 5 6-5' : 'm6 4 6 5 6-5M6 20l6-5 6 5');
    byId('sections-toggle').setAttribute('aria-expanded', String(!allCollapsed));
  }
  for (const section of sections) {
    const heading = section.querySelector('h2'), title = heading.textContent;
    const body = node('div', undefined, 'section-body');
    body.id = `section-${section.dataset.section}-body`;
    body.hidden = collapsed.has(section.dataset.section);
    for (const child of [...section.childNodes]) if (child !== heading) body.append(child);
    section.append(body);
    const header = node('div', undefined, 'section-header');
    const details = node('div', undefined, 'section-summary');
    details.id = `${section.dataset.section}-summary`;
    header.append(heading, details);
    section.prepend(header);
    const actions = node('div', undefined, 'section-actions');
    for (const action of ['up', 'down', 'hide']) {
      const button = node('button', undefined, `icon-button section-${action}`);
      button.type = 'button';
      button.title = action === 'hide' ? `Hide ${title}` : `Move ${title} ${action}`;
      button.setAttribute('aria-label', button.title);
      if (action === 'hide') button.setAttribute('aria-controls', body.id);
      const path = action === 'up' ? 'M12 20V4m-6 6 6-6 6 6' : action === 'down' ?
        'M12 4v16m-6-6 6 6 6-6' : eyeOff;
      button.innerHTML = `<svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="${path}"/></svg>`;
      button.addEventListener('click', () => {
        const current = currentSections(), index = current.indexOf(section);
        if (action === 'hide') {
          body.hidden = !body.hidden;
          updateButtons(); save();
          if (section.dataset.section === 'jobs') refresh();
          return;
        }
        const target = current[index + (action === 'up' ? -1 : 1)];
        if (!target) return;
        if (action === 'up') dashboard.insertBefore(section, target);
        else dashboard.insertBefore(target, section);
        updateButtons(); save();
        const focusTarget = button.disabled ? section.querySelector(`.section-${action === 'up' ? 'down' : 'up'}`) : button;
        focusTarget.focus({preventScroll: true});
        section.scrollIntoView({block: 'nearest', behavior: 'smooth'});
      });
      actions.append(button);
    }
    section.prepend(actions);
  }
  byId('sections-toggle').setAttribute('aria-controls', sections.map(section => section.querySelector('.section-body').id).join(' '));
  byId('sections-toggle').addEventListener('click', () => {
    const collapse = !currentSections().every(section => section.querySelector('.section-body').hidden);
    for (const section of currentSections()) section.querySelector('.section-body').hidden = collapse;
    updateButtons(); save();
    refresh();
  });
  updateButtons();
}
function table(id, rows, headers) {
  const body = byId(id);
  body.replaceChildren();
  if (!rows.length) {
    const cell = node('td', 'No records'); cell.colSpan = headers.length;
    const row = node('tr'); row.append(cell); body.append(row); return;
  }
  rows.forEach(values => {
    const row = node('tr');
    values.forEach((value, i) => {
      const cell = node('td'); cell.dataset.label = headers[i];
      cell.append(value instanceof Node ? value : document.createTextNode(String(value ?? '—')));
      row.append(cell);
    });
    body.append(row);
  });
}
function engineVersions(prefix = '') {
  const selected = byId(`${prefix}engine`).value;
  const versions = [...new Set(engines.filter(engine => !selected || engine.name === selected)
    .map(engine => engine.version))];
  options(`${prefix}engine-version`, versions.map(version => ({id: version, label: version})), 'All versions');
}
function addFilters(query, prefix = '') {
  for (const [id, parameter] of [['owner', 'owner_id'], ['worker', 'worker_id'],
    ['engine', 'engine_name'], ['engine-version', 'engine_version']]) {
    const value = byId(`${prefix}${id}`).value;
    if (value) query.set(parameter, value);
  }
}
function renderStats() {
  if (!stats) return;
  const measure = byId('metric').value;
  const value = state => measure === 'runtime_seconds' ? duration(stats[state][measure]) : stats[state][measure].toLocaleString();
  byId('stats').replaceChildren(...['done', 'failed', 'attempts'].map(state => {
    const card = node('div', undefined, 'stat');
    card.append(node('span', state.toUpperCase()),
      node('strong', value(state)));
    return card;
  }));
  const period = {'24h': '24 hours', '7d': '7 days', '1m': '30 days', all: 'All time'}[stats.period];
  summary('job-statistics', [period, byId('metric').selectedOptions[0].text,
    node('span', `Done ${value('done')}`, 'badge job-done'),
    node('span', `Failed ${value('failed')}`, 'badge job-failed')]);
}
function renderRecentJobs(jobs) {
  summary('recent-jobs', [
    node('span', `${jobs.filter(job => job.state === 'processing').length} processing`, 'badge job-processing'),
    node('span', `${jobs.filter(job => job.state === 'queued').length} queued`, 'badge job-queued')]);
  renderJobTable('recent-jobs', jobs, 'created_date', 'Created');
}
function renderJobs(jobs) {
  const pages = Math.max(1, Math.ceil(jobs.total / Number(byId('limit').value)));
  if (jobPage >= pages) { jobPage = pages - 1; refresh(); return; }
  const mode = byId('job-mode');
  const label = mode.selectedOptions[0].text;
  byId('job-date-heading').textContent = label;
  renderJobTable('jobs', jobs.items, mode.value, label);
  byId('jobs-page').textContent = `${jobPage + 1}/${pages}`;
  byId('jobs-page').setAttribute('aria-label', `Page ${jobPage + 1} of ${pages}`);
  byId('jobs-previous').disabled = jobPage === 0;
  jobsHasMore = jobs.hasMore;
  byId('jobs-next').disabled = !jobsHasMore;
}
function renderJobTable(id, jobs, field, label) {
  const visible = new Set(jobs.map(job => `${id}-${job.id}`));
  for (const entry of openLogs.keys()) if (entry.startsWith(`${id}-`) && !visible.has(entry)) closeLogs(entry);
  table(id, jobs.map(job => {
    const jobId = node('span', job.id.slice(0, 8));
    jobId.title = job.id;
    return [jobId, date(job[field]), node('span', job.state.toUpperCase(), `badge job-${job.state}`), `${Math.round(job.progress * 100)}%`,
      job.image_count, job.owner_label, job.worker_label, job.engine_name, job.engine_version, logButton(id, job.id)];
  }),
    ['Job', label, 'State', 'Progress', 'Images', 'Owner', 'Worker', 'Engine', 'Version', 'Logs']);
  [...byId(id).children].forEach((row, index) => {
    const viewer = openLogs.get(`${id}-${jobs[index]?.id}`);
    if (viewer) row.after(viewer.row);
  });
}
function render(info, totals, jobs, workers, database, recentJobs) {
  stats = totals;
  summary('server', [info.hostname, `Uptime ${duration(info.uptime_seconds)}`]);
  summary('workers', ['active', 'idle', 'off'].map(status => node('span',
    `${workers.filter(worker => worker.status === status).length} ${status === 'off' ? 'gone' : status}`, `badge ${status}`)));
  summary('database', [node('span', info.database_connected ? 'Connected' : 'Unavailable',
    `badge ${info.database_connected ? 'active' : 'unavailable'}`), bytes(database.size_bytes), `${database.connections} connections`]);
  const fields = [
    ['Hostname', info.hostname], ['IP addresses', info.ip_addresses.join(', ') || 'Unavailable'],
    ['Started', date(info.started_at)],
    ['Uptime', duration(info.uptime_seconds)],
    ['Server time', date(info.server_time)],
  ];
  byId('info').replaceChildren(...fields.flatMap(([label, value]) => [node('dt', label), node('dd', value)]));
  const dbFields = [
    ['Database', database.database_name], ['Size', bytes(database.size_bytes)],
    ['Connections', database.connections], ['Active', database.active_connections], ['Idle', database.idle_connections],
    ['Idle in transaction', database.idle_in_transaction_connections],
    ['Waiting on locks', database.lock_waiting_connections],
    ['Longest active query', database.longest_active_query_seconds === null ? 'None' : duration(database.longest_active_query_seconds)],
    ['Deadlocks', database.deadlocks], ['Statistics reset', database.stats_reset ? date(database.stats_reset) : 'Unavailable'],
  ];
  byId('database').replaceChildren(...dbFields.flatMap(([label, value]) => [node('dt', label), node('dd', value)]));
  const databaseStatus = node('dd');
  databaseStatus.append(node('span', info.database_connected ? 'Connected' : 'Unavailable',
    `badge ${info.database_connected ? 'active' : 'unavailable'}`));
  byId('database').prepend(node('dt', 'Status'), databaseStatus);
  renderStats();
  if (jobs && !byId('section-jobs-body').hidden) renderJobs(jobs);
  renderRecentJobs(recentJobs);
  table('workers', workers.map(worker => [worker.label, node('span', worker.status === 'off' ? 'GONE' : worker.status.toUpperCase(), `badge ${worker.status}`),
    worker.processing_jobs, date(worker.last_used), worker.enabled ? 'Yes' : 'No']),
    ['Worker key', 'Activity', 'Processing jobs', 'Last seen', 'Key enabled']);
  byId('worker-note').textContent = `PROCESSING job → active. Otherwise, seen within ${info.worker_offline_after_seconds}s → idle; older or never seen → gone.`;
  byId('updated').textContent = `Updated ${new Date().toLocaleTimeString(undefined, {hourCycle: 'h23'})} · refresh ${info.refresh_interval_seconds}s · pauses while this tab is hidden`;
  interval = info.refresh_interval_seconds * 1000;
  byId('dashboard').hidden = false;
  byId('sections-toggle').hidden = false;
}
async function refresh() {
  clearTimeout(timer);
  if (!key || document.hidden) return;
  controller?.abort();
  controller = new AbortController();
  const signal = controller.signal, current = ++generation;
  const query = new URLSearchParams({period: byId('period').value});
  addFilters(query);
  const jobQuery = new URLSearchParams({limit: byId('limit').value, mode: byId('job-mode').value, order: jobOrder, offset: jobPage * Number(byId('limit').value)});
  addFilters(jobQuery, 'job-');
  const recentQuery = new URLSearchParams({limit: byId('recent-limit').value});
  addFilters(recentQuery, 'recent-');
  byId('jobs-previous').disabled = true;
  byId('jobs-next').disabled = true;
  try {
    const values = await Promise.all([get('info', signal), get(`stats?${query}`, signal),
      byId('section-jobs-body').hidden ? null : get(`jobs?${jobQuery}`, signal, true), get('workers', signal), get('database', signal), get(`recent-jobs?${recentQuery}`, signal)]);
    if (current !== generation) return;
    render(...values);
    byId('message').textContent = '';
    await Promise.all([...openLogs].filter(([, viewer]) => viewer.row.isConnected &&
      !viewer.row.closest('.section-body').hidden).map(([id, viewer]) => refreshLogs(id, viewer)));
  } catch (error) {
    if (current === generation && [401, 403].includes(error.status)) {
      signOut();
      byId('message').textContent = error.message;
      return;
    }
    if (error.name !== 'AbortError' && current === generation)
      byId('message').textContent = `Refresh failed: ${error.message}. Previously displayed data may be stale.`;
  } finally {
    if (current === generation) {
      byId('jobs-previous').disabled = jobPage === 0;
      byId('jobs-next').disabled = !jobsHasMore;
    }
    if (current === generation && key && !document.hidden) timer = setTimeout(refresh, interval);
  }
}
async function connect(value) {
  key = value;
  const current = ++generation;
  controller?.abort(); controller = new AbortController();
  try {
    const [principals, engineOptions] = await Promise.all([
      get('principals', controller.signal), get('engines', controller.signal)]);
    if (current !== generation) return;
    engines = engineOptions;
    for (const prefix of ['', 'job-', 'recent-']) {
      options(`${prefix}owner`, principals.filter(principal => ['user', 'manager', 'admin'].includes(principal.role)), 'All owners');
      options(`${prefix}worker`, principals.filter(principal => principal.role === 'worker'), 'All workers');
      options(`${prefix}engine`, [...new Set(engines.map(engine => engine.name))].map(name => ({id: name, label: name})), 'All engines');
      engineVersions(prefix);
    }
    const persisted = storeKey(key);
    byId('login').hidden = true; byId('logout').hidden = false;
    await refresh();
    if (!persisted && key === value)
      byId('message').textContent = 'Browser storage is unavailable. You will need to enter the API key again after reloading.';
  } catch (error) {
    if (current === generation) {
      key = '';
      if ([401, 403].includes(error.status)) storeKey('');
      byId('message').textContent = error.message;
    }
  }
}
byId('login').addEventListener('submit', event => {
  event.preventDefault();
  const value = byId('api-key').value.trim();
  byId('api-key').value = '';
  connect(value);
});
function signOut() {
  for (const id of openLogs.keys()) closeLogs(id);
  const cleared = storeKey('');
  key = ''; jobPage = 0; generation++; clearTimeout(timer); controller?.abort(); stats = undefined;
  byId('dashboard').hidden = true; byId('logout').hidden = true; byId('login').hidden = false;
  byId('sections-toggle').hidden = true;
  document.querySelectorAll('.section-summary').forEach(element => element.replaceChildren());
  for (const id of ['info', 'database', 'stats', 'jobs', 'recent-jobs', 'workers', 'updated', 'message']) byId(id).replaceChildren();
  if (!cleared) byId('message').textContent = 'Browser storage could not be cleared. Clear site data in your browser to remove the saved key.';
}
byId('logout').addEventListener('click', signOut);
byId('layout').addEventListener('change', () => { document.body.dataset.layout = byId('layout').value; });
for (const id of ['period', 'owner', 'worker', 'recent-limit', 'engine-version', 'recent-owner', 'recent-worker', 'recent-engine-version']) byId(id).addEventListener('change', refresh);
for (const prefix of ['', 'job-', 'recent-']) byId(`${prefix}engine`).addEventListener('change', () => {
  byId(`${prefix}engine-version`).value = '';
  engineVersions(prefix);
  if (prefix === 'job-') jobPage = 0;
  refresh();
});
byId('metric').addEventListener('change', renderStats);
for (const id of ['limit', 'job-owner', 'job-worker', 'job-engine-version', 'job-mode'])
  byId(id).addEventListener('change', () => { jobPage = 0; updateJobOrder(); refresh(); });
byId('jobs-previous').addEventListener('click', () => { if (jobPage > 0) { jobPage--; refresh(); } });
byId('jobs-next').addEventListener('click', () => { jobPage++; refresh(); });
byId('job-order').addEventListener('click', () => {
  jobOrder = jobOrder === 'desc' ? 'asc' : 'desc';
  jobPage = 0;
  updateJobOrder();
  refresh();
});
updateJobOrder();
setupSectionOrder();
setupHeaderDrawer();
document.addEventListener('visibilitychange', () => {
  if (!byId('login').hidden) return;
  if (document.hidden) { generation++; clearTimeout(timer); controller?.abort(); }
  else refresh();
});
const restoredKey = savedKey();
if (restoredKey) connect(restoredKey);
