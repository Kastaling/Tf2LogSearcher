const PROGRESS_POLL_MS = 5 * 60 * 1000;

// Monotonic id so stale /api/storage-stats responses cannot wipe a newer "Storage" shell
// after renderProgress() rebuilds the Log Library DOM (progress poll).
let _storageStatsRequestId = 0;

function fmtBytes(bytes) {
  if (bytes == null || !Number.isFinite(bytes) || bytes < 0) return '\u2014';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let v = bytes;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return (i === 0 ? v.toFixed(0) : v.toFixed(2)) + '\u00a0' + units[i];
}

function appendProgressRow(tbody, label, valueText) {
  const tr = document.createElement('tr');
  const th = document.createElement('th');
  th.scope = 'row';
  th.textContent = label;
  const td = document.createElement('td');
  td.className = 'download-progress-value';
  td.textContent = valueText;
  tr.appendChild(th);
  tr.appendChild(td);
  tbody.appendChild(tr);
}

function appendLogsThisUpdateRow(tbody, data) {
  const tr = document.createElement('tr');
  const th = document.createElement('th');
  th.scope = 'row';
  th.textContent = 'Logs this update';
  const td = document.createElement('td');
  td.className = 'download-progress-value download-progress-value--stacked';
  const j = data.logs_json_this_update;
  const r = data.logs_raw_this_update;
  const legacy = data.logs_downloaded_since_last_update;
  const hasNewJson = j != null && typeof j === 'number' && Number.isFinite(j);
  const hasNewRaw = r != null && typeof r === 'number' && Number.isFinite(r);
  const dlJson = data.download_json_enabled !== false;
  const dlRaw = data.download_raw_enabled === true;
  const main = document.createElement('div');
  main.className = 'download-progress-log-line-primary';
  if (hasNewJson || hasNewRaw) {
    const parts = [];
    if (dlJson && hasNewJson) parts.push('JSON ' + fmtProgressNum(j));
    if (dlRaw && hasNewRaw) parts.push('Raw ' + fmtProgressNum(r));
    main.textContent = parts.length ? parts.join(' \u00b7 ') : '\u2014';
  } else if (legacy != null && typeof legacy === 'number' && Number.isFinite(legacy)) {
    main.textContent = fmtProgressNum(legacy);
  } else {
    main.textContent = 'N/A';
  }
  td.appendChild(main);
  function nz(v) {
    if (v == null || typeof v !== 'number' || !Number.isFinite(v)) return 0;
    return Math.trunc(v);
  }
  const jf = nz(data.logs_json_failed_this_update);
  const rfz = nz(data.raw_failed_zip_this_update);
  const rfs = nz(data.raw_failed_save_this_update);
  const rfx = nz(data.raw_failed_extract_this_update);
  const rfi = nz(data.raw_failed_index_this_update);
  const failBits = [];
  if (jf > 0) failBits.push('JSON fetch failed ' + fmtProgressNum(jf));
  const rawParts = [];
  if (rfz > 0) rawParts.push('zip/download ' + fmtProgressNum(rfz));
  if (rfs > 0) rawParts.push('save ' + fmtProgressNum(rfs));
  if (rfx > 0) rawParts.push('read zip ' + fmtProgressNum(rfx));
  if (rfi > 0) rawParts.push('parse/DB ' + fmtProgressNum(rfi));
  if (rawParts.length) failBits.push('Raw: ' + rawParts.join(', '));
  if (failBits.length) {
    const sub = document.createElement('div');
    sub.className = 'download-progress-log-line-sub';
    sub.textContent = 'Issues: ' + failBits.join(' \u00b7 ');
    td.appendChild(sub);
  }
  tr.appendChild(th);
  tr.appendChild(td);
  tbody.appendChild(tr);
}

function appendIndexedPlaceholderRow(tbody, rowId, extraClass, label) {
  const tr = document.createElement('tr');
  tr.id = rowId;
  tr.className = 'download-progress-indexed-row' + (extraClass ? ' ' + extraClass : '');
  const th = document.createElement('th');
  th.scope = 'row';
  th.textContent = label;
  const td = document.createElement('td');
  td.className = 'download-progress-value';
  td.setAttribute('aria-busy', 'true');
  td.textContent = 'Loading…';
  tr.appendChild(th);
  tr.appendChild(td);
  tbody.appendChild(tr);
}

function buildIndexedLibrariesTbody() {
  const tbody = document.createElement('tbody');
  tbody.id = 'downloadProgressIndexedLibrariesTbody';
  appendIndexedPlaceholderRow(tbody, 'downloadProgressChatRow', 'download-progress-chat-row', 'Indexed chat lines (chat.db)');
  appendIndexedPlaceholderRow(tbody, 'downloadProgressRawLogsRow', 'download-progress-row--raw', 'Raw logs indexed (raw_events.db)');
  appendIndexedPlaceholderRow(tbody, 'downloadProgressRawKillsRow', 'download-progress-row--raw', 'Kill events indexed (raw_logs)');
  appendIndexedPlaceholderRow(tbody, 'downloadProgressLogPlayersRow', '', 'Stats DB rows (log_players)');
  appendIndexedPlaceholderRow(tbody, 'downloadProgressLeaderboardRow', '', 'Leaderboard players (player_stats_agg)');
  return tbody;
}

function fetchChatMessageCountAndPatch() {
  const row = document.getElementById('downloadProgressChatRow');
  if (!row) return;
  const td = row.querySelector('td.download-progress-value');
  fetch('/api/chat-message-count')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      if (!row.isConnected || !td) return;
      td.removeAttribute('aria-busy');
      if (d && d.chat_message_count != null && typeof d.chat_message_count === 'number' && Number.isFinite(d.chat_message_count)) {
        td.textContent = fmtProgressNum(d.chat_message_count);
      } else {
        td.textContent = '\u2014';
      }
    })
    .catch(function() {
      if (!row.isConnected || !td) return;
      td.removeAttribute('aria-busy');
      td.textContent = '\u2014';
    });
}

function fetchRawEventsStatsAndPatch() {
  const rLogs = document.getElementById('downloadProgressRawLogsRow');
  const rKills = document.getElementById('downloadProgressRawKillsRow');
  if (!rLogs || !rKills) return;
  const tdLogs = rLogs.querySelector('td.download-progress-value');
  const tdKills = rKills.querySelector('td.download-progress-value');
  fetch('/api/raw-events-stats')
    .then(function(res) { return res.ok ? res.json() : null; })
    .then(function(d) {
      if (!rLogs.isConnected || !rKills.isConnected || !tdLogs || !tdKills) return;
      if (!d || d.download_raw_enabled === false) {
        rLogs.style.display = 'none';
        rKills.style.display = 'none';
        rLogs.setAttribute('aria-hidden', 'true');
        rKills.setAttribute('aria-hidden', 'true');
        tdLogs.removeAttribute('aria-busy');
        tdKills.removeAttribute('aria-busy');
        return;
      }
      tdLogs.removeAttribute('aria-busy');
      tdKills.removeAttribute('aria-busy');
      if (d.raw_logs_count != null && typeof d.raw_logs_count === 'number' && Number.isFinite(d.raw_logs_count)) {
        tdLogs.textContent = fmtProgressNum(d.raw_logs_count);
      } else {
        tdLogs.textContent = '\u2014';
      }
      if (d.kill_events_total != null && typeof d.kill_events_total === 'number' && Number.isFinite(d.kill_events_total)) {
        tdKills.textContent = fmtProgressNum(d.kill_events_total);
      } else {
        tdKills.textContent = '\u2014';
      }
    })
    .catch(function() {
      if (!rLogs.isConnected || !rKills.isConnected || !tdLogs || !tdKills) return;
      tdLogs.removeAttribute('aria-busy');
      tdKills.removeAttribute('aria-busy');
      tdLogs.textContent = '\u2014';
      tdKills.textContent = '\u2014';
    });
}

function fetchStatsIndexCountsAndPatch() {
  const rLp = document.getElementById('downloadProgressLogPlayersRow');
  const rLb = document.getElementById('downloadProgressLeaderboardRow');
  if (!rLp || !rLb) return;
  const tdLp = rLp.querySelector('td.download-progress-value');
  const tdLb = rLb.querySelector('td.download-progress-value');
  fetch('/api/stats-index-counts')
    .then(function(res) { return res.ok ? res.json() : null; })
    .then(function(d) {
      if (!rLp.isConnected || !rLb.isConnected || !tdLp || !tdLb) return;
      tdLp.removeAttribute('aria-busy');
      tdLb.removeAttribute('aria-busy');
      if (d && d.log_players_count != null && typeof d.log_players_count === 'number' && Number.isFinite(d.log_players_count)) {
        tdLp.textContent = fmtProgressNum(d.log_players_count);
      } else {
        tdLp.textContent = '\u2014';
      }
      if (d && d.leaderboard_players_count != null && typeof d.leaderboard_players_count === 'number' && Number.isFinite(d.leaderboard_players_count)) {
        tdLb.textContent = fmtProgressNum(d.leaderboard_players_count);
      } else {
        tdLb.textContent = '\u2014';
      }
    })
    .catch(function() {
      if (!rLp.isConnected || !rLb.isConnected || !tdLp || !tdLb) return;
      tdLp.removeAttribute('aria-busy');
      tdLb.removeAttribute('aria-busy');
      tdLp.textContent = '\u2014';
      tdLb.textContent = '\u2014';
    });
}

function jsonLogsDir() {
  return 'logs/';
}

function _storageStatsShellForRequest(reqId) {
  const el = document.getElementById('downloadProgressStorageBlock');
  if (!el || !el.isConnected) return null;
  if (el.getAttribute('data-storage-req-id') !== String(reqId)) return null;
  return el;
}

function _showStorageStatsLoadError(el) {
  const lw = el.querySelector('.download-progress-storage-loading');
  if (!lw) return;
  const st = lw.querySelector('.loading-state');
  if (!st) return;
  st.setAttribute('aria-busy', 'false');
  const dots = st.querySelector('.loading-dots');
  if (dots) dots.remove();
  const lbl = st.querySelector('.loading-label');
  if (lbl) lbl.textContent = 'Could not load storage sizes';
  if (st.querySelector('.download-progress-storage-error-hint')) return;
  const hint = document.createElement('span');
  hint.className = 'download-progress-storage-error-hint';
  hint.textContent = '\u00a0Will retry when log progress refreshes.';
  st.appendChild(hint);
}

function _fillStorageStatsTableIntoShell(el, d) {
  const lw = el.querySelector('.download-progress-storage-loading');
  if (lw) lw.remove();

  const tbl = document.createElement('table');
  tbl.className = 'download-progress-stats download-progress-stats--storage download-progress-storage-reveal';
  const tbody = document.createElement('tbody');

  if (d.json_logs_bytes != null) {
    appendProgressRow(tbody, 'JSON logs (' + jsonLogsDir() + ')', fmtBytes(d.json_logs_bytes));
  }
  if (d.download_raw_enabled && d.raw_logs_bytes != null) {
    appendProgressRow(tbody, 'Raw log zips', fmtBytes(d.raw_logs_bytes));
  }
  const dbLabels = {
    stats_db: 'stats.db',
    chat_db: 'chat.db',
    raw_events_db: 'raw_events.db',
    avatar_db: 'avatars.db',
  };
  const dbFiles = d.db_files || {};
  let anyDb = false;
  Object.keys(dbLabels).forEach(function(key) {
    if (key === 'raw_events_db' && !d.download_raw_enabled) return;
    const v = dbFiles[key];
    if (v == null) return;
    appendProgressRow(tbody, dbLabels[key], fmtBytes(v));
    anyDb = true;
  });
  if (anyDb && d.db_total_bytes != null) {
    appendProgressRow(tbody, 'DBs total', fmtBytes(d.db_total_bytes));
  }
  if (d.total_bytes != null) {
    const trDiv = document.createElement('tr');
    trDiv.className = 'download-progress-storage-total-row';
    const thDiv = document.createElement('th');
    thDiv.colSpan = 2;
    thDiv.className = 'download-progress-storage-divider';
    trDiv.appendChild(thDiv);
    tbody.appendChild(trDiv);
    appendProgressRow(tbody, 'Total storage', fmtBytes(d.total_bytes));
    const lastTr = tbody.querySelector('tr:last-child');
    if (lastTr) lastTr.classList.add('download-progress-storage-grand-total');
  }

  tbl.appendChild(tbody);
  el.appendChild(tbl);
}

function fetchStorageStatsAndPatch() {
  const anchor = document.getElementById('downloadProgressIndexedLibrariesTbody');
  if (!anchor) return;
  const block = anchor.closest('.download-progress-stats-block');
  if (!block) return;
  const board = block.parentNode;
  if (!board) return;

  const existing = document.getElementById('downloadProgressStorageBlock');
  if (existing) existing.remove();

  const reqId = ++_storageStatsRequestId;
  const shell = document.createElement('div');
  shell.className = 'download-progress-stats-block download-progress-stats-block--storage';
  shell.id = 'downloadProgressStorageBlock';
  shell.setAttribute('data-storage-req-id', String(reqId));

  const storageTitle = document.createElement('div');
  storageTitle.className = 'download-progress-stats-block-title';
  storageTitle.textContent = 'Storage utilization';
  shell.appendChild(storageTitle);

  const loadingWrap = document.createElement('div');
  loadingWrap.className = 'download-progress-storage-loading';
  const loadingState = document.createElement('div');
  loadingState.className = 'loading-state';
  loadingState.setAttribute('role', 'status');
  loadingState.setAttribute('aria-live', 'polite');
  loadingState.setAttribute('aria-busy', 'true');
  const loadingLabel = document.createElement('span');
  loadingLabel.className = 'loading-label';
  loadingLabel.textContent = 'Measuring disk usage';
  const dots = document.createElement('span');
  dots.className = 'loading-dots';
  dots.setAttribute('aria-hidden', 'true');
  for (let i = 0; i < 3; i++) {
    const dot = document.createElement('span');
    dot.textContent = '.';
    dots.appendChild(dot);
  }
  loadingState.appendChild(loadingLabel);
  loadingState.appendChild(dots);
  loadingWrap.appendChild(loadingState);
  shell.appendChild(loadingWrap);

  board.insertBefore(shell, block.nextSibling);

  fetch('/api/storage-stats')
    .then(function(res) { return res.ok ? res.json() : Promise.resolve(null); })
    .then(function(d) {
      const el = _storageStatsShellForRequest(reqId);
      if (!el) return;
      if (d && d.enabled === false) {
        el.remove();
        return;
      }
      if (!d || d.enabled !== true) {
        _showStorageStatsLoadError(el);
        return;
      }
      _fillStorageStatsTableIntoShell(el, d);
    })
    .catch(function() {
      const el = _storageStatsShellForRequest(reqId);
      if (!el) return;
      _showStorageStatsLoadError(el);
    });
}

function fetchIndexedLibraryStatsAndPatch() {
  fetchChatMessageCountAndPatch();
  fetchRawEventsStatsAndPatch();
  fetchStatsIndexCountsAndPatch();
  fetchStorageStatsAndPatch();
}

function renderProgress(data) {
  const el = document.getElementById('downloadProgress');
  if (!el) return;

  if (data.backfill_complete) {
    el.className = 'download-progress download-progress--panel';
    el.innerHTML = '';
    const wrap = document.createElement('div');
    wrap.className = 'download-progress-inner download-progress-inner--wide download-progress-inner--center';
    const p = document.createElement('p');
    p.className = 'download-progress-complete-msg';
    p.appendChild(document.createTextNode('Full backfill complete. New logs are added periodically. You can search across all logs we have. ('));
    const strong = document.createElement('strong');
    strong.textContent = '100% complete';
    p.appendChild(strong);
    p.appendChild(document.createTextNode(')'));
    wrap.appendChild(p);
    const board = document.createElement('div');
    board.className = 'download-progress-stats-board download-progress-stats-board--indexed-only';
    const blockIdx = document.createElement('div');
    blockIdx.className = 'download-progress-stats-block';
    const titleIdx = document.createElement('div');
    titleIdx.className = 'download-progress-stats-block-title';
    titleIdx.textContent = 'Indexed libraries';
    const tableIdx = document.createElement('table');
    tableIdx.className = 'download-progress-stats download-progress-stats--indexed';
    tableIdx.appendChild(buildIndexedLibrariesTbody());
    blockIdx.appendChild(titleIdx);
    blockIdx.appendChild(tableIdx);
    board.appendChild(blockIdx);
    wrap.appendChild(board);
    el.appendChild(wrap);
    fetchIndexedLibraryStatsAndPatch();
    return;
  }

  let pct = null;
  if (data.max_id != null && data.max_id > 0) {
    const remaining = data.remaining != null ? Number(data.remaining) : 0;
    const maxId = Number(data.max_id);
    pct = ((maxId - remaining) / maxId) * 100;
    if (!Number.isFinite(pct)) pct = null;
    else pct = Math.max(0, Math.min(100, pct));
  }

  const hasStats =
    data.total_files != null ||
    (data.min_id != null && data.max_id != null) ||
    (data.remaining != null && data.remaining > 0) ||
    data.eta_human ||
    data.rate_logs_per_sec != null ||
    data.rate_logs_per_sec_aggregated != null ||
    data.updated_at ||
    data.earliest_log_timestamp != null ||
    data.logs_downloaded_since_last_update != null ||
    data.logs_json_this_update != null ||
    data.logs_raw_this_update != null;

  if (!hasStats && pct == null) {
    el.className = 'download-progress';
    el.textContent = 'Progress not available yet.';
    return;
  }

  el.className = 'download-progress download-progress--panel';
  el.innerHTML = '';
  const wrap = document.createElement('div');
  wrap.className = 'download-progress-inner download-progress-inner--wide';

  if (pct != null) {
    const barSection = document.createElement('div');
    barSection.className = 'download-progress-bar-section';
    const track = document.createElement('div');
    track.className = 'download-progress-bar-track';
    track.setAttribute('role', 'progressbar');
    track.setAttribute('aria-valuenow', String(Math.round(pct * 100) / 100));
    track.setAttribute('aria-valuemin', '0');
    track.setAttribute('aria-valuemax', '100');
    track.setAttribute('aria-label', 'Log backfill progress');
    const fill = document.createElement('div');
    fill.className = 'download-progress-bar-fill';
    fill.style.width = pct.toFixed(2) + '%';
    track.appendChild(fill);
    const pctLabel = document.createElement('div');
    pctLabel.className = 'download-progress-pct-label';
    pctLabel.textContent = pct.toFixed(2) + '% complete';
    barSection.appendChild(track);
    barSection.appendChild(pctLabel);
    wrap.appendChild(barSection);
  }

  const board = document.createElement('div');
  board.className = 'download-progress-stats-board';

  const blockQueue = document.createElement('div');
  blockQueue.className = 'download-progress-stats-block';
  const titleQueue = document.createElement('div');
  titleQueue.className = 'download-progress-stats-block-title';
  titleQueue.textContent = 'Download queue';
  const tableQueue = document.createElement('table');
  tableQueue.className = 'download-progress-stats';
  const tbodyQueue = document.createElement('tbody');

  if (data.total_files != null) {
    appendProgressRow(tbodyQueue, 'Log files', fmtProgressNum(Number(data.total_files)) + ' indexed');
  }
  if (data.min_id != null && data.max_id != null) {
    appendProgressRow(
      tbodyQueue,
      'ID range',
      fmtProgressNum(Number(data.max_id)) + ' \u2013 ' + fmtProgressNum(Number(data.min_id))
    );
  }
  if (data.remaining != null && data.remaining > 0) {
    appendProgressRow(tbodyQueue, 'Remaining to log 1', fmtProgressNum(Number(data.remaining)));
  }
  if (data.eta_human) {
    appendProgressRow(tbodyQueue, 'ETA', String(data.eta_human));
  }
  if (data.rate_logs_per_sec != null || data.rate_logs_per_sec_aggregated != null) {
    let rateText = '';
    if (data.rate_logs_per_sec_aggregated != null && data.rate_logs_per_sec != null) {
      rateText = String(data.rate_logs_per_sec) + ' logs/s, avg ' + String(data.rate_logs_per_sec_aggregated);
    } else if (data.rate_logs_per_sec_aggregated != null) {
      rateText = String(data.rate_logs_per_sec_aggregated) + ' logs/s avg';
    } else {
      rateText = String(data.rate_logs_per_sec) + ' logs/s';
    }
    appendProgressRow(tbodyQueue, 'Download rate', rateText);
  }
  if (data.updated_at) {
    appendProgressRow(tbodyQueue, 'Last updated', formatUpdatedAt(data.updated_at));
  }
  if (data.earliest_log_timestamp != null) {
    const earliestStr = formatEarliestLogDate(data.earliest_log_timestamp);
    if (earliestStr) appendProgressRow(tbodyQueue, 'Earliest log', earliestStr);
  }
  appendLogsThisUpdateRow(tbodyQueue, data);
  tableQueue.appendChild(tbodyQueue);
  blockQueue.appendChild(titleQueue);
  blockQueue.appendChild(tableQueue);

  const blockIdx = document.createElement('div');
  blockIdx.className = 'download-progress-stats-block';
  const titleIdx = document.createElement('div');
  titleIdx.className = 'download-progress-stats-block-title';
  titleIdx.textContent = 'Indexed libraries';
  const tableIdx = document.createElement('table');
  tableIdx.className = 'download-progress-stats download-progress-stats--indexed';
  tableIdx.appendChild(buildIndexedLibrariesTbody());
  blockIdx.appendChild(titleIdx);
  blockIdx.appendChild(tableIdx);

  board.appendChild(blockQueue);
  board.appendChild(blockIdx);
  wrap.appendChild(board);
  el.appendChild(wrap);
  fetchIndexedLibraryStatsAndPatch();
}


async function fetchProgress() {
  try {
    const r = await fetch('/api/download-progress');
    const data = r.ok ? await r.json() : null;
    const el = document.getElementById('downloadProgress');
    if (!el) return null;
    if (!r.ok || !data) {
      el.textContent = 'Progress not available yet.';
      return null;
    }
    renderProgress(data);
    return data;
  } catch (_) {
    const el = document.getElementById('downloadProgress');
    if (el) el.textContent = 'Progress not available yet.';
    return null;
  }
}

(function initProgress() {
  fetchProgress().then(function(data) {
    if (data && !data.backfill_complete) {
      setInterval(fetchProgress, PROGRESS_POLL_MS);
    }
  });
})();
