var leaderboardSortCol = null;
var leaderboardSortDir = -1;

function leaderboardDefaultSortKey(lbType) {
  var t = sanitizeLbTypeInput(lbType);
  if (t === 'kdr') return 'avg_kdr';
  if (t === 'winrate') return 'win_rate';
  if (t === 'logs') return 'log_count';
  if (t === 'ubers' || t === 'drops' || t === 'damage_taken' || t === 'avg_deaths') return 'primary_value';
  return 'avg_dpm';
}

var LEADERBOARD_COLUMNS_PREFIX = [
  { key: 'rank', label: 'Rank', type: 'rank' },
  { key: 'name', label: 'Name', type: 'name' },
  { key: 'log_count', label: 'Logs', type: 'number' },
];
var LEADERBOARD_COLUMNS_SUFFIX = [
  { key: 'avg_dpm', label: 'Avg DPM', type: 'number' },
  { key: 'avg_kdr', label: 'Avg KDR', type: 'number' },
  { key: 'avg_kadr', label: 'Avg KADR', type: 'number' },
  { key: 'win_rate', label: 'Win Rate', type: 'winrate' },
];

function getLeaderboardPrimaryColumn(lbType, statScope) {
  var t = sanitizeLbTypeInput(lbType);
  var ss = sanitizeLeaderboardStatScopeInput(statScope, t);
  if (t === 'ubers') {
    if (ss === 'per_log') return { key: 'primary_value', label: 'Ubers / log', type: 'decimal2' };
    return { key: 'primary_value', label: 'Ubers (total)', type: 'number' };
  }
  if (t === 'drops') {
    if (ss === 'per_log') return { key: 'primary_value', label: 'Drops / log', type: 'decimal2' };
    return { key: 'primary_value', label: 'Drops (total)', type: 'number' };
  }
  if (t === 'damage_taken') {
    if (ss === 'per_log') return { key: 'primary_value', label: 'Damage taken / log', type: 'decimal2' };
    return { key: 'primary_value', label: 'Damage taken (total)', type: 'number' };
  }
  if (t === 'avg_deaths') return { key: 'primary_value', label: 'Deaths/log', type: 'decimal2' };
  return null;
}

function getLeaderboardColumns(lbType, statScope) {
  var t = sanitizeLbTypeInput(lbType);
  var cols = LEADERBOARD_COLUMNS_PREFIX.slice();
  var prim = getLeaderboardPrimaryColumn(t, statScope);
  if (prim) cols.push(prim);
  return cols.concat(LEADERBOARD_COLUMNS_SUFFIX);
}

var LEADERBOARD_CSV_SUFFIX = [
  { key: 'avg_dpm', label: 'Avg DPM' },
  { key: 'avg_kdr', label: 'Avg KDR' },
  { key: 'avg_kadr', label: 'Avg KADR' },
  { key: 'win_rate_csv', label: 'Win Rate' },
];

function getLeaderboardCsvColumns(lbType, statScope) {
  var t = sanitizeLbTypeInput(lbType);
  var out = [
    { key: 'rank', label: 'Rank' },
    { key: 'name', label: 'Name' },
    { key: 'steamid64', label: 'SteamID64' },
    { key: 'log_count', label: 'Logs' },
  ];
  var prim = getLeaderboardPrimaryColumn(t, statScope);
  if (prim) out.push({ key: 'primary_value', label: prim.label });
  return out.concat(LEADERBOARD_CSV_SUFFIX);
}

var CHAT_LB_CSV_COLUMNS = [
  { key: 'rank', label: 'Rank' },
  { key: 'name', label: 'Name' },
  { key: 'steamid64', label: 'SteamID64' },
  { key: 'occurrences', label: 'Occurrences' },
  { key: 'logs_count', label: 'Logs' },
  { key: 'word_per_log', label: 'Word / log' },
  { key: 'top_log_id', label: 'Top log ID' },
  { key: 'top_log_url', label: 'Top log URL' },
];

function leaderboardWinRateCell(row) {
  var w = row.win_rate;
  if (w == null || !Number.isFinite(Number(w))) return '\u2014';
  return (Math.round(Number(w) * 10000) / 100) + '%';
}

function getLeaderboardSortValue(row, key, type) {
  if (type === 'rank') return 0;
  if (key === 'win_rate') {
    var wr = Number(row.win_rate);
    return Number.isFinite(wr) ? wr : -Infinity;
  }
  if (type === 'decimal2' || type === 'number') {
    return getSortValue(row, key, 'number');
  }
  if (type === 'name') {
    return getSortValue(row, 'name', 'text');
  }
  return getSortValue(row, key, type);
}

function getSortedLeaderboardRows(originalRows, lbType, statScope) {
  var t = sanitizeLbTypeInput(lbType || 'dpm');
  var ss = sanitizeLeaderboardStatScopeInput(statScope, t);
  var sortedRows = originalRows.slice();
  var sortCol = leaderboardSortCol;
  var sortDir = leaderboardSortDir;
  var colDef = getLeaderboardColumns(t, ss).find(function(c) { return c.key === sortCol; });
  if (colDef && colDef.type !== 'rank') {
    sortedRows.sort(function(a, b) {
      var va = getLeaderboardSortValue(a, sortCol, colDef.type);
      var vb = getLeaderboardSortValue(b, sortCol, colDef.type);
      if (colDef.type === 'text' || colDef.type === 'name') {
        if (va < vb) return -sortDir;
        if (va > vb) return sortDir;
        return 0;
      }
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    });
  }
  return sortedRows;
}

function buildLeaderboardCsvContent(sortedRows, lbType, statScope) {
  var t = sanitizeLbTypeInput(lbType || 'dpm');
  var ss = sanitizeLeaderboardStatScopeInput(statScope, t);
  var cols = getLeaderboardCsvColumns(t, ss);
  var header = cols.map(function(c) {
    return escapeCsvField(c.label);
  }).join(',');
  var lines = [header];
  for (var i = 0; i < sortedRows.length; i++) {
    var row = sortedRows[i];
    var rank = i + 1;
    var parts = [];
    for (var j = 0; j < cols.length; j++) {
      var ck = cols[j].key;
      var raw;
      if (ck === 'rank') raw = rank;
      else if (ck === 'win_rate_csv') {
        var w = row.win_rate;
        raw = (w != null && Number.isFinite(Number(w))) ? Number(w) : '';
      } else raw = row[ck];
      parts.push(statsValueForCsvCell(raw));
    }
    lines.push(parts.join(','));
  }
  return '\uFEFF' + lines.join('\r\n');
}

function leaderboardLeaderCellHtml(row, colDef) {
  var key = colDef.key;
  var type = colDef.type;
  if (type === 'name') return coplayersNameCellHtml(row);
  if (key === 'win_rate' || type === 'winrate') return escapeHtml(leaderboardWinRateCell(row));
  if (key === 'primary_value') {
    var v = row.primary_value;
    if (v == null || !Number.isFinite(Number(v))) return '\u2014';
    if (type === 'decimal2') return escapeHtml(String(Math.round(Number(v) * 100) / 100));
    return escapeHtml(String(Math.round(Number(v))));
  }
  var raw = row[key];
  if (raw == null || raw === '') return '\u2014';
  return escapeHtml(String(raw));
}

function bindLeaderboardCsvDownload(container) {
  var btn = container.querySelector('.js-leaderboard-csv-download');
  if (!btn || btn.getAttribute('data-csv-bound')) return;
  btn.setAttribute('data-csv-bound', '1');
  btn.addEventListener('click', function() {
    var rows = container._leaderboardRows;
    if (!rows || !rows.length) return;
    var lb = container._leaderboardLbType || 'dpm';
    var sc = container._leaderboardStatScope != null ? container._leaderboardStatScope : 'total';
    var sorted = getSortedLeaderboardRows(rows, lb, sc);
    var csv = buildLeaderboardCsvContent(sorted, lb, sc);
    var d = new Date();
    var scopePart = (lb === 'ubers' || lb === 'drops' || lb === 'damage_taken' || lb === 'winrate') ? '-' + String(sc).replace(/[^a-z0-9_-]/gi, '_') : '';
    var fn = 'tf2-stats-leaderboard-' + lb + scopePart + '-' + d.getFullYear() + '-' + (d.getMonth() + 1 < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate() + '.csv';
    triggerCsvDownload(fn, csv);
  });
}

function sanitizeChatLbWordForFilename(w) {
  var s = String(w || '').trim().slice(0, 60);
  if (!s) return 'word';
  var t = s.replace(/[^a-zA-Z0-9\-_]+/g, '_').replace(/^_+|_+$/g, '');
  return t || 'word';
}

function buildChatLeaderboardCsvContent(sortedRows) {
  var header = CHAT_LB_CSV_COLUMNS.map(function(c) {
    return escapeCsvField(c.label);
  }).join(',');
  var lines = [header];
  for (var i = 0; i < sortedRows.length; i++) {
    var row = sortedRows[i];
    var rank = i + 1;
    var sid64 = row.steamid64 != null ? String(row.steamid64).trim() : '';
    var displayName = row.name != null ? String(row.name) : '';
    if (!displayName) displayName = sid64 || 'Unknown';
    var parts = [];
    for (var j = 0; j < CHAT_LB_CSV_COLUMNS.length; j++) {
      var ck = CHAT_LB_CSV_COLUMNS[j].key;
      var raw;
      if (ck === 'rank') raw = rank;
      else if (ck === 'name') raw = displayName;
      else if (ck === 'word_per_log') {
        var opl = Number(row.word_per_log);
        raw = Number.isFinite(opl) ? opl : '';
      } else raw = row[ck];
      parts.push(statsValueForCsvCell(raw));
    }
    lines.push(parts.join(','));
  }
  return '\uFEFF' + lines.join('\r\n');
}

function bindChatLeaderboardCsvDownload(el) {
  var btn = el.querySelector('.js-chat-leaderboard-csv-download');
  if (!btn || btn.getAttribute('data-csv-bound')) return;
  btn.setAttribute('data-csv-bound', '1');
  btn.addEventListener('click', function() {
    var rows = el._chatLbRows;
    if (!rows || !rows.length) return;
    var sorted = getSortedChatLeaderboardRows(rows);
    var csv = buildChatLeaderboardCsvContent(sorted);
    var d = new Date();
    var wordPart = sanitizeChatLbWordForFilename(el._chatLbWord);
    var fn = 'tf2-chat-leaderboard-' + wordPart + '-' + d.getFullYear() + '-' + (d.getMonth() + 1 < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate() + '.csv';
    triggerCsvDownload(fn, csv);
  });
}

function renderLeaderboard(container, data, elapsedMs) {
  var rows = data && data.rows ? data.rows : [];
  var totalLogs = data && data.total_logs != null ? data.total_logs : null;
  var lbType = data && data.lb_type ? sanitizeLbTypeInput(data.lb_type) : 'dpm';
  var statScope = sanitizeLeaderboardStatScopeInput(
    data && data.stat_scope != null ? data.stat_scope : (sanitizeLbTypeInput(lbType) === 'winrate' ? 'highest' : 'total'),
    lbType
  );
  if (rows !== container._leaderboardRows || lbType !== container._leaderboardLbType || statScope !== container._leaderboardStatScope) {
    container._leaderboardRows = rows;
    container._leaderboardLbType = lbType;
    container._leaderboardStatScope = statScope;
    leaderboardSortCol = leaderboardDefaultSortKey(lbType);
    leaderboardSortDir = -1;
  }
  if (typeof elapsedMs === 'number' && Number.isFinite(elapsedMs)) {
    container._leaderboardRequestMs = elapsedMs;
  }
  container._leaderboardTotalLogs = totalLogs;

  var originalRows = container._leaderboardRows;
  if (!originalRows.length) {
    var emptyMsg = '<p class="stats-summary-meta">No players match these filters.</p>';
    if (totalLogs != null && Number.isFinite(Number(totalLogs))) {
      emptyMsg += '<p class="stats-summary-meta">Logs matching filters in database: ' + escapeHtml(String(totalLogs)) + '</p>';
    }
    container.innerHTML = emptyMsg + requestTimingFooter(container._leaderboardRequestMs);
    return;
  }

  var sortedRows = getSortedLeaderboardRows(originalRows, lbType, statScope);
  var sortCol = leaderboardSortCol;
  var sortDir = leaderboardSortDir;
  var lbCols = getLeaderboardColumns(lbType, statScope);

  var totalLine = '<p class="stats-summary-meta">Top ' + escapeHtml(String(originalRows.length)) + ' players';
  if (totalLogs != null && Number.isFinite(Number(totalLogs))) {
    totalLine += ' across ' + escapeHtml(String(totalLogs)) + ' log(s)';
  }
  totalLine += '.</p>';

  var thead = '<tr>';
  lbCols.forEach(function(c) {
    if (c.type === 'rank') {
      thead += '<th scope="col">' + escapeHtml(c.label) + '</th>';
      return;
    }
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';

  var tbody = '';
  sortedRows.forEach(function(x, idx) {
    var rank = idx + 1;
    tbody += '<tr>';
    lbCols.forEach(function(c) {
      if (c.key === 'rank') {
        tbody += '<td>' + escapeHtml(String(rank)) + '</td>';
        return;
      }
      tbody += '<td>' + leaderboardLeaderCellHtml(x, c) + '</td>';
    });
    tbody += '</tr>';
  });

  container.innerHTML =
    totalLine +
    '<div class="stats-csv-toolbar"><button type="button" class="js-leaderboard-csv-download" aria-label="Download leaderboard table as a CSV file">Download as CSV</button></div>' +
    '<div class="stats-table-wrap"><table class="stats-table playername-results-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>' +
    requestTimingFooter(container._leaderboardRequestMs);
  container._leaderboardRows = originalRows;

  bindLeaderboardCsvDownload(container);
  loadAvatarsInContainer(container);

  container.querySelectorAll('th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var col = th.getAttribute('data-col');
      if (!col) return;
      if (leaderboardSortCol === col) {
        leaderboardSortDir = leaderboardSortDir === 1 ? -1 : 1;
      } else {
        leaderboardSortCol = col;
        leaderboardSortDir = 1;
      }
      renderLeaderboard(container, {
        rows: container._leaderboardRows,
        total_logs: container._leaderboardTotalLogs,
        lb_type: container._leaderboardLbType,
        stat_scope: container._leaderboardStatScope,
      }, container._leaderboardRequestMs);
    });
  });
}
