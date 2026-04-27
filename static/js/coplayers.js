var coplayersSortCol = 'total_games';
var coplayersSortDir = -1;

var COPLAYERS_COLUMNS = [
  { key: 'name', label: 'Name', type: 'text' },
  { key: 'total_games', label: 'Total', type: 'number' },
  { key: 'games_with', label: 'With', type: 'number' },
  { key: 'wins_with', label: 'Wins with', type: 'number' },
  { key: 'losses_with', label: 'Losses with', type: 'number' },
  { key: 'winpct_with', label: 'Win% with', type: 'winpct' },
  { key: 'games_against', label: 'Against', type: 'number' },
  { key: 'wins_against', label: 'Wins against', type: 'number' },
  { key: 'losses_against', label: 'Losses against', type: 'number' },
  { key: 'winpct_against', label: 'Win% against', type: 'winpct' },
  { key: 'actions', label: 'Search', type: 'actions' },
];

var COPLAYERS_CSV_COLUMNS = [
  { key: 'name', label: 'Name' },
  { key: 'total_games', label: 'Total games' },
  { key: 'games_with', label: 'Games with' },
  { key: 'wins_with', label: 'Wins with' },
  { key: 'losses_with', label: 'Losses with' },
  { key: '_csv_winpct_with', label: 'Win% with' },
  { key: 'games_against', label: 'Games against' },
  { key: 'wins_against', label: 'Wins against' },
  { key: 'losses_against', label: 'Losses against' },
  { key: '_csv_winpct_against', label: 'Win% against' },
  { key: 'steamid3', label: 'SteamID3' },
  { key: 'steamid64', label: 'SteamID64' },
];

/** Internal profile link for co-players table and leaderboard. */
function coplayersNameCellHtml(row) {
  var name = row.name != null ? String(row.name) : '';
  var sid64 = row.steamid64 != null ? String(row.steamid64).trim() : '';
  var display = name.trim() ? name : (sid64 || '\u2014');
  var avatar = steamAvatarPlaceholder(sid64);
  if (/^\d{17}$/.test(sid64)) {
    var href = internalProfileHref(sid64);
    return avatar + '<a href="' + escapeAttr(href) + '">' + escapeHtml(display) + '</a>';
  }
  return avatar + escapeHtml(display);
}

function coplayersWinPctSortValue(row, which) {
  if (which === 'with') {
    var w = Number(row.wins_with) || 0;
    var l = Number(row.losses_with) || 0;
    if (w + l === 0) return NaN;
    return w / (w + l);
  }
  var w = Number(row.wins_against) || 0;
  var l = Number(row.losses_against) || 0;
  if (w + l === 0) return NaN;
  return w / (w + l);
}

function coplayersWinPctDisplay(row, which) {
  var r = coplayersWinPctSortValue(row, which);
  if (Number.isNaN(r)) return '\u2014';
  return String(Math.round(r * 100)) + '%';
}

function coplayersWinPctCsvDecimal(row, which) {
  var r = coplayersWinPctSortValue(row, which);
  if (Number.isNaN(r)) return '';
  return String(r);
}

function getCoplayersSortValue(row, colKey, type) {
  if (type === 'actions') {
    return '';
  }
  if (type === 'winpct') {
    return coplayersWinPctSortValue(row, colKey === 'winpct_with' ? 'with' : 'against');
  }
  if (type === 'number') {
    var n = Number(row[colKey]);
    return Number.isNaN(n) ? -Infinity : n;
  }
  var v = row[colKey];
  return v != null ? String(v).toLowerCase() : '';
}

function compareCoplayersSort(va, vb, type, sortDir) {
  if (type === 'winpct') {
    var aNaN = Number.isNaN(va);
    var bNaN = Number.isNaN(vb);
    if (aNaN && bNaN) return 0;
    if (aNaN) return 1;
    if (bNaN) return -1;
  }
  if (type === 'text') {
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  }
  if (va < vb) return -sortDir;
  if (va > vb) return sortDir;
  return 0;
}

function getSortedCoplayersRows(originalRows) {
  var sortedRows = originalRows.slice();
  var sortCol = coplayersSortCol;
  var sortDir = coplayersSortDir;
  var colDef = COPLAYERS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (colDef && colDef.type !== 'actions') {
    sortedRows.sort(function(a, b) {
      var va = getCoplayersSortValue(a, sortCol, colDef.type);
      var vb = getCoplayersSortValue(b, sortCol, colDef.type);
      return compareCoplayersSort(va, vb, colDef.type, sortDir);
    });
  }
  return sortedRows;
}

function buildCoplayersCsvContent(sortedRows) {
  var header = COPLAYERS_CSV_COLUMNS.map(function(c) {
    return escapeCsvField(c.label);
  }).join(',');
  var lines = [header];
  for (var i = 0; i < sortedRows.length; i++) {
    var row = sortedRows[i];
    var parts = [];
    for (var j = 0; j < COPLAYERS_CSV_COLUMNS.length; j++) {
      var ck = COPLAYERS_CSV_COLUMNS[j].key;
      var raw;
      if (ck === '_csv_winpct_with') raw = coplayersWinPctCsvDecimal(row, 'with');
      else if (ck === '_csv_winpct_against') raw = coplayersWinPctCsvDecimal(row, 'against');
      else raw = row[ck];
      parts.push(statsValueForCsvCell(raw));
    }
    lines.push(parts.join(','));
  }
  return '\uFEFF' + lines.join('\r\n');
}

function bindCoplayersCsvDownload(container) {
  var btn = container.querySelector('.js-coplayers-csv-download');
  if (!btn || btn.getAttribute('data-csv-bound')) return;
  btn.setAttribute('data-csv-bound', '1');
  btn.addEventListener('click', function() {
    var rows = container._coplayersRows;
    if (!rows || !rows.length) return;
    var sorted = getSortedCoplayersRows(rows);
    var csv = buildCoplayersCsvContent(sorted);
    var d = new Date();
    var fn = 'tf2-coplayers-' + d.getFullYear() + '-' + (d.getMonth() + 1 < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate() + '.csv';
    triggerCsvDownload(fn, csv);
  });
}

/** Home URL for logmatch with searched player + co-player (newline-separated steamids param). */
function coplayersLogmatchUrl(searchedSteamid64, rowSteamid64) {
  var base = window.location.origin + '/';
  var a = searchedSteamid64 != null ? String(searchedSteamid64).trim() : '';
  var b = rowSteamid64 != null ? String(rowSteamid64).trim() : '';
  if (!/^\d{17}$/.test(a) || !/^\d{17}$/.test(b)) return '';
  return base + '?mode=logmatch&steamids=' + encodeURIComponent(a + '\n' + b);
}

function coplayersSearchActionsHtml(row, searchedSteamid64) {
  var base = window.location.origin + '/';
  var sid = row.steamid64 != null ? String(row.steamid64).trim() : '';
  if (!/^\d{17}$/.test(sid)) return '';
  var chatHref = base + '?mode=chat&steamid=' + encodeURIComponent(sid);
  var statsHref = base + '?mode=stats&steamid=' + encodeURIComponent(sid);
  var coplayHref = base + '?mode=coplayers&steamid=' + encodeURIComponent(sid);
  var parts = [
    '<a class="playername-action-link" href="' + escapeAttr(chatHref) + '">Chat</a>',
    '<a class="playername-action-link" href="' + escapeAttr(statsHref) + '">Stats</a>',
    '<a class="playername-action-link" href="' + escapeAttr(coplayHref) + '">Co-players</a>'
  ];
  var searched = searchedSteamid64 != null ? String(searchedSteamid64).trim() : '';
  if (/^\d{17}$/.test(searched)) {
    var logmatchHref = coplayersLogmatchUrl(searched, sid);
    if (logmatchHref) {
      parts.push('<a class="playername-action-link" href="' + escapeAttr(logmatchHref) + '">Logmatch</a>');
    }
  }
  return parts.join('');
}

function coplayersSummaryLine(nCo, logsSearched) {
  var n = Number(nCo) || 0;
  var base = 'Found ' + n + ' co-player' + (n === 1 ? '' : 's');
  if (logsSearched != null && Number.isFinite(Number(logsSearched))) {
    var x = Number(logsSearched);
    return base + ' across ' + x + ' log' + (x === 1 ? '' : 's') + '.';
  }
  return base + '.';
}

function renderCoplayers(container, data, elapsedMs, searchedSteamid64) {
  if (searchedSteamid64 !== undefined) {
    container._coplayersSearchedSteamid64 = searchedSteamid64;
  }
  var searched = container._coplayersSearchedSteamid64;
  var rows = data && data.rows ? data.rows : [];
  if (rows !== container._coplayersRows) {
    container._coplayersRows = rows;
    coplayersSortCol = 'total_games';
    coplayersSortDir = -1;
  }
  container._coplayersLogsSearched = data && data.logs_searched != null ? data.logs_searched : null;
  if (typeof elapsedMs === 'number' && Number.isFinite(elapsedMs)) {
    container._coplayersRequestMs = elapsedMs;
  }
  var logsSearched = container._coplayersLogsSearched;
  var originalRows = container._coplayersRows;
  var sortedRows = getSortedCoplayersRows(originalRows);
  var sortCol = coplayersSortCol;
  var sortDir = coplayersSortDir;

  var summaryP = '<p class="stats-summary-meta">' + escapeHtml(coplayersSummaryLine(originalRows.length, logsSearched)) + '</p>';

  var thead = '<tr>';
  COPLAYERS_COLUMNS.forEach(function(c) {
    if (c.type === 'actions') {
      thead += '<th scope="col">' + escapeHtml(c.label) + '</th>';
      return;
    }
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';

  var tbody = '';
  sortedRows.forEach(function(x) {
    var lm = coplayersLogmatchUrl(searched, x.steamid64);
    var trOpen = lm
      ? '<tr data-logmatch-href="' + escapeAttr(lm) + '" style="cursor:pointer">'
      : '<tr>';
    tbody += trOpen;
    COPLAYERS_COLUMNS.forEach(function(c) {
      if (c.type === 'actions') {
        tbody += '<td class="playername-actions">' + coplayersSearchActionsHtml(x, searched) + '</td>';
      } else if (c.key === 'name') {
        tbody += '<td>' + coplayersNameCellHtml(x) + '</td>';
      } else if (c.type === 'winpct') {
        tbody += '<td>' + escapeHtml(coplayersWinPctDisplay(x, c.key === 'winpct_with' ? 'with' : 'against')) + '</td>';
      } else {
        var cell = x[c.key] != null ? String(x[c.key]) : '';
        tbody += '<td>' + escapeHtml(cell) + '</td>';
      }
    });
    tbody += '</tr>';
  });

  container.innerHTML =
    summaryP +
    '<div class="stats-csv-toolbar"><button type="button" class="js-coplayers-csv-download" aria-label="Download co-players table as a CSV file">Download as CSV</button></div>' +
    '<div class="stats-table-wrap"><table class="stats-table playername-results-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>' +
    requestTimingFooter(container._coplayersRequestMs);
  container._coplayersRows = originalRows;

  bindCoplayersCsvDownload(container);
  loadAvatarsInContainer(container);

  container.querySelectorAll('tbody tr[data-logmatch-href]').forEach(function(tr) {
    tr.addEventListener('click', function(e) {
      if (e.target.closest('a')) return;
      var h = tr.getAttribute('data-logmatch-href');
      if (h) window.location.href = h;
    });
  });

  container.querySelectorAll('th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var col = th.getAttribute('data-col');
      if (!col) return;
      if (coplayersSortCol === col) {
        coplayersSortDir = coplayersSortDir === 1 ? -1 : 1;
      } else {
        coplayersSortCol = col;
        coplayersSortDir = 1;
      }
      renderCoplayers(container, { rows: container._coplayersRows, logs_searched: container._coplayersLogsSearched }, container._coplayersRequestMs);
    });
  });
}
