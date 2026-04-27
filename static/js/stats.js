
var statsSortCol = null;
var statsSortDir = 1;

/** CSV includes Team (not shown as its own column in the HTML table). */
var STATS_CSV_COLUMNS = [
  { key: 'alias', label: 'Alias' },
  { key: 'team', label: 'Team' },
  { key: 'character', label: 'Class' },
  { key: 'kills', label: 'K' },
  { key: 'assists', label: 'A' },
  { key: 'deaths', label: 'D' },
  { key: 'kdr', label: 'KDR' },
  { key: 'kadr', label: 'KADR' },
  { key: 'dpm', label: 'DPM' },
  { key: 'dmg', label: 'Dmg' },
  { key: 'headshots_hit', label: 'HS' },
  { key: 'backstabs', label: 'BS' },
  { key: 'map', label: 'Map' },
  { key: 'date', label: 'Date' },
  { key: 'url', label: 'Log URL' }
];

var STATS_COLUMNS = [
  { key: 'alias', label: 'Alias', type: 'text' },
  { key: 'character', label: 'Class', type: 'text' },
  { key: 'kills', label: 'K', type: 'number' },
  { key: 'assists', label: 'A', type: 'number' },
  { key: 'deaths', label: 'D', type: 'number' },
  { key: 'kdr', label: 'KDR', type: 'number' },
  { key: 'kadr', label: 'KADR', type: 'number' },
  { key: 'dpm', label: 'DPM', type: 'number' },
  { key: 'dmg', label: 'Dmg', type: 'number' },
  { key: 'headshots_hit', label: 'HS', type: 'number' },
  { key: 'backstabs', label: 'BS', type: 'number' },
  { key: 'map', label: 'Map', type: 'text' },
  { key: 'date', label: 'Date', type: 'date' },
  { key: 'url', label: 'Log', type: 'text' }
];


function buildStatsCsvContent(sortedRows) {
  var header = STATS_CSV_COLUMNS.map(function(c) {
    return escapeCsvField(c.label);
  }).join(',');
  var lines = [header];
  for (var i = 0; i < sortedRows.length; i++) {
    var row = sortedRows[i];
    var parts = [];
    for (var j = 0; j < STATS_CSV_COLUMNS.length; j++) {
      parts.push(statsValueForCsvCell(row[STATS_CSV_COLUMNS[j].key]));
    }
    lines.push(parts.join(','));
  }
  return '\uFEFF' + lines.join('\r\n');
}

function getSortedStatsRows(originalRows) {
  var sortedRows = originalRows.slice();
  var sortCol = statsSortCol;
  var sortDir = statsSortDir;
  var colDef = STATS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (colDef) {
    sortedRows.sort(function(a, b) {
      var va = getSortValue(a, sortCol, colDef.type);
      var vb = getSortValue(b, sortCol, colDef.type);
      if (colDef.type === 'text') {
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


function bindStatsCsvDownload(container) {
  var btn = container.querySelector('.js-stats-csv-download');
  if (!btn || btn.getAttribute('data-csv-bound')) return;
  btn.setAttribute('data-csv-bound', '1');
  btn.addEventListener('click', function() {
    var rows = container._statsRows;
    if (!rows || !rows.length) return;
    var sorted = getSortedStatsRows(rows);
    var csv = buildStatsCsvContent(sorted);
    var d = new Date();
    var fn = 'tf2-stats-sorter-' + d.getFullYear() + '-' + (d.getMonth() + 1 < 10 ? '0' : '') + (d.getMonth() + 1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate() + '.csv';
    triggerCsvDownload(fn, csv);
  });
}

function formatAvgNumber(key, value) {
  if (!Number.isFinite(value)) return '';
  var integerLike = { kills: true, assists: true, deaths: true, dmg: true, headshots_hit: true, backstabs: true };
  if (integerLike[key]) return String(Math.round(value));
  if (Math.abs(value - Math.round(value)) < 0.005) return String(Math.round(value));
  return value.toFixed(2);
}

/** Ordered unique aliases from result rows (one pass). */
function uniqueAliasesFromRows(rows) {
  var seen = new Set();
  var list = [];
  rows.forEach(function(r) {
    if (!r || r.alias == null) return;
    var a = String(r.alias).trim();
    if (a && !seen.has(a)) {
      seen.add(a);
      list.push(a);
    }
  });
  return list;
}

function computeStatsAverages(rows) {
  var numericCols = STATS_COLUMNS.filter(function(c) { return c.type === 'number'; }).map(function(c) { return c.key; });
  var sums = {};
  var counts = {};
  numericCols.forEach(function(k) { sums[k] = 0; counts[k] = 0; });
  rows.forEach(function(r) {
    numericCols.forEach(function(k) {
      var n = Number(r[k]);
      if (Number.isFinite(n)) {
        sums[k] += n;
        counts[k] += 1;
      }
    });
  });
  var avg = {};
  numericCols.forEach(function(k) {
    avg[k] = counts[k] > 0 ? formatAvgNumber(k, sums[k] / counts[k]) : '';
  });
  return avg;
}

function renderStatsSummary(rows) {
  var avg = computeStatsAverages(rows);
  var aliases = uniqueAliasesFromRows(rows);
  var aliasCellHtml = '\u2014';
  if (aliases.length) {
    var aliasesJoined = aliases.map(escapeHtml).join(' / ');
    aliasCellHtml = '<details class="stats-aliases"><summary>Show aliases (' + escapeHtml(aliases.length) + ')</summary><div class="stats-aliases-list">' + aliasesJoined + '</div></details>';
  }
  var summaryItems = [
    { key: 'Class', value: 'All classes' },
    { key: 'K', value: avg.kills || '\u2014' },
    { key: 'A', value: avg.assists || '\u2014' },
    { key: 'D', value: avg.deaths || '\u2014' },
    { key: 'KDR', value: avg.kdr || '\u2014' },
    { key: 'KADR', value: avg.kadr || '\u2014' },
    { key: 'DPM', value: avg.dpm || '\u2014' },
    { key: 'Dmg', value: avg.dmg || '\u2014' },
    { key: 'HS', value: avg.headshots_hit || '\u2014' },
    { key: 'BS', value: avg.backstabs || '\u2014' }
  ];
  var summaryGrid = summaryItems.map(function(item) {
    return '<div class="stats-summary-item"><span class="stats-summary-key">' + escapeHtml(item.key) + '</span><span class="stats-summary-value">' + escapeHtml(item.value) + '</span></div>';
  }).join('');
  var trendHtml = '';
  if (rows.length >= 2) {
    trendHtml =
      '<div class="stats-trend js-stats-trend">' +
        '<div class="stats-trend-toggle" role="tablist" aria-label="Trends over time metric">' +
          '<button type="button" class="stats-trend-btn js-stats-trend-btn active" data-metric="dpm" role="tab" aria-selected="true">DPM</button>' +
          '<button type="button" class="stats-trend-btn js-stats-trend-btn" data-metric="kpair" role="tab" aria-selected="false">KDR / KADR</button>' +
        '</div>' +
        '<div class="stats-trend-canvas-wrap"><canvas class="js-trend-chart-canvas" aria-label="Trends over time chart"></canvas></div>' +
        '<p class="stats-trend-note">20-game rolling average with per-game points. Y-axis trimmed to the middle ~96% of values so bad logs do not flatten the curve; tooltips show exact stats.</p>' +
      '</div>';
  }
  return '<div class="stats-summary">' +
    '<p class="stats-summary-title">Average stats</p>' +
    '<p class="stats-summary-meta">Computed across ' + escapeHtml(rows.length) + ' matching log(s).</p>' +
    '<div class="stats-summary-grid"><div class="stats-summary-item aliases"><span class="stats-summary-key">Aliases</span><span class="stats-summary-value">' + aliasCellHtml + '</span></div>' + summaryGrid + '</div>' +
    trendHtml +
    '</div>';
}

function renderStatsTable(container, rows, elapsedMs) {
  if (rows !== container._statsRows) {
    container._statsRows = rows;
    statsSortCol = null;
    statsSortDir = 1;
    statsTrendMetric = 'dpm';
    container._statsSummaryHtml = renderStatsSummary(rows);
  }
  if (typeof elapsedMs === 'number' && Number.isFinite(elapsedMs)) {
    container._statsRequestMs = elapsedMs;
  }
  var originalRows = container._statsRows;
  var sortedRows = getSortedStatsRows(originalRows);
  var sortCol = statsSortCol;
  var sortDir = statsSortDir;

  var thead = '<tr>';
  STATS_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';

  var tbody = '';
  sortedRows.forEach(function(x) {
    var teamClass = (x.team === 'Red') ? ' chat-team-red' : ((x.team === 'Blue') ? ' chat-team-blue' : '');
    var aliasCell = teamClass ? ('<span class="' + teamClass + '">' + escapeHtml(x.alias) + '</span>') : escapeHtml(x.alias);
    tbody += '<tr><td>' + aliasCell + '</td><td>' + escapeHtml(x.character) + '</td><td>' + escapeHtml(x.kills) + '</td><td>' + escapeHtml(x.assists) + '</td><td>' + escapeHtml(x.deaths) + '</td><td>' + escapeHtml(x.kdr) + '</td><td>' + escapeHtml(x.kadr) + '</td><td>' + escapeHtml(x.dpm) + '</td><td>' + escapeHtml(x.dmg) + '</td><td>' + escapeHtml(x.headshots_hit) + '</td><td>' + escapeHtml(x.backstabs) + '</td><td>' + escapeHtml(x.map) + '</td><td>' + escapeHtml(x.date) + '</td><td><a href="' + escapeHtml(x.url) + '" target="_blank" rel="noopener noreferrer">Link</a></td></tr>';
  });

  container.innerHTML = (container._statsSummaryHtml || '') +
    '<div class="stats-csv-toolbar"><button type="button" class="js-stats-csv-download" aria-label="Download stats table as a CSV file">Download as CSV</button></div>' +
    '<div class="stats-table-wrap"><table class="stats-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>' + requestTimingFooter(container._statsRequestMs);
  container._statsRows = originalRows;

  bindStatsCsvDownload(container);
  statsTrendHost = container.querySelector('.js-stats-trend');
  statsTrendRows = originalRows;
  destroyStatsTrendChart();
  if (statsTrendHost && statsTrendRows && statsTrendRows.length >= 2) {
    bindStatsTrendControls(container);
    renderStatsTrendChart(statsTrendHost, statsTrendRows, statsTrendMetric);
  }

  container.querySelectorAll('th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var col = th.getAttribute('data-col');
      if (!col) return;
      if (statsSortCol === col) {
        statsSortDir = statsSortDir === 1 ? -1 : 1;
      } else {
        statsSortCol = col;
        statsSortDir = 1;
      }
      renderStatsTable(container, container._statsRows);
    });
  });
}
