
function profileFormatUnixDate(ts) {
  if (ts == null || !Number.isFinite(Number(ts))) return '\u2014';
  try {
    return new Date(Number(ts) * 1000).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  } catch (e) {
    return '\u2014';
  }
}

/** Safe link element to log page; null/invalid id renders an em dash (security). */
function profileLogsTfLogIdLinkEl(logId, optionalUrl) {
  var td = document.createElement('td');
  if (logId == null || logId === '') {
    td.textContent = '\u2014';
    return td;
  }
  var idStr = String(logId).trim();
  if (!/^\d+$/.test(idStr)) {
    td.textContent = '\u2014';
    return td;
  }
  var url = logPageHref(idStr, optionalUrl);
  if (!url) {
    td.textContent = '\u2014';
    return td;
  }
  var a = document.createElement('a');
  a.href = url;
  if (!isInternalLogHref(url)) {
    a.target = '_blank';
    a.rel = 'noopener noreferrer';
  }
  a.textContent = '#' + idStr;
  td.appendChild(a);
  return td;
}

/** Link for a numeric log id; otherwise escape label as plain text (security). */
function profileLogsTfDateLink(logId, dateLabel, optionalUrl) {
  var label = dateLabel != null ? String(dateLabel) : '\u2014';
  if (logId == null || logId === '') return escapeHtml(label);
  var idStr = String(logId).trim();
  if (!/^\d+$/.test(idStr)) return escapeHtml(label);
  var url = logPageHref(idStr, optionalUrl);
  if (!url) return escapeHtml(label);
  var ext = isInternalLogHref(url) ? '' : ' target="_blank" rel="noopener noreferrer"';
  return '<a href="' + escapeAttr(url) + '"' + ext + '>' + escapeHtml(label) + '</a>';
}

function profileFormatPlaytime(totalSec) {
  var n = Math.max(0, Math.floor(Number(totalSec) || 0));
  var h = Math.floor(n / 3600);
  var m = Math.floor((n % 3600) / 60);
  return h + 'h ' + m + 'm';
}

function profileFormatDurationMinSec(sec) {
  if (sec == null || sec === '') return '\u2014';
  var x = Number(sec);
  if (!Number.isFinite(x) || x < 0) return '\u2014';
  var mi = Math.floor(x / 60);
  var s = Math.round(x - mi * 60);
  return mi + 'm ' + s + 's';
}

function profileClassIconImg(cls) {
  if (cls == null || cls === '') return '';
  var key = String(cls).toLowerCase();
  var src = LOGMATCH_CLASS_ICON[key];
  if (!src) return '';
  var label = LOGMATCH_CLASS_LABEL[key] || cls;
  return '<img class="logmatch-class-icon" src="' + escapeAttr(src) + '" alt="" width="22" height="22" loading="lazy" title="' + escapeAttr(label) + '">';
}

function profileClassDisplayName(cls) {
  if (cls == null || cls === '') return '\u2014';
  var key = String(cls).toLowerCase();
  return LOGMATCH_CLASS_LABEL[key] ? LOGMATCH_CLASS_LABEL[key] : String(cls);
}

function profileClassCell(cls) {
  var disp = profileClassDisplayName(cls);
  var icon = profileClassIconImg(cls);
  return (icon ? icon + ' ' : '') + '<span>' + escapeHtml(disp) + '</span>';
}

function profileFormatHealing(n) {
  var v = Number(n);
  if (!Number.isFinite(v)) return '\u2014';
  try {
    return v.toLocaleString() + ' HP';
  } catch (e) {
    return String(Math.round(v)) + ' HP';
  }
}

function formatTopLogPrimaryValue(r) {
  var m = r.metric != null ? String(r.metric) : '';
  var v = r.value;
  if (v == null || (typeof v === 'number' && !Number.isFinite(v))) return '\u2014';
  if (m === 'dpm' || m === 'kdr' || m === 'kadr' || m === 'dtm') {
    return (Math.round(Number(v) * 100) / 100).toString();
  }
  return String(Math.round(Number(v)));
}

function profileClassRowHtml(c) {
  var cn = c && c['class'] != null ? c['class'] : '';
  return '<tr><td>' + profileClassCell(cn) + '</td><td>' + escapeHtml(String(c.logs_count != null ? c.logs_count : '')) + '</td><td>' +
    escapeHtml(profileFormatPlaytime(c.total_playtime_secs)) + '</td><td>' + escapeHtml(String(c.total_kills != null ? c.total_kills : '')) + '</td><td>' +
    escapeHtml(String(c.total_deaths != null ? c.total_deaths : '')) + '</td><td>' + (c.avg_dpm != null ? escapeHtml(String(c.avg_dpm)) : '\u2014') + '</td><td>' +
    (c.avg_kdr != null ? escapeHtml(String(c.avg_kdr)) : '\u2014') + '</td></tr>';
}

var PROFILE_CLASS_COLUMNS = [
  { key: 'class', label: 'Class', type: 'text' },
  { key: 'logs_count', label: 'Logs', type: 'number' },
  { key: 'total_playtime_secs', label: 'Playtime', type: 'number' },
  { key: 'total_kills', label: 'Kills', type: 'number' },
  { key: 'total_deaths', label: 'Deaths', type: 'number' },
  { key: 'avg_dpm', label: 'Avg DPM', type: 'number' },
  { key: 'avg_kdr', label: 'Avg KDR', type: 'number' }
];

function profileClassSortValue(row, colKey, type) {
  if (type === 'text') {
    var t = row[colKey];
    return (t != null ? String(t) : '').toLowerCase();
  }
  var v = row[colKey];
  var n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function profileClassSortedRows(rows, sortCol, sortDir) {
  var colDef = PROFILE_CLASS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileClassSortValue(a, colDef.key, colDef.type);
    var vb = profileClassSortValue(b, colDef.key, colDef.type);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.type === 'text') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    }
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  return sorted;
}

function profileClassTableInnerHtml(rows, sortCol, sortDir) {
  var thead = '<tr>';
  PROFILE_CLASS_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var bodyRows = profileClassSortedRows(rows, sortCol, sortDir);
  var tbody = bodyRows.map(profileClassRowHtml).join('');
  return '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function bindProfileClassTableSort(table) {
  table.addEventListener('click', function(ev) {
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    var rows = table._profileClassRows;
    if (!rows || !rows.length) return;
    if (table._sortCol === col) {
      table._sortDir *= -1;
    } else {
      table._sortCol = col;
      table._sortDir = col === 'class' ? 1 : -1;
    }
    table.innerHTML = profileClassTableInnerHtml(rows, table._sortCol, table._sortDir);
  });
}

/** Co-players search for the profile subject; gamemode/map match profile filters (dates are profile-only). */
function profileCoplayersSearchHref(data) {
  var base = window.location.origin + '/';
  var sid = data.steamid64 != null ? String(data.steamid64).trim() : '';
  if (!/^\d{17}$/.test(sid)) return '';
  var fa = data.filters_applied || {};
  var p = new URLSearchParams();
  p.set('mode', 'coplayers');
  p.set('steamid', sid);
  var gm = fa.gamemode != null ? String(fa.gamemode).trim() : '';
  if (gm) p.set('gamemode', gm);
  var mq = fa.map_query != null ? String(fa.map_query).trim() : '';
  if (mq) p.set('map_query', mq);
  return base + '?' + p.toString();
}

var PROFILE_COPLAYERS_WITH_COLUMNS = [
  { key: 'player', label: 'Player', kind: 'text' },
  { key: 'logs', label: 'Logs', kind: 'number' },
  { key: 'winpct', label: 'Win% (with)', kind: 'winpct' },
  { key: 'class', label: 'Class (with you)', kind: 'class' },
  { key: 'playtime', label: 'Playtime (with you)', kind: 'playtime' },
];

var PROFILE_COPLAYERS_AGAINST_COLUMNS = [
  { key: 'player', label: 'Player', kind: 'text' },
  { key: 'logs', label: 'Logs', kind: 'number' },
  { key: 'winpct', label: 'Win% (vs)', kind: 'winpct' },
  { key: 'class', label: 'Class (vs you)', kind: 'class' },
  { key: 'playtime', label: 'Playtime (vs you)', kind: 'playtime' },
];

function profileTopCoplayersColumns(mode) {
  return mode === 'against' ? PROFILE_COPLAYERS_AGAINST_COLUMNS : PROFILE_COPLAYERS_WITH_COLUMNS;
}

function profileTopCoplayersSortValue(r, colDef, mode) {
  if (colDef.kind === 'text') {
    var name = r.name != null ? String(r.name) : '';
    var sid = r.steamid64 != null ? String(r.steamid64).trim() : '';
    var display = name.trim() ? name : sid;
    return display.toLowerCase();
  }
  if (colDef.kind === 'number') {
    var n = Number(r.total_logs);
    return Number.isFinite(n) ? n : null;
  }
  if (colDef.kind === 'winpct') {
    var wr = mode === 'against' ? r.win_rate_against : r.win_rate_with;
    var wn = Number(wr);
    return Number.isFinite(wn) ? wn : null;
  }
  if (colDef.kind === 'class') {
    var mc = mode === 'against' ? r.most_common_class_against : r.most_common_class_with;
    return mc != null ? String(mc).toLowerCase() : '';
  }
  if (colDef.kind === 'playtime') {
    var pt = mode === 'against' ? r.total_playtime_opposing_secs : r.total_playtime_together_secs;
    var pn = Number(pt);
    return Number.isFinite(pn) ? pn : null;
  }
  return null;
}

function profileTopCoplayersSortedRows(rows, sortCol, sortDir, mode) {
  var columns = profileTopCoplayersColumns(mode);
  var colDef = columns.find(function(c) { return c.key === sortCol; });
  if (!colDef || !rows || !rows.length) return rows ? rows.slice() : [];
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileTopCoplayersSortValue(a, colDef, mode);
    var vb = profileTopCoplayersSortValue(b, colDef, mode);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.kind === 'text' || colDef.kind === 'class') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    }
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  return sorted;
}

function profileTopCoplayersTableInnerHtml(rows, sortCol, sortDir, mode) {
  var columns = profileTopCoplayersColumns(mode);
  var thead = '<tr>';
  columns.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var bodyRows = rows && rows.length
    ? profileTopCoplayersSortedRows(rows, sortCol, sortDir, mode)
    : [];
  var tbody = bodyRows.length
    ? bodyRows.map(function(r) { return profileTopCoplayersRowHtml(r, mode); }).join('')
    : '<tr><td colspan="5" class="stats-summary-meta">' +
      escapeHtml(mode === 'against' ? 'No qualifying opponents in this filter.' : 'No qualifying co-players in this filter.') +
      '</td></tr>';
  return '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function profileTopCoplayersRowHtml(r, mode) {
  var name = r.name != null ? String(r.name) : '';
  var sid = r.steamid64 != null ? String(r.steamid64).trim() : '';
  var display = name.trim() ? name : sid;
  var avatar = steamAvatarPlaceholder(sid);
  var nameCell;
  if (/^\d{17}$/.test(sid)) {
    var phref = internalProfileHref(sid);
    nameCell = avatar + '<a href="' + escapeAttr(phref) + '">' + escapeHtml(display) + '</a>';
  } else {
    nameCell = avatar + escapeHtml(display || '\u2014');
  }
  var tl;
  var wr;
  var mcHtml;
  var pt;
  if (mode === 'against') {
    tl = r.total_logs != null ? String(r.total_logs) : '\u2014';
    wr = r.win_rate_against;
    mcHtml = r.most_common_class_against != null ? profileClassCell(r.most_common_class_against) : '\u2014';
    pt = profileFormatPlaytime(r.total_playtime_opposing_secs);
  } else {
    tl = r.total_logs != null ? String(r.total_logs) : '\u2014';
    wr = r.win_rate_with;
    mcHtml = r.most_common_class_with != null ? profileClassCell(r.most_common_class_with) : '\u2014';
    pt = profileFormatPlaytime(r.total_playtime_together_secs);
  }
  var wrStr = (wr != null && Number.isFinite(Number(wr)))
    ? (Math.round(Number(wr) * 10000) / 100 + '%')
    : '\u2014';
  return '<tr><td>' + nameCell + '</td><td>' + escapeHtml(tl) + '</td><td>' + escapeHtml(wrStr) + '</td><td>' + mcHtml + '</td><td>' + escapeHtml(pt) + '</td></tr>';
}

function profileTopCoplayersBlock(data) {
  var withRows = data.top_coplayers;
  var againstRows = data.top_coplayers_opposing;
  var hasWith = withRows && withRows.length;
  var hasAgainst = againstRows && againstRows.length;
  if (!hasWith && !hasAgainst) return '';
  var href = profileCoplayersSearchHref(data);
  var more = href
    ? ('<p class="stats-summary-meta"><a href="' + escapeAttr(href) + '">Open full co-players search</a> (same gamemode / map filter).</p>')
    : '';
  var theadWith = '';
  var theadAgainst = '';
  var bodyWith = '';
  var bodyAgainst = '';
  var emptyWith = hasWith ? '0' : '1';
  var emptyAgainst = hasAgainst ? '0' : '1';
  var defaultSortCol = 'logs';
  var defaultSortDir = -1;
  return '<div class="stats-summary profile-top-coplayers js-profile-coplayers" data-with-empty="' + emptyWith + '" data-against-empty="' + emptyAgainst + '">' +
    '<p class="stats-summary-title">Most frequent co-players</p>' +
    '<div class="profile-coplayers-toolbar">' +
    '<span class="stats-summary-meta profile-coplayers-toolbar-label">Show</span>' +
    '<div class="stats-trend-toggle profile-coplayers-toggle" role="tablist" aria-label="Co-player relation">' +
    '<button type="button" class="stats-trend-btn js-coplayers-tab active" data-pane="with" role="tab" aria-selected="true">With you</button>' +
    '<button type="button" class="stats-trend-btn js-coplayers-tab" data-pane="against" role="tab" aria-selected="false">Against you</button>' +
    '</div></div>' +
    '<p class="stats-summary-meta js-coplayers-desc" data-pane="with">Top 5 by shared logs (teammate + opponent). Win rate, class, and playtime count only games on the same team. Click a column header to sort.</p>' +
    '<p class="stats-summary-meta js-coplayers-desc" data-pane="against" hidden>Top 5 by games on opposite teams. Logs column: total shared logs (teammate + opponent), same meaning as With you. Win rate, class, and playtime: opposite-team games only. Click a column header to sort.</p>' +
    more +
    '<div class="js-coplayers-pane" data-pane="with"><div class="stats-table-wrap"><table class="stats-table js-profile-coplayers-table" data-coplayers-mode="with">' +
    profileTopCoplayersTableInnerHtml(hasWith ? withRows : [], defaultSortCol, defaultSortDir, 'with') +
    '</table></div></div>' +
    '<div class="js-coplayers-pane" data-pane="against" hidden><div class="stats-table-wrap"><table class="stats-table js-profile-coplayers-table" data-coplayers-mode="against">' +
    profileTopCoplayersTableInnerHtml(hasAgainst ? againstRows : [], defaultSortCol, defaultSortDir, 'against') +
    '</table></div></div>' +
    '</div>';
}

function bindProfileCoplayersTableSort(wrap) {
  if (!wrap) return;
  wrap.querySelectorAll('table.js-profile-coplayers-table').forEach(function(table) {
    if (table._tf2lsCoplayersSortBound) return;
    table._tf2lsCoplayersSortBound = true;
    var mode = (table.getAttribute('data-coplayers-mode') || 'with').trim();
    if (mode !== 'with' && mode !== 'against') mode = 'with';
    table._coplayersMode = mode;
    table.addEventListener('click', function(ev) {
      var th = ev.target.closest('th.sortable');
      if (!th || !table.contains(th)) return;
      var col = th.getAttribute('data-col');
      if (!col) return;
      var rows = table._profileCoplayersRows;
      if (!rows || !rows.length) return;
      if (table._sortCol === col) {
        table._sortDir *= -1;
      } else {
        table._sortCol = col;
        table._sortDir = col === 'player' || col === 'class' ? 1 : -1;
      }
      table.innerHTML = profileTopCoplayersTableInnerHtml(
        rows,
        table._sortCol,
        table._sortDir,
        table._coplayersMode
      );
      loadAvatarsInContainer(table);
    });
  });
}

function bindProfileCoplayersTables(root, data) {
  var wrap = root.querySelector('.js-profile-coplayers');
  if (!wrap) return;
  var withRows = data.top_coplayers;
  var againstRows = data.top_coplayers_opposing;
  wrap.querySelectorAll('table.js-profile-coplayers-table').forEach(function(table) {
    var mode = (table.getAttribute('data-coplayers-mode') || 'with').trim();
    if (mode === 'against') {
      table._profileCoplayersRows = againstRows && againstRows.length ? againstRows.slice() : [];
    } else {
      table._profileCoplayersRows = withRows && withRows.length ? withRows.slice() : [];
    }
    table._coplayersMode = mode;
    table._sortCol = 'logs';
    table._sortDir = -1;
  });
  bindProfileCoplayersTableSort(wrap);
}

var PROFILE_CLASS_KILLS_COLUMNS = [
  { key: 'victim_class', label: 'Victim class', kind: 'class' },
  { key: 'total_kills', label: 'Kills', kind: 'number' },
  { key: 'logs_count', label: 'Logs', kind: 'number' },
  { key: 'avg_kills_per_log', label: 'Kills/log', kind: 'number' },
  { key: 'peak_total_kills', label: 'Most kills (log)', kind: 'peak_kills' },
  { key: 'peak_kills_per_min', label: 'Highest kills/min (log)', kind: 'peak_kpm' }
];
var PROFILE_CLASS_KILLS_COLSPAN = String(PROFILE_CLASS_KILLS_COLUMNS.length);

function profileClassKillsVictimSkipped(vc) {
  return !vc || vc === 'undefined' || vc === 'none' || vc === 'unknown';
}

function profileClassKillsFilterRows(rows) {
  if (!rows || !rows.length) return [];
  return rows.filter(function(r) {
    var vc = r.victim_class != null ? String(r.victim_class).trim().toLowerCase() : '';
    return !profileClassKillsVictimSkipped(vc);
  });
}

/** One log peak for class kills: logs.tf link + kills, duration, kills/min (server fields only). */
function profileClassKillsPeakHtml(peak) {
  if (!peak || peak.log_id == null || peak.log_id === '') return '\u2014';
  var lid = Number(peak.log_id);
  if (!Number.isFinite(lid) || lid < 1) return '\u2014';
  var idStr = String(Math.floor(lid));
  if (!/^\d+$/.test(idStr)) return '\u2014';
  var k = peak.kills;
  var kStr = k != null && Number.isFinite(Number(k)) ? String(Math.floor(Number(k))) : '\u2014';
  var link = profileLogsTfDateLink(idStr, '#' + idStr);
  var parts = [link + ' <span class="stats-summary-meta">(' + escapeHtml(kStr) + ')</span>'];
  var d = peak.duration_secs;
  if (d != null && Number.isFinite(Number(d)) && Number(d) > 0) {
    parts.push('<span class="stats-summary-meta">' + escapeHtml(profileFormatDurationMinSec(d)) + '</span>');
  }
  var kpm = peak.kills_per_min;
  if (kpm != null && Number.isFinite(Number(kpm))) {
    parts.push('<span class="stats-summary-meta">' + escapeHtml(String(Math.round(Number(kpm) * 100) / 100)) + ' kills/min</span>');
  }
  return '<div class="profile-heal-peak-cell">' + parts.join(' ') + '</div>';
}

function profileClassKillsSortValue(row, colDef) {
  if (colDef.kind === 'class') {
    return row.victim_class != null ? String(row.victim_class).toLowerCase() : '';
  }
  if (colDef.kind === 'peak_kills') {
    var pt = row.peak_total_kills;
    if (!pt || pt.kills == null) return null;
    var pk = Number(pt.kills);
    return Number.isFinite(pk) ? pk : null;
  }
  if (colDef.kind === 'peak_kpm') {
    var pm = row.peak_kills_per_min;
    if (!pm || pm.kills_per_min == null) return null;
    var pv = Number(pm.kills_per_min);
    return Number.isFinite(pv) ? pv : null;
  }
  var v = row[colDef.key];
  var n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function profileClassKillsSortedRows(rows, sortCol, sortDir) {
  var colDef = PROFILE_CLASS_KILLS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileClassKillsSortValue(a, colDef);
    var vb = profileClassKillsSortValue(b, colDef);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.kind === 'class') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    }
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  return sorted;
}

function profileClassKillsTableRow(r) {
  var kpl = r.avg_kills_per_log != null && Number.isFinite(Number(r.avg_kills_per_log))
    ? String(Math.round(Number(r.avg_kills_per_log) * 100) / 100)
    : '\u2014';
  return '<tr><td>' + profileClassCell(r.victim_class) + '</td><td>' +
    escapeHtml(r.total_kills != null ? String(r.total_kills) : '\u2014') + '</td><td>' +
    escapeHtml(r.logs_count != null ? String(r.logs_count) : '\u2014') + '</td><td>' +
    escapeHtml(kpl) + '</td><td>' +
    profileClassKillsPeakHtml(r.peak_total_kills) + '</td><td>' +
    profileClassKillsPeakHtml(r.peak_kills_per_min) + '</td></tr>';
}

function profileClassKillsTableInnerHtml(rows, sortCol, sortDir) {
  var thead = '<tr>';
  PROFILE_CLASS_KILLS_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var bodyRows = profileClassKillsSortedRows(rows, sortCol, sortDir);
  var tbody = bodyRows.length
    ? bodyRows.map(profileClassKillsTableRow).join('')
    : '<tr><td colspan="' + escapeHtml(PROFILE_CLASS_KILLS_COLSPAN) + '" class="stats-summary-meta">\u2014</td></tr>';
  return '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function profileClassKillsBlock(rows) {
  var filtered = profileClassKillsFilterRows(rows);
  if (!filtered.length) return '';
  var inner = profileClassKillsTableInnerHtml(filtered, 'total_kills', -1);
  return '<div class="stats-summary js-profile-class-kills">' +
    '<p class="stats-summary-title">Kills by victim class</p>' +
    '<p class="stats-summary-meta">Kills/log uses logs where you have at least one kill on that class. Peak columns link to logs.tf. Click column headers to sort.</p>' +
    '<div class="stats-table-wrap"><table class="stats-table js-profile-class-kills-table">' + inner + '</table></div></div>';
}

function bindProfileClassKillsTableSort(table) {
  if (table._tf2lsClassKillsSortBound) return;
  table._tf2lsClassKillsSortBound = true;
  table.addEventListener('click', function(ev) {
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    var rows = table._profileClassKillsRows;
    if (!rows || !rows.length) return;
    if (table._sortCol === col) {
      table._sortDir *= -1;
    } else {
      table._sortCol = col;
      table._sortDir = col === 'victim_class' ? 1 : -1;
    }
    table.innerHTML = profileClassKillsTableInnerHtml(rows, table._sortCol, table._sortDir);
  });
}

function bindProfileClassKillsTable(root, data) {
  var table = root.querySelector('table.js-profile-class-kills-table');
  if (!table) return;
  var rows = profileClassKillsFilterRows(data.class_kills);
  table._profileClassKillsRows = rows.length ? rows : [];
  table._sortCol = 'total_kills';
  table._sortDir = -1;
  bindProfileClassKillsTableSort(table);
}

var PROFILE_WEAPONS_COLUMNS = [
  { key: 'weapon', label: 'Weapon', kind: 'text' },
  { key: 'logs_count', label: 'Logs', kind: 'number' },
  { key: 'total_kills', label: 'Kills', kind: 'number' },
  { key: 'total_damage', label: 'Damage', kind: 'number' },
  { key: 'accuracy', label: 'Accuracy', kind: 'number' },
  { key: 'avg_damage_per_shot', label: 'Avg dmg/hit', kind: 'number' }
];
var PROFILE_WEAPONS_COLSPAN = String(PROFILE_WEAPONS_COLUMNS.length);

function profileWeaponsDisplayName(w) {
  if (w.weapon_display && String(w.weapon_display).trim()) return String(w.weapon_display).trim();
  return w.weapon != null ? String(w.weapon) : '';
}

function profileWeaponsSortValue(row, colDef) {
  if (colDef.kind === 'text') {
    return profileWeaponsDisplayName(row).toLowerCase();
  }
  var v = row[colDef.key];
  var n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function profileWeaponsSortedRows(rows, sortCol, sortDir) {
  var colDef = PROFILE_WEAPONS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileWeaponsSortValue(a, colDef);
    var vb = profileWeaponsSortValue(b, colDef);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.kind === 'text') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    }
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  return sorted;
}

function profileWeaponsTableRow(w) {
  var wname = profileWeaponsDisplayName(w);
  var acc = w.accuracy != null ? (Math.round(Number(w.accuracy) * 10000) / 100 + '%') : '\u2014';
  var adh = w.avg_damage_per_shot != null ? String(w.avg_damage_per_shot) : '\u2014';
  return '<tr><td>' + escapeHtml(wname) + '</td><td>' +
    escapeHtml(w.logs_count != null ? String(w.logs_count) : '\u2014') + '</td><td>' +
    escapeHtml(w.total_kills != null ? String(w.total_kills) : '\u2014') + '</td><td>' +
    escapeHtml(w.total_damage != null ? String(w.total_damage) : '\u2014') + '</td><td>' +
    escapeHtml(acc) + '</td><td>' + escapeHtml(adh) + '</td></tr>';
}

function profileWeaponsTableInnerHtml(rows, sortCol, sortDir) {
  var thead = '<tr>';
  PROFILE_WEAPONS_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var bodyRows = profileWeaponsSortedRows(rows, sortCol, sortDir);
  var tbody = bodyRows.length
    ? bodyRows.map(profileWeaponsTableRow).join('')
    : '<tr><td colspan="' + escapeHtml(PROFILE_WEAPONS_COLSPAN) + '" class="stats-summary-meta">\u2014</td></tr>';
  return '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function profileWeaponsBlock(rows) {
  if (!rows || !rows.length) return '';
  var inner = profileWeaponsTableInnerHtml(rows, 'total_kills', -1);
  return '<div class="stats-summary js-profile-weapons">' +
    '<p class="stats-summary-title">Weapons</p>' +
    '<p class="stats-summary-meta">Click a column header to sort; click again to reverse. Default: kills (high to low).</p>' +
    '<div class="stats-table-wrap"><table class="stats-table js-profile-weapons-table">' + inner + '</table></div></div>';
}

function bindProfileWeaponsTableSort(table) {
  if (table._tf2lsWeaponsSortBound) return;
  table._tf2lsWeaponsSortBound = true;
  table.addEventListener('click', function(ev) {
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    var rows = table._profileWeaponsRows;
    if (!rows || !rows.length) return;
    if (table._sortCol === col) {
      table._sortDir *= -1;
    } else {
      table._sortCol = col;
      table._sortDir = col === 'weapon' ? 1 : -1;
    }
    table.innerHTML = profileWeaponsTableInnerHtml(rows, table._sortCol, table._sortDir);
  });
}

function bindProfileWeaponsTable(root, data) {
  var table = root.querySelector('table.js-profile-weapons-table');
  if (!table) return;
  var rows = data.weapons;
  table._profileWeaponsRows = rows && rows.length ? rows.slice() : [];
  table._sortCol = 'total_kills';
  table._sortDir = -1;
  bindProfileWeaponsTableSort(table);
}

/** One log peak for heal spread: logs.tf link + healing, duration, HP/min (server fields only). */
function profileHealspreadPeakHtml(peak) {
  if (!peak || peak.log_id == null || peak.log_id === '') return '\u2014';
  var lid = Number(peak.log_id);
  if (!Number.isFinite(lid) || lid < 1) return '\u2014';
  var idStr = String(Math.floor(lid));
  if (!/^\d+$/.test(idStr)) return '\u2014';
  var h = peak.healing;
  var hStr = h != null && Number.isFinite(Number(h)) ? profileFormatHealing(Number(h)) : '\u2014';
  var link = profileLogsTfDateLink(idStr, '#' + idStr);
  var parts = [link + ' <span class="stats-summary-meta">(' + escapeHtml(hStr) + ')</span>'];
  var d = peak.duration_secs;
  if (d != null && Number.isFinite(Number(d)) && Number(d) > 0) {
    parts.push('<span class="stats-summary-meta">' + escapeHtml(profileFormatDurationMinSec(d)) + '</span>');
  }
  var hpm = peak.heals_per_min;
  if (hpm != null && Number.isFinite(Number(hpm))) {
    parts.push('<span class="stats-summary-meta">' + escapeHtml(String(Math.round(Number(hpm) * 100) / 100)) + ' HP/min</span>');
  }
  return '<div class="profile-heal-peak-cell">' + parts.join(' ') + '</div>';
}

var PROFILE_HEALSPREAD_COLUMNS = [
  { key: 'player', label: 'Player', kind: 'text' },
  { key: 'total_healing', label: 'Total healing', kind: 'number' },
  { key: 'logs_count', label: 'Logs', kind: 'number' },
  { key: 'heals_per_log', label: 'Heals/log', kind: 'number' },
  { key: 'peak_total_heal', label: 'Most healing (log)', kind: 'peak_healing' },
  { key: 'peak_heals_per_min', label: 'Highest HP/min (log)', kind: 'peak_hpm' }
];
var PROFILE_HEALSPREAD_COLSPAN = String(PROFILE_HEALSPREAD_COLUMNS.length);

function profileHealspreadSortValue(row, colDef) {
  if (colDef.kind === 'text') {
    var name = row.name != null ? String(row.name) : '';
    var sid = row.steamid64 != null ? String(row.steamid64).trim() : '';
    var display = name.trim() ? name : sid;
    return display.toLowerCase();
  }
  if (colDef.kind === 'peak_healing') {
    var pt = row.peak_total_heal;
    if (!pt || pt.healing == null) return null;
    var ph = Number(pt.healing);
    return Number.isFinite(ph) ? ph : null;
  }
  if (colDef.kind === 'peak_hpm') {
    var pm = row.peak_heals_per_min;
    if (!pm || pm.heals_per_min == null) return null;
    var pv = Number(pm.heals_per_min);
    return Number.isFinite(pv) ? pv : null;
  }
  var v = row[colDef.key];
  var n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function profileHealspreadSortedRows(rows, sortCol, sortDir) {
  var colDef = PROFILE_HEALSPREAD_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileHealspreadSortValue(a, colDef);
    var vb = profileHealspreadSortValue(b, colDef);
    if (va === null && vb === null) return 0;
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.kind === 'text') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
      return 0;
    }
    if (va < vb) return -sortDir;
    if (va > vb) return sortDir;
    return 0;
  });
  return sorted;
}

function profileHealspreadTableInnerHtml(rows, sortCol, sortDir) {
  var thead = '<tr>';
  PROFILE_HEALSPREAD_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var bodyRows = profileHealspreadSortedRows(rows, sortCol, sortDir);
  var tbody = bodyRows.length
    ? bodyRows.map(profileHealspreadTableRow).join('')
    : '<tr><td colspan="' + escapeHtml(PROFILE_HEALSPREAD_COLSPAN) + '" class="stats-summary-meta">\u2014</td></tr>';
  return '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function profileHealspreadEmptyTableInnerHtml() {
  var thead = '<tr>';
  PROFILE_HEALSPREAD_COLUMNS.forEach(function(c) {
    thead += '<th scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  return '<thead>' + thead + '</thead><tbody><tr><td colspan="' + escapeHtml(PROFILE_HEALSPREAD_COLSPAN) + '" class="stats-summary-meta">\u2014</td></tr></tbody>';
}

function bindProfileHealspreadTableSort(table) {
  table.addEventListener('click', function(ev) {
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    var rows = table._profileHealspreadRows;
    if (!rows || !rows.length) return;
    if (table._sortCol === col) {
      table._sortDir *= -1;
    } else {
      table._sortCol = col;
      table._sortDir = col === 'player' ? 1 : -1;
    }
    table.innerHTML = profileHealspreadTableInnerHtml(rows, table._sortCol, table._sortDir);
    loadAvatarsInContainer(table);
  });
}

function profileHealspreadTableRow(r) {
  var name = r.name != null ? String(r.name) : '';
  var sid = r.steamid64 != null ? String(r.steamid64).trim() : '';
  var display = name.trim() ? name : sid;
  var avatar = steamAvatarPlaceholder(sid);
  var nameCell;
  if (/^\d{17}$/.test(sid)) {
    var phref = internalProfileHref(sid);
    nameCell = avatar + '<a href="' + escapeAttr(phref) + '">' + escapeHtml(display) + '</a>';
  } else {
    nameCell = avatar + escapeHtml(display || '\u2014');
  }
  var totalH = r.total_healing != null ? profileFormatHealing(r.total_healing) : '\u2014';
  var logs = r.logs_count != null ? String(r.logs_count) : '\u2014';
  var hpl = r.heals_per_log != null && Number.isFinite(Number(r.heals_per_log))
    ? String(Math.round(Number(r.heals_per_log) * 100) / 100)
    : '\u2014';
  return '<tr><td>' + nameCell + '</td><td>' + escapeHtml(totalH) + '</td><td>' + escapeHtml(logs) + '</td><td>' + escapeHtml(hpl) + '</td><td>' +
    profileHealspreadPeakHtml(r.peak_total_heal) + '</td><td>' +
    profileHealspreadPeakHtml(r.peak_heals_per_min) + '</td></tr>';
}

function bindProfileCoplayersToggle(root) {
  var wrap = root.querySelector('.js-profile-coplayers');
  if (!wrap) return;
  var tabs = wrap.querySelectorAll('.js-coplayers-tab');
  var panes = wrap.querySelectorAll('.js-coplayers-pane');
  var descs = wrap.querySelectorAll('.js-coplayers-desc');
  function show(pane) {
    var i;
    for (i = 0; i < panes.length; i++) {
      panes[i].hidden = panes[i].getAttribute('data-pane') !== pane;
    }
    for (i = 0; i < descs.length; i++) {
      descs[i].hidden = descs[i].getAttribute('data-pane') !== pane;
    }
    for (i = 0; i < tabs.length; i++) {
      var t = tabs[i];
      var on = t.getAttribute('data-pane') === pane;
      t.classList.toggle('active', on);
      t.setAttribute('aria-selected', on ? 'true' : 'false');
    }
  }
  var preferAgainst = wrap.getAttribute('data-with-empty') === '1' && wrap.getAttribute('data-against-empty') === '0';
  show(preferAgainst ? 'against' : 'with');
  tabs.forEach(function(btn) {
    btn.addEventListener('click', function() {
      var p = btn.getAttribute('data-pane');
      if (p) show(p);
    });
  });
}

function renderFavoriteWords(words, container) {
  if (!Array.isArray(words) || words.length === 0 || !container) {
    return;
  }
  container.innerHTML = '';
  container.className = 'stats-summary profile-favorite-words';

  var normalized = words.map(function(w) {
    var word = w && w.word != null ? String(w.word).trim() : '';
    var count = w && w.count != null ? Number(w.count) : 0;
    var pct = w && w.pct != null ? Number(w.pct) : 0;
    var latestLid = w && w.latest_log_id != null ? Number(w.latest_log_id) : NaN;
    var peakLid = w && w.peak_log_id != null ? Number(w.peak_log_id) : NaN;
    return {
      word: word,
      count: Number.isFinite(count) ? count : 0,
      pct: Number.isFinite(pct) ? pct : 0,
      latest_log_id: Number.isFinite(latestLid) ? latestLid : null,
      peak_log_id: Number.isFinite(peakLid) ? peakLid : null
    };
  }).filter(function(w) {
    return w.word && w.count > 0;
  });
  if (!normalized.length) {
    return;
  }
  normalized.sort(function(a, b) {
    return b.count - a.count || (a.word < b.word ? -1 : a.word > b.word ? 1 : 0);
  });
  normalized.forEach(function(w, i) {
    w.rank = i + 1;
  });

  var h3 = document.createElement('p');
  h3.className = 'stats-summary-title';
  h3.textContent = 'Favorite Words';
  container.appendChild(h3);

  var meta = document.createElement('p');
  meta.className = 'stats-summary-meta';
  meta.textContent = 'Most-used non-filler words across all indexed chat messages for this player. Hover a word to highlight its row in the table below.';
  container.appendChild(meta);

  var cloudHost = document.createElement('div');
  cloudHost.className = 'profile-word-cloud-host';
  container.appendChild(cloudHost);

  var details = document.createElement('details');
  details.className = 'profile-word-table-details';
  var summary = document.createElement('summary');
  summary.textContent = 'Show ranked list';
  details.appendChild(summary);

  var tableWrap = document.createElement('div');
  tableWrap.className = 'stats-table-wrap';
  var table = document.createElement('table');
  table.className = 'stats-table profile-word-table';
  var thead = document.createElement('thead');
  thead.innerHTML = '<tr><th>#</th><th>Word</th><th>Count</th><th>% of words</th>'
    + '<th title="Log with the most recent chat line containing this word">Latest</th>'
    + '<th title="Log where this word appears most often (ties: more recent log)">Peak</th></tr>';
  table.appendChild(thead);
  var tbody = document.createElement('tbody');

  normalized.forEach(function(w, i) {
    var tr = document.createElement('tr');
    tr.className = 'profile-word-table-row';
    tr.setAttribute('data-word', w.word);
    var rankTd = document.createElement('td');
    rankTd.textContent = String(i + 1);
    var wordTd = document.createElement('td');
    var strong = document.createElement('strong');
    strong.textContent = w.word;
    wordTd.appendChild(strong);
    var countTd = document.createElement('td');
    countTd.textContent = w.count.toLocaleString();
    var pctTd = document.createElement('td');
    pctTd.textContent = w.pct + '%';
    tr.appendChild(rankTd);
    tr.appendChild(wordTd);
    tr.appendChild(countTd);
    tr.appendChild(pctTd);
    tr.appendChild(profileLogsTfLogIdLinkEl(w.latest_log_id));
    tr.appendChild(profileLogsTfLogIdLinkEl(w.peak_log_id));
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  tableWrap.appendChild(table);
  details.appendChild(tableWrap);
  container.appendChild(details);

  function setTableRowHighlight(word) {
    tbody.querySelectorAll('.profile-word-table-row').forEach(function(row) {
      var on = word && row.getAttribute('data-word') === word;
      row.classList.toggle('is-linked-hover', !!on);
    });
  }

  tbody.querySelectorAll('.profile-word-table-row').forEach(function(row) {
    row.addEventListener('mouseenter', function() {
      var w = row.getAttribute('data-word');
      setTableRowHighlight(w);
      if (cloudHost._profileWordCloudSetActive) {
        cloudHost._profileWordCloudSetActive(w);
      }
    });
    row.addEventListener('mouseleave', function() {
      setTableRowHighlight(null);
      if (cloudHost._profileWordCloudSetActive) {
        cloudHost._profileWordCloudSetActive(null);
      }
    });
  });

  if (typeof scheduleProfileWordCloudMount === 'function') {
    scheduleProfileWordCloudMount(cloudHost, normalized, {
      linkHover: setTableRowHighlight
    });
  } else if (typeof mountProfileWordCloud === 'function') {
    mountProfileWordCloud(cloudHost, normalized, {
      linkHover: setTableRowHighlight
    });
  }
}

function profileMapsPctDisplay(pct) {
  if (pct == null || !Number.isFinite(Number(pct))) return '\u2014';
  return (Math.round(Number(pct) * 10000) / 100).toFixed(2) + '%';
}

function profileMapsWinRateDisplay(wr) {
  if (wr == null || !Number.isFinite(Number(wr))) return '\u2014';
  return (Math.round(Number(wr) * 10000) / 100).toFixed(2) + '%';
}

var PROFILE_MAPS_COLUMNS = [
  { key: 'map_label', label: 'Map', type: 'text' },
  { key: 'logs_count', label: 'Logs', type: 'number' },
  { key: 'pct_of_total', label: '% of logs', type: 'number' },
  { key: 'win_rate', label: 'Win%', type: 'number' },
  { key: 'wins', label: 'W', type: 'number' },
  { key: 'losses', label: 'L', type: 'number' }
];

function profileMapsSortValue(row, colKey, type) {
  if (type === 'text') {
    var t = row[colKey];
    if ((t == null || t === '') && colKey === 'map_label' && row.map != null) {
      t = row.map;
    }
    return (t != null ? String(t) : '').toLowerCase();
  }
  var v = row[colKey];
  if (v == null || (typeof v === 'number' && !Number.isFinite(v))) return null;
  return Number(v);
}

/** Stable tie-break for map parents and per-version rows. */
function profileMapsSortTieKey(row) {
  return String(row.map_key != null ? row.map_key : row.map != null ? row.map : '');
}

function profileMapsSortedParents(rows, sortCol, sortDir) {
  var colDef = PROFILE_MAPS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileMapsSortValue(a, colDef.key, colDef.type);
    var vb = profileMapsSortValue(b, colDef.key, colDef.type);
    if (va === null && vb === null) {
      return profileMapsSortTieKey(a).localeCompare(profileMapsSortTieKey(b));
    }
    if (va === null) return 1;
    if (vb === null) return -1;
    if (colDef.type === 'text') {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
    } else {
      if (va < vb) return -sortDir;
      if (va > vb) return sortDir;
    }
    return profileMapsSortTieKey(a).localeCompare(profileMapsSortTieKey(b));
  });
  return sorted;
}

/**
 * Map filename prefix (segment before the first ``_``) → display name.
 * Includes TF2 Wiki “Official map prefixes” plus common community / Hammer prefixes (Wiki “User created map prefixes” and related).
 * Add keys in lowercase when you encounter new prefixes.
 */
var PROFILE_MAP_PREFIX_LABELS = {
  ach: 'Achievement',
  achievement: 'Achievement',
  achievements: 'Achievement',
  arena: 'Arena',
  ccp: 'Cyclic capture point',
  cp: 'Control Point',
  ctf: 'Capture the Flag and Mannpower',
  db: 'Dodgeball',
  dbs: 'Dodgeball',
  dcp: 'Dual capture progress',
  dm: 'Deathmatch',
  duel: 'Duel',
  dz: 'DZ (community)',
  es: 'Escort',
  fy: 'FY (community)',
  fw: 'FortWars',
  gf: 'GF (community)',
  gg: 'Gun game',
  gungame: 'Gun game',
  hs: 'Sniper duel',
  htf: 'Hold the Flag',
  ig: 'Instagib',
  jump: 'Jump',
  ktf: 'Hold the Flag',
  koth: 'King of the Hill',
  mvm: 'Mann vs. Machine',
  od: 'Object destruction',
  pass: 'PASS Time',
  pd: 'Player Destruction',
  pf: 'Parkour Fortress',
  ph: 'Prop Hunt',
  pl: 'Payload',
  plr: 'Payload Race',
  pz: 'PZ (community)',
  rats: 'Rats',
  rc: 'Random capture',
  rd: 'Robot Destruction',
  rj: 'Rocket jump',
  sd: 'Special Delivery',
  sn: 'Sniper duel',
  sniper: 'Sniper duel',
  soccer: 'Soccer',
  surf: 'Surf',
  tc: 'Territorial Control',
  tfdb: 'Dodgeball',
  toy: 'Toy / rats',
  trade: 'Trade',
  tow: 'Tug of War',
  tr: 'Training Mode',
  ud: 'Ultiduo',
  ultiduo: 'Ultiduo',
  vsh: 'Versus Saxton Hale',
  zi: 'Zombie Infection',
  zf: 'Zombie Fortress',
  zs: 'Zombie survival',
};

/** First path segment of ``map_key`` (e.g. ``koth`` from ``koth_product``); ``other`` if unknown. */
function profileMapGamemodePrefix(mapKey) {
  var s = mapKey != null ? String(mapKey).trim() : '';
  if (!s || s === '(unknown)') return 'other';
  var i = s.indexOf('_');
  if (i <= 0) return 'other';
  return s.slice(0, i).toLowerCase();
}

function profileMapGamemodeLabel(prefix) {
  if (prefix === 'other') return 'Other';
  var p = String(prefix).toLowerCase();
  if (Object.prototype.hasOwnProperty.call(PROFILE_MAP_PREFIX_LABELS, p)) {
    return PROFILE_MAP_PREFIX_LABELS[p];
  }
  return String(prefix) + '_';
}

/**
 * Roll ``top_maps`` rows into gamemode buckets (prefix before first ``_``).
 * Each bucket matches parent-row fields for sorting + thead columns.
 * ``sortCol`` / ``sortDir`` order maps inside each gamemode like the active table sort.
 */
function profileMapsFoldGamemodes(mapRows, logsTotal, sortCol, sortDir) {
  var sc = sortCol != null ? sortCol : 'logs_count';
  var sd = sortDir != null ? sortDir : -1;
  var byGm = Object.create(null);
  mapRows.forEach(function(p) {
    var gk = profileMapGamemodePrefix(p.map_key);
    if (!byGm[gk]) {
      byGm[gk] = { maps: [], logs_count: 0, wins: 0, losses: 0, undecided_logs: 0 };
    }
    var g = byGm[gk];
    g.maps.push(p);
    g.logs_count += Number(p.logs_count) || 0;
    g.wins += Number(p.wins) || 0;
    g.losses += Number(p.losses) || 0;
    g.undecided_logs += Number(p.undecided_logs) || 0;
  });
  var lt = Number(logsTotal);
  if (!Number.isFinite(lt)) lt = 0;
  return Object.keys(byGm).map(function(gk) {
    var g = byGm[gk];
    g.maps = profileMapsSortedParents(g.maps.slice(), sc, sd);
    var decided = g.wins + g.losses;
    var winRate = decided > 0 ? Math.round((g.wins / decided) * 1e6) / 1e6 : null;
    var pct = lt > 0 ? Math.round((g.logs_count / lt) * 1e6) / 1e6 : null;
    return {
      gamemode_key: gk,
      map_key: '__gm__' + gk,
      map_label: profileMapGamemodeLabel(gk),
      logs_count: g.logs_count,
      wins: g.wins,
      losses: g.losses,
      undecided_logs: g.undecided_logs,
      win_rate: winRate,
      pct_of_total: pct,
      maps: g.maps,
    };
  });
}

function profileMapsTheadHtml(sortCol, sortDir) {
  var h = '<tr><th class="profile-maps-expand-cell" scope="col"></th>';
  PROFILE_MAPS_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    h += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  h += '</tr>';
  return '<thead>' + h + '</thead>';
}

function profileMapsRowsOnly(sortedParents, expandedMap, sortCol, sortDir) {
  var map = expandedMap || {};
  var sc = sortCol != null ? sortCol : 'logs_count';
  var sd = sortDir != null ? sortDir : -1;
  var html = '';
  sortedParents.forEach(function(p) {
    var mkey = p.map_key != null ? String(p.map_key) : '';
    var versRaw = Array.isArray(p.versions) ? p.versions : [];
    var vers = profileMapsSortedParents(versRaw.slice(), sc, sd);
    var hasVers = vers.length > 1;
    var exp = !!map[mkey];
    html += profileMapsParentRowHtml(p, hasVers, exp, false);
    if (hasVers && exp) {
      vers.forEach(function(v) {
        html += profileMapsVersionRowHtml(v, false);
      });
    }
  });
  return html;
}

function profileMapsRowsGamemode(sortedGamemodes, expandedGm, expandedMap, sortCol, sortDir) {
  var gmState = expandedGm || {};
  var mapState = expandedMap || {};
  var sc = sortCol != null ? sortCol : 'logs_count';
  var sd = sortDir != null ? sortDir : -1;
  var html = '';
  sortedGamemodes.forEach(function(g) {
    var gk = g.gamemode_key != null ? String(g.gamemode_key) : '';
    var expGm = !!gmState[gk];
    html += profileMapsGamemodeRowHtml(g, expGm);
    if (expGm) {
      (g.maps || []).forEach(function(p) {
        var mkey = p.map_key != null ? String(p.map_key) : '';
        var versRaw = Array.isArray(p.versions) ? p.versions : [];
        var vers = profileMapsSortedParents(versRaw.slice(), sc, sd);
        var hasVers = vers.length > 1;
        var expMap = !!mapState[mkey];
        html += profileMapsParentRowHtml(p, hasVers, expMap, true);
        if (hasVers && expMap) {
          vers.forEach(function(v) {
            html += profileMapsVersionRowHtml(v, true);
          });
        }
      });
    }
  });
  return html;
}

function profileMapsGamemodeRowHtml(g, expanded) {
  var pl = g.map_label != null ? String(g.map_label) : '';
  var openTitle = 'Show maps — ' + pl;
  var btn = '<button type="button" class="profile-maps-expand-btn profile-maps-gm-expand-btn" aria-expanded="' + (expanded ? 'true' : 'false') + '" data-gamemode-key="' + escapeAttr(String(g.gamemode_key)) + '" title="' + escapeAttr(openTitle) + '">' +
    (expanded ? '\u25BC' : '\u25B6') + '</button>';
  var pct = profileMapsPctDisplay(g.pct_of_total);
  var wr = profileMapsWinRateDisplay(g.win_rate);
  var u = g.undecided_logs != null ? Number(g.undecided_logs) : 0;
  var undec = (u > 0) ? (' <span class="stats-summary-meta">(' + escapeHtml(String(u)) + ' undecided)</span>') : '';
  return '<tr class="profile-maps-gamemode" data-gamemode-key="' + escapeAttr(String(g.gamemode_key)) + '">' +
    '<td class="profile-maps-expand-cell">' + btn + '</td>' +
    '<td>' + escapeHtml(pl) + undec + '</td>' +
    '<td>' + escapeHtml(String(g.logs_count != null ? g.logs_count : '')) + '</td>' +
    '<td>' + (pct === '\u2014' ? '\u2014' : escapeHtml(pct)) + '</td>' +
    '<td>' + (wr === '\u2014' ? '\u2014' : escapeHtml(wr)) + '</td>' +
    '<td>' + escapeHtml(String(g.wins != null ? g.wins : '')) + '</td>' +
    '<td>' + escapeHtml(String(g.losses != null ? g.losses : '')) + '</td>' +
    '</tr>';
}

function profileMapsParentRowHtml(p, hasVers, expanded, underGamemode) {
  var btn = '';
  if (hasVers) {
    btn = '<button type="button" class="profile-maps-expand-btn" aria-expanded="' + (expanded ? 'true' : 'false') + '" data-map-key="' + escapeAttr(String(p.map_key)) + '" title="Show versions">' +
      (expanded ? '\u25BC' : '\u25B6') + '</button>';
  } else {
    btn = '<span class="profile-maps-expand-placeholder" aria-hidden="true">\u00a0</span>';
  }
  var pl = p.map_label != null ? String(p.map_label) : '';
  var pct = profileMapsPctDisplay(p.pct_of_total);
  var wr = profileMapsWinRateDisplay(p.win_rate);
  var u = p.undecided_logs != null ? Number(p.undecided_logs) : 0;
  var undec = (u > 0) ? (' <span class="stats-summary-meta">(' + escapeHtml(String(u)) + ' undecided)</span>') : '';
  var rowCls = 'profile-maps-parent' + (underGamemode ? ' profile-maps-parent--under-gm' : '');
  return '<tr class="' + rowCls + '" data-map-key="' + escapeAttr(String(p.map_key)) + '">' +
    '<td class="profile-maps-expand-cell">' + btn + '</td>' +
    '<td>' + escapeHtml(pl) + undec + '</td>' +
    '<td>' + escapeHtml(String(p.logs_count != null ? p.logs_count : '')) + '</td>' +
    '<td>' + (pct === '\u2014' ? '\u2014' : escapeHtml(pct)) + '</td>' +
    '<td>' + (wr === '\u2014' ? '\u2014' : escapeHtml(wr)) + '</td>' +
    '<td>' + escapeHtml(String(p.wins != null ? p.wins : '')) + '</td>' +
    '<td>' + escapeHtml(String(p.losses != null ? p.losses : '')) + '</td>' +
    '</tr>';
}

function profileMapsVersionRowHtml(v, underGamemode) {
  var pl = v.map != null ? String(v.map) : '';
  var pct = profileMapsPctDisplay(v.pct_of_total);
  var wr = profileMapsWinRateDisplay(v.win_rate);
  var u = v.undecided_logs != null ? Number(v.undecided_logs) : 0;
  var undec = (u > 0) ? (' <span class="stats-summary-meta">(' + escapeHtml(String(u)) + ' undecided)</span>') : '';
  var rowCls = 'profile-maps-version' + (underGamemode ? ' profile-maps-version--under-gm' : '');
  return '<tr class="' + rowCls + '">' +
    '<td class="profile-maps-expand-cell"></td>' +
    '<td class="profile-maps-version-map">' + escapeHtml(pl) + undec + '</td>' +
    '<td>' + escapeHtml(String(v.logs_count != null ? v.logs_count : '')) + '</td>' +
    '<td>' + (pct === '\u2014' ? '\u2014' : escapeHtml(pct)) + '</td>' +
    '<td>' + (wr === '\u2014' ? '\u2014' : escapeHtml(wr)) + '</td>' +
    '<td>' + escapeHtml(String(v.wins != null ? v.wins : '')) + '</td>' +
    '<td>' + escapeHtml(String(v.losses != null ? v.losses : '')) + '</td>' +
    '</tr>';
}

function profileMapsTableShellHtml() {
  return profileMapsTheadHtml('logs_count', -1) + '<tbody class="js-profile-maps-tbody"></tbody>';
}

var PROFILE_MAPS_META_MAPS = 'One row per base map (variants combined). Expand to see each version. Column headers sort this main list.';
var PROFILE_MAPS_META_GAMEMODE = 'One row per game type. Expand for maps, then for versions when there is more than one. Column headers sort the type list.';

function profileTopMapsBlock() {
  return '<div class="stats-summary profile-top-maps">' +
    '<p class="stats-summary-title">Most played maps</p>' +
    '<div class="profile-maps-toolbar">' +
    '<span class="stats-summary-meta profile-maps-toolbar-label">View</span>' +
    '<div class="stats-trend-toggle profile-maps-view-toggle" role="tablist" aria-label="Most played maps grouping">' +
    '<button type="button" class="stats-trend-btn js-profile-maps-view active" data-view="maps" role="tab" aria-selected="true">By map</button>' +
    '<button type="button" class="stats-trend-btn js-profile-maps-view" data-view="gamemode" role="tab" aria-selected="false">By gamemode</button>' +
    '</div></div>' +
    '<p class="stats-summary-meta js-profile-maps-meta">' + escapeHtml(PROFILE_MAPS_META_MAPS) + '</p>' +
    '<div class="stats-table-wrap"><table class="stats-table js-profile-maps">' + profileMapsTableShellHtml() + '</table></div></div>';
}

function bindProfileMapsTable(table) {
  var tbody = table.querySelector('tbody.js-profile-maps-tbody');
  if (!tbody) return;
  var card = table.closest('.profile-top-maps');
  function render() {
    var parents = table._profileMapRows || [];
    var view = table._mapsView || 'maps';
    var logsTotal = table._profileLogsTotal;
    if (logsTotal == null || !Number.isFinite(Number(logsTotal))) {
      logsTotal = parents.reduce(function(acc, p) { return acc + (Number(p.logs_count) || 0); }, 0);
    }
    var sorted;
    var bodyHtml;
    if (view === 'gamemode') {
      var groups = profileMapsFoldGamemodes(parents, logsTotal, table._sortCol, table._sortDir);
      sorted = profileMapsSortedParents(groups, table._sortCol, table._sortDir);
      bodyHtml = profileMapsRowsGamemode(sorted, table._mapsGmExpanded || {}, table._mapsExpanded || {}, table._sortCol, table._sortDir);
    } else {
      sorted = profileMapsSortedParents(parents, table._sortCol, table._sortDir);
      bodyHtml = profileMapsRowsOnly(sorted, table._mapsExpanded || {}, table._sortCol, table._sortDir);
    }
    tbody.innerHTML = bodyHtml;
    var oldThead = table.querySelector('thead');
    if (oldThead) {
      oldThead.outerHTML = profileMapsTheadHtml(table._sortCol, table._sortDir);
    }
  }
  table._mapsView = table._mapsView || 'maps';
  table._mapsGmExpanded = table._mapsGmExpanded || {};
  if (card && !card.hasAttribute('data-profile-maps-view-bound')) {
    card.setAttribute('data-profile-maps-view-bound', '1');
    card.addEventListener('click', function(ev) {
      var vbtn = ev.target.closest('.js-profile-maps-view');
      if (!vbtn || !card.contains(vbtn)) return;
      var v = vbtn.getAttribute('data-view');
      if (v !== 'maps' && v !== 'gamemode') return;
      if (table._mapsView === v) return;
      table._mapsView = v;
      table._mapsExpanded = {};
      table._mapsGmExpanded = {};
      card.querySelectorAll('.js-profile-maps-view').forEach(function(b) {
        var on = b.getAttribute('data-view') === v;
        b.classList.toggle('active', on);
        b.setAttribute('aria-selected', on ? 'true' : 'false');
      });
      var m = card.querySelector('.js-profile-maps-meta');
      if (m) m.textContent = v === 'gamemode' ? PROFILE_MAPS_META_GAMEMODE : PROFILE_MAPS_META_MAPS;
      render();
    });
  }
  table.addEventListener('click', function(ev) {
    var gmBtn = ev.target.closest('button.profile-maps-gm-expand-btn');
    if (gmBtn && table.contains(gmBtn)) {
      ev.preventDefault();
      var gk = gmBtn.getAttribute('data-gamemode-key');
      if (!gk) return;
      var gex = table._mapsGmExpanded || {};
      gex[gk] = !gex[gk];
      table._mapsGmExpanded = gex;
      render();
      return;
    }
    var btn = ev.target.closest('button.profile-maps-expand-btn:not(.profile-maps-gm-expand-btn)');
    if (btn && table.contains(btn)) {
      ev.preventDefault();
      var mk = btn.getAttribute('data-map-key');
      if (!mk) return;
      var ex = table._mapsExpanded || {};
      ex[mk] = !ex[mk];
      table._mapsExpanded = ex;
      render();
      return;
    }
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    var rows = table._profileMapRows;
    if (!rows || !rows.length) return;
    if (table._sortCol === col) {
      table._sortDir *= -1;
    } else {
      table._sortCol = col;
      table._sortDir = col === 'map_label' ? 1 : -1;
    }
    render();
  });
  render();
}

var PROFILE_LAYOUT_COOKIE = 'tf2ls_profile_layout_v1';
var PROFILE_LAYOUT_COOKIE_MAX_AGE = 31536000;
// Default order (new users / no cookie). Keep in sync with layout-share.js PROFILE_SECTION_IDS.
var PROFILE_LAYOUT_SECTION_IDS = window.TF2LS_PROFILE_SECTION_IDS || [
  'trend',
  'favorite_words',
  'coplayers',
  'top_maps',
  'classes',
  'class_kills',
  'top_logs',
  'weapons',
  'healspread',
  'rounds',
];
var PROFILE_LAYOUT_LABELS = {
  trend: 'DPM / KDR over time',
  top_logs: 'Top logs',
  coplayers: 'Most frequent co-players',
  favorite_words: 'Favorite Words',
  top_maps: 'Most played maps',
  classes: 'Class statistics',
  weapons: 'Weapons',
  class_kills: 'Kills by victim class',
  rounds: 'Rounds',
  healspread: 'Heal spread'
};

function sanitizeProfileLayoutOrder(raw) {
  var seen = Object.create(null);
  var out = [];
  if (Array.isArray(raw)) {
    raw.forEach(function(id) {
      if (PROFILE_LAYOUT_SECTION_IDS.indexOf(id) >= 0 && !seen[id]) {
        seen[id] = true;
        out.push(id);
      }
    });
  }
  PROFILE_LAYOUT_SECTION_IDS.forEach(function(id) {
    if (!seen[id]) out.push(id);
  });
  return out;
}

function readProfileLayoutSettings() {
  var d = { order: PROFILE_LAYOUT_SECTION_IDS.slice(), collapseDefault: false };
  try {
    var all = typeof document !== 'undefined' && document.cookie ? document.cookie : '';
    if (!all) return d;
    var prefix = PROFILE_LAYOUT_COOKIE + '=';
    var idx = all.indexOf(prefix);
    if (idx < 0) return d;
    var start = idx + prefix.length;
    var end = all.indexOf(';', start);
    var raw = decodeURIComponent(end < 0 ? all.slice(start) : all.slice(start, end));
    var o = JSON.parse(raw);
    if (o && typeof o === 'object') {
      if (Array.isArray(o.order)) d.order = sanitizeProfileLayoutOrder(o.order);
      if (typeof o.collapseDefault === 'boolean') d.collapseDefault = o.collapseDefault;
    }
  } catch (e) {}
  return d;
}

function writeProfileLayoutSettings(settings) {
  try {
    var payload = encodeURIComponent(JSON.stringify({
      order: sanitizeProfileLayoutOrder(settings.order || []),
      collapseDefault: !!settings.collapseDefault
    }));
    document.cookie = PROFILE_LAYOUT_COOKIE + '=' + payload + ';path=/;max-age=' + PROFILE_LAYOUT_COOKIE_MAX_AGE + ';SameSite=Lax';
  } catch (e) {}
}

function wrapProfileOverview(innerHtml) {
  return '<div class="profile-section profile-section-overview" data-section="overview" data-layout-fixed="1">' + innerHtml + '</div>';
}

function wrapProfileSection(id, innerHtml, startCollapsed) {
  if (!innerHtml) return '';
  var label = PROFILE_LAYOUT_LABELS[id] || id;
  var exp = startCollapsed ? 'false' : 'true';
  var hiddenAttr = startCollapsed ? ' hidden' : '';
  var chev = startCollapsed ? '\u25B6' : '\u25BC';
  var panelId = 'psp_' + id.replace(/[^a-z0-9_]/gi, '_');
  return '<section class="profile-section js-profile-section' + (startCollapsed ? ' is-collapsed' : '') + '" data-section="' + escapeAttr(id) + '">' +
    '<div class="profile-section-toolbar">' +
    '<button type="button" class="profile-section-collapse-toggle" aria-expanded="' + exp + '" aria-controls="' + escapeAttr(panelId) + '" id="psh_' + escapeAttr(id) + '">' +
    '<span class="profile-section-chevron-wrap" aria-hidden="true"><span class="profile-section-chevron">' + chev + '</span></span>' +
    '<span class="profile-section-toolbar-label">' + escapeHtml(label) + '</span></button></div>' +
    '<div class="profile-section-panel" id="' + escapeAttr(panelId) + '" role="region"' + hiddenAttr + '>' + innerHtml + '</div></section>';
}

function profileLayoutSettingsPanelHtml() {
  return '<details class="profile-layout-settings-details stats-summary profile-layout-settings js-profile-layout-settings">' +
    '<summary class="profile-layout-settings-summary">Profile layout</summary>' +
    '<div class="profile-layout-settings-body">' +
    '<p class="stats-summary-meta">Order and display options are stored in a cookie on this browser only. The player summary at the top cannot be moved or collapsed.</p>' +
    '<p class="stats-summary-meta">Dimmed entries with “(not shown)” have no data for this player or filter. You can still reorder them: the order is saved and applies when that section appears (another player, class, or filters).</p>' +
    '<label class="profile-layout-collapse-opt"><input type="checkbox" class="js-profile-layout-collapse-default" /> <span>Collapse all sections below the summary by default when opening a profile</span></label>' +
    '<p class="stats-summary-meta">Drag to reorder sections (applies to profiles on this device):</p>' +
    '<ul class="profile-layout-sort-list js-profile-layout-sort-list" role="list" aria-label="Section order"></ul>' +
    '<div class="layout-share-tools">' +
    '<p class="stats-summary-meta">Import/export layout (saved in this browser). Text lists home section order/visibility and profile section order/collapse. Share links apply layout when opened.</p>' +
    '<textarea class="layout-share-text js-layout-share-text" rows="5" spellcheck="false" autocomplete="off" placeholder="tf2ls-layout-v1&#10;home.order ...&#10;... or paste a layout token from a link"></textarea>' +
    '<p class="layout-share-actions">' +
    '<button type="button" class="js-layout-share-import">Apply import</button> ' +
    '<button type="button" class="js-layout-share-export">Copy text</button> ' +
    '<button type="button" class="js-layout-share-link-page" title="Copies a link to the page you are currently viewing with your layout embedded. Use this when you want someone to open the same search/profile page and see it with your layout.">Copy link (this page)</button> ' +
    '<button type="button" class="js-layout-share-link-home" title="Copies a link to the home page with your layout embedded. Use this when you only want to share your home/profile layout settings, not the current search or profile URL.">Copy link (home)</button>' +
    '</p>' +
    '<p class="layout-share-status js-layout-share-status stats-summary-meta" aria-live="polite"></p>' +
    '</div>' +
    '<p class="browser-settings-actions">' +
    '<button type="button" class="js-profile-layout-settings-reset">Reset profile layout to defaults</button>' +
    '</p>' +
    '</div></details>';
}

function bindProfileSectionCollapsers(profileRoot) {
  profileRoot.addEventListener('click', function(ev) {
    var btn = ev.target.closest('.profile-section-collapse-toggle');
    if (!btn || !profileRoot.contains(btn)) return;
    var sec = btn.closest('.profile-section');
    if (!sec) return;
    var panel = sec.querySelector('.profile-section-panel');
    var chev = btn.querySelector('.profile-section-chevron');
    if (!panel) return;
    var expanded = btn.getAttribute('aria-expanded') === 'true';
    var next = !expanded;
    btn.setAttribute('aria-expanded', next ? 'true' : 'false');
    panel.hidden = !next;
    sec.classList.toggle('is-collapsed', !next);
    if (chev) chev.textContent = next ? '\u25BC' : '\u25B6';
    if (next && sec.getAttribute('data-section') === 'trend' && typeof renderProfileTrendChart === 'function' &&
        profileTrendRows && profileTrendRows.length >= 2 && profileTrendHost) {
      try {
        renderProfileTrendChart(profileTrendHost, profileTrendRows, profileTrendMetric || 'dpm');
      } catch (e) {}
    }
    if (next && sec.getAttribute('data-section') === 'favorite_words' &&
        typeof scheduleProfileWordCloudMount === 'function') {
      var cloudHost = sec.querySelector('.profile-word-cloud-host');
      if (cloudHost && cloudHost._profileWordCloudWords) {
        scheduleProfileWordCloudMount(
          cloudHost,
          cloudHost._profileWordCloudWords,
          cloudHost._profileWordCloudOptions || {}
        );
      }
    }
  });
}

function applyProfileStackOrderFromList(ul, profileRoot) {
  var order = [];
  ul.querySelectorAll('li[data-section]').forEach(function(li) {
    order.push(li.getAttribute('data-section'));
  });
  var full = sanitizeProfileLayoutOrder(order);
  var cur = readProfileLayoutSettings();
  writeProfileLayoutSettings({ order: full, collapseDefault: !!cur.collapseDefault });

  var stack = profileRoot.querySelector('.js-profile-layout-stack');
  if (!stack) return;
  var overview = stack.querySelector('[data-section="overview"]');
  var byId = Object.create(null);
  stack.querySelectorAll('[data-section]').forEach(function(node) {
    var id = node.getAttribute('data-section');
    if (id && id !== 'overview') byId[id] = node;
  });
  // Reorder collapsible sections only (overview is not in PROFILE_LAYOUT_SECTION_IDS / full).
  // Do not appendChild(overview) here — that moved the summary to the end and left it there.
  full.forEach(function(id) {
    var n = byId[id];
    if (n) stack.appendChild(n);
  });
  if (overview) stack.prepend(overview);
}

function applyProfileLayoutOrderToStack(profileRoot, order) {
  var stack = profileRoot.querySelector('.js-profile-layout-stack');
  if (!stack) return;
  var full = sanitizeProfileLayoutOrder(order || []);
  var overview = stack.querySelector('[data-section="overview"]');
  var byId = Object.create(null);
  stack.querySelectorAll('[data-section]').forEach(function(node) {
    var id = node.getAttribute('data-section');
    if (id && id !== 'overview') byId[id] = node;
  });
  full.forEach(function(id) {
    var n = byId[id];
    if (n) stack.appendChild(n);
  });
  if (overview) stack.prepend(overview);
}

function rebuildProfileLayoutSortListItems(ul, profileRoot, layoutSettings) {
  if (!ul) return;
  ul.innerHTML = '';
  layoutSettings.order.forEach(function(sid) {
    if (PROFILE_LAYOUT_SECTION_IDS.indexOf(sid) < 0) return;
    var li = document.createElement('li');
    li.setAttribute('draggable', 'true');
    li.setAttribute('data-section', sid);
    li.className = 'profile-layout-sort-item';
    var stack = profileRoot.querySelector('.js-profile-layout-stack');
    var onPage = stack && stack.querySelector('[data-section="' + sid + '"]');
    if (!onPage) li.classList.add('profile-layout-sort-item-missing');
    li.appendChild(document.createTextNode(PROFILE_LAYOUT_LABELS[sid] || sid));
    if (!onPage) {
      li.setAttribute('title', 'No data for this profile with current filters. Order is still saved for when this section appears.');
      var hint = document.createElement('span');
      hint.className = 'profile-layout-sort-hint';
      hint.appendChild(document.createTextNode(' (not shown)'));
      li.appendChild(hint);
    }
    ul.appendChild(li);
  });
}

function bindProfileLayoutSortList(ul, profileRoot) {
  if (!ul || ul._tf2lsProfileSortBound) return;
  ul._tf2lsProfileSortBound = true;
  var dragEl = null;
  ul.addEventListener('dragstart', function(e) {
    var li = e.target && e.target.closest ? e.target.closest('li') : null;
    if (!li || !ul.contains(li)) return;
    dragEl = li;
    try {
      e.dataTransfer.setData('text/plain', li.getAttribute('data-section') || '');
      e.dataTransfer.effectAllowed = 'move';
    } catch (err) {}
    li.classList.add('dragging');
  });
  ul.addEventListener('dragend', function() {
    if (dragEl) dragEl.classList.remove('dragging');
    dragEl = null;
  });
  ul.addEventListener('dragover', function(e) {
    e.preventDefault();
    try { e.dataTransfer.dropEffect = 'move'; } catch (err) {}
  });
  ul.addEventListener('drop', function(e) {
    e.preventDefault();
    var target = e.target && e.target.closest ? e.target.closest('li') : null;
    if (!target || !ul.contains(target) || !dragEl || dragEl === target) return;
    var rect = target.getBoundingClientRect();
    var before = e.clientY < rect.top + rect.height / 2;
    if (before) {
      ul.insertBefore(dragEl, target);
    } else {
      ul.insertBefore(dragEl, target.nextSibling);
    }
    applyProfileStackOrderFromList(ul, profileRoot);
  });
}

function initProfileLayoutSettings(profileRoot, layoutSettings) {
  var wrap = profileRoot.querySelector('.js-profile-layout-settings');
  if (!wrap) return;
  var cb = wrap.querySelector('.js-profile-layout-collapse-default');
  if (cb) {
    cb.checked = !!layoutSettings.collapseDefault;
    if (!cb._tf2lsCollapseBound) {
      cb._tf2lsCollapseBound = true;
      cb.addEventListener('change', function() {
        var s = readProfileLayoutSettings();
        s.collapseDefault = cb.checked;
        writeProfileLayoutSettings(s);
      });
    }
  }
  var ul = wrap.querySelector('.js-profile-layout-sort-list');
  if (ul) {
    rebuildProfileLayoutSortListItems(ul, profileRoot, layoutSettings);
    bindProfileLayoutSortList(ul, profileRoot);
  }
  if (window.tf2lsLayoutShare && wrap && !wrap._tf2lsLayoutShareBound) {
    wrap._tf2lsLayoutShareBound = true;
    window.tf2lsLayoutShare.bindPanel(wrap, {
      afterApply: function() {
        var s = readProfileLayoutSettings();
        applyProfileLayoutOrderToStack(profileRoot, s.order);
        var cb2 = wrap.querySelector('.js-profile-layout-collapse-default');
        if (cb2) cb2.checked = s.collapseDefault;
        var ul2 = wrap.querySelector('.js-profile-layout-sort-list');
        rebuildProfileLayoutSortListItems(ul2, profileRoot, s);
        bindProfileLayoutSortList(ul2, profileRoot);
      },
    });
  }

  function clearProfileLayoutShareFields(panel) {
    if (!panel) return;
    var ta = panel.querySelector('.js-layout-share-text');
    var st = panel.querySelector('.js-layout-share-status');
    if (ta) ta.value = '';
    if (st) st.textContent = '';
  }

  function resetProfileLayoutSettings() {
    clearTf2lsCookie(PROFILE_LAYOUT_COOKIE);
    var defaults = {
      order: PROFILE_LAYOUT_SECTION_IDS.slice(),
      collapseDefault: false,
    };
    applyProfileLayoutOrderToStack(profileRoot, defaults.order);
    if (cb) cb.checked = false;
    if (ul) rebuildProfileLayoutSortListItems(ul, profileRoot, defaults);
    clearProfileLayoutShareFields(wrap);
  }

  var resetBtn = wrap.querySelector('.js-profile-layout-settings-reset');
  if (resetBtn && !resetBtn._tf2lsProfileLayoutResetBound) {
    resetBtn._tf2lsProfileLayoutResetBound = true;
    resetBtn.addEventListener('click', function(ev) {
      ev.preventDefault();
      resetProfileLayoutSettings();
    });
  }
}

/** Current /results URL with query string; fragment omitted so the link is the search itself. */
function profileResultsShareUrl() {
  try {
    var u = new URL(window.location.href);
    u.hash = '';
    return u.toString();
  } catch (err) {
    var href = window.location.href || '';
    var hashIdx = href.indexOf('#');
    return hashIdx >= 0 ? href.slice(0, hashIdx) : href;
  }
}

function copyProfileShareUrlFallback(text, onOk, onFail) {
  try {
    var ta = document.createElement('textarea');
    ta.value = text;
    ta.setAttribute('readonly', '');
    ta.setAttribute('aria-hidden', 'true');
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    ta.style.top = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    ta.setSelectionRange(0, text.length);
    var ok = document.execCommand('copy');
    document.body.removeChild(ta);
    if (ok) onOk(); else onFail();
  } catch (e) {
    onFail();
  }
}

/** Chain-link icon for profile copy-link (two interlocking hooks; readable at small sizes). */
function profileCopyLinkButtonHtml() {
  return '<button type="button" class="chat-hit-link profile-copy-link-btn js-profile-copy-link" title="Copy link to this search" aria-label="Copy link to this profile search">' +
    '<svg class="profile-copy-link-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" focusable="false">' +
    '<g fill="none" stroke-linecap="round" stroke-linejoin="round">' +
    '<path class="profile-copy-link-icon-outline" stroke-width="3.25" d="M9.5 14.5l-3.25-3.25a3.75 3.75 0 0 1 5.3-5.3l1.7 1.7"/>' +
    '<path class="profile-copy-link-icon-outline" stroke-width="3.25" d="M14.5 9.5l3.25 3.25a3.75 3.75 0 0 1-5.3 5.3l-1.7-1.7"/>' +
    '<path class="profile-copy-link-icon-stroke" stroke-width="2" d="M9.5 14.5l-3.25-3.25a3.75 3.75 0 0 1 5.3-5.3l1.7 1.7"/>' +
    '<path class="profile-copy-link-icon-stroke" stroke-width="2" d="M14.5 9.5l3.25 3.25a3.75 3.75 0 0 1-5.3 5.3l-1.7-1.7"/>' +
    '</g></svg></button>';
}

function bindProfileCopyLinkButton(profileRoot) {
  var btn = profileRoot.querySelector('.js-profile-copy-link');
  if (!btn) return;
  btn.addEventListener('click', function() {
    var url = profileResultsShareUrl();
    var defaultLabel = 'Copy link to this profile search';
    var defaultTitle = 'Copy link to this search';
    function onOk() {
      btn.setAttribute('aria-label', 'Copied to clipboard');
      btn.setAttribute('title', 'Copied!');
      setTimeout(function() {
        btn.setAttribute('aria-label', defaultLabel);
        btn.setAttribute('title', defaultTitle);
      }, 1200);
    }
    function onFail() {
      try {
        btn.setAttribute('title', 'Could not copy — copy from the address bar or use HTTPS.');
      } catch (e2) {}
      setTimeout(function() {
        try { btn.removeAttribute('title'); } catch (e3) {}
      }, 4000);
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(url).then(onOk).catch(function() {
        copyProfileShareUrlFallback(url, onOk, onFail);
      });
    } else {
      copyProfileShareUrlFallback(url, onOk, onFail);
    }
  });
}

function renderProfileResult(el, data, elapsedMs) {
  destroyStatsTrendChart();
  destroyProfileTrendChart();
  profileTrendHost = null;
  profileTrendRows = null;
  var name = data.display_name != null && String(data.display_name).trim() ? escapeHtml(String(data.display_name)) : escapeHtml(String(data.steamid64 || ''));
  var ov = data.overview || {};
  var mpc = ov.most_played_class;
  var mpcLine = mpc
    ? ('<p class="stats-summary-meta">Most played: ' + profileClassCell(mpc) + '</p>')
    : '';
  var pv = data.profile_views;
  var pvLine = '';
  if (pv && typeof pv.total === 'number' && Number.isFinite(pv.total) && pv.total > 0) {
    var uq = typeof pv.unique === 'number' && Number.isFinite(pv.unique) ? pv.unique : null;
    pvLine =
      '<p class="stats-summary-meta profile-view-stats" title="Total views count every profile load. Unique visitors are approximated from a hashed network + browser signature (not logins).">' +
      escapeHtml(fmtProgressNum(pv.total)) +
      ' page views' +
      (uq != null ? (' \u00b7 ' + escapeHtml(fmtProgressNum(uq)) + ' unique visitors') : '') +
      '</p>';
  }
  var dateRange = '<p class="stats-summary-meta">Logs: ' + escapeHtml(String(data.logs_count != null ? data.logs_count : '')) +
    ' &middot; ' + profileLogsTfDateLink(ov.first_log_id, profileFormatUnixDate(ov.first_log_ts)) +
    ' \u2192 ' + profileLogsTfDateLink(ov.last_log_id, profileFormatUnixDate(ov.last_log_ts)) + '</p>';
  var gridItems = [
    { key: 'Wins', value: ov.wins != null ? String(ov.wins) : '\u2014' },
    { key: 'Losses', value: ov.losses != null ? String(ov.losses) : '\u2014' },
    { key: 'Draws', value: ov.draws != null ? String(ov.draws) : '\u2014' },
    { key: 'Win rate', value: ov.win_rate != null ? (Math.round(Number(ov.win_rate) * 10000) / 100 + '%') : '\u2014' },
    { key: 'Avg DPM', value: ov.avg_dpm != null ? String(ov.avg_dpm) : '\u2014' },
    { key: 'Avg KDR', value: ov.avg_kdr != null ? String(ov.avg_kdr) : '\u2014' },
    { key: 'Avg KADR', value: ov.avg_kadr != null ? String(ov.avg_kadr) : '\u2014' },
    { key: 'Avg K', value: ov.avg_kills != null ? String(ov.avg_kills) : '\u2014' },
    { key: 'Avg D', value: ov.avg_deaths != null ? String(ov.avg_deaths) : '\u2014' },
    { key: 'Best killstreak', value: ov.best_killstreak != null ? String(ov.best_killstreak) : '\u2014' },
    { key: 'Total damage', value: ov.total_damage != null ? String(ov.total_damage) : '\u2014' },
    { key: 'Total kills', value: ov.total_kills != null ? String(ov.total_kills) : '\u2014' },
    { key: 'Total ubers', value: ov.total_ubers != null ? String(ov.total_ubers) : '\u2014' },
    { key: 'Total drops', value: ov.total_drops != null ? String(ov.total_drops) : '\u2014' }
  ];
  var grid = gridItems.map(function(item) {
    return '<div class="stats-summary-item"><span class="stats-summary-key">' + escapeHtml(item.key) + '</span><span class="stats-summary-value">' + escapeHtml(item.value) + '</span></div>';
  }).join('');
  var sid64 = data.steamid64 != null ? String(data.steamid64).trim() : '';
  var steamCommunityProfile = /^\d{17}$/.test(sid64)
    ? ('https://steamcommunity.com/profiles/' + encodeURIComponent(sid64))
    : '';
  var logsTfProfile = /^\d{17}$/.test(sid64) ? ('https://logs.tf/profile/' + encodeURIComponent(sid64)) : '';
  var av = data.avatar_url;
  var avatarHtml = '';
  if (av != null && typeof av === 'string' && /^https:\/\//i.test(av.trim())) {
    var imgTag = '<img class="profile-avatar" src="' + escapeAttr(av.trim()) + '" alt="" width="184" height="184" loading="eager" decoding="async" fetchpriority="high" referrerpolicy="no-referrer" />';
    avatarHtml = steamCommunityProfile
      ? '<div class="profile-avatar-wrap"><a class="profile-avatar-link" href="' + escapeAttr(steamCommunityProfile) + '" target="_blank" rel="noopener noreferrer" title="Open Steam profile">' + imgTag + '</a></div>'
      : '<div class="profile-avatar-wrap">' + imgTag + '</div>';
  }
  var logsTfLinkHtml = logsTfProfile
    ? (' <a href="' + escapeAttr(logsTfProfile) + '" target="_blank" rel="noopener noreferrer" title="View on logs.tf" class="profile-logstf-link">\ud83c\uddfa\ud83c\udde6</a>')
    : '';
  var overviewCard =
    '<div class="stats-summary profile-overview">' +
    profileCopyLinkButtonHtml() +
    '<div class="profile-overview-head">' + avatarHtml + '<div class="profile-overview-text">' +
    '<p class="stats-summary-title">' + name + logsTfLinkHtml + '</p>' +
    dateRange + mpcLine + pvLine +
    '</div></div>' +
    '<div class="stats-summary-grid">' + grid + '</div></div>';

  var trendRows = Array.isArray(data.trend_rows) ? data.trend_rows : [];
  var trendBlock = '';
  if (trendRows.length >= 2) {
    trendBlock =
      '<div class="stats-summary profile-trend-summary">' +
        '<p class="stats-summary-title">DPM, KDR, deaths over time</p>' +
        '<p class="stats-summary-meta">Up to 10,000 most recent logs matching filters. Deaths are per game (one row per log).</p>' +
        '<div class="stats-trend js-profile-trend">' +
          '<div class="stats-trend-toggle" role="tablist" aria-label="Profile trend metric">' +
            '<button type="button" class="stats-trend-btn js-profile-trend-btn active" data-metric="dpm" role="tab" aria-selected="true">DPM</button>' +
            '<button type="button" class="stats-trend-btn js-profile-trend-btn" data-metric="kpair" role="tab" aria-selected="false">KDR / KADR</button>' +
            '<button type="button" class="stats-trend-btn js-profile-trend-btn" data-metric="deaths" role="tab" aria-selected="false">Deaths / game</button>' +
          '</div>' +
          '<div class="stats-trend-canvas-wrap"><canvas class="js-trend-chart-canvas" aria-label="Profile per-game trends"></canvas></div>' +
          '<p class="stats-trend-note">20-game rolling average with per-game points. Y-axis trimmed to the middle ~96% of values so bad logs do not flatten the curve; tooltips show exact stats.</p>' +
        '</div>' +
      '</div>';
  }

  var topLogs = Array.isArray(data.top_logs) ? data.top_logs : [];
  var topLogsBlock = '';
  if (data.logs_count > 0) {
    if (topLogs.length) {
      var tlHead = '<tr><th>Record</th><th>Best</th><th>Map</th><th>Date</th><th></th></tr>';
      var tlBody = topLogs.map(function(r) {
        var best = formatTopLogPrimaryValue(r);
        var mapStr = r.map != null ? String(r.map) : '';
        var logId = r.log_id != null ? String(r.log_id) : '';
        var logUrl = logPageHref(logId, r.log_url);
        var k = r.kills != null ? String(r.kills) : '';
        var d = r.deaths != null ? String(r.deaths) : '';
        var a = r.assists != null ? String(r.assists) : '';
        var dmg = r.damage != null ? String(r.damage) : '';
        var metaLine = escapeHtml(k + '/' + d + '/' + a + ' K/D/A')
          + (dmg ? escapeHtml(' · ' + dmg + ' dmg') : '')
          + (r.dapm != null ? escapeHtml(' · ' + String(r.dapm) + ' DPM') : '');
        var rowTitle = (r.title != null && String(r.title).trim()) ? ('<br><span class="stats-summary-meta">' + escapeHtml(String(r.title)) + '</span>') : '';
        var teamStr = r.team != null && String(r.team).trim() ? (' <span class="stats-summary-meta">(' + escapeHtml(String(r.team)) + ')</span>') : '';
        var linkExt = logUrl && !isInternalLogHref(logUrl) ? ' target="_blank" rel="noopener noreferrer"' : '';
        var linkCell = logUrl
          ? ('<a href="' + escapeAttr(logUrl) + '"' + linkExt + '>View log</a>')
          : '\u2014';
        return '<tr><td>' + escapeHtml(String(r.label || '')) + '</td><td><strong>' + escapeHtml(best) + '</strong>' + teamStr + '<br><span class="stats-summary-meta">' + metaLine + '</span>' + rowTitle + '</td><td>' + escapeHtml(mapStr) + '</td><td>' + profileFormatUnixDate(r.date_ts) + '</td><td>' + linkCell + '</td></tr>';
      }).join('');
      topLogsBlock = '<div class="stats-summary profile-top-logs"><p class="stats-summary-title">Top logs</p><p class="stats-summary-meta">Best single-game lines on Red or Blue in this filter. Merged (&ldquo;combined&rdquo;) uploads are excluded: empty map, multi-map map field, single-word maps without an underscore (e.g. placeholders), series-style titles, combined-style phrases, and (when chat is indexed) combiner chat lines.</p><div class="stats-table-wrap"><table class="stats-table"><thead>' + tlHead + '</thead><tbody>' + tlBody + '</tbody></table></div></div>';
    } else {
      topLogsBlock = '<div class="stats-summary profile-top-logs"><p class="stats-summary-title">Top logs</p><p class="stats-summary-meta">No Red or Blue team rows in this filter; widen filters or backfill stats to see bests.</p></div>';
    }
  }

  var topCoplayersBlock = profileTopCoplayersBlock(data);
  var favoriteWords = Array.isArray(data.favorite_words) ? data.favorite_words : [];
  var favoriteWordsBlock = favoriteWords.length
    ? '<div class="stats-summary profile-favorite-words js-profile-favorite-words"></div>'
    : '';

  var mapRows = Array.isArray(data.top_maps) ? data.top_maps : [];
  var topMapsBlock = '';
  if (data.logs_count > 0 && mapRows.length) {
    topMapsBlock = profileTopMapsBlock();
  }

  var classes = data.classes || [];
  var classesOther = data.classes_other || [];
  var classesTable = '';
  var profileClassDefaultSortCol = 'total_playtime_secs';
  var profileClassDefaultSortDir = -1;
  if (classes.length) {
    var cInner = profileClassTableInnerHtml(classes, profileClassDefaultSortCol, profileClassDefaultSortDir);
    classesTable = '<div class="stats-summary"><p class="stats-summary-title">By class</p><p class="stats-summary-meta">Click a column header to sort; click again to reverse.</p><div class="stats-table-wrap"><table class="stats-table js-profile-classes-main">' + cInner + '</table></div></div>';
  }
  if (classesOther.length) {
    var oInner = profileClassTableInnerHtml(classesOther, profileClassDefaultSortCol, profileClassDefaultSortDir);
    classesTable += '<details class="profile-classes-other"><summary>Other classes (' + escapeHtml(String(classesOther.length)) + ')</summary>' +
      '<p class="stats-summary-meta">Spectator, mod classes, or legacy labels not in the standard nine. Click column headers to sort.</p>' +
      '<div class="stats-table-wrap"><table class="stats-table js-profile-classes-other">' + oInner + '</table></div></details>';
  }

  var weapons = data.weapons || [];
  var weaponsTable = weapons.length ? profileWeaponsBlock(weapons) : '';

  var ck = profileClassKillsFilterRows(data.class_kills || []);
  var ckTable = ck.length ? profileClassKillsBlock(ck) : '';

  var rounds = data.rounds || {};
  var roundsCard = '';
  if (rounds.total_rounds > 0) {
    var fbr = rounds.first_blood_rate != null ? (Math.round(Number(rounds.first_blood_rate) * 10000) / 100 + '%') : '\u2014';
    var rwr = rounds.round_win_rate_on_team != null ? (Math.round(Number(rounds.round_win_rate_on_team) * 10000) / 100 + '%') : '\u2014';
    var ritems = [
      { key: 'Total rounds', value: String(rounds.total_rounds) },
      { key: 'Avg duration', value: profileFormatDurationMinSec(rounds.avg_round_duration_secs) },
      { key: 'First bloods', value: rounds.first_bloods != null ? String(rounds.first_bloods) : '\u2014' },
      { key: 'First blood rate', value: fbr },
      { key: 'Round win rate (your team)', value: rwr }
    ];
    var rgrid = ritems.map(function(item) {
      return '<div class="stats-summary-item"><span class="stats-summary-key">' + escapeHtml(item.key) + '</span><span class="stats-summary-value">' + escapeHtml(String(item.value)) + '</span></div>';
    }).join('');
    roundsCard = '<div class="stats-summary"><p class="stats-summary-title">Rounds</p><div class="stats-summary-grid">' + rgrid + '</div></div>';
  }

  var hs = data.healspread || {};
  var ht = hs.healed_to || [];
  var hb = hs.healed_by || [];
  var healCard = '';
  if (ht.length || hb.length) {
    healCard = '<div class="stats-summary profile-healspread-wrap"><p class="stats-summary-title">Heal spread</p><p class="stats-summary-meta">Per-log peaks use logs.tf match length when available. HP/min = healing in that log &divide; log duration. Click column headers to sort.</p><div class="profile-healspread-cols">' +
      '<div class="profile-heal-col"><h3 class="stats-summary-title">Healed to</h3>' +
      '<div class="stats-table-wrap profile-heal-table-wrap"><table class="stats-table js-profile-healspread-to"></table></div></div>' +
      '<div class="profile-heal-col"><h3 class="stats-summary-title">Healed by</h3>' +
      '<div class="stats-table-wrap profile-heal-table-wrap"><table class="stats-table js-profile-healspread-by"></table></div></div>' +
      '</div></div>';
  }

  var layoutSettings = readProfileLayoutSettings();
  var sectionChunks = {
    trend: trendBlock,
    top_logs: topLogsBlock,
    coplayers: topCoplayersBlock,
    favorite_words: favoriteWordsBlock,
    top_maps: topMapsBlock,
    classes: classesTable,
    weapons: weaponsTable,
    class_kills: ckTable,
    rounds: roundsCard,
    healspread: healCard
  };
  var stackParts = [wrapProfileOverview(overviewCard)];
  layoutSettings.order.forEach(function(sid) {
    var chunk = sectionChunks[sid];
    if (chunk) stackParts.push(wrapProfileSection(sid, chunk, layoutSettings.collapseDefault));
  });
  el.innerHTML = '<div class="js-profile-layout-stack">' + stackParts.join('') + '</div>' +
    requestTimingFooter(elapsedMs) + profileLayoutSettingsPanelHtml();

  profileTrendMetric = 'dpm';
  profileTrendHost = el.querySelector('.js-profile-trend');
  profileTrendRows = trendRows;
  if (profileTrendHost && profileTrendRows && profileTrendRows.length >= 2) {
    bindProfileTrendControls(el);
    var trendPanelInit = el.querySelector('[data-section="trend"] .profile-section-panel');
    if (!trendPanelInit || !trendPanelInit.hidden) {
      renderProfileTrendChart(profileTrendHost, profileTrendRows, profileTrendMetric);
    }
  }

  var tm = el.querySelector('table.js-profile-classes-main');
  if (tm && classes.length) {
    tm._profileClassRows = classes;
    tm._sortCol = profileClassDefaultSortCol;
    tm._sortDir = profileClassDefaultSortDir;
    bindProfileClassTableSort(tm);
  }
  var to = el.querySelector('table.js-profile-classes-other');
  if (to && classesOther.length) {
    to._profileClassRows = classesOther;
    to._sortCol = profileClassDefaultSortCol;
    to._sortDir = profileClassDefaultSortDir;
    bindProfileClassTableSort(to);
  }

  bindProfileCoplayersToggle(el);
  bindProfileCoplayersTables(el, data);
  bindProfileClassKillsTable(el, data);
  bindProfileWeaponsTable(el, data);

  var favoriteWordsRoot = el.querySelector('.js-profile-favorite-words');
  if (favoriteWordsRoot && favoriteWords.length) {
    renderFavoriteWords(favoriteWords, favoriteWordsRoot);
  }

  var healToTable = el.querySelector('table.js-profile-healspread-to');
  if (healToTable) {
    if (ht.length) {
      healToTable._profileHealspreadRows = ht;
      healToTable._sortCol = 'total_healing';
      healToTable._sortDir = -1;
      healToTable.innerHTML = profileHealspreadTableInnerHtml(ht, healToTable._sortCol, healToTable._sortDir);
      bindProfileHealspreadTableSort(healToTable);
    } else {
      healToTable.innerHTML = profileHealspreadEmptyTableInnerHtml();
    }
  }
  var healByTable = el.querySelector('table.js-profile-healspread-by');
  if (healByTable) {
    if (hb.length) {
      healByTable._profileHealspreadRows = hb;
      healByTable._sortCol = 'total_healing';
      healByTable._sortDir = -1;
      healByTable.innerHTML = profileHealspreadTableInnerHtml(hb, healByTable._sortCol, healByTable._sortDir);
      bindProfileHealspreadTableSort(healByTable);
    } else {
      healByTable.innerHTML = profileHealspreadEmptyTableInnerHtml();
    }
  }

  var mapTable = el.querySelector('table.js-profile-maps');
  if (mapTable && mapRows.length) {
    mapTable._profileMapRows = mapRows;
    mapTable._sortCol = 'logs_count';
    mapTable._sortDir = -1;
    mapTable._mapsExpanded = {};
    mapTable._mapsGmExpanded = {};
    mapTable._mapsView = 'maps';
    mapTable._profileLogsTotal = data.logs_count != null ? Number(data.logs_count) : null;
    bindProfileMapsTable(mapTable);
  }

  loadAvatarsInContainer(el);

  bindProfileSectionCollapsers(el);
  initProfileLayoutSettings(el, layoutSettings);
  bindProfileCopyLinkButton(el);
}
