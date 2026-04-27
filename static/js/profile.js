
function profileFormatUnixDate(ts) {
  if (ts == null || !Number.isFinite(Number(ts))) return '\u2014';
  try {
    return new Date(Number(ts) * 1000).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  } catch (e) {
    return '\u2014';
  }
}

/** Link to logs.tf for a numeric log id; otherwise escape label as plain text (security). */
function profileLogsTfDateLink(logId, dateLabel) {
  var label = dateLabel != null ? String(dateLabel) : '\u2014';
  if (logId == null || logId === '') return escapeHtml(label);
  var idStr = String(logId).trim();
  if (!/^\d+$/.test(idStr)) return escapeHtml(label);
  var url = 'https://logs.tf/' + encodeURIComponent(idStr);
  return '<a href="' + escapeAttr(url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(label) + '</a>';
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
  var theadWith = '<tr><th>Player</th><th>Logs</th><th>Win% (with)</th><th>Class (with you)</th><th>Playtime (with you)</th></tr>';
  var theadAgainst = '<tr><th>Player</th><th>Logs</th><th>Win% (vs)</th><th>Class (vs you)</th><th>Playtime (vs you)</th></tr>';
  var bodyWith = hasWith
    ? withRows.map(function(r) { return profileTopCoplayersRowHtml(r, 'with'); }).join('')
    : '<tr><td colspan="5" class="stats-summary-meta">No qualifying co-players in this filter.</td></tr>';
  var bodyAgainst = hasAgainst
    ? againstRows.map(function(r) { return profileTopCoplayersRowHtml(r, 'against'); }).join('')
    : '<tr><td colspan="5" class="stats-summary-meta">No qualifying opponents in this filter.</td></tr>';
  var emptyWith = hasWith ? '0' : '1';
  var emptyAgainst = hasAgainst ? '0' : '1';
  return '<div class="stats-summary profile-top-coplayers js-profile-coplayers" data-with-empty="' + emptyWith + '" data-against-empty="' + emptyAgainst + '">' +
    '<p class="stats-summary-title">Most frequent co-players</p>' +
    '<div class="profile-coplayers-toolbar">' +
    '<span class="stats-summary-meta profile-coplayers-toolbar-label">Show</span>' +
    '<div class="stats-trend-toggle profile-coplayers-toggle" role="tablist" aria-label="Co-player relation">' +
    '<button type="button" class="stats-trend-btn js-coplayers-tab active" data-pane="with" role="tab" aria-selected="true">With you</button>' +
    '<button type="button" class="stats-trend-btn js-coplayers-tab" data-pane="against" role="tab" aria-selected="false">Against you</button>' +
    '</div></div>' +
    '<p class="stats-summary-meta js-coplayers-desc" data-pane="with">Top 5 by shared logs (teammate + opponent). Win rate, class, and playtime count only games on the same team.</p>' +
    '<p class="stats-summary-meta js-coplayers-desc" data-pane="against" hidden>Top 5 by games on opposite teams. Logs column: total shared logs (teammate + opponent), same meaning as With you. Win rate, class, and playtime: opposite-team games only.</p>' +
    more +
    '<div class="js-coplayers-pane" data-pane="with"><div class="stats-table-wrap"><table class="stats-table"><thead>' + theadWith + '</thead><tbody>' + bodyWith + '</tbody></table></div></div>' +
    '<div class="js-coplayers-pane" data-pane="against" hidden><div class="stats-table-wrap"><table class="stats-table"><thead>' + theadAgainst + '</thead><tbody>' + bodyAgainst + '</tbody></table></div></div>' +
    '</div>';
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
    return (t != null ? String(t) : '').toLowerCase();
  }
  var v = row[colKey];
  if (v == null || (typeof v === 'number' && !Number.isFinite(v))) return null;
  return Number(v);
}

function profileMapsSortedParents(rows, sortCol, sortDir) {
  var colDef = PROFILE_MAPS_COLUMNS.find(function(c) { return c.key === sortCol; });
  if (!colDef) return rows.slice();
  var sorted = rows.slice();
  sorted.sort(function(a, b) {
    var va = profileMapsSortValue(a, colDef.key, colDef.type);
    var vb = profileMapsSortValue(b, colDef.key, colDef.type);
    if (va === null && vb === null) {
      return String(a.map_key || '').localeCompare(String(b.map_key || ''));
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
    return String(a.map_key || '').localeCompare(String(b.map_key || ''));
  });
  return sorted;
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

function profileMapsRowsOnly(sortedParents, expandedMap) {
  var map = expandedMap || {};
  var html = '';
  sortedParents.forEach(function(p) {
    var mkey = p.map_key != null ? String(p.map_key) : '';
    var vers = Array.isArray(p.versions) ? p.versions : [];
    var hasVers = vers.length > 1;
    var exp = !!map[mkey];
    html += profileMapsParentRowHtml(p, hasVers, exp);
    if (hasVers && exp) {
      vers.forEach(function(v) {
        html += profileMapsVersionRowHtml(v);
      });
    }
  });
  return html;
}

function profileMapsParentRowHtml(p, hasVers, expanded) {
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
  return '<tr class="profile-maps-parent" data-map-key="' + escapeAttr(String(p.map_key)) + '">' +
    '<td class="profile-maps-expand-cell">' + btn + '</td>' +
    '<td>' + escapeHtml(pl) + undec + '</td>' +
    '<td>' + escapeHtml(String(p.logs_count != null ? p.logs_count : '')) + '</td>' +
    '<td>' + (pct === '\u2014' ? '\u2014' : escapeHtml(pct)) + '</td>' +
    '<td>' + (wr === '\u2014' ? '\u2014' : escapeHtml(wr)) + '</td>' +
    '<td>' + escapeHtml(String(p.wins != null ? p.wins : '')) + '</td>' +
    '<td>' + escapeHtml(String(p.losses != null ? p.losses : '')) + '</td>' +
    '</tr>';
}

function profileMapsVersionRowHtml(v) {
  var pl = v.map != null ? String(v.map) : '';
  var pct = profileMapsPctDisplay(v.pct_of_total);
  var wr = profileMapsWinRateDisplay(v.win_rate);
  var u = v.undecided_logs != null ? Number(v.undecided_logs) : 0;
  var undec = (u > 0) ? (' <span class="stats-summary-meta">(' + escapeHtml(String(u)) + ' undecided)</span>') : '';
  return '<tr class="profile-maps-version">' +
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

function profileTopMapsBlock() {
  return '<div class="stats-summary profile-top-maps">' +
    '<p class="stats-summary-title">Most played maps</p>' +
    '<p class="stats-summary-meta">Maps with the same base name are grouped (RC / beta / final suffixes). Expand a row to see per-version stats. Column headers sort grouped rows only.</p>' +
    '<div class="stats-table-wrap"><table class="stats-table js-profile-maps">' + profileMapsTableShellHtml() + '</table></div></div>';
}

function bindProfileMapsTable(table) {
  var tbody = table.querySelector('tbody.js-profile-maps-tbody');
  if (!tbody) return;
  function render() {
    var parents = table._profileMapRows || [];
    var sorted = profileMapsSortedParents(parents, table._sortCol, table._sortDir);
    tbody.innerHTML = profileMapsRowsOnly(sorted, table._mapsExpanded || {});
    var oldThead = table.querySelector('thead');
    if (oldThead) {
      oldThead.outerHTML = profileMapsTheadHtml(table._sortCol, table._sortDir);
    }
  }
  table.addEventListener('click', function(ev) {
    var btn = ev.target.closest('button.profile-maps-expand-btn');
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
var PROFILE_LAYOUT_SECTION_IDS = [
  'trend', 'top_logs', 'coplayers', 'top_maps', 'classes', 'weapons', 'class_kills', 'rounds', 'healspread'
];
var PROFILE_LAYOUT_LABELS = {
  trend: 'DPM / KDR over time',
  top_logs: 'Top logs',
  coplayers: 'Most frequent co-players',
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

function bindProfileLayoutSortList(ul, profileRoot) {
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
    cb.addEventListener('change', function() {
      var s = readProfileLayoutSettings();
      s.collapseDefault = cb.checked;
      writeProfileLayoutSettings(s);
    });
  }
  var ul = wrap.querySelector('.js-profile-layout-sort-list');
  if (ul) {
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
    bindProfileLayoutSortList(ul, profileRoot);
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
    '<button type="button" class="chat-hit-link profile-copy-link-btn js-profile-copy-link" title="Copy link to this search" aria-label="Copy link to this profile search">\ud83d\udd17</button>' +
    '<div class="profile-overview-head">' + avatarHtml + '<div class="profile-overview-text">' +
    '<p class="stats-summary-title">' + name + logsTfLinkHtml + '</p>' +
    dateRange + mpcLine +
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
        var logUrl = logId ? ('https://logs.tf/' + encodeURIComponent(logId)) : '';
        var k = r.kills != null ? String(r.kills) : '';
        var d = r.deaths != null ? String(r.deaths) : '';
        var a = r.assists != null ? String(r.assists) : '';
        var dmg = r.damage != null ? String(r.damage) : '';
        var metaLine = escapeHtml(k + '/' + d + '/' + a + ' K/D/A')
          + (dmg ? escapeHtml(' · ' + dmg + ' dmg') : '')
          + (r.dapm != null ? escapeHtml(' · ' + String(r.dapm) + ' DPM') : '');
        var rowTitle = (r.title != null && String(r.title).trim()) ? ('<br><span class="stats-summary-meta">' + escapeHtml(String(r.title)) + '</span>') : '';
        var teamStr = r.team != null && String(r.team).trim() ? (' <span class="stats-summary-meta">(' + escapeHtml(String(r.team)) + ')</span>') : '';
        var linkCell = logUrl
          ? ('<a href="' + escapeAttr(logUrl) + '" target="_blank" rel="noopener noreferrer">View log</a>')
          : '\u2014';
        return '<tr><td>' + escapeHtml(String(r.label || '')) + '</td><td><strong>' + escapeHtml(best) + '</strong>' + teamStr + '<br><span class="stats-summary-meta">' + metaLine + '</span>' + rowTitle + '</td><td>' + escapeHtml(mapStr) + '</td><td>' + profileFormatUnixDate(r.date_ts) + '</td><td>' + linkCell + '</td></tr>';
      }).join('');
      topLogsBlock = '<div class="stats-summary profile-top-logs"><p class="stats-summary-title">Top logs</p><p class="stats-summary-meta">Best single-game lines on Red or Blue in this filter. Merged (&ldquo;combined&rdquo;) uploads are excluded: empty map, multi-map map field, single-word maps without an underscore (e.g. placeholders), series-style titles, combined-style phrases, and (when chat is indexed) combiner chat lines.</p><div class="stats-table-wrap"><table class="stats-table"><thead>' + tlHead + '</thead><tbody>' + tlBody + '</tbody></table></div></div>';
    } else {
      topLogsBlock = '<div class="stats-summary profile-top-logs"><p class="stats-summary-title">Top logs</p><p class="stats-summary-meta">No Red or Blue team rows in this filter; widen filters or backfill stats to see bests.</p></div>';
    }
  }

  var topCoplayersBlock = profileTopCoplayersBlock(data);

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
  var weaponsTable = '';
  if (weapons.length) {
    var wthead = '<tr><th>Weapon</th><th>Logs</th><th>Kills</th><th>Damage</th><th>Accuracy</th><th>Avg dmg/hit</th></tr>';
    var wbody = weapons.map(function(w) {
      var acc = w.accuracy != null ? (Math.round(Number(w.accuracy) * 10000) / 100 + '%') : '\u2014';
      var adh = w.avg_damage_per_shot != null ? String(w.avg_damage_per_shot) : '\u2014';
      return '<tr><td>' + escapeHtml(String(w.weapon)) + '</td><td>' + escapeHtml(String(w.logs_count)) + '</td><td>' +
        escapeHtml(String(w.total_kills)) + '</td><td>' + escapeHtml(String(w.total_damage)) + '</td><td>' + escapeHtml(acc) + '</td><td>' + escapeHtml(adh) + '</td></tr>';
    }).join('');
    weaponsTable = '<div class="stats-summary"><p class="stats-summary-title">Weapons</p><div class="stats-table-wrap"><table class="stats-table"><thead>' + wthead + '</thead><tbody>' + wbody + '</tbody></table></div></div>';
  }

  var ck = data.class_kills || [];
  var ckTable = '';
  if (ck.length) {
    var ckhead = '<tr><th>Victim class</th><th>Kills</th></tr>';
    var ckbody = ck.map(function(r) {
      return '<tr><td>' + profileClassCell(r.victim_class) + '</td><td>' + escapeHtml(String(r.total_kills != null ? r.total_kills : '')) + '</td></tr>';
    }).join('');
    ckTable = '<div class="stats-summary"><p class="stats-summary-title">Kills by victim class</p><div class="stats-table-wrap"><table class="stats-table"><thead>' + ckhead + '</thead><tbody>' + ckbody + '</tbody></table></div></div>';
  }

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
    function healList(title, rows) {
      var lis = rows.map(function(r) {
        var nm = r.name && String(r.name).trim() ? escapeHtml(String(r.name)) : escapeHtml(String(r.steamid64));
        return '<li>' + nm + ' &mdash; ' + escapeHtml(profileFormatHealing(r.total_healing)) + ' <span class="stats-summary-meta">(' + escapeHtml(String(r.logs_count)) + ' logs)</span></li>';
      }).join('');
      return '<div class="profile-heal-col"><h3 class="stats-summary-title">' + escapeHtml(title) + '</h3><ul class="profile-heal-list">' + lis + '</ul></div>';
    }
    healCard = '<div class="stats-summary profile-healspread-wrap"><p class="stats-summary-title">Heal spread</p><div class="profile-healspread-cols">' +
      healList('Healed to', ht) + healList('Healed by', hb) + '</div></div>';
  }

  var layoutSettings = readProfileLayoutSettings();
  var sectionChunks = {
    trend: trendBlock,
    top_logs: topLogsBlock,
    coplayers: topCoplayersBlock,
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

  var mapTable = el.querySelector('table.js-profile-maps');
  if (mapTable && mapRows.length) {
    mapTable._profileMapRows = mapRows;
    mapTable._sortCol = 'logs_count';
    mapTable._sortDir = -1;
    mapTable._mapsExpanded = {};
    bindProfileMapsTable(mapTable);
  }

  loadAvatarsInContainer(el);

  bindProfileSectionCollapsers(el);
  initProfileLayoutSettings(el, layoutSettings);
  bindProfileCopyLinkButton(el);
}
