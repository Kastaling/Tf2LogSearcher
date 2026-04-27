
function logmatchColumnsForRows(rows) {
  var cols = [
    { key: 'alias', label: 'Alias', type: 'text' },
    { key: 'kills', label: 'K', type: 'number' },
    { key: 'assists', label: 'A', type: 'number' },
    { key: 'deaths', label: 'D', type: 'number' },
    { key: 'kdr', label: 'KDR', type: 'number' },
    { key: 'kadr', label: 'KADR', type: 'number' },
    { key: 'dpm', label: 'DPM', type: 'number' },
    { key: 'dmg', label: 'Dmg', type: 'number' }
  ];
  function anyPos(key) {
    return rows.some(function(r) { return Number(r[key] || 0) > 0; });
  }
  if (anyPos('headshots_hit')) cols.push({ key: 'headshots_hit', label: 'HS', type: 'number' });
  if (anyPos('backstabs')) cols.push({ key: 'backstabs', label: 'BS', type: 'number' });
  if (anyPos('ubers') || anyPos('drops')) {
    cols.push({ key: 'ubers', label: 'Ubers', type: 'number' });
    cols.push({ key: 'drops', label: 'Drops', type: 'number' });
  }
  return cols;
}

function logmatchGetSortValue(row, key, type) {
  var v = row[key];
  if (type === 'number') {
    var n = Number(v);
    return Number.isNaN(n) ? -Infinity : n;
  }
  return v != null ? String(v).toLowerCase() : '';
}

function renderLogmatchTableContents(table, rows, cols, sortCol, sortDir) {
  var sorted = rows.slice();
  var colDef = cols.find(function(c) { return c.key === sortCol; });
  if (colDef) {
    sorted.sort(function(a, b) {
      var va = logmatchGetSortValue(a, sortCol, colDef.type);
      var vb = logmatchGetSortValue(b, sortCol, colDef.type);
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
  var thead = '<tr>';
  cols.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === sortCol) cls += sortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';
  var tbody = '';
  sorted.forEach(function(x) {
    tbody += '<tr>';
    cols.forEach(function(c) {
      if (c.key === 'alias') {
        var teamClass = (x.team === 'Red') ? ' chat-team-red' : ((x.team === 'Blue') ? ' chat-team-blue' : '');
        var val = x.alias != null ? String(x.alias) : '';
        var tip = logmatchAliasTooltip(x);
        var tipAttr = tip ? ' data-tip="' + escapeAttr(tip) + '"' : '';
        var inner = teamClass ? ('<span class="' + teamClass + '">' + escapeHtml(val) + '</span>') : escapeHtml(val);
        var icons = logmatchClassIconsHtml(x);
        tbody += '<td><span class="logmatch-alias-cell">' + icons + '<span class="logmatch-alias has-tooltip"' + tipAttr + '>' + inner + '</span></span></td>';
      } else {
        var cell = x[c.key];
        tbody += '<td>' + escapeHtml(cell != null ? String(cell) : '') + '</td>';
      }
    });
    tbody += '</tr>';
  });
  table.innerHTML = '<thead>' + thead + '</thead><tbody>' + tbody + '</tbody>';
}

function bindLogmatchSortableTable(table) {
  if (table.getAttribute('data-logmatch-bound')) return;
  table.setAttribute('data-logmatch-bound', '1');
  table._lmSortCol = 'kills';
  table._lmSortDir = 1;
  renderLogmatchTableContents(table, table._logmatchRows, table._logmatchCols, table._lmSortCol, table._lmSortDir);
  table.addEventListener('click', function(ev) {
    var th = ev.target.closest('th.sortable');
    if (!th || !table.contains(th)) return;
    var col = th.getAttribute('data-col');
    if (!col) return;
    if (table._lmSortCol === col) table._lmSortDir *= -1;
    else { table._lmSortCol = col; table._lmSortDir = 1; }
    renderLogmatchTableContents(table, table._logmatchRows, table._logmatchCols, table._lmSortCol, table._lmSortDir);
  });
}

var H2H_STAT_ORDER = ['kills', 'dpm', 'kdr', 'dmg', 'assists', 'deaths', 'kadr', 'ubers', 'drops'];
var H2H_STAT_LABELS = { kills: 'Kills', assists: 'Assists', deaths: 'Deaths', dpm: 'DPM', dmg: 'DMG', kdr: 'KDR', kadr: 'KADR', ubers: 'Ubers', drops: 'Drops' };

function h2hFormatDiffHtml(val) {
  var v = Number(val);
  if (!Number.isFinite(v)) return '<span class="h2h-diff-zero">\u2014</span>';
  if (v === 0) return '<span class="h2h-diff-zero">0</span>';
  var dec = (Math.abs(v % 1) < 1e-9) ? 0 : 2;
  var raw = dec ? v.toFixed(2) : String(Math.round(v));
  var cls = v > 0 ? 'h2h-diff-pos' : 'h2h-diff-neg';
  var shown = (v > 0 ? '+' : '') + raw;
  return '<span class="' + cls + '">' + escapeHtml(shown) + '</span>';
}

function renderLogmatchHeadToHeadCard(h2h) {
  if (!h2h) return '';
  var la = h2h.player_a_label != null ? String(h2h.player_a_label) : '';
  var lb = h2h.player_b_label != null ? String(h2h.player_b_label) : '';
  var opp = h2h.opposing || {};
  var st = h2h.same_team || {};
  var oppN = Number(opp.logs_count) || 0;
  var stN = Number(st.logs_count) || 0;
  var oppSection = '';
  if (oppN === 0) {
    oppSection += '<p class="logmatch-h2h-wins" style="color:var(--text-muted)">No opposing-team logs in these results.</p>';
  } else {
    oppSection += '<div class="logmatch-h2h-vs"><span>' + escapeHtml(la) + '</span> <span>vs</span> <span>' + escapeHtml(lb) + '</span></div>';
    var aw = opp.player_a_wins != null ? opp.player_a_wins : 0;
    var bw = opp.player_b_wins != null ? opp.player_b_wins : 0;
    oppSection += '<p class="logmatch-h2h-wins">Wins: <strong>' + escapeHtml(String(aw)) + '</strong> (' + escapeHtml(la) + ') vs <strong>' + escapeHtml(String(bw)) + '</strong> (' + escapeHtml(lb) + ')</p>';
    var draws = Number(opp.draws) || 0;
    if (draws > 0) {
      oppSection += '<p class="logmatch-h2h-draw-note">\u2014 ' + escapeHtml(String(draws)) + ' draw(s) \u2014</p>';
    }
    var avg = opp.avg_stat_diff || {};
    var diffParts = [];
    H2H_STAT_ORDER.forEach(function(key) {
      if (avg[key] === undefined || avg[key] === null) return;
      diffParts.push('<span class="logmatch-h2h-diff-chip"><strong>' + escapeHtml(H2H_STAT_LABELS[key] || key) + '</strong> ' + h2hFormatDiffHtml(avg[key]) + '</span>');
    });
    if (diffParts.length) {
      oppSection += '<p class="logmatch-h2h-diff-line"><strong>Avg diff when opposing (A\u2212B):</strong></p><p class="logmatch-h2h-diff-line">' + diffParts.join(' ') + '</p>';
    }
  }
  var stSection = '';
  if (stN === 0) {
    stSection += '<p class="logmatch-h2h-wins" style="color:var(--text-muted)">No same-team logs in these results.</p>';
  } else {
    var w = st.wins != null ? st.wins : 0;
    var l = st.losses != null ? st.losses : 0;
    var d = st.draws != null ? st.draws : 0;
    stSection += '<p class="logmatch-h2h-wins">Together: <strong>' + escapeHtml(String(w)) + ' W</strong> / <strong>' + escapeHtml(String(l)) + ' L</strong> / <strong>' + escapeHtml(String(d)) + ' D</strong></p>';
  }
  return (
    '<div class="stats-summary logmatch-h2h" role="region" aria-label="Head-to-head summary">' +
    '<p class="logmatch-h2h-title">Head-to-head</p>' +
    '<div class="logmatch-h2h-panels">' +
    '<div class="logmatch-h2h-panel"><h4>Opposing teams</h4>' + oppSection + '</div>' +
    '<div class="logmatch-h2h-panel"><h4>Same team</h4>' + stSection + '</div>' +
    '</div></div>'
  );
}

function renderLogmatchResult(el, data, elapsedMs) {
  var results = data.results || [];
  var h2hHtml = data.head_to_head ? renderLogmatchHeadToHeadCard(data.head_to_head) : '';
  var html = h2hHtml + '<p class="logmatch-total">Total ' + escapeHtml(String(data.total || 0)) + ' matching log(s).</p>';
  if (results.length === 0) {
    el.innerHTML = html + 'No matches.' + requestTimingFooter(elapsedMs);
    return;
  }
  results.forEach(function(x) {
    var rows = x.player_stats || [];
    var dateShown = formatUnixLogTimestamp(x.date_ts);
    if (!dateShown) dateShown = x.date ? String(x.date) : '\u2014';
    else dateShown = String(dateShown);
    html += '<div class="logmatch-item" data-log-id="' + escapeHtml(String(x.log_id != null ? x.log_id : '')) + '">';
    html += '<div class="logmatch-header">';
    html += '<div class="logmatch-title">' + escapeHtml(x.title || '') + '</div>';
    html += '<div class="logmatch-meta">' + escapeHtml(x.map || '') + ' \u00b7 ' + escapeHtml(dateShown) + '</div>';
    html += '<a href="' + escapeHtml(x.url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(x.url) + '</a>';
    html += '</div>';
    if (rows.length === 0) {
      html += '<details class="logmatch-details"><summary>Matched players \u2014 Stats</summary><p class="logmatch-no-stats">No player stats available for this log.</p></details>';
    } else {
      html += '<details class="logmatch-details"><summary>Matched players \u2014 Stats <span class="logmatch-count">(' + escapeHtml(String(rows.length)) + ')</span></summary>';
      html += '<div class="stats-table-wrap logmatch-table-wrap"><table class="stats-table logmatch-stats-table"></table></div></details>';
    }
    html += '</div>';
  });
  el.innerHTML = html + requestTimingFooter(elapsedMs);
  var items = el.querySelectorAll('.logmatch-item');
  for (var i = 0; i < items.length; i++) {
    var x = results[i];
    var table = items[i].querySelector('table.logmatch-stats-table');
    if (!x || !table) continue;
    var pr = x.player_stats || [];
    table._logmatchRows = pr;
    table._logmatchCols = logmatchColumnsForRows(pr);
    bindLogmatchSortableTable(table);
  }
}

function renderPlayerNameResult(el, data, queryLabel, elapsedMs) {
  var rows = data.rows || [];
  var cap = data.limit != null && Number.isFinite(Number(data.limit)) ? Number(data.limit) : 200;
  var meta = '<p class="stats-summary-meta">Found ' + escapeHtml(String(rows.length)) + ' account(s)';
  if (queryLabel) {
    meta += ' matching <strong>' + escapeHtml(queryLabel) + '</strong>';
  }
  meta += ' (up to ' + escapeHtml(String(cap)) + ' shown).</p>';
  if (rows.length === 0) {
    var emptyNote = data.note ? String(data.note) : 'No matches in indexed chat.';
    el.innerHTML = meta + '<p>' + escapeHtml(emptyNote) + '</p>' + requestTimingFooter(elapsedMs);
    return;
  }
  var thead = '<tr><th scope="col">Name</th><th scope="col">SteamID64</th><th scope="col">Logs</th><th scope="col">Chat lines</th><th scope="col">Search</th></tr>';
  var base = window.location.origin + '/';
  var tbody = '';
  for (var i = 0; i < rows.length; i++) {
    var x = rows[i];
    var sid = x.steamid64 != null ? String(x.steamid64) : '';
    var name = x.display_name != null ? String(x.display_name) : '';
    var href = internalProfileHref(sid);
    var nameCell = steamAvatarPlaceholder(sid) + (
      href
        ? ('<a href="' + escapeAttr(href) + '">' + escapeHtml(name) + '</a>')
        : escapeHtml(name || sid));
    var chatHref = base + '?mode=chat&steamid=' + encodeURIComponent(sid);
    var statsHref = base + '?mode=stats&steamid=' + encodeURIComponent(sid);
    var coplayHref = base + '?mode=coplayers&steamid=' + encodeURIComponent(sid);
    var profileHref = base + '?mode=profile&steamid=' + encodeURIComponent(sid);
    var actionsCell = '<td class="playername-actions">' +
      '<a class="playername-action-link" href="' + escapeAttr(chatHref) + '">Chat</a>' +
      '<a class="playername-action-link" href="' + escapeAttr(statsHref) + '">Stats</a>' +
      '<a class="playername-action-link" href="' + escapeAttr(coplayHref) + '">Co-players</a>' +
      '<a class="playername-action-link" href="' + escapeAttr(profileHref) + '">Profile</a>' +
      '</td>';
    tbody += '<tr><td>' + nameCell + '</td><td>' + escapeHtml(sid) + '</td><td>' + escapeHtml(String(x.logs_count != null ? x.logs_count : '')) + '</td><td>' + escapeHtml(String(x.messages_count != null ? x.messages_count : '')) + '</td>' + actionsCell + '</tr>';
  }
  el.innerHTML = meta + '<div class="stats-table-wrap"><table class="stats-table playername-results-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>' + requestTimingFooter(elapsedMs);
  loadAvatarsInContainer(el);
}
