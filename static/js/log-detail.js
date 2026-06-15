/**
 * Built-in log detail page (/log/{id}).
 * Renders API payload with text escaping; large sections collapsed by default.
 */
(function initLogDetailPage() {
  var LOG_DETAIL_TITLE_SUFFIX = ' \u2014 TF2 Log Searcher';
  var m = (window.location.pathname || '').match(/^\/log\/(\d+)\/?$/);
  if (!m) return;
  var logId = m[1];
  var homePage = document.getElementById('homePage');
  var resultsPage = document.getElementById('resultsPage');
  var resultsContent = document.getElementById('resultsContent');
  if (!homePage || !resultsPage || !resultsContent) return;

  document.body.classList.add('tf2ls-log-detail-page');
  homePage.style.display = 'none';
  resultsPage.style.display = 'block';

  var back = document.getElementById('resultsBackLink');
  if (back) {
    back.href = '/';
    back.textContent = '\u2190 Back to search';
  }

  function stopLoadingTabTitle() {
    if (typeof stopLoadingTitleAnimation === 'function') {
      stopLoadingTitleAnimation();
    }
  }

  function setLogDetailTabTitle(data) {
    var s = (data && data.summary) ? data.summary : {};
    var parts = [];
    var title = s.title != null ? String(s.title).trim() : '';
    if (title) parts.push(title);
    var mapName = s.map != null ? String(s.map).trim() : '';
    if (mapName) parts.push(mapName);
    var durSec = s.duration_secs;
    if (durSec != null && Number.isFinite(Number(durSec)) && Number(durSec) > 0) {
      parts.push(profileFormatDurationMinSec(durSec));
    }
    if (!parts.length) {
      parts.push('Log #' + logId);
    } else {
      parts.push('#' + logId);
    }
    var main = parts.join(' \u00b7 ');
    if (main.length > 120) {
      main = main.slice(0, 117) + '\u2026';
    }
    document.title = main + LOG_DETAIL_TITLE_SUFFIX;
  }

  function setLogDetailTabTitleLoading() {
    document.title = 'Log #' + logId + LOG_DETAIL_TITLE_SUFFIX;
  }

  function showLogDetailLoading(el) {
    setLogDetailTabTitleLoading();
    if (typeof showResultsLoading === 'function') {
      showResultsLoading(el);
      return;
    }
    if (el) {
      el.innerHTML = '<div class="loading-state" role="status" aria-live="polite"><span class="loading-label">Loading log</span></div>';
    }
  }

  showLogDetailLoading(resultsContent);

  fetch('/api/log/' + encodeURIComponent(logId), { credentials: 'same-origin' })
    .then(function(res) {
      if (!res.ok) {
        return res.json().catch(function() { return {}; }).then(function(body) {
          var err = (body && body.error) ? String(body.error) : ('HTTP ' + res.status);
          throw new Error(err);
        });
      }
      return res.json();
    })
    .then(function(data) {
      stopLoadingTabTitle();
      setLogDetailTabTitle(data);
      resultsContent.innerHTML = renderLogDetail(data);
      loadAvatarsInContainer(resultsContent);
      bindLogDetailCollapsibles(resultsContent);
      bindLogDetailSort(resultsContent);
      bindLogDetailTimeToggle(resultsContent);
      applyLogDetailTimeMode(resultsContent, readLogDetailEventsTimeMode());
    })
    .catch(function(e) {
      stopLoadingTabTitle();
      document.title = 'Log #' + logId + ' (error)' + LOG_DETAIL_TITLE_SUFFIX;
      resultsContent.innerHTML = '<span class="error">' + escapeHtml(e && e.message ? e.message : 'Failed to load log') + '</span>';
    });

  function bindLogDetailCollapsibles(root) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('details.log-detail-collapsible').forEach(function(det) {
      det.addEventListener('toggle', function() {
        if (!det.open) return;
        var lazy = det.querySelector('.log-detail-lazy-body[data-lazy="1"]');
        if (!lazy || lazy.getAttribute('data-rendered') === '1') return;
        var kind = lazy.getAttribute('data-lazy-kind');
        var payload = window.__logDetailPayload;
        if (!payload) return;
        if (kind === 'chat') {
          lazy.innerHTML = renderChatSection(payload.chat);
        } else if (kind === 'killstreaks') {
          lazy.innerHTML = renderKillstreaksSection(payload.killstreaks);
        } else if (kind === 'events') {
          lazy.innerHTML = renderEventsSection(payload.events);
          bindLogDetailEventsToolbar(lazy);
        }
        lazy.setAttribute('data-rendered', '1');
        lazy.removeAttribute('data-lazy');
      });
    });
  }

  function bindLogDetailSortTable(table, opts) {
    if (!table) return;
    var thead = table.querySelector('thead');
    var tbody = table.querySelector('tbody');
    if (!thead || !tbody) return;
    opts = opts || {};
    var sortKey = opts.defaultSort || 'dmg';
    var sortDir = opts.defaultDir != null ? opts.defaultDir : -1;

    function rerender() {
      var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
      rows.sort(function(a, b) {
        var av = a.getAttribute('data-' + sortKey) || '';
        var bv = b.getAttribute('data-' + sortKey) || '';
        if (sortKey === 'alias' || sortKey === 'team' || sortKey === 'class') {
          av = av.toLowerCase();
          bv = bv.toLowerCase();
          if (av === bv) return 0;
          return sortDir * (av < bv ? -1 : 1);
        }
        var an = parseFloat(av);
        var bn = parseFloat(bv);
        if (!Number.isFinite(an)) an = 0;
        if (!Number.isFinite(bn)) bn = 0;
        if (an === bn) return 0;
        return sortDir * (an < bn ? -1 : 1);
      });
      rows.forEach(function(tr) { tbody.appendChild(tr); });
      thead.querySelectorAll('th.sortable').forEach(function(th) {
        th.classList.remove('sorted-asc', 'sorted-desc');
        if (th.getAttribute('data-sort') === sortKey) {
          th.classList.add(sortDir > 0 ? 'sorted-asc' : 'sorted-desc');
        }
      });
    }

    thead.querySelectorAll('th.sortable').forEach(function(th) {
      th.addEventListener('click', function() {
        var k = th.getAttribute('data-sort');
        if (!k) return;
        if (sortKey === k) sortDir = -sortDir;
        else { sortKey = k; sortDir = -1; }
        rerender();
      });
    });
    rerender();
  }

  function bindLogDetailSort(root) {
    if (!root) return;
    bindLogDetailSortTable(root.querySelector('.js-log-detail-players'), { defaultSort: 'dmg', defaultDir: -1 });
    bindLogDetailSortTable(root.querySelector('.js-log-detail-matrix'), { defaultSort: 'total', defaultDir: -1 });
  }

  function renderLogDetail(data) {
    window.__logDetailPayload = data;
    var s = data.summary || {};
    var logsTfUrl = data.external_log_url || s.external_log_url;
    if (!logsTfUrl && /^\d+$/.test(String(logId))) {
      logsTfUrl = 'https://logs.tf/' + encodeURIComponent(logId);
    }
    var title = s.title || ('Log #' + logId);
    var mapName = s.map || '\u2014';
    var dateStr = s.date_display || '\u2014';
    var dur = profileFormatDurationMinSec(s.duration_secs);
    var winner = s.winner ? String(s.winner) : '\u2014';
    var rs = s.red_score != null ? String(s.red_score) : '?';
    var bs = s.blue_score != null ? String(s.blue_score) : '?';

    var header =
      '<div class="log-detail-header stats-summary">' +
      '<h2 class="log-detail-title">' + escapeHtml(title) + '</h2>' +
      '<p class="stats-summary-meta">' +
      escapeHtml(mapName) + ' &middot; ' + escapeHtml(dateStr) + ' &middot; ' + escapeHtml(dur) +
      ' &middot; Score <span class="team-red">Red ' + escapeHtml(rs) + '</span> \u2013 <span class="team-blue">Blue ' + escapeHtml(bs) + '</span>' +
      (winner !== '\u2014' ? (' &middot; Winner: ' + escapeHtml(winner)) : '') +
      '</p>' +
      '<p class="log-detail-actions">' +
      '<a href="' + escapeAttr(logsTfUrl) + '" target="_blank" rel="noopener noreferrer">View on logs.tf</a>';

    var raw = data.raw_availability || {};
    if (raw.raw_zip_on_disk && raw.raw_zip_url) {
      header += ' &middot; <a href="' + escapeAttr(raw.raw_zip_url) + '" target="_blank" rel="noopener noreferrer">Raw log (.zip)</a>';
    }
    if (raw.events_indexed) {
      header += ' &middot; <span class="stats-summary-meta">Raw events indexed (' +
        escapeHtml(String(raw.kill_count || 0)) + ' kills)</span>';
    }
    header += '</p></div>';

    return header +
      renderTeamsSection(data.teams) +
      renderPlayersSection(data.players) +
      renderRoundsSection(data.rounds) +
      renderEventsCollapsible(data.events) +
      renderMedicsSection(data.medics) +
      renderClassMatrixSection(data.class_matrix) +
      '<details class="log-detail-collapsible stats-summary"><summary>Chat</summary>' +
      '<div class="log-detail-lazy-body" data-lazy="1" data-lazy-kind="chat"><p class="stats-summary-meta">Expand to load chat.</p></div></details>' +
      '<details class="log-detail-collapsible stats-summary"><summary>Killstreaks</summary>' +
      '<div class="log-detail-lazy-body" data-lazy="1" data-lazy-kind="killstreaks"><p class="stats-summary-meta">Expand to load killstreaks.</p></div></details>' +
      (raw.heatmaps_available ? renderHeatmapsSection(raw) : '');
  }

  var TEAM_SUMMARY_STATS = [
    { key: 'kills', label: 'Kills', higherBetter: true },
    { key: 'dmg', label: 'Damage', higherBetter: true },
    { key: 'assists', label: 'Assists', higherBetter: true },
    { key: 'ubers', label: 'Ubers', higherBetter: true },
    { key: 'drops', label: 'Drops', higherBetter: false },
    { key: 'captures', label: 'Caps', higherBetter: true },
    { key: 'score', label: 'Score', higherBetter: true, optional: true }
  ];

  function teamSummaryCompare(redVal, blueVal, higherBetter) {
    var r = Number(redVal) || 0;
    var b = Number(blueVal) || 0;
    if (r === b) {
      return { sym: '=', redWin: false, blueWin: false, tie: true };
    }
    var sym = r > b ? '&gt;' : '&lt;';
    var redBetter;
    if (higherBetter) {
      redBetter = r > b;
    } else {
      redBetter = r < b;
    }
    return {
      sym: sym,
      redWin: redBetter,
      blueWin: !redBetter,
      tie: false
    };
  }

  function teamSummaryStatRow(label, redVal, blueVal, higherBetter) {
    var cmp = teamSummaryCompare(redVal, blueVal, higherBetter);
    var redCellCls = 'log-detail-team-stat-red' +
      (cmp.redWin ? ' log-detail-team-stat--winner' : '');
    var blueCellCls = 'log-detail-team-stat-blue' +
      (cmp.blueWin ? ' log-detail-team-stat--winner' : '');
    return '<li class="log-detail-team-stat-row">' +
      '<div class="' + redCellCls + '">' +
      '<span class="log-detail-stat-val">' + escapeHtml(String(Number(redVal) || 0)) + '</span>' +
      '</div>' +
      '<div class="log-detail-team-stat-mid">' +
      '<span class="log-detail-stat-label">' + escapeHtml(label) + '</span>' +
      '<span class="log-detail-team-stat-cmp log-detail-cmp' +
      (cmp.tie ? ' log-detail-cmp-tie' : '') + '" aria-hidden="true">' + cmp.sym + '</span>' +
      '</div>' +
      '<div class="' + blueCellCls + '">' +
      '<span class="log-detail-stat-val">' + escapeHtml(String(Number(blueVal) || 0)) + '</span>' +
      '</div></li>';
  }

  function renderTeamsSection(teamsWrap) {
    var teams = (teamsWrap && teamsWrap.teams) ? teamsWrap.teams : {};
    var red = teams.Red || {};
    var blue = teams.Blue || {};
    var rows = '';
    TEAM_SUMMARY_STATS.forEach(function(spec) {
      if (spec.optional && red[spec.key] == null && blue[spec.key] == null) {
        return;
      }
      rows += teamSummaryStatRow(
        spec.label,
        red[spec.key],
        blue[spec.key],
        spec.higherBetter
      );
    });
    return '<div class="stats-summary log-detail-teams"><p class="stats-summary-title">Team summary</p>' +
      '<div class="log-detail-team-versus">' +
      '<div class="log-detail-team-versus-head">' +
      '<span class="log-detail-team-versus-name log-detail-team-versus-name--red">Red</span>' +
      '<span class="log-detail-team-versus-name log-detail-team-versus-name--blue">Blue</span>' +
      '</div>' +
      '<ul class="log-detail-stat-list log-detail-team-stat-list">' + rows + '</ul></div></div>';
  }

  function playerNameCell(p) {
    if (typeof playerNameMenuHtml === 'function') {
      return playerNameMenuHtml(p);
    }
    return escapeHtml(p.alias || '');
  }

  function primaryClassPlaytimeSec(p, classKey) {
    var arr = p && p.class_playtime;
    if (!arr || !Array.isArray(arr) || !classKey) return 0;
    var key = String(classKey).toLowerCase();
    for (var i = 0; i < arr.length; i++) {
      var row = arr[i];
      if (row && String(row.class).toLowerCase() === key) {
        return Math.max(0, Math.floor(Number(row.seconds) || 0));
      }
    }
    return 0;
  }

  function logDetailPlayerClassCell(p) {
    if (!p) return '\u2014';
    if (typeof logmatchClassIconsHtml === 'function') {
      var multi = logmatchClassIconsHtml(p);
      if (multi) {
        return '<span class="log-detail-class-cell">' + multi + '</span>';
      }
    }
    var cls = p.primary_class;
    if (!cls) return '\u2014';
    var key = String(cls).toLowerCase();
    var label = (typeof LOGMATCH_CLASS_LABEL !== 'undefined' && LOGMATCH_CLASS_LABEL[key])
      ? LOGMATCH_CLASS_LABEL[key]
      : (typeof profileClassDisplayName === 'function' ? profileClassDisplayName(cls) : String(cls));
    var src = typeof LOGMATCH_CLASS_ICON !== 'undefined' ? LOGMATCH_CLASS_ICON[key] : '';
    if (!src) {
      return '<span>' + escapeHtml(label) + '</span>';
    }
    var sec = primaryClassPlaytimeSec(p, key);
    var tip = label + ' \u2014 ' + (typeof formatClassTimeMinSec === 'function'
      ? formatClassTimeMinSec(sec)
      : String(sec) + 's') + ' (min:sec)';
    return '<span class="log-detail-class-cell" role="img" aria-label="' + escapeAttr(tip) + '">' +
      '<img class="logmatch-class-icon has-tooltip" src="' + escapeAttr(src) + '" alt="" width="22" height="22" loading="lazy" ' +
      'data-tip="' + escapeAttr(tip) + '"></span>';
  }

  function renderPlayersSection(playersWrap) {
    var players = (playersWrap && playersWrap.players) ? playersWrap.players : [];
    if (!players.length) return '';
    var head = '<tr>' +
      '<th class="sortable" data-sort="alias">Player</th>' +
      '<th class="sortable" data-sort="team">Team</th>' +
      '<th class="sortable" data-sort="class">Class</th>' +
      '<th class="sortable" data-sort="kills">K</th>' +
      '<th class="sortable" data-sort="assists">A</th>' +
      '<th class="sortable" data-sort="deaths">D</th>' +
      '<th class="sortable" data-sort="dmg">DMG</th>' +
      '<th class="sortable" data-sort="dpm">DPM</th>' +
      '<th class="sortable" data-sort="ubers">Ubers</th>' +
      '</tr>';
    var body = players.map(function(p) {
      var team = p.team || '';
      var teamCls = team === 'Red' ? 'team-red' : (team === 'Blue' ? 'team-blue' : '');
      var pc = p.primary_class || '';
      var classSort = (typeof profileClassDisplayName === 'function' ? profileClassDisplayName(pc) : pc).toLowerCase();
      return '<tr data-alias="' + escapeAttr((p.alias || '').toLowerCase()) + '" data-team="' + escapeAttr(team) + '" data-class="' + escapeAttr(classSort) + '" data-kills="' + escapeAttr(p.kills) + '" data-assists="' + escapeAttr(p.assists) + '" data-deaths="' + escapeAttr(p.deaths) + '" data-dmg="' + escapeAttr(p.dmg) + '" data-dpm="' + escapeAttr(p.dpm) + '" data-ubers="' + escapeAttr(p.ubers) + '">' +
        '<td>' + playerNameCell(p) + '</td>' +
        '<td class="' + teamCls + '">' + escapeHtml(team) + '</td>' +
        '<td class="log-detail-class-td">' + logDetailPlayerClassCell(p) + '</td>' +
        '<td>' + escapeHtml(p.kills) + '</td>' +
        '<td>' + escapeHtml(p.assists) + '</td>' +
        '<td>' + escapeHtml(p.deaths) + '</td>' +
        '<td>' + escapeHtml(p.dmg) + '</td>' +
        '<td>' + escapeHtml(p.dpm) + '</td>' +
        '<td>' + escapeHtml(p.ubers) + '</td>' +
        '</tr>';
    }).join('');
    return '<div class="stats-summary log-detail-players-wrap"><p class="stats-summary-title">Players</p>' +
      '<div class="stats-table-wrap"><table class="stats-table js-log-detail-players js-log-detail-sortable"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div></div>';
  }

  function logDetailRoundEventTypeLabel(type) {
    var t = String(type || '').trim();
    if (!t) return '\u2014';
    return t.replace(/_/g, ' ');
  }

  function logDetailRoundEventPlayersHtml(ev) {
    var t = ev.type || '';
    if (t === 'medic_death') {
      var killerHtml = logDetailRoundEventPlayerHtml(ev.killer);
      var victimHtml = logDetailRoundEventPlayerHtml(ev.victim);
      if (killerHtml && victimHtml) return killerHtml + ' killed ' + victimHtml;
      if (victimHtml) return victimHtml;
      if (killerHtml) return killerHtml;
      return '\u2014';
    }
    if (t === 'pointcap') {
      var plist = [];
      if (ev.players && ev.players.length) {
        ev.players.forEach(function(p) { plist.push(p); });
      } else if (ev.player) {
        plist.push(ev.player);
      }
      if (!plist.length) return '\u2014';
      var namesHtml = plist.map(function(p) {
        return logDetailRoundEventPlayerHtml(p);
      }).join(', ');
      var capText = namesHtml + ' captured';
      if (ev.cp_name) {
        capText += ' <span class="log-detail-event-cp">' + escapeHtml(String(ev.cp_name)) + '</span>';
      } else if (ev.point != null && ev.point !== '') {
        capText += ' CP #' + escapeHtml(String(ev.point));
      }
      return capText;
    }
    var parts = [];
    if (ev.killer) parts.push(logDetailRoundEventPlayerHtml(ev.killer));
    if (ev.victim) {
      if (parts.length) parts.push(' \u2192 ');
      parts.push(logDetailRoundEventPlayerHtml(ev.victim));
    }
    if (ev.player && !ev.killer && !ev.victim) {
      parts.push(logDetailRoundEventPlayerHtml(ev.player));
    } else if (ev.player) {
      parts.push(' \u2014 ' + logDetailRoundEventPlayerHtml(ev.player));
    }
    return parts.length ? parts.join('') : '\u2014';
  }

  function logDetailRoundEventTimeLabel(tickAttr, useTime) {
    if (!tickAttr || !/^\d+$/.test(tickAttr)) return '\u2014';
    if (useTime) return logDetailFormatEventTickSec(Number(tickAttr));
    return 'tick ' + tickAttr;
  }

  function logDetailRoundEventRowHtml(ev, useTime) {
    var type = ev && ev.type ? String(ev.type) : '';
    var timeRaw = ev && ev.time;
    var tickAttr = '';
    if (timeRaw != null && Number.isFinite(Number(timeRaw))) {
      tickAttr = String(Math.floor(Number(timeRaw)));
    }
    var timeLabel = tickAttr ? logDetailRoundEventTimeLabel(tickAttr, useTime) : '\u2014';
    return '<tr class="log-detail-round-event-row' +
      (type ? (' log-detail-round-event-row--' + escapeAttr(type)) : '') + '">' +
      '<td class="log-detail-round-event-type">' + escapeHtml(logDetailRoundEventTypeLabel(type)) + '</td>' +
      '<td class="log-detail-round-event-players">' + logDetailRoundEventPlayersHtml(ev) + '</td>' +
      '<td class="log-detail-round-event-time-td stats-summary-meta">' +
      (tickAttr
        ? ('<span class="log-detail-round-event-time-val" data-match-tick="' + escapeAttr(tickAttr) + '">' +
          escapeHtml(timeLabel) + '</span>')
        : '\u2014') +
      '</td></tr>';
  }

  function logDetailRoundEventsTableHtml(events, useTime) {
    if (!events || !events.length) return '';
    var rows = events.map(function(ev) {
      return logDetailRoundEventRowHtml(ev, useTime);
    }).join('');
    return '<div class="stats-table-wrap log-detail-round-events-wrap">' +
      '<table class="stats-table log-detail-round-events-table">' +
      '<thead><tr><th>Event</th><th>Players</th><th>Time</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table></div>';
  }

  function logDetailRoundEventPlayerHtml(p) {
    if (!p) return '';
    if (typeof p === 'string') return escapeHtml(p);
    var iconHtml = logDetailPlayerClassCell(p);
    var iconPart = iconHtml !== '\u2014'
      ? ('<span class="log-detail-round-ev-icon">' + iconHtml + '</span>')
      : '';
    var namePart = '<span class="log-detail-round-ev-name">' + playerNameCell(p) + '</span>';
    return '<span class="log-detail-player-label-cell">' + iconPart + namePart + '</span>';
  }

  function logDetailRoundWinnerClass(winner) {
    if (winner === 'Red') return 'team-red';
    if (winner === 'Blue') return 'team-blue';
    return '';
  }

  function renderRoundsSection(roundsWrap) {
    var rounds = (roundsWrap && roundsWrap.rounds) ? roundsWrap.rounds : [];
    if (!rounds.length) {
      return '<div class="stats-summary"><p class="stats-summary-title">Rounds</p><p class="stats-summary-meta">No round data in this log.</p></div>';
    }
    var useTime = readLogDetailEventsTimeMode();
    var hasAnyEvents = rounds.some(function(rnd) {
      return (rnd.events || []).length > 0;
    });
    var toolbar = hasAnyEvents
      ? ('<div class="log-detail-rounds-toolbar">' + renderLogDetailTimeToggleHtml(useTime) + '</div>')
      : '';
    var blocks = rounds.map(function(rnd, i) {
      var w = rnd.winner || '\u2014';
      var dur = profileFormatDurationMinSec(rnd.duration_secs);
      var sc = rnd.score || rnd.kills || {};
      var rk = sc.Red != null ? sc.Red : 0;
      var bk = sc.Blue != null ? sc.Blue : 0;
      var evHtml = '';
      var events = rnd.events || [];
      if (events.length) {
        evHtml = logDetailRoundEventsTableHtml(events, useTime);
        if (rnd.events_truncated) {
          evHtml += '<p class="stats-summary-meta">Events truncated for display.</p>';
        }
      }
      var winCls = logDetailRoundWinnerClass(w);
      var summaryCls = 'log-detail-round-summary' + (winCls ? (' ' + winCls) : '');
      return '<details class="log-detail-round' + (winCls ? (' log-detail-round--' + w.toLowerCase()) : '') + '">' +
        '<summary class="' + escapeAttr(summaryCls) + '">Round ' + escapeHtml(String(rnd.round_idx != null ? rnd.round_idx + 1 : i + 1)) +
        ': ' + escapeHtml(w) + ' (' + escapeHtml(dur) + ', ' + escapeHtml(String(rk)) + '\u2013' + escapeHtml(String(bk)) + ')</summary>' + evHtml + '</details>';
    }).join('');
    return '<div class="stats-summary log-detail-rounds"><p class="stats-summary-title">Rounds</p>' +
      toolbar + blocks + '</div>';
  }

  function logDetailMedicStatsMod(team) {
    if (team === 'Red') return 'log-detail-medic-stats--red';
    if (team === 'Blue') return 'log-detail-medic-stats--blue';
    return '';
  }

  function logDetailMedicCardMod(team) {
    if (team === 'Red') return 'log-detail-medic-card--red';
    if (team === 'Blue') return 'log-detail-medic-card--blue';
    return '';
  }

  function logDetailMedicMetaLine(m) {
    var mod = logDetailMedicStatsMod(m.team);
    var advLost = m.biggest_advantage_lost;
    var advPart = '';
    if (advLost != null && advLost !== '') {
      advPart = ' &middot; <span class="log-detail-stat-label">Biggest Advantage Lost:</span> ' +
        '<span class="log-detail-stat-val">' + escapeHtml(String(advLost)) + 's</span>';
    }
    return '<p class="log-detail-medic-stats ' + mod + '">' +
      '<span class="log-detail-stat-label">Healing done:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.healing_done || 0)) + '</span>' +
      ' &middot; <span class="log-detail-stat-label">Ubers:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.ubers || 0)) + '</span>' +
      ' &middot; <span class="log-detail-stat-label">Drops:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.drops || 0)) + '</span>' +
      advPart + '</p>';
  }

  function logDetailMedicHealPct(healing, totalHeal) {
    var h = Number(healing) || 0;
    var t = Number(totalHeal) || 0;
    if (t <= 0 || h < 0) return '';
    return (Math.round((h / t) * 10000) / 100).toFixed(1) + '%';
  }

  function logDetailMedicPatientLine(p, totalHeal) {
    var nameHtml = playerNameCell(p);
    var iconHtml = logDetailPlayerClassCell(p);
    var iconPart = iconHtml !== '\u2014'
      ? ('<span class="log-detail-medic-patient-icon">' + iconHtml + '</span>')
      : '';
    var pct = logDetailMedicHealPct(p.healing, totalHeal);
    var healHtml = '<span class="log-detail-stat-val">' + escapeHtml(String(p.healing)) + '</span> HP';
    if (pct) {
      healHtml += ' <span class="log-detail-medic-patient-pct stats-summary-meta">(' + escapeHtml(pct) + ')</span>';
    }
    return '<li class="log-detail-medic-patient">' + iconPart +
      '<span class="log-detail-medic-patient-name">' + nameHtml + '</span>' +
      '<span class="log-detail-medic-patient-heal">' + healHtml + '</span></li>';
  }

  function renderMedicsSection(medicsWrap) {
    var medics = (medicsWrap && medicsWrap.medics) ? medicsWrap.medics : [];
    if (!medics.length) {
      return '<div class="stats-summary"><p class="stats-summary-title">Medics</p><p class="stats-summary-meta">No medic / healspread data.</p></div>';
    }
    var blocks = medics.map(function(m) {
      var totalHeal = m.healing_done != null ? m.healing_done : 0;
      var patients = (m.top_patients || []).map(function(p) {
        return logDetailMedicPatientLine(p, totalHeal);
      }).join('');
      return '<div class="log-detail-medic-card ' + logDetailMedicCardMod(m.team) + '">' +
        '<h4>' + playerNameCell(m) + '</h4>' +
        logDetailMedicMetaLine(m) +
        (patients ? ('<ul class="log-detail-medic-patients">' + patients + '</ul>') : '') +
        '</div>';
    }).join('');
    return '<div class="stats-summary log-detail-medics"><p class="stats-summary-title">Medics</p>' + blocks + '</div>';
  }

  var LOG_DETAIL_CLASS_ORDER = [
    'scout', 'soldier', 'pyro', 'demoman', 'heavyweapons',
    'engineer', 'medic', 'sniper', 'spy'
  ];

  function logDetailOrderedVictimClasses(victims) {
    var list = victims || [];
    var byKey = {};
    list.forEach(function(vc) {
      byKey[String(vc).toLowerCase()] = vc;
    });
    var order = (typeof KILLS_BY_CLASS_VICTIMS !== 'undefined' && KILLS_BY_CLASS_VICTIMS.length)
      ? KILLS_BY_CLASS_VICTIMS.map(function(c) { return c.id; })
      : LOG_DETAIL_CLASS_ORDER;
    var out = [];
    order.forEach(function(id) {
      if (byKey[id] != null) {
        out.push(byKey[id]);
        delete byKey[id];
      }
    });
    Object.keys(byKey).sort().forEach(function(k) {
      out.push(byKey[k]);
    });
    return out;
  }

  function logDetailClassHeaderIcon(vc) {
    var key = String(vc).toLowerCase();
    var label = (typeof LOGMATCH_CLASS_LABEL !== 'undefined' && LOGMATCH_CLASS_LABEL[key])
      ? LOGMATCH_CLASS_LABEL[key]
      : key;
    var src = typeof LOGMATCH_CLASS_ICON !== 'undefined' ? LOGMATCH_CLASS_ICON[key] : '';
    if (!src) return escapeHtml(label);
    return '<img class="logmatch-class-icon has-tooltip log-detail-matrix-class-icon" src="' + escapeAttr(src) + '" alt="" width="22" height="22" loading="lazy" data-tip="' + escapeAttr(label) + '">';
  }

  function logDetailMatrixKillerCell(k) {
    var icon = logDetailPlayerClassCell(k);
    var iconPart = icon !== '\u2014' ? ('<span class="log-detail-matrix-killer-icon">' + icon + '</span>') : '';
    return '<span class="log-detail-matrix-killer-cell">' + iconPart +
      '<span class="log-detail-matrix-killer-name">' + playerNameCell(k) + '</span></span>';
  }

  function renderClassMatrixSection(cm) {
    if (!cm || !(cm.killers && cm.killers.length)) {
      return '<div class="stats-summary"><p class="stats-summary-title">Class matrix</p><p class="stats-summary-meta">No class kill matrix in this log.</p></div>';
    }
    var victims = logDetailOrderedVictimClasses(cm.victim_classes || []);
    var head = '<tr><th class="sortable log-detail-matrix-killer-th" data-sort="alias">Killer</th>' + victims.map(function(vc) {
      var key = String(vc).toLowerCase();
      return '<th class="sortable log-detail-matrix-class-th" data-sort="vc_' + escapeAttr(key) + '">' + logDetailClassHeaderIcon(vc) + '</th>';
    }).join('') + '</tr>';
    var body = cm.killers.map(function(k) {
      var by = k.kills_by_victim_class || {};
      var total = 0;
      var dataAttrs = ' data-alias="' + escapeAttr((k.alias || '').toLowerCase()) + '"';
      var cells = victims.map(function(vc) {
        var key = String(vc).toLowerCase();
        var n = by[vc] != null ? by[vc] : (by[key] != null ? by[key] : 0);
        total += n;
        dataAttrs += ' data-vc_' + escapeAttr(key) + '="' + escapeAttr(n) + '"';
        return '<td class="log-detail-matrix-num">' + (n ? escapeHtml(String(n)) : '') + '</td>';
      }).join('');
      dataAttrs += ' data-total="' + escapeAttr(total) + '"';
      return '<tr' + dataAttrs + '><td class="log-detail-matrix-killer-td">' + logDetailMatrixKillerCell(k) + '</td>' + cells + '</tr>';
    }).join('');
    return '<div class="stats-summary log-detail-matrix"><p class="stats-summary-title">Class matrix (kills by victim class)</p>' +
      '<div class="stats-table-wrap"><table class="stats-table js-log-detail-matrix js-log-detail-sortable"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div></div>';
  }

  function logDetailPlayerLabelCell(p, opts) {
    opts = opts || {};
    var icon = logDetailPlayerClassCell(p);
    var iconPart = icon !== '\u2014'
      ? ('<span class="' + (opts.iconCls || 'log-detail-player-icon') + '">' + icon + '</span>')
      : '';
    var nameInner = opts.useProfileLink !== false
      ? (typeof playerNameMenuHtml === 'function'
        ? playerNameMenuHtml(p, { extraClass: opts.nameCls || '' })
        : playerNameCell(p))
      : escapeHtml(p.alias || '');
    return '<span class="log-detail-player-label-cell">' + iconPart + nameInner + '</span>';
  }

  function logDetailChatElapsedHtml(msg) {
    var t = Number(msg.elapsed_secs);
    if (!Number.isFinite(t) || t < 0) return '';
    var fmt = typeof formatClassTimeMinSec === 'function'
      ? formatClassTimeMinSec(t)
      : (Math.floor(t / 60) + ':' + String(Math.floor(t % 60)).padStart(2, '0'));
    return '<span class="log-detail-chat-time" title="Time into match">' +
      escapeHtml(fmt) + '</span>';
  }

  function renderChatSection(chat) {
    var messages = (chat && chat.messages) ? chat.messages : [];
    if (!messages.length) return '<p class="stats-summary-meta">No chat in this log.</p>';
    var lines = messages.map(function(msg) {
      var label = logDetailPlayerLabelCell(msg, {
        iconCls: 'log-detail-chat-icon',
        nameCls: 'log-detail-chat-alias'
      });
      var timeHtml = logDetailChatElapsedHtml(msg);
      return '<div class="log-detail-chat-line">' +
        (timeHtml ? timeHtml : '') + label +
        '<span class="log-detail-chat-msg">: ' + escapeHtml(msg.msg || '') + '</span></div>';
    }).join('');
    var note = chat.truncated ? '<p class="stats-summary-meta">Chat list truncated.</p>' : '';
    return note + '<div class="log-detail-chat">' + lines + '</div>';
  }

  var LOG_DETAIL_EVENT_KIND_LABEL = {
    kill: 'Kill',
    uber: 'Uber',
    charge_end: 'Charge end',
    charge_ready: 'Charge ready',
    lost_advantage: 'Advantage lost',
    medic_death: 'Medic death',
    empty_uber: 'Empty uber',
    capture: 'Capture',
    capture_blocked: 'Capture blocked',
    round_start: 'Round start',
    round_win: 'Round win',
    spawn: 'Spawn',
    pass_score: 'Pass score',
    pass_score_assist: 'Pass assist',
    pass_get: 'Pass pickup',
    pass_free: 'Pass throw',
    pass_pass_caught: 'Pass caught',
    pass_ball_stolen: 'Ball stolen',
    pass_splash_defense: 'Splash defense'
  };

  var LOG_DETAIL_PASSTIME_KINDS = [
    'pass_score', 'pass_score_assist', 'pass_get', 'pass_free',
    'pass_pass_caught', 'pass_ball_stolen', 'pass_splash_defense'
  ];

  /** Filter groups: checkbox data-kind -> event kinds it controls. */
  var LOG_DETAIL_EVENT_FILTERS = [
    { id: 'kill', label: 'Kills', kinds: ['kill'] },
    { id: 'uber', label: 'Ubers', kinds: ['uber', 'charge_ready', 'lost_advantage', 'empty_uber'] },
    { id: 'medic_death', label: 'Medic deaths', kinds: ['medic_death'] },
    { id: 'charge_end', label: 'Charge ends', kinds: ['charge_end'] },
    { id: 'capture', label: 'Captures', kinds: ['capture', 'capture_blocked'] },
    { id: 'passtime', label: 'Passtime', kinds: LOG_DETAIL_PASSTIME_KINDS },
    { id: 'round', label: 'Rounds', kinds: ['round_start', 'round_win'] },
    { id: 'spawn', label: 'Spawns', kinds: ['spawn'], defaultOn: false }
  ];

  function logDetailEventPlayerCell(p) {
    if (!p || !p.alias) return '\u2014';
    return typeof playerNameMenuHtml === 'function'
      ? playerNameMenuHtml(p, { extraClass: 'log-detail-event-name' })
      : escapeHtml(p.alias);
  }

  /** Max seconds from first pop in a fight; avoids chaining sequential ubers into one row. */
  var LOG_DETAIL_UBER_EXCHANGE_SPAN_SEC = 18;
  var LOG_DETAIL_UBER_POST_IMPACT_SEC = 18;
  var LOG_DETAIL_UBER_TIE_SCORE_GAP = 2;
  /** Longest realistic single uber (vac, quick-fix, etc.); used to reject bad deploy/charge_end pairing. */
  var LOG_DETAIL_UBER_MAX_DURATION_SEC = 14;

  function resolveUberEndTick(uber) {
    var deploy = uber && uber.deployTick;
    if (!Number.isFinite(deploy)) return null;
    var dur = uber && uber.duration;
    if (dur != null && Number.isFinite(Number(dur))) {
      return deploy + Math.max(1, Math.round(Number(dur)));
    }
    var end = uber && uber.endTick;
    if (end != null && Number.isFinite(end) && end >= deploy &&
        end <= deploy + LOG_DETAIL_UBER_MAX_DURATION_SEC) {
      return end;
    }
    return deploy;
  }

  function chargeEndMatchesDeploy(deployTick, endTick, durationSec) {
    if (!Number.isFinite(deployTick) || !Number.isFinite(endTick) || endTick < deployTick) {
      return false;
    }
    if (endTick > deployTick + LOG_DETAIL_UBER_MAX_DURATION_SEC) return false;
    if (durationSec != null && Number.isFinite(Number(durationSec))) {
      var expectedEnd = deployTick + Math.max(1, Math.round(Number(durationSec)));
      if (endTick > expectedEnd + 2) return false;
    }
    return true;
  }

  function logDetailEventTickNum(ev, key) {
    var raw = ev && ev[key];
    if (raw == null || raw === '') return null;
    var n = Number(raw);
    return Number.isFinite(n) ? Math.floor(n) : null;
  }

  function logDetailMedicIconHtml() {
    var src = typeof LOGMATCH_CLASS_ICON !== 'undefined' ? LOGMATCH_CLASS_ICON.medic : '';
    if (!src) return '';
    return '<img class="logmatch-class-icon log-detail-uber-medic-icon" src="' + escapeAttr(src) +
      '" alt="" width="18" height="18" loading="lazy" title="Medic">';
  }

  function logDetailUberPlayerSid(p) {
    if (!p || p.steamid64 == null) return '';
    var sid = String(p.steamid64).trim();
    return /^\d{17}$/.test(sid) ? sid : '';
  }

  function logDetailPrimaryMedicSidByTeam(events) {
    var counts = { Red: {}, Blue: {} };
    (events || []).forEach(function(ev) {
      if (!ev || ev.kind !== 'uber') return;
      var med = ev.medic;
      if (!med || (med.team !== 'Red' && med.team !== 'Blue')) return;
      var sid = logDetailUberPlayerSid(med);
      if (!sid) return;
      counts[med.team][sid] = (counts[med.team][sid] || 0) + 1;
    });
    var out = { Red: '', Blue: '' };
    ['Red', 'Blue'].forEach(function(team) {
      var bestSid = '';
      var bestN = 0;
      Object.keys(counts[team]).forEach(function(sid) {
        if (counts[team][sid] > bestN) {
          bestN = counts[team][sid];
          bestSid = sid;
        }
      });
      out[team] = bestSid;
    });
    return out;
  }

  function logDetailUberChargeReadyTimeline(events) {
    var bySid = {};
    (events || []).forEach(function(ev) {
      if (!ev || ev.kind !== 'charge_ready') return;
      var sid = logDetailUberPlayerSid(ev.medic);
      var tick = logDetailEventTickNum(ev, 'tick');
      if (!sid || tick == null) return;
      if (!bySid[sid]) bySid[sid] = [];
      bySid[sid].push(tick);
    });
    Object.keys(bySid).forEach(function(sid) {
      bySid[sid].sort(function(a, b) { return a - b; });
    });
    return bySid;
  }

  function logDetailUberReadyBeforeTick(sid, tick, readyTimeline) {
    var list = readyTimeline[sid] || [];
    for (var i = list.length - 1; i >= 0; i--) {
      if (list[i] <= tick) return list[i];
    }
    return null;
  }

  function logDetailUberFirstReadyAfter(sid, afterTick, readyTimeline) {
    var list = readyTimeline[sid] || [];
    for (var i = 0; i < list.length; i++) {
      if (list[i] > afterTick) return list[i];
    }
    return null;
  }

  function logDetailUberEnemySid(medic, primaryByTeam) {
    if (!medic || !primaryByTeam) return '';
    if (medic.team === 'Red') return primaryByTeam.Blue || '';
    if (medic.team === 'Blue') return primaryByTeam.Red || '';
    return '';
  }

  /** Seconds of uber advantage when popping (chargeready timing). Null if none or medic died. */
  function logDetailUberAdvantageAtDeploy(medic, deployTick, readyTimeline, enemySid, died) {
    if (died) return null;
    var sid = logDetailUberPlayerSid(medic);
    if (!sid || !enemySid) return null;
    var myReady = logDetailUberReadyBeforeTick(sid, deployTick, readyTimeline);
    if (myReady == null) return null;
    var enemyReadyAtPop = logDetailUberReadyBeforeTick(enemySid, deployTick, readyTimeline);
    if (enemyReadyAtPop != null && enemyReadyAtPop <= myReady) return null;
    var enemyCatchUp = logDetailUberFirstReadyAfter(enemySid, myReady, readyTimeline);
    if (enemyCatchUp == null) return null;
    var adv = enemyCatchUp - myReady;
    if (!Number.isFinite(adv) || adv <= 0) return null;
    return Math.round(adv);
  }

  function logDetailUberLostAdvantagesNear(events, centerTick, spanSec) {
    var out = [];
    var center = Math.floor(Number(centerTick));
    if (!Number.isFinite(center)) return out;
    var span = Math.max(1, Math.floor(Number(spanSec) || LOG_DETAIL_UBER_EXCHANGE_SPAN_SEC));
    (events || []).forEach(function(ev) {
      if (!ev || ev.kind !== 'lost_advantage') return;
      var tick = logDetailEventTickNum(ev, 'tick');
      if (tick == null || tick < center - span || tick > center + span) return;
      var sec = ev.advantage_sec;
      var adv = null;
      if (sec != null && Number.isFinite(Number(sec))) adv = Math.round(Number(sec));
      out.push({
        tick: tick,
        medic: ev.medic,
        advantageSec: adv
      });
    });
    out.sort(function(a, b) { return a.tick - b.tick; });
    return out;
  }

  function logDetailUberAdvantageMetaHtml(advantageSec, advantageLostSec) {
    var parts = [];
    if (advantageSec != null && advantageSec > 0) {
      parts.push('<span class="log-detail-uber-adv">+' + escapeHtml(String(advantageSec)) + 's adv</span>');
    }
    if (advantageLostSec != null && advantageLostSec > 0) {
      parts.push('<span class="log-detail-uber-adv-lost">lost ' +
        escapeHtml(String(advantageLostSec)) + 's adv</span>');
    }
    return parts.join(' \u00b7 ');
  }

  /** Pair uber deploys with charge ends; cluster fights; score post-uber impact. */
  function buildLogDetailUberExchanges(events) {
    var list = Array.isArray(events) ? events : [];
    var readyTimeline = logDetailUberChargeReadyTimeline(list);
    var primaryByTeam = logDetailPrimaryMedicSidByTeam(list);
    var deployQueues = {};
    var ubers = [];

    list.forEach(function(ev) {
      var kind = ev && ev.kind ? String(ev.kind) : '';
      if (kind === 'uber') {
        var medic = ev.medic;
        var sid = logDetailUberPlayerSid(medic);
        var deployTick = logDetailEventTickNum(ev, 'tick');
        if (!sid || deployTick == null) return;
        if (!deployQueues[sid]) deployQueues[sid] = [];
        deployQueues[sid].push({ medic: medic, deployTick: deployTick, endTick: null, duration: null });
        return;
      }
      if (kind === 'charge_end') {
        var med = ev.medic;
        var msid = logDetailUberPlayerSid(med);
        var endTick = logDetailEventTickNum(ev, 'tick');
        if (!msid || endTick == null) return;
        var q = deployQueues[msid];
        if (!q || !q.length) return;
        var dur = ev.duration_sec;
        var duration = null;
        if (dur != null && Number.isFinite(Number(dur))) {
          duration = Math.round(Number(dur) * 10) / 10;
        }
        var pending = null;
        for (var qi = q.length - 1; qi >= 0; qi--) {
          if (q[qi].endTick != null) continue;
          if (!chargeEndMatchesDeploy(q[qi].deployTick, endTick, duration)) continue;
          pending = q[qi];
          break;
        }
        if (!pending) return;
        pending.endTick = endTick;
        pending.duration = duration;
        ubers.push(pending);
      }
    });

    Object.keys(deployQueues).forEach(function(sid) {
      deployQueues[sid].forEach(function(p) {
        if (p.endTick == null) ubers.push(p);
      });
    });

    ubers.sort(function(a, b) { return a.deployTick - b.deployTick; });
    if (!ubers.length) return [];

    var rawClusters = [];
    var current = null;
    ubers.forEach(function(u) {
      var anchor = current && current.length ? current[0].deployTick : null;
      if (!current || anchor == null || u.deployTick > anchor + LOG_DETAIL_UBER_EXCHANGE_SPAN_SEC) {
        if (current) rawClusters.push(current);
        current = [u];
      } else {
        current.push(u);
      }
    });
    if (current) rawClusters.push(current);

    function primaryUberPerTeam(cluster) {
      var byTeam = {};
      cluster.forEach(function(u) {
        var team = u.medic && u.medic.team;
        if (team !== 'Red' && team !== 'Blue') team = '_other';
        var prev = byTeam[team];
        if (!prev || u.deployTick < prev.deployTick) byTeam[team] = u;
      });
      return Object.keys(byTeam).map(function(t) { return byTeam[t]; })
        .sort(function(a, b) { return a.deployTick - b.deployTick; });
    }

    var clusters = rawClusters.map(primaryUberPerTeam);

    function impactForUber(uber) {
      var sid = logDetailUberPlayerSid(uber.medic);
      var windowStart = uber.deployTick;
      var endTick = resolveUberEndTick(uber);
      if (endTick == null) endTick = uber.deployTick;
      var windowEnd = endTick + LOG_DETAIL_UBER_POST_IMPACT_SEC;
      var kills = 0;
      var assists = 0;
      var died = false;
      var deathTick = null;
      var contrib = [];
      list.forEach(function(ev) {
        if (!ev || ev.kind !== 'kill') return;
        var tick = logDetailEventTickNum(ev, 'tick');
        if (tick == null || tick < windowStart || tick > windowEnd) return;
        if (logDetailUberPlayerSid(ev.attacker) === sid) {
          kills += 1;
          if (ev.victim && ev.victim.alias) {
            contrib.push({
              kind: 'kill',
              tick: tick,
              player: ev.victim,
              actorSid: sid,
              victimSid: logDetailUberPlayerSid(ev.victim)
            });
          }
        }
        if (logDetailUberPlayerSid(ev.assister) === sid) {
          assists += 1;
          if (ev.victim && ev.victim.alias) {
            contrib.push({
              kind: 'assist',
              tick: tick,
              player: ev.victim,
              actorSid: sid,
              victimSid: logDetailUberPlayerSid(ev.victim)
            });
          }
        }
        if (logDetailUberPlayerSid(ev.victim) === sid) {
          died = true;
          if (deathTick == null || tick < deathTick) deathTick = tick;
        }
      });
      contrib.sort(function(a, b) {
        if (a.tick !== b.tick) return a.tick - b.tick;
        if (a.kind === b.kind) return 0;
        return a.kind === 'kill' ? -1 : 1;
      });
      var duration = uber.duration != null ? uber.duration : 0;
      var score = duration * 2 + kills * 6 + assists * 3 - (died ? 12 : 0);
      return {
        kills: kills,
        assists: assists,
        died: died,
        deathTick: deathTick,
        duration: uber.duration,
        endTick: endTick,
        score: score,
        contrib: contrib
      };
    }

    return clusters.map(function(cluster, idx) {
      var impacts = cluster.map(function(u) {
        return { uber: u, impact: impactForUber(u) };
      });
      impacts.sort(function(a, b) { return b.impact.score - a.impact.score; });
      var deployTick = cluster[0].deployTick;
      var lostNear = logDetailUberLostAdvantagesNear(list, deployTick, LOG_DETAIL_UBER_EXCHANGE_SPAN_SEC);
      var lostBySid = {};
      lostNear.forEach(function(row) {
        var sid = logDetailUberPlayerSid(row.medic);
        if (!sid || row.advantageSec == null) return;
        if (!lostBySid[sid] || row.advantageSec > lostBySid[sid]) {
          lostBySid[sid] = row.advantageSec;
        }
      });
      impacts = impacts.map(function(row) {
        var medic = row.uber.medic;
        var sid = logDetailUberPlayerSid(medic);
        var enemySid = logDetailUberEnemySid(medic, primaryByTeam);
        var advantageSec = logDetailUberAdvantageAtDeploy(
          medic,
          row.uber.deployTick,
          readyTimeline,
          enemySid,
          row.impact.died
        );
        var advantageLostSec = lostBySid[sid] != null ? lostBySid[sid] : null;
        return {
          uber: row.uber,
          impact: row.impact,
          advantageSec: advantageSec,
          advantageLostSec: advantageLostSec
        };
      });
      var top = impacts[0];
      var second = impacts.length > 1 ? impacts[1] : null;
      var outcome = 'solo';
      var winnerSid = '';
      if (impacts.length > 1) {
        if (second && Math.abs(top.impact.score - second.impact.score) < LOG_DETAIL_UBER_TIE_SCORE_GAP) {
          outcome = 'tie';
        } else {
          outcome = 'win';
          winnerSid = logDetailUberPlayerSid(top.uber.medic);
        }
      } else {
        winnerSid = logDetailUberPlayerSid(top.uber.medic);
      }
      return {
        idx: idx + 1,
        deployTick: cluster[0].deployTick,
        outcome: outcome,
        winnerSid: winnerSid,
        medics: impacts
      };
    });
  }

  function logDetailUberDurationTip(startTick, endTick, useTime) {
    var start = Math.floor(Number(startTick));
    var end = Math.floor(Number(endTick));
    if (!Number.isFinite(start) || !Number.isFinite(end)) return '';
    if (useTime) {
      return 'Uber start ' + logDetailFormatEventTickSec(start) +
        ', end ' + logDetailFormatEventTickSec(end);
    }
    return 'Uber start tick ' + start + ', end tick ' + end;
  }

  function logDetailUberDurationHtml(uber, impact, useTime) {
    if (impact.duration == null || !Number.isFinite(Number(impact.duration))) return '';
    var start = uber && uber.deployTick;
    var end = resolveUberEndTick(uber);
    if (end == null) end = impact.endTick != null ? impact.endTick : start;
    if (!Number.isFinite(Number(start)) || !Number.isFinite(Number(end))) return '';
    var durText = Number(impact.duration).toFixed(1) + 's';
    var tip = logDetailUberDurationTip(start, end, useTime);
    return '<span class="log-detail-uber-duration has-tooltip" data-uber-start="' + escapeAttr(String(start)) +
      '" data-uber-end="' + escapeAttr(String(end)) + '" data-tip="' + escapeAttr(tip) + '">' +
      escapeHtml(durText) + '</span>';
  }

  function logDetailUberKaPlayerCell(player) {
    if (!player || !player.alias) return '\u2014';
    return typeof playerNameMenuHtml === 'function'
      ? playerNameMenuHtml(player, { extraClass: 'log-detail-uber-ka-name' })
      : escapeHtml(player.alias);
  }

  function logDetailUberKaTimeLabel(tick, useTime) {
    var n = Math.floor(Number(tick));
    if (!Number.isFinite(n)) return '\u2014';
    if (useTime) return logDetailFormatEventTickSec(n);
    return 'tick ' + String(n);
  }

  function logDetailUberKaCardHtml(contrib, useTime) {
    var rows = (contrib || []).map(function(c) {
      var tickAttr = String(Math.floor(Number(c.tick)));
      var kaLabel = c.kind === 'kill' ? 'Kill' : 'Assist';
      var kaCls = c.kind === 'kill' ? 'log-detail-uber-ka-kind--kill' : 'log-detail-uber-ka-kind--assist';
      var actorSid = c.actorSid ? String(c.actorSid) : '';
      var victimSid = c.victimSid ? String(c.victimSid) : '';
      return '<tr class="log-detail-uber-ka-row js-log-detail-uber-ka-jump"' +
        ' data-jump-tick="' + escapeAttr(tickAttr) + '"' +
        ' data-jump-kind="' + escapeAttr(c.kind || '') + '"' +
        ' data-jump-sid="' + escapeAttr(actorSid) + '"' +
        ' data-jump-victim-sid="' + escapeAttr(victimSid) + '"' +
        ' tabindex="0" role="button"' +
        ' title="Jump to this event in the feed">' +
        '<td class="log-detail-uber-ka-name-td">' + logDetailUberKaPlayerCell(c.player) + '</td>' +
        '<td class="log-detail-uber-ka-kind-td"><span class="' + kaCls + '">' + escapeHtml(kaLabel) + '</span></td>' +
        '<td class="log-detail-uber-ka-time-td"><span class="log-detail-uber-ka-time" data-match-tick="' +
        escapeAttr(tickAttr) + '">' + escapeHtml(logDetailUberKaTimeLabel(tickAttr, useTime)) + '</span></td>' +
        '</tr>';
    }).join('');
    return '<div class="log-detail-uber-ka-card" role="dialog" aria-label="Uber kills and assists">' +
      '<table class="log-detail-uber-ka-table">' +
      '<thead><tr><th>Name</th><th>K/A</th><th>Time</th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table></div>';
  }

  function logDetailUberKaTriggerHtml(impact, useTime) {
    var contrib = impact && impact.contrib;
    if (!contrib || !contrib.length) return '';
    var labelParts = [];
    if (impact.kills) labelParts.push(impact.kills + 'K');
    if (impact.assists) labelParts.push(impact.assists + 'A');
    if (!labelParts.length) return '';
    return '<span class="log-detail-uber-ka-pop">' +
      '<button type="button" class="log-detail-uber-ka-trigger" aria-expanded="false" aria-haspopup="dialog">' +
      escapeHtml(labelParts.join(', ')) + '</button>' +
      logDetailUberKaCardHtml(contrib, useTime) +
      '</span>';
  }

  function logDetailUberImpactMetaHtml(uber, impact, useTime, advantageSec, advantageLostSec) {
    var parts = [];
    var dur = logDetailUberDurationHtml(uber, impact, useTime);
    if (dur) parts.push(dur);
    var adv = logDetailUberAdvantageMetaHtml(advantageSec, advantageLostSec);
    if (adv) parts.push(adv);
    var ka = logDetailUberKaTriggerHtml(impact, useTime);
    if (ka) parts.push(ka);
    if (impact.died) {
      var diedText;
      if (impact.deathTick != null && impact.endTick != null && impact.deathTick >= impact.endTick) {
        diedText = 'died +' + (impact.deathTick - impact.endTick) + 's';
      } else {
        diedText = 'died';
      }
      parts.push(escapeHtml(diedText));
    }
    return parts.join(' \u00b7 ');
  }

  function placeLogDetailUberKaCard(pop) {
    if (!pop) return;
    var trigger = pop.querySelector('.log-detail-uber-ka-trigger');
    var card = pop.querySelector('.log-detail-uber-ka-card');
    if (!trigger || !card) return;
    var rect = trigger.getBoundingClientRect();
    var left = rect.left;
    var top = rect.bottom + 4;
    card.style.left = Math.round(left) + 'px';
    card.style.top = Math.round(top) + 'px';
    card.style.display = 'block';
    var cardRect = card.getBoundingClientRect();
    if (cardRect.right > window.innerWidth - 8) {
      left = Math.max(8, window.innerWidth - cardRect.width - 8);
      card.style.left = Math.round(left) + 'px';
    }
    if (cardRect.bottom > window.innerHeight - 8) {
      top = Math.max(8, rect.top - cardRect.height - 4);
      card.style.top = Math.round(top) + 'px';
    }
  }

  function hideLogDetailUberKaCard(pop) {
    if (!pop) return;
    var card = pop.querySelector('.log-detail-uber-ka-card');
    if (!card) return;
    if (!pop.classList.contains('is-stuck')) card.style.display = '';
  }

  function updateLogDetailUberKaPopTimes(root, useTime) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('.log-detail-uber-ka-time').forEach(function(el) {
      var tick = el.getAttribute('data-match-tick') || '';
      if (!tick || !Number.isFinite(Number(tick))) return;
      var label = logDetailUberKaTimeLabel(tick, useTime);
      if (label) el.textContent = label;
    });
  }

  function repositionOpenLogDetailUberKaCards() {
    document.querySelectorAll('.log-detail-uber-ka-pop').forEach(function(pop) {
      var card = pop.querySelector('.log-detail-uber-ka-card');
      if (!card) return;
      if (!pop.classList.contains('is-stuck') && card.style.display !== 'block') return;
      placeLogDetailUberKaCard(pop);
    });
  }

  function bindLogDetailUberKaScrollTargets(el) {
    if (!el || el.nodeType !== 1) return;
    if (el.getAttribute('data-uber-ka-scroll-bound') === '1') return;
    el.setAttribute('data-uber-ka-scroll-bound', '1');
    el.addEventListener('scroll', repositionOpenLogDetailUberKaCards, { passive: true });
  }

  function bindLogDetailUberKaScrollAncestors(el) {
    var node = el;
    while (node && node !== document.documentElement) {
      bindLogDetailUberKaScrollTargets(node);
      if (node === document.body) break;
      node = node.parentElement;
    }
    bindLogDetailUberKaScrollTargets(document.documentElement);
  }

  function bindLogDetailUberKaPopovers(root) {
    if (!root || root.getAttribute('data-uber-ka-bound') === '1') return;
    root.setAttribute('data-uber-ka-bound', '1');

    if (!window._logDetailUberKaDocBound) {
      window._logDetailUberKaDocBound = true;
      document.addEventListener('click', function(ev) {
        if (ev.target && ev.target.closest && ev.target.closest('.log-detail-uber-ka-pop')) return;
        closeStuckLogDetailUberKaPopovers(document);
      });
      document.addEventListener('keydown', function(ev) {
        if (ev.key === 'Escape') closeStuckLogDetailUberKaPopovers(document);
      });
      window.addEventListener('resize', repositionOpenLogDetailUberKaCards, { passive: true });
      window.addEventListener('scroll', repositionOpenLogDetailUberKaCards, { passive: true });
    }

    bindLogDetailUberKaScrollAncestors(root);
    root.querySelectorAll('.log-detail-uber-tracker-list, .log-detail-raw-events').forEach(
      bindLogDetailUberKaScrollTargets
    );
    var resultsContent = document.getElementById('resultsContent');
    if (resultsContent) bindLogDetailUberKaScrollTargets(resultsContent);

    root.querySelectorAll('.log-detail-uber-ka-pop').forEach(function(pop) {
      pop.addEventListener('mouseenter', function() {
        placeLogDetailUberKaCard(pop);
      });
      pop.addEventListener('mouseleave', function() {
        hideLogDetailUberKaCard(pop);
      });
    });

    root.addEventListener('click', function(ev) {
      var jumpRow = ev.target && ev.target.closest
        ? ev.target.closest('.js-log-detail-uber-ka-jump')
        : null;
      if (jumpRow) {
        if (ev.target && ev.target.closest && ev.target.closest('.player-name-menu-btn')) return;
        ev.preventDefault();
        ev.stopPropagation();
        performLogDetailUberKaJump(root, jumpRow);
        return;
      }
      var trigger = ev.target && ev.target.closest
        ? ev.target.closest('.log-detail-uber-ka-trigger')
        : null;
      if (!trigger) return;
      ev.preventDefault();
      ev.stopPropagation();
      var pop = trigger.closest('.log-detail-uber-ka-pop');
      if (!pop) return;
      var stuck = pop.classList.toggle('is-stuck');
      trigger.setAttribute('aria-expanded', stuck ? 'true' : 'false');
      if (stuck) {
        placeLogDetailUberKaCard(pop);
        root.querySelectorAll('.log-detail-uber-ka-pop.is-stuck').forEach(function(other) {
          if (other === pop) return;
          other.classList.remove('is-stuck');
          var ot = other.querySelector('.log-detail-uber-ka-trigger');
          if (ot) ot.setAttribute('aria-expanded', 'false');
          hideLogDetailUberKaCard(other);
        });
      } else {
        hideLogDetailUberKaCard(pop);
      }
    });

    root.addEventListener('keydown', function(ev) {
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      var jumpRow = ev.target && ev.target.closest
        ? ev.target.closest('.js-log-detail-uber-ka-jump')
        : null;
      if (!jumpRow) return;
      ev.preventDefault();
      performLogDetailUberKaJump(root, jumpRow);
    });
  }

  function findLogDetailEventLi(root, tick, kind, actorSid, victimSid) {
    var list = root && root.querySelector ? root.querySelector('.log-detail-raw-events') : null;
    if (!list) return null;
    var tickStr = String(tick);
    if (!/^\d+$/.test(tickStr)) return null;
    var candidates = list.querySelectorAll('.log-detail-raw-event--kill');
    for (var i = 0; i < candidates.length; i++) {
      var li = candidates[i];
      var key = li.getAttribute('data-event-key') || '';
      var parts = key.split(':');
      if (parts[0] !== 'kill' || parts[1] !== tickStr) continue;
      var liAtk = parts[2] || '';
      var liVic = parts[3] || '';
      var liAss = parts[4] || '';
      if (kind === 'kill' && actorSid && liAtk === actorSid) {
        if (!victimSid || liVic === victimSid) return li;
      }
      if (kind === 'assist' && actorSid && liAss === actorSid) {
        if (!victimSid || liVic === victimSid) return li;
      }
    }
    return null;
  }

  function jumpToLogDetailEvent(root, li) {
    if (!li || !root) return;
    var list = li.closest('.log-detail-raw-events');
    if (list && list.classList.contains('log-detail-hide-kill')) {
      list.classList.remove('log-detail-hide-kill');
      var cb = root.querySelector('.js-log-detail-event-filter[data-kind="kill"]');
      if (cb) cb.checked = true;
    }
    li.scrollIntoView({ behavior: 'smooth', block: 'center' });
    li.classList.add('log-detail-event-jump-highlight');
    window.setTimeout(function() {
      li.classList.remove('log-detail-event-jump-highlight');
    }, 1600);
  }

  function logDetailEventsSearchRoot(fromRoot) {
    var details = fromRoot && fromRoot.closest
      ? fromRoot.closest('details.log-detail-events-wrap')
      : null;
    var lazy = details ? details.querySelector('.log-detail-lazy-body') : null;
    if (lazy && lazy.getAttribute('data-rendered') === '1') {
      return lazy;
    }
    return fromRoot;
  }

  function performLogDetailUberKaJump(root, jumpRow) {
    if (!root || !jumpRow) return;
    var tick = jumpRow.getAttribute('data-jump-tick') || '';
    var kind = jumpRow.getAttribute('data-jump-kind') || '';
    var sid = jumpRow.getAttribute('data-jump-sid') || '';
    var vicSid = jumpRow.getAttribute('data-jump-victim-sid') || '';
    if (!/^\d+$/.test(tick)) return;
    var details = root.closest ? root.closest('details.log-detail-events-wrap') : null;
    if (details && !details.open) {
      details.open = true;
    }
    var lazy = details ? details.querySelector('.log-detail-lazy-body') : null;
    function attempt() {
      var searchRoot = logDetailEventsSearchRoot(root);
      var li = findLogDetailEventLi(searchRoot, tick, kind, sid, vicSid);
      jumpToLogDetailEvent(searchRoot, li);
    }
    if (lazy && lazy.getAttribute('data-rendered') !== '1') {
      window.setTimeout(attempt, 50);
    } else {
      attempt();
    }
  }

  function closeStuckLogDetailUberKaPopovers(root) {
    var scope = root || document;
    scope.querySelectorAll('.log-detail-uber-ka-pop.is-stuck').forEach(function(pop) {
      pop.classList.remove('is-stuck');
      var trigger = pop.querySelector('.log-detail-uber-ka-trigger');
      if (trigger) trigger.setAttribute('aria-expanded', 'false');
      hideLogDetailUberKaCard(pop);
    });
  }

  function updateLogDetailUberDurationTips(root, useTime) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('.log-detail-uber-duration').forEach(function(el) {
      var start = el.getAttribute('data-uber-start') || '';
      var end = el.getAttribute('data-uber-end') || '';
      var tip = logDetailUberDurationTip(start, end, useTime);
      if (tip) el.setAttribute('data-tip', tip);
    });
  }

  function logDetailUberMedicCell(medic, uber, impact, isWinner, useTime, advantageSec, advantageLostSec) {
    var teamMod = medic && medic.team === 'Red'
      ? 'log-detail-uber-medic--red'
      : (medic && medic.team === 'Blue' ? 'log-detail-uber-medic--blue' : '');
    var winMod = isWinner ? ' log-detail-uber-medic--winner' : '';
    var meta = logDetailUberImpactMetaHtml(
      uber, impact, useTime, advantageSec, advantageLostSec
    );
    return '<span class="log-detail-uber-medic ' + teamMod + winMod + '">' +
      logDetailMedicIconHtml() +
      '<span class="log-detail-uber-medic-name">' + logDetailEventPlayerCell(medic) + '</span>' +
      (meta ? (' <span class="log-detail-uber-medic-meta stats-summary-meta">' + meta + '</span>') : '') +
      '</span>';
  }

  function logDetailUberOutcomeLabel(ex) {
    if (ex.outcome === 'tie') return 'Tie';
    if (ex.outcome === 'solo') return 'Solo';
    var winner = null;
    for (var i = 0; i < ex.medics.length; i++) {
      if (logDetailUberPlayerSid(ex.medics[i].uber.medic) === ex.winnerSid) {
        winner = ex.medics[i].uber.medic;
        break;
      }
    }
    if (!winner || !winner.team) return 'Winner';
    return winner.team + ' won';
  }

  function logDetailUberOutcomeClass(ex) {
    if (ex.outcome === 'tie') return 'log-detail-uber-exchange--tie';
    if (ex.outcome === 'solo') {
      var solo = ex.medics[0] && ex.medics[0].uber.medic;
      if (solo && solo.team === 'Red') return 'log-detail-uber-exchange--red-won';
      if (solo && solo.team === 'Blue') return 'log-detail-uber-exchange--blue-won';
      return '';
    }
    if (ex.winnerSid) {
      for (var i = 0; i < ex.medics.length; i++) {
        var med = ex.medics[i].uber.medic;
        if (logDetailUberPlayerSid(med) === ex.winnerSid) {
          if (med.team === 'Red') return 'log-detail-uber-exchange--red-won';
          if (med.team === 'Blue') return 'log-detail-uber-exchange--blue-won';
        }
      }
    }
    return '';
  }

  function logDetailUberExchangeTickLabel(deployTick, useTime) {
    var n = Math.floor(Number(deployTick));
    if (!Number.isFinite(n)) return '';
    if (useTime) return logDetailFormatEventTickSec(n);
    return 'tick ' + String(n);
  }

  function updateLogDetailUberTrackerTicks(root, useTime) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('.log-detail-uber-exchange-tick').forEach(function(el) {
      var tick = el.getAttribute('data-match-tick') || '';
      if (!tick || !Number.isFinite(Number(tick))) return;
      var label = logDetailUberExchangeTickLabel(tick, useTime);
      if (label) el.textContent = label;
    });
  }

  function renderLogDetailUberTracker(events) {
    var exchanges = buildLogDetailUberExchanges(events);
    if (!exchanges.length) return '';
    var useTime = readLogDetailEventsTimeMode();
    var rows = exchanges.map(function(ex) {
      var medicCells = ex.medics.map(function(row) {
        var sid = logDetailUberPlayerSid(row.uber.medic);
        var isWinner = ex.outcome === 'win' && sid === ex.winnerSid;
        return logDetailUberMedicCell(
          row.uber.medic,
          row.uber,
          row.impact,
          isWinner,
          useTime,
          row.advantageSec,
          row.advantageLostSec
        );
      });
      var sep = ex.medics.length > 1 ? ' <span class="log-detail-uber-vs stats-summary-meta">vs</span> ' : '';
      var tickAttr = String(Math.floor(Number(ex.deployTick)));
      var tickLabel = logDetailUberExchangeTickLabel(tickAttr, useTime);
      var outcomeCls = logDetailUberOutcomeClass(ex);
      return '<li class="log-detail-uber-exchange' + (outcomeCls ? (' ' + outcomeCls) : '') + '">' +
        '<span class="log-detail-uber-exchange-tick stats-summary-meta" data-match-tick="' + escapeAttr(tickAttr) + '">' +
        escapeHtml(tickLabel) + '</span>' +
        '<span class="log-detail-uber-outcome">' + escapeHtml(logDetailUberOutcomeLabel(ex)) + '</span>' +
        '<span class="log-detail-uber-exchange-medics">' + medicCells.join(sep) + '</span>' +
        '</li>';
    }).join('');
    return '<details class="log-detail-uber-tracker stats-summary">' +
      '<summary>Uber tracking (' + escapeHtml(String(exchanges.length)) + ')</summary>' +
      '<ol class="log-detail-uber-tracker-list">' + rows + '</ol></details>';
  }

  var LOG_DETAIL_EVENTS_TIME_KEY = 'tf2ls-log-detail-events-time';

  function readLogDetailEventsTimeMode() {
    try {
      var v = localStorage.getItem(LOG_DETAIL_EVENTS_TIME_KEY);
      if (v === null) return true;
      return v === '1';
    } catch (e) {
      return true;
    }
  }

  function writeLogDetailEventsTimeMode(on) {
    try {
      localStorage.setItem(LOG_DETAIL_EVENTS_TIME_KEY, on ? '1' : '0');
    } catch (e) {}
  }

  function renderLogDetailTimeToggleHtml(useTime) {
    if (useTime == null) useTime = readLogDetailEventsTimeMode();
    return '<label class="log-detail-events-time-toggle">' +
      '<span class="log-detail-events-time-label' + (useTime ? '' : ' is-active') + '">Ticks</span>' +
      '<span class="log-detail-events-time-switch">' +
      '<input type="checkbox" class="js-log-detail-event-time-mode" role="switch"' +
      ' aria-label="Show event timestamps as minutes and seconds"' +
      (useTime ? ' checked' : '') + '>' +
      '<span class="log-detail-events-time-switch-ui" aria-hidden="true"></span></span>' +
      '<span class="log-detail-events-time-label' + (useTime ? ' is-active' : '') + '">Time</span>' +
      '</label>';
  }

  function updateLogDetailRoundEventTimes(root, useTime) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('.log-detail-round-event-time-val').forEach(function(el) {
      var tick = el.getAttribute('data-match-tick') || '';
      if (!tick || !/^\d+$/.test(tick)) return;
      var label = logDetailRoundEventTimeLabel(tick, useTime);
      if (label) el.textContent = label;
    });
  }

  function applyLogDetailTimeMode(scope, useTime) {
    if (!scope) return;
    scope.querySelectorAll('.log-detail-raw-events').forEach(function(list) {
      list.classList.toggle('log-detail-events-time-mode', useTime);
    });
    updateLogDetailEventTicks(scope, useTime);
    updateLogDetailUberTrackerTicks(scope, useTime);
    updateLogDetailUberDurationTips(scope, useTime);
    updateLogDetailUberKaPopTimes(scope, useTime);
    updateLogDetailRoundEventTimes(scope, useTime);
    scope.querySelectorAll('.log-detail-events-time-toggle').forEach(function(toggle) {
      var sw = toggle.querySelector('.js-log-detail-event-time-mode');
      if (sw) sw.checked = useTime;
      toggle.querySelectorAll('.log-detail-events-time-label').forEach(function(lab, i) {
        lab.classList.toggle('is-active', useTime ? i === 1 : i === 0);
      });
    });
  }

  function bindLogDetailTimeToggle(root) {
    if (!root || root.getAttribute('data-time-toggle-bound') === '1') return;
    root.setAttribute('data-time-toggle-bound', '1');
    root.addEventListener('change', function(ev) {
      var timeSw = ev.target && ev.target.classList
        && ev.target.classList.contains('js-log-detail-event-time-mode')
        ? ev.target
        : null;
      if (!timeSw) return;
      var useTime = !!timeSw.checked;
      writeLogDetailEventsTimeMode(useTime);
      applyLogDetailTimeMode(root, useTime);
    });
  }

  function logDetailFormatEventTickSec(totalSec) {
    if (typeof formatClassTimeMinSec === 'function') {
      return formatClassTimeMinSec(totalSec);
    }
    var n = Math.max(0, Math.floor(Number(totalSec) || 0));
    var m = Math.floor(n / 60);
    var s = n % 60;
    return m + ':' + (s < 10 ? '0' : '') + s;
  }

  function logDetailEventTickParts(matchTick, roundTick, useTime) {
    var parts = [];
    var hasMatch = matchTick != null && matchTick !== '' && Number.isFinite(Number(matchTick));
    var hasRound = roundTick != null && roundTick !== '' && Number.isFinite(Number(roundTick));
    if (hasMatch) {
      var mt = Math.floor(Number(matchTick));
      if (useTime) {
        var matchFmt = logDetailFormatEventTickSec(mt);
        parts.push(hasRound ? ('match ' + matchFmt) : matchFmt);
      } else {
        parts.push('tick ' + String(mt));
      }
    }
    if (hasRound) {
      var rt = Math.floor(Number(roundTick));
      if (useTime) {
        parts.push('round ' + logDetailFormatEventTickSec(rt));
      } else {
        parts.push('round tick ' + String(rt));
      }
    }
    return parts.join(' \u00b7 ');
  }

  function logDetailEventTickHtml(ev, useTime) {
    if (useTime == null) useTime = readLogDetailEventsTimeMode();
    var matchTick = ev && ev.tick != null && Number.isFinite(Number(ev.tick))
      ? String(Math.floor(Number(ev.tick)))
      : '';
    var roundTick = ev && ev.round_tick != null && Number.isFinite(Number(ev.round_tick))
      ? String(Math.floor(Number(ev.round_tick)))
      : '';
    if (!matchTick && !roundTick) return '';
    var label = logDetailEventTickParts(matchTick, roundTick, useTime);
    var attrs = ' class="log-detail-event-tick stats-summary-meta"';
    if (matchTick) attrs += ' data-match-tick="' + escapeAttr(matchTick) + '"';
    if (roundTick) attrs += ' data-round-tick="' + escapeAttr(roundTick) + '"';
    return '<span' + attrs + '>' + escapeHtml(label) + '</span>';
  }

  function updateLogDetailEventTicks(root, useTime) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('.log-detail-event-tick').forEach(function(el) {
      var matchTick = el.getAttribute('data-match-tick') || '';
      var roundTick = el.getAttribute('data-round-tick') || '';
      var label = logDetailEventTickParts(matchTick, roundTick, useTime);
      if (label) el.textContent = label;
    });
  }

  function logDetailEventKey(ev) {
    var kind = ev && ev.kind ? String(ev.kind) : '';
    var tick = ev && ev.tick != null ? String(Math.floor(Number(ev.tick))) : '';
    var a = (ev.attacker && ev.attacker.steamid64) ? String(ev.attacker.steamid64) : '';
    var v = (ev.victim && ev.victim.steamid64) ? String(ev.victim.steamid64) : '';
    var ass = (ev.assister && ev.assister.steamid64) ? String(ev.assister.steamid64) : '';
    var p = (ev.player && ev.player.steamid64) ? String(ev.player.steamid64) : '';
    var fifth = kind === 'kill' ? ass : p;
    if (!kind || !tick) return '';
    return [kind, tick, a, v, fifth].join(':');
  }

  function logDetailEventLine(ev) {
    var kind = ev && ev.kind ? String(ev.kind) : '';
    var label = LOG_DETAIL_EVENT_KIND_LABEL[kind] || kind || 'Event';
    var body = '';
    if (kind === 'kill') {
      var atk = logDetailEventPlayerCell(ev.attacker);
      var vic = logDetailEventPlayerCell(ev.victim);
      body = atk + ' killed ' + vic;
      if (ev.weapon) {
        body += ' with <span class="log-detail-event-weapon">' + escapeHtml(String(ev.weapon)) + '</span>';
      }
      if (ev.assister && ev.assister.alias) {
        body += ' <span class="stats-summary-meta">(assist: ' + logDetailEventPlayerCell(ev.assister) + ')</span>';
      }
    } else if (kind === 'uber') {
      body = logDetailEventPlayerCell(ev.medic) + ' deployed uber';
    } else if (kind === 'charge_end') {
      body = logDetailEventPlayerCell(ev.medic) + ' charge ended';
      if (ev.duration_sec != null && Number.isFinite(Number(ev.duration_sec))) {
        body += ' <span class="stats-summary-meta">(' + escapeHtml(String(ev.duration_sec)) + 's)</span>';
      }
    } else if (kind === 'charge_ready') {
      body = logDetailEventPlayerCell(ev.medic) + ' reached 100% uber';
    } else if (kind === 'lost_advantage') {
      body = logDetailEventPlayerCell(ev.medic) + ' lost uber advantage';
      if (ev.advantage_sec != null && Number.isFinite(Number(ev.advantage_sec))) {
        body += ' <span class="stats-summary-meta">(' + escapeHtml(String(Math.round(Number(ev.advantage_sec)))) + 's)</span>';
      }
    } else if (kind === 'medic_death') {
      var killer = logDetailEventPlayerCell(ev.killer);
      var medic = logDetailEventPlayerCell(ev.medic);
      body = killer + ' killed medic ' + medic;
      if (ev.dropped) {
        body += ' <span class="log-detail-uber-adv-lost">drop</span>';
      }
      if (ev.uber_pct != null && Number.isFinite(Number(ev.uber_pct))) {
        body += ' <span class="stats-summary-meta">(' + escapeHtml(String(ev.uber_pct)) + '% uber)</span>';
      }
      if (ev.healing != null && Number.isFinite(Number(ev.healing))) {
        body += ' <span class="stats-summary-meta">healed ' + escapeHtml(String(ev.healing)) + '</span>';
      }
    } else if (kind === 'empty_uber') {
      body = logDetailEventPlayerCell(ev.medic) + ' started uber build';
    } else if (kind === 'capture_blocked') {
      body = logDetailEventPlayerCell(ev.player) + ' blocked capture';
      if (ev.cp_name) {
        body += ' at <span class="log-detail-event-cp">' + escapeHtml(String(ev.cp_name)) + '</span>';
      } else if (ev.cp_index != null) {
        body += ' at CP #' + escapeHtml(String(ev.cp_index));
      }
    } else if (kind === 'pass_score') {
      body = logDetailEventPlayerCell(ev.player) + ' scored';
      if (ev.points != null) body += ' <span class="stats-summary-meta">+' + escapeHtml(String(ev.points)) + '</span>';
      if (ev.speed != null) body += ' <span class="stats-summary-meta">(' + escapeHtml(String(ev.speed)) + ' u/s)</span>';
    } else if (kind === 'pass_score_assist') {
      body = logDetailEventPlayerCell(ev.player) + ' scored (assist)';
    } else if (kind === 'pass_get') {
      body = logDetailEventPlayerCell(ev.player) + ' picked up the ball';
      if (ev.first_contact) body += ' <span class="stats-summary-meta">(first touch)</span>';
    } else if (kind === 'pass_free') {
      body = logDetailEventPlayerCell(ev.player) + ' threw the ball';
    } else if (kind === 'pass_pass_caught') {
      body = logDetailEventPlayerCell(ev.player) + ' caught pass from ' +
        logDetailEventPlayerCell(ev.other_player);
      if (ev.interception) body += ' <span class="stats-summary-meta">(pick)</span>';
      if (ev.save) body += ' <span class="stats-summary-meta">(save)</span>';
      if (ev.handoff) body += ' <span class="stats-summary-meta">(handoff)</span>';
    } else if (kind === 'pass_ball_stolen') {
      body = logDetailEventPlayerCell(ev.player) + ' stole ball from ' +
        logDetailEventPlayerCell(ev.other_player);
      if (ev.steal_defense) body += ' <span class="stats-summary-meta">(defense)</span>';
    } else if (kind === 'pass_splash_defense') {
      body = logDetailEventPlayerCell(ev.player) + ' splash defense';
    } else if (kind === 'capture') {
      body = logDetailEventPlayerCell(ev.player) + ' captured';
      if (ev.cp_name) {
        body += ' <span class="log-detail-event-cp">' + escapeHtml(String(ev.cp_name)) + '</span>';
      } else if (ev.cp_index != null) {
        body += ' CP #' + escapeHtml(String(ev.cp_index));
      }
    } else if (kind === 'round_start') {
      body = 'Round started';
    } else if (kind === 'round_win') {
      body = 'Round won';
      if (ev.winner_team === 'Red' || ev.winner_team === 'Blue') {
        body += ' by <span class="' + (ev.winner_team === 'Red' ? 'team-red' : 'team-blue') + '">' +
          escapeHtml(ev.winner_team) + '</span>';
      }
    } else if (kind === 'spawn') {
      body = logDetailEventPlayerCell(ev.player) + ' spawned';
      if (ev.class_name) {
        body += ' as <span class="log-detail-event-class">' + escapeHtml(String(ev.class_name)) + '</span>';
      }
    } else {
      body = escapeHtml(label);
    }
    var tickHtml = logDetailEventTickHtml(ev);
    var eventKey = logDetailEventKey(ev);
    return '<li class="log-detail-raw-event log-detail-raw-event--' + escapeAttr(kind) + '"' +
      (eventKey ? (' data-event-key="' + escapeAttr(eventKey) + '"') : '') + '>' +
      '<span class="log-detail-event-kind">' + escapeHtml(label) + '</span> ' +
      '<span class="log-detail-event-body">' + body + '</span>' +
      (tickHtml ? tickHtml : '') +
      '</li>';
  }

  function renderEventsToolbar(events) {
    var counts = {};
    events.forEach(function(ev) {
      var k = ev && ev.kind ? String(ev.kind) : '';
      counts[k] = (counts[k] || 0) + 1;
    });
    var boxes = LOG_DETAIL_EVENT_FILTERS.map(function(f) {
      var n = f.kinds.reduce(function(acc, k) { return acc + (counts[k] || 0); }, 0);
      if (!n) return '';
      var checked = f.defaultOn !== false ? ' checked' : '';
      return '<label class="log-detail-events-filter-item">' +
        '<input type="checkbox" class="js-log-detail-event-filter" data-kind="' + escapeAttr(f.id) + '"' +
        checked + '>' +
        ' ' + escapeHtml(f.label) + ' <span class="stats-summary-meta">(' + escapeHtml(String(n)) + ')</span></label>';
    }).filter(Boolean);
    var useTime = readLogDetailEventsTimeMode();
    var checksHtml = boxes.length >= 2
      ? ('<div class="log-detail-events-filter-checks" role="group" aria-label="Filter event types">' +
        boxes.join('') + '</div>')
      : '';
    return '<div class="log-detail-events-toolbar">' + checksHtml +
      renderLogDetailTimeToggleHtml(useTime) + '</div>';
  }

  function bindLogDetailEventsToolbar(root) {
    if (!root || !root.querySelector) return;
    var bar = root.querySelector('.log-detail-events-toolbar');
    var list = root.querySelector('.log-detail-raw-events');
    if (!bar || !list) return;
    applyLogDetailTimeMode(root, readLogDetailEventsTimeMode());
    bindLogDetailUberKaPopovers(root);
    bar.querySelectorAll('.js-log-detail-event-filter').forEach(function(cb) {
      var id = cb.getAttribute('data-kind') || '';
      if (!/^[a-z_]+$/.test(id)) return;
      list.classList.toggle('log-detail-hide-' + id, !cb.checked);
    });
    bar.addEventListener('change', function(ev) {
      if (ev.target && ev.target.classList
        && ev.target.classList.contains('js-log-detail-event-time-mode')) {
        return;
      }
      var cb = ev.target && ev.target.closest
        ? ev.target.closest('.js-log-detail-event-filter')
        : null;
      if (!cb) return;
      var id = cb.getAttribute('data-kind') || '';
      if (!/^[a-z_]+$/.test(id)) return;
      list.classList.toggle('log-detail-hide-' + id, !cb.checked);
    });
  }

  function renderEventsSection(eventsWrap) {
    var wrap = eventsWrap || {};
    if (!wrap.available) {
      return '<p class="stats-summary-meta">Raw events are not indexed for this log yet. After raw log backfill runs, reload this page to see kills, ubers, captures, and other parsed events.</p>';
    }
    var events = Array.isArray(wrap.events) ? wrap.events : [];
    if (!events.length) {
      return '<p class="stats-summary-meta">No parsed events stored for this log.</p>';
    }
    var uberTracker = renderLogDetailUberTracker(events);
    var filterBar = renderEventsToolbar(events);
    var lines = events.map(logDetailEventLine).join('');
    var note = '';
    if (wrap.truncated) {
      note = '<p class="stats-summary-meta">Showing first ' + escapeHtml(String(events.length)) +
        ' of ' + escapeHtml(String(wrap.total_count || events.length)) + ' events.</p>';
    }
    return uberTracker + filterBar + note + '<ul class="log-detail-raw-events">' + lines + '</ul>';
  }

  function renderEventsCollapsible(eventsWrap) {
    var wrap = eventsWrap || {};
    var summaryLabel = 'Events';
    if (wrap.available) {
      var n = wrap.total_count != null ? Number(wrap.total_count) : (wrap.events ? wrap.events.length : 0);
      if (Number.isFinite(n) && n > 0) {
        summaryLabel += ' (' + String(n) + ')';
      }
    }
    var lazyPlaceholder = wrap.available
      ? '<p class="stats-summary-meta">Expand to load event feed.</p>'
      : '<p class="stats-summary-meta">Expand for raw event availability.</p>';
    return '<details class="log-detail-collapsible stats-summary log-detail-events-wrap">' +
      '<summary>' + escapeHtml(summaryLabel) + '</summary>' +
      '<div class="log-detail-lazy-body" data-lazy="1" data-lazy-kind="events">' + lazyPlaceholder + '</div></details>';
  }

  function renderKillstreaksSection(ksWrap) {
    var list = (ksWrap && ksWrap.killstreaks) ? ksWrap.killstreaks : [];
    if (!list.length) return '<p class="stats-summary-meta">No killstreak records.</p>';
    var rows = list.map(function(k) {
      return '<tr><td class="log-detail-ks-player-td">' +
        logDetailPlayerLabelCell(k, { iconCls: 'log-detail-ks-icon', nameCls: 'log-detail-ks-name' }) +
        '</td><td class="log-detail-ks-streak">' + escapeHtml(String(k.streak || 0)) + '</td></tr>';
    }).join('');
    return '<div class="stats-table-wrap"><table class="stats-table log-detail-killstreaks"><thead><tr><th>Player</th><th>Streak</th></tr></thead><tbody>' + rows + '</tbody></table></div>';
  }

  /** Map overlays — only when API sets raw_availability.heatmaps_available (see LOG_DETAIL_HEATMAPS_ENABLED). */
  function renderHeatmapsSection(raw) {
    if (!raw || !raw.heatmaps_available) return '';
    return '';
  }
})();
