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
      renderMedicsSection(data.medics) +
      renderClassMatrixSection(data.class_matrix) +
      '<details class="log-detail-collapsible stats-summary"><summary>Chat</summary>' +
      '<div class="log-detail-lazy-body" data-lazy="1" data-lazy-kind="chat"><p class="stats-summary-meta">Expand to load chat.</p></div></details>' +
      '<details class="log-detail-collapsible stats-summary"><summary>Killstreaks</summary>' +
      '<div class="log-detail-lazy-body" data-lazy="1" data-lazy-kind="killstreaks"><p class="stats-summary-meta">Expand to load killstreaks.</p></div></details>' +
      renderRawSection(raw);
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

  function teamSummaryStatLine(label, myVal, oppVal, higherBetter) {
    var mine = Number(myVal) || 0;
    var opp = Number(oppVal) || 0;
    var sym = '';
    var cmpCls = '';
    if (mine === opp) {
      sym = '=';
      cmpCls = 'log-detail-cmp-tie';
    } else {
      var ahead;
      if (higherBetter) {
        ahead = mine > opp;
      } else {
        ahead = mine < opp;
      }
      if (ahead) {
        sym = '&gt;';
        cmpCls = 'log-detail-cmp-better';
      } else {
        sym = '&lt;';
        cmpCls = 'log-detail-cmp-worse';
      }
    }
    var valHtml = '<span class="log-detail-stat-val">' + escapeHtml(String(mine)) + '</span>';
    var cmpHtml = sym
      ? (' <span class="log-detail-cmp ' + cmpCls + '" aria-hidden="true">' + sym + '</span>')
      : '';
    return '<li><span class="log-detail-stat-label">' + escapeHtml(label) + ':</span> ' + valHtml + cmpHtml + '</li>';
  }

  function renderTeamsSection(teamsWrap) {
    var teams = (teamsWrap && teamsWrap.teams) ? teamsWrap.teams : {};
    var red = teams.Red || {};
    var blue = teams.Blue || {};
    var html = '<div class="stats-summary log-detail-teams"><p class="stats-summary-title">Team summary</p><div class="log-detail-team-cols">';
    [
      { name: 'Red', data: red, opp: blue, colCls: 'log-detail-team-col--red' },
      { name: 'Blue', data: blue, opp: red, colCls: 'log-detail-team-col--blue' }
    ].forEach(function(block) {
      html += '<div class="log-detail-team-col ' + block.colCls + '"><h3>' + escapeHtml(block.name) + '</h3><ul class="log-detail-stat-list">';
      TEAM_SUMMARY_STATS.forEach(function(spec) {
        if (spec.optional && block.data[spec.key] == null) {
          return;
        }
        html += teamSummaryStatLine(
          spec.label,
          block.data[spec.key],
          block.opp[spec.key],
          spec.higherBetter
        );
      });
      html += '</ul></div>';
    });
    return html + '</div></div>';
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
    var cls = p && p.primary_class;
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

  function renderRoundsSection(roundsWrap) {
    var rounds = (roundsWrap && roundsWrap.rounds) ? roundsWrap.rounds : [];
    if (!rounds.length) {
      return '<div class="stats-summary"><p class="stats-summary-title">Rounds</p><p class="stats-summary-meta">No round data in this log.</p></div>';
    }
    var blocks = rounds.map(function(rnd, i) {
      var w = rnd.winner || '\u2014';
      var dur = profileFormatDurationMinSec(rnd.duration_secs);
      var rk = (rnd.kills && rnd.kills.Red != null) ? rnd.kills.Red : 0;
      var bk = (rnd.kills && rnd.kills.Blue != null) ? rnd.kills.Blue : 0;
      var evHtml = '';
      var events = rnd.events || [];
      if (events.length) {
        evHtml = '<ul class="log-detail-events">' + events.map(function(ev) {
          var line = escapeHtml(ev.type || '');
          if (ev.killer) line += ' \u2014 ' + escapeHtml(ev.killer);
          if (ev.victim) line += ' \u2192 ' + escapeHtml(ev.victim);
          if (ev.time != null) line += ' <span class="stats-summary-meta">@' + escapeHtml(String(ev.time)) + 's</span>';
          return '<li>' + line + '</li>';
        }).join('') + '</ul>';
        if (rnd.events_truncated) {
          evHtml += '<p class="stats-summary-meta">Events truncated for display.</p>';
        }
      }
      return '<details class="log-detail-round"><summary>Round ' + escapeHtml(String(rnd.round_idx != null ? rnd.round_idx + 1 : i + 1)) +
        ': ' + escapeHtml(w) + ' (' + escapeHtml(dur) + ', ' + escapeHtml(String(rk)) + '\u2013' + escapeHtml(String(bk)) + ')</summary>' + evHtml + '</details>';
    }).join('');
    return '<div class="stats-summary log-detail-rounds"><p class="stats-summary-title">Rounds</p>' + blocks + '</div>';
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
    return '<p class="log-detail-medic-stats ' + mod + '">' +
      '<span class="log-detail-stat-label">Healing done:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.healing_done || 0)) + '</span>' +
      ' &middot; <span class="log-detail-stat-label">Ubers:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.ubers || 0)) + '</span>' +
      ' &middot; <span class="log-detail-stat-label">Drops:</span> <span class="log-detail-stat-val">' + escapeHtml(String(m.drops || 0)) + '</span></p>';
  }

  function logDetailMedicPatientLine(p) {
    var nameHtml = playerNameCell(p);
    var iconHtml = logDetailPlayerClassCell(p);
    var iconPart = iconHtml !== '\u2014'
      ? ('<span class="log-detail-medic-patient-icon">' + iconHtml + '</span>')
      : '';
    return '<li class="log-detail-medic-patient">' + iconPart +
      '<span class="log-detail-medic-patient-name">' + nameHtml + '</span>' +
      '<span class="log-detail-medic-patient-heal"><span class="log-detail-stat-val">' + escapeHtml(String(p.healing)) + '</span> HP</span></li>';
  }

  function renderMedicsSection(medicsWrap) {
    var medics = (medicsWrap && medicsWrap.medics) ? medicsWrap.medics : [];
    if (!medics.length) {
      return '<div class="stats-summary"><p class="stats-summary-title">Medics</p><p class="stats-summary-meta">No medic / healspread data.</p></div>';
    }
    var blocks = medics.map(function(m) {
      var patients = (m.top_patients || []).map(logDetailMedicPatientLine).join('');
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

  function renderChatSection(chat) {
    var messages = (chat && chat.messages) ? chat.messages : [];
    if (!messages.length) return '<p class="stats-summary-meta">No chat in this log.</p>';
    var lines = messages.map(function(msg) {
      var label = logDetailPlayerLabelCell(msg, {
        iconCls: 'log-detail-chat-icon',
        nameCls: 'log-detail-chat-alias'
      });
      return '<div class="log-detail-chat-line">' + label +
        '<span class="log-detail-chat-msg">: ' + escapeHtml(msg.msg || '') + '</span></div>';
    }).join('');
    var note = chat.truncated ? '<p class="stats-summary-meta">Chat list truncated.</p>' : '';
    return note + '<div class="log-detail-chat">' + lines + '</div>';
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

  function renderRawSection(raw) {
    if (!raw || !raw.events_indexed) {
      return '<div class="stats-summary log-detail-raw"><p class="stats-summary-title">Raw / heatmaps</p>' +
        '<p class="stats-summary-meta">Coordinate heatmaps are not available in this build. ' +
        (raw && raw.raw_zip_on_disk ? 'Raw server log zip is on disk.' : 'No local raw zip.') + '</p></div>';
    }
    return '<div class="stats-summary log-detail-raw"><p class="stats-summary-title">Raw / heatmaps</p>' +
      '<p class="stats-summary-meta">Indexed raw events: ' + escapeHtml(String(raw.kill_count || 0)) + ' kills, ' +
      escapeHtml(String(raw.uber_count || 0)) + ' ubers. Heatmaps coming later.</p></div>';
  }
})();
