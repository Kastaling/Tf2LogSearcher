/**
 * Built-in log detail page (/log/{id}).
 * Renders API payload with text escaping; large sections collapsed by default.
 */
(function initLogDetailPage() {
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
  document.title = 'Log #' + logId + ' — TF2 Log Searcher';

  var back = document.getElementById('resultsBackLink');
  if (back) {
    back.href = '/';
    back.textContent = '\u2190 Back to search';
  }

  showResultsLoading(resultsContent);

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
      resultsContent.innerHTML = renderLogDetail(data);
      loadAvatarsInContainer(resultsContent);
      bindLogDetailCollapsibles(resultsContent);
      bindLogDetailSort(resultsContent);
    })
    .catch(function(e) {
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

  function bindLogDetailSort(root) {
    var table = root && root.querySelector('.js-log-detail-players');
    if (!table) return;
    var thead = table.querySelector('thead');
    var tbody = table.querySelector('tbody');
    if (!thead || !tbody) return;
    var sortKey = 'dmg';
    var sortDir = -1;

    function rerender() {
      var rows = Array.prototype.slice.call(tbody.querySelectorAll('tr'));
      rows.sort(function(a, b) {
        var av = a.getAttribute('data-' + sortKey);
        var bv = b.getAttribute('data-' + sortKey);
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

  function renderTeamsSection(teamsWrap) {
    var teams = (teamsWrap && teamsWrap.teams) ? teamsWrap.teams : {};
    var html = '<div class="stats-summary log-detail-teams"><p class="stats-summary-title">Team summary</p><div class="log-detail-team-cols">';
    ['Red', 'Blue'].forEach(function(team) {
      var t = teams[team] || {};
      var cls = team === 'Red' ? 'team-red' : 'team-blue';
      html += '<div class="log-detail-team-col ' + cls + '"><h3>' + escapeHtml(team) + '</h3><ul class="log-detail-stat-list">' +
        '<li>Kills: ' + escapeHtml(String(t.kills || 0)) + '</li>' +
        '<li>Damage: ' + escapeHtml(String(t.dmg || 0)) + '</li>' +
        '<li>Assists: ' + escapeHtml(String(t.assists || 0)) + '</li>' +
        '<li>Ubers: ' + escapeHtml(String(t.ubers || 0)) + '</li>' +
        '<li>Drops: ' + escapeHtml(String(t.drops || 0)) + '</li>' +
        '<li>Caps: ' + escapeHtml(String(t.captures || 0)) + '</li>' +
        (t.score != null ? ('<li>Score: ' + escapeHtml(String(t.score)) + '</li>') : '') +
        '</ul></div>';
    });
    return html + '</div></div>';
  }

  function playerNameCell(p) {
    var alias = escapeHtml(p.alias || '');
    var href = p.profile_href || internalProfileHref(p.steamid64);
    if (href) {
      return '<a href="' + escapeAttr(href) + '">' + alias + '</a>';
    }
    return alias;
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
      return '<tr data-alias="' + escapeAttr((p.alias || '').toLowerCase()) + '" data-team="' + escapeAttr(team) + '" data-class="' + escapeAttr(pc) + '" data-kills="' + escapeAttr(p.kills) + '" data-assists="' + escapeAttr(p.assists) + '" data-deaths="' + escapeAttr(p.deaths) + '" data-dmg="' + escapeAttr(p.dmg) + '" data-dpm="' + escapeAttr(p.dpm) + '" data-ubers="' + escapeAttr(p.ubers) + '">' +
        '<td>' + playerNameCell(p) + '</td>' +
        '<td class="' + teamCls + '">' + escapeHtml(team) + '</td>' +
        '<td>' + escapeHtml(pc) + '</td>' +
        '<td>' + escapeHtml(p.kills) + '</td>' +
        '<td>' + escapeHtml(p.assists) + '</td>' +
        '<td>' + escapeHtml(p.deaths) + '</td>' +
        '<td>' + escapeHtml(p.dmg) + '</td>' +
        '<td>' + escapeHtml(p.dpm) + '</td>' +
        '<td>' + escapeHtml(p.ubers) + '</td>' +
        '</tr>';
    }).join('');
    return '<div class="stats-summary log-detail-players-wrap"><p class="stats-summary-title">Players</p>' +
      '<div class="stats-table-wrap"><table class="stats-table js-log-detail-players"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div></div>';
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

  function renderMedicsSection(medicsWrap) {
    var medics = (medicsWrap && medicsWrap.medics) ? medicsWrap.medics : [];
    if (!medics.length) {
      return '<div class="stats-summary"><p class="stats-summary-title">Medics</p><p class="stats-summary-meta">No medic / healspread data.</p></div>';
    }
    var blocks = medics.map(function(m) {
      var patients = (m.top_patients || []).map(function(p) {
        return '<li>' + escapeHtml(p.alias || p.steamid3) + ': ' + escapeHtml(String(p.healing)) + ' HP</li>';
      }).join('');
      return '<div class="log-detail-medic-card"><h4>' + playerNameCell(m) + '</h4>' +
        '<p class="stats-summary-meta">Healing done: ' + escapeHtml(String(m.healing_done || 0)) +
        ' &middot; Ubers: ' + escapeHtml(String(m.ubers || 0)) +
        ' &middot; Drops: ' + escapeHtml(String(m.drops || 0)) + '</p>' +
        (patients ? ('<ul>' + patients + '</ul>') : '') + '</div>';
    }).join('');
    return '<div class="stats-summary log-detail-medics"><p class="stats-summary-title">Medics</p>' + blocks + '</div>';
  }

  function renderClassMatrixSection(cm) {
    if (!cm || !(cm.killers && cm.killers.length)) {
      return '<div class="stats-summary"><p class="stats-summary-title">Class matrix</p><p class="stats-summary-meta">No class kill matrix in this log.</p></div>';
    }
    var victims = cm.victim_classes || [];
    var head = '<tr><th>Killer</th>' + victims.map(function(vc) {
      return '<th>' + escapeHtml(vc) + '</th>';
    }).join('') + '</tr>';
    var body = cm.killers.map(function(k) {
      var by = k.kills_by_victim_class || {};
      return '<tr><td>' + playerNameCell(k) + '</td>' + victims.map(function(vc) {
        var n = by[vc] != null ? by[vc] : (by[vc.toLowerCase()] != null ? by[vc.toLowerCase()] : 0);
        return '<td>' + (n ? escapeHtml(String(n)) : '') + '</td>';
      }).join('') + '</tr>';
    }).join('');
    return '<div class="stats-summary log-detail-matrix"><p class="stats-summary-title">Class matrix (kills by victim class)</p>' +
      '<div class="stats-table-wrap"><table class="stats-table"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div></div>';
  }

  function renderChatSection(chat) {
    var messages = (chat && chat.messages) ? chat.messages : [];
    if (!messages.length) return '<p class="stats-summary-meta">No chat in this log.</p>';
    var lines = messages.map(function(msg) {
      var teamCls = msg.team === 'Red' ? 'team-red' : (msg.team === 'Blue' ? 'team-blue' : '');
      return '<div class="log-detail-chat-line ' + teamCls + '"><strong>' + escapeHtml(msg.alias || '') + ':</strong> ' + escapeHtml(msg.msg || '') + '</div>';
    }).join('');
    var note = chat.truncated ? '<p class="stats-summary-meta">Chat list truncated.</p>' : '';
    return note + '<div class="log-detail-chat">' + lines + '</div>';
  }

  function renderKillstreaksSection(ksWrap) {
    var list = (ksWrap && ksWrap.killstreaks) ? ksWrap.killstreaks : [];
    if (!list.length) return '<p class="stats-summary-meta">No killstreak records.</p>';
    var rows = list.map(function(k) {
      return '<tr><td>' + escapeHtml(k.alias || '') + '</td><td>' + escapeHtml(String(k.streak || 0)) + '</td></tr>';
    }).join('');
    return '<div class="stats-table-wrap"><table class="stats-table"><thead><tr><th>Player</th><th>Streak</th></tr></thead><tbody>' + rows + '</tbody></table></div>';
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
