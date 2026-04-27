
function chatTeamClass(teamVal) {
  return teamVal === 'Red' ? 'chat-team-red' : (teamVal === 'Blue' ? 'chat-team-blue' : 'chat-team-none');
}

function chatContextLineHtml(prefix, ctx) {
  if (!ctx) return '';
  var name = '';
  var msg = '';
  var team = null;
  if (typeof ctx === 'string') {
    var s = String(ctx);
    var i = s.indexOf(':');
    if (i > -1) {
      name = s.slice(0, i).trim();
      msg = s.slice(i + 1).trim();
    } else {
      msg = s.trim();
    }
  } else if (typeof ctx === 'object') {
    name = (ctx.name != null ? String(ctx.name) : '').trim();
    msg = (ctx.msg != null ? String(ctx.msg) : '').trim();
    team = ctx.team;
  }
  if (!msg) return '';
  var who = name ? ('<span class="' + chatTeamClass(team) + '">' + escapeHtml(name) + '</span>: ') : '';
  return '<div class="chat-line chat-context">' + escapeHtml(prefix) + ': ' + who + escapeHtml(msg) + '</div>';
}

(function initChatContextToggle() {
  if (window._chatContextToggleBound) return;
  window._chatContextToggleBound = true;
  function isCoarsePointer() {
    return typeof matchMedia === 'function' && matchMedia('(pointer: coarse)').matches;
  }
  document.addEventListener('mouseover', function(ev) {
    var trigger = ev.target && ev.target.closest ? ev.target.closest('.chat-hit-main') : null;
    if (!trigger) return;
    var item = trigger.closest('.chat-hit');
    if (!item) return;
    item.classList.add('chat-hit-hover');
  });
  document.addEventListener('mouseout', function(ev) {
    var trigger = ev.target && ev.target.closest ? ev.target.closest('.chat-hit-main') : null;
    if (!trigger) return;
    var rel = ev.relatedTarget;
    if (rel && trigger.contains(rel)) return;
    var item = trigger.closest('.chat-hit');
    if (!item) return;
    item.classList.remove('chat-hit-hover');
  });
  document.addEventListener('click', function(ev) {
    var t = ev.target;
    if (!t || !t.closest) return;
    var trigger = t.closest('.chat-hit-main');
    var item = null;
    if (trigger) {
      item = trigger.closest('.chat-hit');
    } else if (isCoarsePointer()) {
      // Mobile-friendly: allow tapping anywhere on a chat card to toggle,
      // except interactive elements (links/buttons/inputs/etc).
      item = t.closest('.chat-hit');
      if (!item) return;
      if (t.closest('a, button, input, textarea, select, label, summary, details')) return;
    } else {
      return;
    }
    if (!item) return;
    item.classList.toggle('chat-hit-open');
  });
})();

var CHAT_LAZY_CHUNK_SIZE = 100;
var CHAT_AUTOLOAD_STORAGE_KEY = 'tf2log-chat-autoload';

function scrollChatHitIntoViewIfPresent(anchorId) {
  if (!anchorId || !/^chat-hit-[A-Za-z0-9_-]+$/.test(anchorId)) return false;
  var target = document.getElementById(anchorId);
  if (!target) return false;
  target.scrollIntoView({ behavior: 'smooth', block: 'center' });
  target.classList.add('chat-hit-open');
  return true;
}

/** Append up to maxN chat hits starting at current renderedCount. */
function appendNextChatChunk(container, maxN) {
  maxN = maxN == null ? CHAT_LAZY_CHUNK_SIZE : maxN;
  var state = container._chatLazyState;
  var list = container.querySelector('.chat-results-list');
  if (!state || !list || !state.results || state.results.length === 0) return;
  var remaining = state.results.length - state.renderedCount;
  if (remaining <= 0) return;
  var n = Math.min(maxN, remaining);
  var html = '';
  var end = state.renderedCount + n;
  var word = state.word || '';
  for (var i = state.renderedCount; i < end; i++) {
    html += chatHitHtml(state.results[i], i, word);
  }
  state.renderedCount = end;
  list.insertAdjacentHTML('beforeend', html);
}

function updateChatLazyUI(container) {
  var state = container._chatLazyState;
  var ctrl = container.querySelector('.chat-lazy-controls');
  if (!state || !ctrl) return;
  if (state.renderedCount >= state.results.length) {
    ctrl.style.display = 'none';
    if (container._chatLazyObserver) {
      try { container._chatLazyObserver.disconnect(); } catch (e) {}
      container._chatLazyObserver = null;
    }
  } else {
    ctrl.style.display = '';
  }
}

function disconnectChatLazyObserver(container) {
  if (container._chatLazyObserver) {
    try { container._chatLazyObserver.disconnect(); } catch (e) {}
    container._chatLazyObserver = null;
  }
}

function setupChatLazyObserver(container) {
  disconnectChatLazyObserver(container);
  var cb = container.querySelector('.js-chat-autoload');
  if (!cb || !cb.checked) return;
  var state = container._chatLazyState;
  var sentinel = container.querySelector('.chat-lazy-sentinel');
  if (!state || !sentinel || state.renderedCount >= state.results.length) return;
  var obs = new IntersectionObserver(function(entries) {
    for (var i = 0; i < entries.length; i++) {
      if (!entries[i].isIntersecting) continue;
      var st = container._chatLazyState;
      if (!st || st.renderedCount >= st.results.length) return;
      appendNextChatChunk(container, CHAT_LAZY_CHUNK_SIZE);
      updateChatLazyUI(container);
      break;
    }
  }, { root: null, rootMargin: '80px', threshold: 0 });
  obs.observe(sentinel);
  container._chatLazyObserver = obs;
}

/**
 * Load chunks until hash target exists or all results are in the DOM.
 * Does not scroll until the target element is found (then scrolls once).
 */
function resolveChatAnchorProgressive(container) {
  if (!container || !window.location.hash) return;
  var raw = window.location.hash.slice(1);
  if (!raw || !/^chat-hit-[A-Za-z0-9_-]+$/.test(raw)) return;
  var state = container._chatLazyState;
  if (!state || !state.results || state.results.length === 0) return;
  var maxSteps = Math.ceil(state.results.length / CHAT_LAZY_CHUNK_SIZE) + 3;
  for (var step = 0; step < maxSteps; step++) {
    if (scrollChatHitIntoViewIfPresent(raw)) return;
    if (state.renderedCount >= state.results.length) return;
    appendNextChatChunk(container, CHAT_LAZY_CHUNK_SIZE);
    updateChatLazyUI(container);
  }
}

(function initChatShareLinks() {
  if (window._chatShareLinksBound) return;
  window._chatShareLinksBound = true;
  document.addEventListener('click', function(ev) {
    var link = ev.target && ev.target.closest ? ev.target.closest('.js-chat-hit-link') : null;
    if (!link) return;
    ev.preventDefault();
    var shareUrl = link.getAttribute('data-share-url') || '';
    if (!shareUrl) return;
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(shareUrl).then(function() {
        link.setAttribute('title', 'Copied!');
        setTimeout(function() { link.setAttribute('title', 'Copy link to this result'); }, 1200);
      }).catch(function() {
        window.location.hash = link.getAttribute('data-anchor') || '';
      });
    } else {
      window.location.hash = link.getAttribute('data-anchor') || '';
    }
  });
  window.addEventListener('hashchange', function() {
    var content = document.getElementById('resultsContent');
    resolveChatAnchorProgressive(content);
  });
})();

function chatHitHtml(x, i, word) {
  var teamClass = (x.team === 'Red') ? ' chat-team-red' : ((x.team === 'Blue') ? ' chat-team-blue' : '');
  var p = x.context_prev || null;
  var n = x.context_next || null;
  var prevLine = chatContextLineHtml('Previous', p);
  var nextLine = chatContextLineHtml('Next', n);
  var idBase = String((x.log_id != null ? x.log_id : 'na')) + '-' + String(i);
  var safeId = idBase.replace(/[^A-Za-z0-9_-]/g, '_');
  var hitId = 'chat-hit-' + safeId;
  var shareUrl = window.location.origin + window.location.pathname + window.location.search + '#' + hitId;
  var html = '';
  html += '<div class="chat-hit" id="' + hitId + '">';
  html += '<a href="#' + hitId + '" class="chat-hit-link js-chat-hit-link" data-anchor="' + hitId + '" data-share-url="' + escapeAttr(shareUrl) + '" title="Copy link to this result" aria-label="Copy link to this result">🔗</a>';
  html += '<div class="chat-context-wrap">' + prevLine + '</div>';
  html += '<div class="chat-line chat-main chat-hit-main"><span class="' + teamClass + '">' + escapeHtml(x.alias) + '</span>: ' + highlightChatMatch(x.msg, word) + '</div>';
  html += '<div class="chat-context-wrap">' + nextLine + '</div>';
  html += '<a class="chat-log-link" href="' + escapeHtml(x.url) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(x.url) + '</a>';
  html += '</div>';
  return html;
}

function bindChatLazyControls(el) {
  var loadBtn = el.querySelector('.js-chat-load-more');
  var autoCb = el.querySelector('.js-chat-autoload');
  if (loadBtn) {
    loadBtn.addEventListener('click', function() {
      appendNextChatChunk(el, CHAT_LAZY_CHUNK_SIZE);
      updateChatLazyUI(el);
      setupChatLazyObserver(el);
    });
  }
  if (autoCb) {
    autoCb.addEventListener('change', function() {
      try {
        localStorage.setItem(CHAT_AUTOLOAD_STORAGE_KEY, autoCb.checked ? '1' : '0');
      } catch (err) {}
      if (autoCb.checked) {
        setupChatLazyObserver(el);
      } else {
        disconnectChatLazyObserver(el);
      }
    });
  }
}

var chatLbSortCol = 'occurrences';
var chatLbSortDir = -1;
var CHAT_LB_COLUMNS = [
  { key: 'name', label: 'Name', type: 'text' },
  { key: 'occurrences', label: 'Occurrences', type: 'number' },
  { key: 'logs_count', label: 'Logs', type: 'number' },
  { key: 'word_per_log', label: 'Word / log', type: 'number' },
  { key: 'top_log_id', label: 'Top log', type: 'number' }
];

function getSortedChatLeaderboardRows(rows) {
  var out = rows.slice();
  var col = chatLbSortCol;
  var dir = chatLbSortDir;
  var def = CHAT_LB_COLUMNS.find(function(c) { return c.key === col; });
  if (!def) return out;
  out.sort(function(a, b) {
    var va = a[col];
    var vb = b[col];
    if (def.type === 'number') {
      va = Number(va);
      vb = Number(vb);
      if (!Number.isFinite(va)) va = -Infinity;
      if (!Number.isFinite(vb)) vb = -Infinity;
      if (va < vb) return -dir;
      if (va > vb) return dir;
      return 0;
    }
    va = va != null ? String(va).toLowerCase() : '';
    vb = vb != null ? String(vb).toLowerCase() : '';
    if (va < vb) return -dir;
    if (va > vb) return dir;
    return 0;
  });
  return out;
}

function chatLeaderboardWebhookSubscribeHtml(word) {
  if (!word || word.length < 3) return '';
  return (
    '<div class="webhook-subscribe" data-steamid="" data-word="' + escapeHtml(word) + '">' +
    '<label for="webhookUrlChatLb">Notify me when new logs contain this word (Discord webhook):</label><br>' +
    '<input type="url" id="webhookUrlChatLb" class="js-webhook-url" placeholder="https://discord.com/api/webhooks/..." autocomplete="url"> ' +
    '<button type="button" class="js-chat-subscribe">Subscribe</button>' +
    '<div class="subscribe-msg js-subscribe-msg" aria-live="polite"></div>' +
    '</div>'
  );
}

function bindWebhookSubscribeBox(box) {
  if (!box) return;
  var btn = box.querySelector('.js-chat-subscribe');
  var msgEl = box.querySelector('.js-subscribe-msg');
  var inputEl = box.querySelector('.js-webhook-url');
  if (!btn || !msgEl || !inputEl) return;
  btn.addEventListener('click', function() {
    var url = (inputEl.value || '').trim();
    var sid = (box.getAttribute('data-steamid') || '').trim();
    var w = (box.getAttribute('data-word') || '').trim();
    msgEl.textContent = '';
    if (!url) { msgEl.textContent = 'Enter a Discord webhook URL.'; msgEl.className = 'subscribe-msg js-subscribe-msg error'; return; }
    btn.disabled = true;
    var body = new URLSearchParams({ webhook_url: url, word: w });
    if (sid) body.set('steamid', sid);
    fetch('/api/chat-subscriptions', { method: 'POST', body: body, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } })
      .then(function(r) { return r.json(); })
      .then(function(res) {
        btn.disabled = false;
        if (res.ok) { msgEl.textContent = 'Subscribed. You will get a Discord message when new logs match this search.'; msgEl.className = 'subscribe-msg js-subscribe-msg'; }
        else { msgEl.textContent = res.error || 'Subscription failed.'; msgEl.className = 'subscribe-msg js-subscribe-msg error'; }
      })
      .catch(function(err) { btn.disabled = false; msgEl.textContent = err.message || 'Request failed.'; msgEl.className = 'subscribe-msg js-subscribe-msg error'; });
  });
}

function renderChatLeaderboard(el, data, wordParam, elapsedMs) {
  var rows = data && Array.isArray(data.rows) ? data.rows : [];
  if (rows !== el._chatLbRows) {
    el._chatLbRows = rows;
    chatLbSortCol = 'occurrences';
    chatLbSortDir = -1;
  }
  var sorted = getSortedChatLeaderboardRows(el._chatLbRows || []);
  var logsSearched = (data && Number.isFinite(Number(data.logs_searched))) ? Number(data.logs_searched) : null;
  var word = (wordParam || '').trim();
  var lbSubscribe = chatLeaderboardWebhookSubscribeHtml(word);
  if (!sorted.length) {
    el.innerHTML = 'No matches.' + lbSubscribe + requestTimingFooter(elapsedMs);
    bindWebhookSubscribeBox(el.querySelector('.webhook-subscribe'));
    return;
  }

  var thead = '<tr>';
  CHAT_LB_COLUMNS.forEach(function(c) {
    var cls = 'sortable';
    if (c.key === chatLbSortCol) cls += chatLbSortDir === 1 ? ' sorted-asc' : ' sorted-desc';
    thead += '<th class="' + cls + '" data-col="' + escapeHtml(c.key) + '" scope="col">' + escapeHtml(c.label) + '</th>';
  });
  thead += '</tr>';

  var tbody = '';
  sorted.forEach(function(r) {
    var sid64 = r.steamid64 != null ? String(r.steamid64).trim() : '';
    var displayName = r.name != null ? String(r.name) : '';
    if (!displayName) displayName = sid64 || 'Unknown';
    var profile = internalProfileHref(sid64);
    var nameCell = steamAvatarPlaceholder(sid64) + (
      profile
        ? ('<a href="' + escapeAttr(profile) + '">' + escapeHtml(displayName) + '</a>')
        : escapeHtml(displayName));
    var opl = Number(r.word_per_log);
    var oplCell = Number.isFinite(opl) ? opl.toFixed(2) : '0.00';
    var topLogId = (r.top_log_id != null) ? String(r.top_log_id) : '';
    var topLogUrl = (r.top_log_url != null) ? String(r.top_log_url) : '';
    var topLogCell = (topLogId && topLogUrl)
      ? ('<a href="' + escapeAttr(topLogUrl) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(topLogId) + '</a>')
      : '—';
    tbody += '<tr><td>' + nameCell + '</td><td>' + escapeHtml(String(r.occurrences != null ? r.occurrences : 0)) + '</td><td>' + escapeHtml(String(r.logs_count != null ? r.logs_count : 0)) + '</td><td>' + escapeHtml(oplCell) + '</td><td>' + topLogCell + '</td></tr>';
  });

  var summary = 'Top ' + escapeHtml(String(sorted.length)) + ' player(s) for "' + escapeHtml(word) + '"';
  if (logsSearched != null) summary += ' across ' + escapeHtml(String(logsSearched)) + ' log(s)';
  summary += '.';
  el._chatLbWord = word;
  el.innerHTML =
    '<p class="chat-summary-line">' + summary + '</p>' +
    '<div class="stats-csv-toolbar"><button type="button" class="js-chat-leaderboard-csv-download" aria-label="Download chat leaderboard table as a CSV file">Download as CSV</button></div>' +
    '<div class="stats-table-wrap"><table class="stats-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>' +
    lbSubscribe +
    requestTimingFooter(elapsedMs);

  loadAvatarsInContainer(el);
  bindChatLeaderboardCsvDownload(el);
  bindWebhookSubscribeBox(el.querySelector('.webhook-subscribe'));

  el.querySelectorAll('th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var col = th.getAttribute('data-col');
      if (!col) return;
      if (chatLbSortCol === col) chatLbSortDir = chatLbSortDir === 1 ? -1 : 1;
      else { chatLbSortCol = col; chatLbSortDir = 1; }
      renderChatLeaderboard(el, { rows: el._chatLbRows, logs_searched: logsSearched }, word, elapsedMs);
    });
  });
}

function renderChatResult(el, data, steamidParam, wordParam, showWebhookBox, elapsedMs) {
  disconnectChatLazyObserver(el);
  el._chatLazyState = null;

  var steamid = steamidParam || '';
  var word = (wordParam || '').trim();
  var html = '';
  if (showWebhookBox && word) {
    html += '<div class="webhook-subscribe" data-steamid="' + escapeHtml(steamid) + '" data-word="' + escapeHtml(word) + '">';
    html += '<label for="webhookUrl">Notify me when new logs match this search (Discord webhook):</label><br>';
    html += '<input type="url" id="webhookUrl" class="js-webhook-url" placeholder="https://discord.com/api/webhooks/..." autocomplete="url"> ';
    html += '<button type="button" class="js-chat-subscribe">Subscribe</button>';
    html += '<div class="subscribe-msg js-subscribe-msg" aria-live="polite"></div>';
    html += '</div>';
  }
  var resolved64 = data.resolved_steamid64 && /^\d{17}$/.test(String(data.resolved_steamid64)) ? String(data.resolved_steamid64) : null;
  var sidForProfile = resolved64 || ((steamid && /^\d{17}$/.test(String(steamid).trim())) ? String(steamid).trim() : '');
  var profileUrl = sidForProfile ? internalProfileHref(sidForProfile) : '#';
  var sidForAvatar = resolved64;
  if (!sidForAvatar && steamid && /^\d{17}$/.test(String(steamid).trim())) sidForAvatar = String(steamid).trim();
  var label = data.searched_user_name ? escapeHtml(data.searched_user_name) : escapeHtml(sidForProfile || steamid);
  var logsAcross = null;
  if (data && typeof data.logs_searched === 'number' && Number.isFinite(data.logs_searched)) {
    logsAcross = data.logs_searched;
  } else {
    var ids = new Set();
    (data.results || []).forEach(function(r) { if (r && r.log_id != null) ids.add(String(r.log_id)); });
    logsAcross = ids.size;
  }
  var results = data.results || [];
  if (results.length === 0) {
    el.innerHTML = html + 'No matches.' + requestTimingFooter(elapsedMs);
    return;
  }

  html += '<p class="chat-summary-line">Total ' + escapeHtml(String(data.total || 0)) + ' occurrence(s) for ' + steamAvatarPlaceholder(sidForAvatar) + '<a href="' + escapeAttr(profileUrl) + '">' + label + '</a> across ' + escapeHtml(String(logsAcross)) + ' log(s).</p>';
  var firstN = Math.min(CHAT_LAZY_CHUNK_SIZE, results.length);
  var listHtml = '';
  for (var j = 0; j < firstN; j++) {
    listHtml += chatHitHtml(results[j], j, word);
  }
  html += '<div class="chat-results-list">' + listHtml + '</div>';
  html += '<div class="chat-lazy-controls">';
  html += '<button type="button" class="js-chat-load-more">Load more</button>';
  html += '<label class="chat-autoload-label"><input type="checkbox" class="js-chat-autoload"> Auto-load more as I scroll</label>';
  html += '<div class="chat-lazy-sentinel" aria-hidden="true"></div>';
  html += '</div>';
  html += requestTimingFooter(elapsedMs);

  el.innerHTML = html;
  loadAvatarsInContainer(el);
  el._chatLazyState = {
    results: results,
    renderedCount: firstN,
    word: word
  };
  updateChatLazyUI(el);

  bindChatLazyControls(el);
  var autoCb = el.querySelector('.js-chat-autoload');
  var autoloadOn = false;
  try {
    autoloadOn = localStorage.getItem(CHAT_AUTOLOAD_STORAGE_KEY) === '1';
  } catch (e) {}
  if (autoCb) autoCb.checked = autoloadOn;

  resolveChatAnchorProgressive(el);

  if (autoloadOn) {
    setupChatLazyObserver(el);
  }

  if (showWebhookBox && word) {
    bindWebhookSubscribeBox(el.querySelector('.webhook-subscribe'));
  }
}
