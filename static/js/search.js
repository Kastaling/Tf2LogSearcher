var THEME_KEY = 'tf2log-theme';

function getTheme() { return document.documentElement.getAttribute('data-theme') || 'light'; }
function setTheme(theme) {
  theme = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', theme);
  try { localStorage.setItem(THEME_KEY, theme); } catch (e) {}
  var btn = document.getElementById('themeToggle');
  if (btn) { btn.textContent = theme === 'dark' ? '\u263C' : '\u263E'; btn.setAttribute('aria-label', theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'); }
  refreshStatsTrendChart();
  refreshProfileTrendChart();
}
(function initThemeToggle() {
  var btn = document.getElementById('themeToggle');
  if (!btn) return;
  setTheme(getTheme());
  btn.addEventListener('click', function() { setTheme(getTheme() === 'dark' ? 'light' : 'dark'); });
})();

function formData(form) {
  const fd = new FormData(form);
  return new URLSearchParams(fd).toString();
}

/** Last search params for restoring forms when returning to home (sessionStorage; tab-scoped). */
var SEARCH_STATE_KEY = 'tf2log-search-state';
var MAX_STEAMID_INPUT_LEN = 2048;
var MAX_CHAT_WORD_LEN = 200;
var MAX_MAP_QUERY_LEN = 100;
var MAX_PLAYER_NAME_QUERY_LEN = 64;
var MIN_PLAYER_NAME_QUERY_LEN = 3;
var MAX_LOGMATCH_STEAMIDS_LEN = 32768;
var VALID_STATS_GAMEMODES = { hl: 1, '7s': 1, '6s': 1, ud: 1 };
var VALID_STATS_CLASSES = { scout: 1, soldier: 1, pyro: 1, demoman: 1, heavyweapons: 1, engineer: 1, medic: 1, sniper: 1, spy: 1 };

function sanitizeDateInput(s) {
  if (s == null) return '';
  s = String(s).trim();
  if (!s) return '';
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : '';
}

function sanitizeGamemodeInput(g) {
  g = (g == null ? '' : String(g)).trim();
  return VALID_STATS_GAMEMODES[g] ? g : 'hl';
}

/** Empty string = all gamemodes; otherwise same whitelist as stats. */
function sanitizeCoplayersGamemodeInput(g) {
  g = (g == null ? '' : String(g)).trim();
  if (g === '') return '';
  return VALID_STATS_GAMEMODES[g] ? g : '';
}

function sanitizeClassesCsv(s) {
  var parts = (s == null ? '' : String(s)).split(',');
  var seen = {};
  var out = [];
  for (var i = 0; i < parts.length; i++) {
    var c = parts[i].trim();
    if (c && VALID_STATS_CLASSES[c] && !seen[c]) {
      seen[c] = true;
      out.push(c);
    }
  }
  return out.join(',');
}

function sanitizeMapQueryInput(s) {
  s = (s == null ? '' : String(s)).trim();
  if (s.length > MAX_MAP_QUERY_LEN) s = s.slice(0, MAX_MAP_QUERY_LEN);
  return s;
}


function syncLeaderboardStatScopeStrip(form) {
  if (!form) return;
  var stripTp = form.querySelector('.js-lb-stat-scope-strip');
  var stripWr = form.querySelector('.js-lb-winrate-scope-strip');
  var hidden = form.elements.stat_scope;
  var lb = sanitizeLbTypeInput(form.elements.lb_type && form.elements.lb_type.value ? form.elements.lb_type.value : 'dpm');
  var showTp = (lb === 'ubers' || lb === 'drops' || lb === 'damage_taken');
  var showWr = (lb === 'winrate');
  if (stripTp) {
    if (showTp) stripTp.removeAttribute('hidden');
    else stripTp.setAttribute('hidden', '');
  }
  if (stripWr) {
    if (showWr) stripWr.removeAttribute('hidden');
    else stripWr.setAttribute('hidden', '');
  }
  var cur = hidden ? sanitizeLeaderboardStatScopeInput(hidden.value, lb) : 'total';
  if (hidden) hidden.value = cur;
  if (stripTp) {
    stripTp.querySelectorAll('.js-lb-stat-scope-btn').forEach(function(b) {
      var sc = b.getAttribute('data-stat-scope');
      var on = showTp && sc === cur;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
  }
  if (stripWr) {
    stripWr.querySelectorAll('.js-lb-winrate-scope-btn').forEach(function(b) {
      var sc = b.getAttribute('data-stat-scope');
      var on = showWr && sc === cur;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
  }
}
function sanitizeLeaderboardMinLogs(v) {
  var n = parseInt(String(v == null ? '' : v).trim(), 10);
  if (!Number.isFinite(n)) return '10';
  n = Math.max(1, Math.min(5000, n));
  return String(n);
}

/**
 * Build a safe state object from URL-style params (no HTML; length-limited).
 * mode: chat | stats | logmatch | coplayers | profile | leaderboard | playername
 */
function buildSanitizedSearchState(mode, params) {
  mode = (mode || '').trim();
  if (mode === 'chat') {
    var sid = (params.get('steamid') || '').trim();
    var word = (params.get('word') || '').trim();
    if (word.length > MAX_CHAT_WORD_LEN) word = word.slice(0, MAX_CHAT_WORD_LEN);
    if (sid.length > MAX_STEAMID_INPUT_LEN) sid = sid.slice(0, MAX_STEAMID_INPUT_LEN);
    if (!sid && word.length < 3) return null;
    return {
      mode: 'chat',
      steamid: sid,
      word: word,
      date_from: sanitizeDateInput(params.get('date_from')),
      date_to: sanitizeDateInput(params.get('date_to')),
      map_query: sanitizeMapQueryInput(params.get('map_query')),
    };
  }
  if (mode === 'stats') {
    var steamid = (params.get('steamid') || '').trim();
    if (!steamid) return null;
    if (steamid.length > MAX_STEAMID_INPUT_LEN) steamid = steamid.slice(0, MAX_STEAMID_INPUT_LEN);
    return {
      mode: 'stats',
      steamid: steamid,
      gamemode: sanitizeGamemodeInput(params.get('gamemode')),
      classes: sanitizeClassesCsv(params.get('classes')),
      date_from: sanitizeDateInput(params.get('date_from')),
      date_to: sanitizeDateInput(params.get('date_to')),
      map_query: sanitizeMapQueryInput(params.get('map_query')),
    };
  }
  if (mode === 'logmatch') {
    var steamids = (params.get('steamids') || '').trim();
    if (!steamids) return null;
    if (steamids.length > MAX_LOGMATCH_STEAMIDS_LEN) steamids = steamids.slice(0, MAX_LOGMATCH_STEAMIDS_LEN);
    return { mode: 'logmatch', steamids: steamids, map_query: sanitizeMapQueryInput(params.get('map_query')) };
  }
  if (mode === 'coplayers') {
    var cSid = (params.get('steamid') || '').trim();
    if (!cSid) return null;
    if (cSid.length > MAX_STEAMID_INPUT_LEN) cSid = cSid.slice(0, MAX_STEAMID_INPUT_LEN);
    return {
      mode: 'coplayers',
      steamid: cSid,
      gamemode: sanitizeCoplayersGamemodeInput(params.get('gamemode')),
      map_query: sanitizeMapQueryInput(params.get('map_query')),
    };
  }
  if (mode === 'profile') {
    var pSid = (params.get('steamid') || '').trim();
    if (!pSid) return null;
    if (pSid.length > MAX_STEAMID_INPUT_LEN) pSid = pSid.slice(0, MAX_STEAMID_INPUT_LEN);
    return {
      mode: 'profile',
      steamid: pSid,
      gamemode: sanitizeCoplayersGamemodeInput(params.get('gamemode')),
      date_from: sanitizeDateInput(params.get('date_from')),
      date_to: sanitizeDateInput(params.get('date_to')),
      map_query: sanitizeMapQueryInput(params.get('map_query')),
    };
  }
  if (mode === 'leaderboard') {
    var lbSt = sanitizeLbTypeInput(params.get('lb_type'));
    return {
      mode: 'leaderboard',
      lb_type: lbSt,
      stat_scope: sanitizeLeaderboardStatScopeInput(params.get('stat_scope'), lbSt),
      gamemode: sanitizeCoplayersGamemodeInput(params.get('gamemode')),
      class_filter: sanitizeLeaderboardClassFilter(params.get('class_filter'), lbSt),
      map_query: sanitizeMapQueryInput(params.get('map_query')),
      date_from: sanitizeDateInput(params.get('date_from')),
      date_to: sanitizeDateInput(params.get('date_to')),
      min_logs: sanitizeLeaderboardMinLogs(params.get('min_logs')),
    };
  }
  if (mode === 'playername') {
    var pnq = (params.get('q') || '').trim();
    if (pnq.length > MAX_PLAYER_NAME_QUERY_LEN) pnq = pnq.slice(0, MAX_PLAYER_NAME_QUERY_LEN);
    pnq = pnq.replace(/[\u0000-\u001F\u007F]/g, '');
    if (pnq.length < MIN_PLAYER_NAME_QUERY_LEN) return null;
    return { mode: 'playername', q: pnq };
  }
  return null;
}

function persistSearchState(state) {
  if (!state) return;
  try {
    sessionStorage.setItem(SEARCH_STATE_KEY, JSON.stringify(state));
  } catch (e) {}
}

function applySearchStateToForms(state) {
  if (!state || !state.mode) return;
  if (state.mode === 'chat') {
    var fc = document.getElementById('frmChat');
    if (!fc) return;
    if (fc.steamid) fc.steamid.value = state.steamid || '';
    if (fc.word) fc.word.value = state.word || '';
    if (fc.elements.date_from) fc.elements.date_from.value = state.date_from || '';
    if (fc.elements.date_to) fc.elements.date_to.value = state.date_to || '';
    if (fc.elements.map_query) fc.elements.map_query.value = state.map_query || '';
    return;
  }
  if (state.mode === 'stats') {
    var fs = document.getElementById('frmStats');
    if (!fs) return;
    if (fs.steamid) fs.steamid.value = state.steamid || '';
    if (fs.gamemode) fs.gamemode.value = sanitizeGamemodeInput(state.gamemode);
    if (fs.elements.date_from) fs.elements.date_from.value = state.date_from || '';
    if (fs.elements.date_to) fs.elements.date_to.value = state.date_to || '';
    if (fs.elements.map_query) fs.elements.map_query.value = state.map_query || '';
    var checks = fs.querySelectorAll('input[name="classes"]');
    var want = {};
    (state.classes || '').split(',').forEach(function(c) {
      c = c.trim();
      if (VALID_STATS_CLASSES[c]) want[c] = true;
    });
    for (var i = 0; i < checks.length; i++) {
      checks[i].checked = !!want[checks[i].value];
    }
    return;
  }
  if (state.mode === 'logmatch') {
    var fl = document.getElementById('frmLogmatch');
    if (!fl || !fl.steamids) return;
    fl.steamids.value = state.steamids || '';
    if (fl.elements.map_query) fl.elements.map_query.value = state.map_query || '';
    return;
  }
  if (state.mode === 'coplayers') {
    var fcp = document.getElementById('frmCoplayers');
    if (!fcp) return;
    if (fcp.steamid) fcp.steamid.value = state.steamid || '';
    if (fcp.gamemode) fcp.gamemode.value = sanitizeCoplayersGamemodeInput(state.gamemode);
    if (fcp.elements.map_query) fcp.elements.map_query.value = state.map_query || '';
    return;
  }
  if (state.mode === 'profile') {
    var fp = document.getElementById('frmProfile');
    if (!fp) return;
    if (fp.steamid) fp.steamid.value = state.steamid || '';
    if (fp.gamemode) fp.gamemode.value = sanitizeCoplayersGamemodeInput(state.gamemode);
    if (fp.elements.date_from) fp.elements.date_from.value = state.date_from || '';
    if (fp.elements.date_to) fp.elements.date_to.value = state.date_to || '';
    if (fp.elements.map_query) fp.elements.map_query.value = state.map_query || '';
    return;
  }
  if (state.mode === 'leaderboard') {
    var flb = document.getElementById('frmLeaderboard');
    if (!flb) return;
    var lbt = sanitizeLbTypeInput(state.lb_type);
    if (flb.elements.lb_type) flb.elements.lb_type.value = lbt;
    flb.querySelectorAll('.js-lb-type-btn').forEach(function(b) {
      var on = b.getAttribute('data-lb-type') === lbt;
      b.classList.toggle('active', on);
      b.setAttribute('aria-selected', on ? 'true' : 'false');
    });
    if (flb.gamemode) flb.gamemode.value = sanitizeCoplayersGamemodeInput(state.gamemode);
    if (flb.elements.stat_scope) {
      flb.elements.stat_scope.value = sanitizeLeaderboardStatScopeInput(state.stat_scope, state.lb_type);
    }
    syncLeaderboardStatScopeStrip(flb);
    if (flb.elements.class_filter) {
      flb.elements.class_filter.value = sanitizeLeaderboardClassFilter(state.class_filter, state.lb_type);
    }
    syncLeaderboardClassSelectForMedicLeaderboards(flb);
    if (flb.elements.map_query) flb.elements.map_query.value = state.map_query || '';
    if (flb.elements.date_from) flb.elements.date_from.value = state.date_from || '';
    if (flb.elements.date_to) flb.elements.date_to.value = state.date_to || '';
    if (flb.elements.min_logs) flb.elements.min_logs.value = sanitizeLeaderboardMinLogs(state.min_logs);
    return;
  }
  if (state.mode === 'playername') {
    var fpn = document.getElementById('frmPlayerName');
    if (!fpn || !fpn.q) return;
    fpn.q.value = state.q != null ? String(state.q) : '';
  }
}

function isHomePathname() {
  var p = window.location.pathname || '';
  return p === '/' || p === '/index.html';
}

/** Scroll the home endpoint section (h2 + form) into view — must run after home layout reorders DOM. */
function scrollHomeEndpointIntoView(mode) {
  var m = (mode || '').trim();
  var allow = { chat: 1, stats: 1, logmatch: 1, coplayers: 1, profile: 1, leaderboard: 1, playername: 1 };
  if (!allow[m]) return;
  var ep = document.querySelector(
    '#homePage .js-home-endpoints-stack .js-home-endpoint[data-home-endpoint="' + m + '"]'
  );
  if (!ep || ep.hasAttribute('hidden')) return;
  requestAnimationFrame(function() {
    requestAnimationFrame(function() {
      ep.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}

function pulseDeepLinkEl(el) {
  if (!el) return;
  function fin() {
    el.classList.remove('deep-link-field-flash');
    el.removeEventListener('animationend', fin);
  }
  el.addEventListener('animationend', fin, { once: true });
  el.classList.add('deep-link-field-flash');
}

function flashDeepLinkFilledFields(state) {
  if (!state || !state.mode) return;
  var epRoot = document.querySelector(
    '#homePage .js-home-endpoints-stack .js-home-endpoint[data-home-endpoint="' + state.mode + '"]'
  );
  if (!epRoot || epRoot.hasAttribute('hidden')) return;
  function ne(v) {
    return v != null && String(v).trim() !== '';
  }
  if (state.mode === 'chat') {
    var fc = document.getElementById('frmChat');
    if (!fc) return;
    if (ne(state.steamid) && fc.steamid) pulseDeepLinkEl(fc.steamid);
    if (ne(state.word) && fc.word) pulseDeepLinkEl(fc.word);
    if (ne(state.map_query) && fc.elements.map_query) pulseDeepLinkEl(fc.elements.map_query);
    if (ne(state.date_from) && fc.elements.date_from) pulseDeepLinkEl(fc.elements.date_from);
    if (ne(state.date_to) && fc.elements.date_to) pulseDeepLinkEl(fc.elements.date_to);
    return;
  }
  if (state.mode === 'stats') {
    var fs = document.getElementById('frmStats');
    if (!fs) return;
    if (ne(state.steamid) && fs.steamid) pulseDeepLinkEl(fs.steamid);
    if (ne(state.gamemode) && fs.gamemode) pulseDeepLinkEl(fs.gamemode);
    if (ne(state.map_query) && fs.elements.map_query) pulseDeepLinkEl(fs.elements.map_query);
    if (ne(state.date_from) && fs.elements.date_from) pulseDeepLinkEl(fs.elements.date_from);
    if (ne(state.date_to) && fs.elements.date_to) pulseDeepLinkEl(fs.elements.date_to);
    if (ne(state.classes)) {
      fs.querySelectorAll('input[name="classes"]:checked').forEach(function(inp) {
        var lab = inp.closest('label');
        if (lab) pulseDeepLinkEl(lab);
      });
    }
    return;
  }
  if (state.mode === 'logmatch') {
    var fl = document.getElementById('frmLogmatch');
    if (!fl) return;
    if (ne(state.steamids) && fl.steamids) pulseDeepLinkEl(fl.steamids);
    if (ne(state.map_query) && fl.elements.map_query) pulseDeepLinkEl(fl.elements.map_query);
    return;
  }
  if (state.mode === 'coplayers') {
    var fcp = document.getElementById('frmCoplayers');
    if (!fcp) return;
    if (ne(state.steamid) && fcp.steamid) pulseDeepLinkEl(fcp.steamid);
    if (ne(state.gamemode) && fcp.gamemode) pulseDeepLinkEl(fcp.gamemode);
    if (ne(state.map_query) && fcp.elements.map_query) pulseDeepLinkEl(fcp.elements.map_query);
    return;
  }
  if (state.mode === 'profile') {
    var fp = document.getElementById('frmProfile');
    if (!fp) return;
    if (ne(state.steamid) && fp.steamid) pulseDeepLinkEl(fp.steamid);
    if (ne(state.gamemode) && fp.gamemode) pulseDeepLinkEl(fp.gamemode);
    if (ne(state.map_query) && fp.elements.map_query) pulseDeepLinkEl(fp.elements.map_query);
    if (ne(state.date_from) && fp.elements.date_from) pulseDeepLinkEl(fp.elements.date_from);
    if (ne(state.date_to) && fp.elements.date_to) pulseDeepLinkEl(fp.elements.date_to);
    return;
  }
  if (state.mode === 'leaderboard') {
    var flb = document.getElementById('frmLeaderboard');
    if (!flb) return;
    var activeLb = flb.querySelector('.js-lb-type-btn.active');
    if (activeLb) pulseDeepLinkEl(activeLb);
    var scopeStrip = flb.querySelector('.js-lb-stat-scope-strip');
    if (scopeStrip && !scopeStrip.hasAttribute('hidden')) pulseDeepLinkEl(scopeStrip);
    var wrStrip = flb.querySelector('.js-lb-winrate-scope-strip');
    if (wrStrip && !wrStrip.hasAttribute('hidden')) pulseDeepLinkEl(wrStrip);
    if (ne(state.gamemode) && flb.gamemode) pulseDeepLinkEl(flb.gamemode);
    if (ne(state.class_filter) && flb.elements.class_filter) pulseDeepLinkEl(flb.elements.class_filter);
    if (ne(state.map_query) && flb.elements.map_query) pulseDeepLinkEl(flb.elements.map_query);
    if (ne(state.date_from) && flb.elements.date_from) pulseDeepLinkEl(flb.elements.date_from);
    if (ne(state.date_to) && flb.elements.date_to) pulseDeepLinkEl(flb.elements.date_to);
    if (ne(state.min_logs) && flb.elements.min_logs && String(state.min_logs).trim() !== '10') {
      pulseDeepLinkEl(flb.elements.min_logs);
    }
    return;
  }
  if (state.mode === 'playername') {
    var fpn = document.getElementById('frmPlayerName');
    if (!fpn || !fpn.q) return;
    if (ne(state.q)) pulseDeepLinkEl(fpn.q);
  }
}

/** Run after home layout has applied cookie order (and on bfcache restore). */
function runDeepLinkScrollAndFlashFromUrl() {
  if (!isHomePathname()) return;
  var params = new URLSearchParams(window.location.search);
  var mode = (params.get('mode') || '').trim();
  if (!mode) return;
  var st = buildSanitizedSearchState(mode, params);
  if (!st) return;
  scrollHomeEndpointIntoView(mode);
  flashDeepLinkFilledFields(st);
}

/** Restore chat / stats / logmatch forms from ?query or sessionStorage. */
function restoreHomeForms() {
  if (!isHomePathname()) return;
  var params = new URLSearchParams(window.location.search);
  var mode = params.get('mode');
  if (mode) {
    var fromUrl = buildSanitizedSearchState(mode, params);
    if (fromUrl) {
      applySearchStateToForms(fromUrl);
      persistSearchState(fromUrl);
    }
    return;
  }
  var raw;
  try {
    raw = sessionStorage.getItem(SEARCH_STATE_KEY);
  } catch (e) {
    return;
  }
  if (!raw) return;
  var parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    return;
  }
  if (!parsed || typeof parsed.mode !== 'string') return;
  var p2 = new URLSearchParams();
  if (parsed.mode === 'chat') {
    p2.set('steamid', parsed.steamid != null ? String(parsed.steamid) : '');
    p2.set('word', parsed.word != null ? String(parsed.word) : '');
    p2.set('date_from', parsed.date_from != null ? String(parsed.date_from) : '');
    p2.set('date_to', parsed.date_to != null ? String(parsed.date_to) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
  } else if (parsed.mode === 'stats') {
    p2.set('steamid', parsed.steamid != null ? String(parsed.steamid) : '');
    p2.set('gamemode', parsed.gamemode != null ? String(parsed.gamemode) : '');
    p2.set('classes', parsed.classes != null ? String(parsed.classes) : '');
    p2.set('date_from', parsed.date_from != null ? String(parsed.date_from) : '');
    p2.set('date_to', parsed.date_to != null ? String(parsed.date_to) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
  } else if (parsed.mode === 'logmatch') {
    p2.set('steamids', parsed.steamids != null ? String(parsed.steamids) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
  } else if (parsed.mode === 'coplayers') {
    p2.set('steamid', parsed.steamid != null ? String(parsed.steamid) : '');
    p2.set('gamemode', parsed.gamemode != null ? String(parsed.gamemode) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
  } else if (parsed.mode === 'profile') {
    p2.set('steamid', parsed.steamid != null ? String(parsed.steamid) : '');
    p2.set('gamemode', parsed.gamemode != null ? String(parsed.gamemode) : '');
    p2.set('date_from', parsed.date_from != null ? String(parsed.date_from) : '');
    p2.set('date_to', parsed.date_to != null ? String(parsed.date_to) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
  } else if (parsed.mode === 'leaderboard') {
    p2.set('lb_type', parsed.lb_type != null ? String(parsed.lb_type) : '');
    p2.set('stat_scope', parsed.stat_scope != null ? String(parsed.stat_scope) : '');
    p2.set('gamemode', parsed.gamemode != null ? String(parsed.gamemode) : '');
    p2.set('class_filter', parsed.class_filter != null ? String(parsed.class_filter) : '');
    p2.set('map_query', parsed.map_query != null ? String(parsed.map_query) : '');
    p2.set('date_from', parsed.date_from != null ? String(parsed.date_from) : '');
    p2.set('date_to', parsed.date_to != null ? String(parsed.date_to) : '');
    p2.set('min_logs', parsed.min_logs != null ? String(parsed.min_logs) : '');
  } else if (parsed.mode === 'playername') {
    p2.set('q', parsed.q != null ? String(parsed.q) : '');
  } else {
    return;
  }
  var again = buildSanitizedSearchState(parsed.mode, p2);
  if (again) applySearchStateToForms(again);
}

(function initHomeFormRestore() {
  restoreHomeForms();
  window.addEventListener('pageshow', function(ev) {
    if (ev.persisted && isHomePathname()) {
      restoreHomeForms();
      runDeepLinkScrollAndFlashFromUrl();
    }
  });
})();

/** Cookie-backed order + visibility for home page search sections (same-origin, SameSite=Lax). */
(function initHomePageLayout() {
  var HOME_LAYOUT_COOKIE = 'tf2ls_home_layout_v1';
  var HOME_LAYOUT_COOKIE_MAX_AGE = 31536000;
  var HOME_ENDPOINT_IDS = ['chat', 'profile', 'coplayers', 'logmatch', 'stats', 'leaderboard', 'playername', 'log_library'];
  var HOME_ENDPOINT_LABELS = {
    chat: 'Chat Searcher',
    logmatch: 'Multi-party Log Searcher',
    stats: 'Stats Sorter',
    coplayers: 'Frequent Co-players',
    profile: 'Player profile',
    leaderboard: 'Stats Leaderboards',
    playername: 'Player search by name',
    log_library: 'Log library'
  };

  function sanitizeHomeLayoutOrder(raw) {
    var seen = Object.create(null);
    var out = [];
    if (Array.isArray(raw)) {
      raw.forEach(function(id) {
        if (HOME_ENDPOINT_IDS.indexOf(id) >= 0 && !seen[id]) {
          seen[id] = true;
          out.push(id);
        }
      });
    }
    HOME_ENDPOINT_IDS.forEach(function(id) {
      if (!seen[id]) out.push(id);
    });
    return out;
  }

  function sanitizeHomeHidden(raw) {
    var h = Object.create(null);
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
      HOME_ENDPOINT_IDS.forEach(function(id) {
        if (raw[id] === true) h[id] = true;
      });
    } else if (Array.isArray(raw)) {
      raw.forEach(function(id) {
        if (HOME_ENDPOINT_IDS.indexOf(id) >= 0) h[id] = true;
      });
    }
    return h;
  }

  function readHomeLayoutSettings() {
    var d = { order: HOME_ENDPOINT_IDS.slice(), hidden: {} };
    try {
      var all = typeof document !== 'undefined' && document.cookie ? document.cookie : '';
      if (!all) return d;
      var prefix = HOME_LAYOUT_COOKIE + '=';
      var idx = all.indexOf(prefix);
      if (idx < 0) return d;
      var start = idx + prefix.length;
      var end = all.indexOf(';', start);
      var raw = decodeURIComponent(end < 0 ? all.slice(start) : all.slice(start, end));
      var o = JSON.parse(raw);
      if (o && typeof o === 'object') {
        if (Array.isArray(o.order)) d.order = sanitizeHomeLayoutOrder(o.order);
        d.hidden = sanitizeHomeHidden(o.hidden);
      }
    } catch (e) {}
    return d;
  }

  function writeHomeLayoutSettings(settings) {
    try {
      var order = sanitizeHomeLayoutOrder(settings.order || []);
      var hidden = sanitizeHomeHidden(settings.hidden);
      var payload = encodeURIComponent(JSON.stringify({ order: order, hidden: hidden }));
      if (payload.length > 3800) return;
      document.cookie = HOME_LAYOUT_COOKIE + '=' + payload + ';path=/;max-age=' + HOME_LAYOUT_COOKIE_MAX_AGE + ';SameSite=Lax';
    } catch (e) {}
  }

  function applyHomeEndpointVisibility(stack, hiddenMap) {
    if (!stack) return;
    stack.querySelectorAll('.js-home-endpoint').forEach(function(el) {
      var id = el.getAttribute('data-home-endpoint');
      if (!id) return;
      if (hiddenMap[id]) el.setAttribute('hidden', '');
      else el.removeAttribute('hidden');
    });
  }

  function applyHomeEndpointOrder(stack, order) {
    var full = sanitizeHomeLayoutOrder(order);
    var byId = Object.create(null);
    stack.querySelectorAll('.js-home-endpoint').forEach(function(el) {
      var id = el.getAttribute('data-home-endpoint');
      if (id) byId[id] = el;
    });
    full.forEach(function(id) {
      var n = byId[id];
      if (n) stack.appendChild(n);
    });
  }

  function rebuildHomeLayoutSortList(ul, settings) {
    ul.innerHTML = '';
    settings.order.forEach(function(id) {
      if (HOME_ENDPOINT_IDS.indexOf(id) < 0) return;
      var li = document.createElement('li');
      li.setAttribute('draggable', 'true');
      li.setAttribute('data-home-endpoint', id);
      li.className = 'profile-layout-sort-item home-layout-sort-item';
      var cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'js-home-endpoint-visible';
      cb.setAttribute('draggable', 'false');
      cb.checked = !settings.hidden[id];
      cb.title = 'Show this section on the home page';
      var lab = document.createElement('label');
      lab.className = 'home-layout-sort-label';
      lab.appendChild(cb);
      lab.appendChild(document.createTextNode(' ' + (HOME_ENDPOINT_LABELS[id] || id)));
      li.appendChild(lab);
      ul.appendChild(li);
    });
  }

  var home = document.getElementById('homePage');
  var stack = home && home.querySelector('.js-home-endpoints-stack');
  var ul = home && home.querySelector('.js-home-layout-sort-list');
  if (!home || !stack || !ul) return;

  var s = readHomeLayoutSettings();
  applyHomeEndpointOrder(stack, s.order);
  applyHomeEndpointVisibility(stack, s.hidden);
  rebuildHomeLayoutSortList(ul, s);

  if (typeof runDeepLinkScrollAndFlashFromUrl === 'function') {
    runDeepLinkScrollAndFlashFromUrl();
  }

  ul.addEventListener('change', function(ev) {
    var t = ev.target;
    if (!t || !t.classList.contains('js-home-endpoint-visible')) return;
    var li = t.closest('li[data-home-endpoint]');
    if (!li || !ul.contains(li)) return;
    var id = li.getAttribute('data-home-endpoint');
    if (HOME_ENDPOINT_IDS.indexOf(id) < 0) return;
    var cur = readHomeLayoutSettings();
    if (t.checked) delete cur.hidden[id];
    else cur.hidden[id] = true;
    writeHomeLayoutSettings(cur);
    applyHomeEndpointVisibility(stack, cur.hidden);
  });

  var dragEl = null;
  ul.addEventListener('dragstart', function(e) {
    var t = e.target;
    if (t && (t.tagName || '').toLowerCase() === 'input') {
      e.preventDefault();
      return;
    }
    var li = t && t.closest ? t.closest('li') : null;
    if (!li || !ul.contains(li)) return;
    dragEl = li;
    try {
      e.dataTransfer.setData('text/plain', li.getAttribute('data-home-endpoint') || '');
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
    var order = [];
    ul.querySelectorAll('li[data-home-endpoint]').forEach(function(row) {
      order.push(row.getAttribute('data-home-endpoint'));
    });
    var cur = readHomeLayoutSettings();
    cur.order = sanitizeHomeLayoutOrder(order);
    writeHomeLayoutSettings(cur);
    applyHomeEndpointOrder(stack, cur.order);
  });
})();

/** Tab title animation while /results API fetch runs (address bar text cannot be changed by script). */
var _loadingTitleTimer = null;
var _resultReadyTitleTimer = null;
var _resultReadyNotified = false;
var _loadingTitleRestore = 'TF2 Log Searcher';
var PAGE_TITLE_SUFFIX = ' \u2014 TF2 Log Searcher';

function startLoadingTitleAnimation() {
  if (_loadingTitleTimer != null) {
    window.clearInterval(_loadingTitleTimer);
    _loadingTitleTimer = null;
  }
  _loadingTitleRestore = document.title || _loadingTitleRestore;
  var seq = ['Loading\u2026', 'Loading.', 'Loading..', 'Loading...'];
  var i = 0;
  document.title = seq[0] + PAGE_TITLE_SUFFIX;
  _loadingTitleTimer = window.setInterval(function() {
    i = (i + 1) % seq.length;
    document.title = seq[i] + PAGE_TITLE_SUFFIX;
  }, 450);
}

function stopResultReadyTitleFlash() {
  if (_resultReadyTitleTimer != null) {
    window.clearInterval(_resultReadyTitleTimer);
    _resultReadyTitleTimer = null;
  }
  _resultReadyNotified = false;
}

/**
 * If the /results response rendered successfully but the tab is in the background, flash
 * the title so the user notices; restore when the tab is visible again (visibility handler).
 */
function startResultReadyTitleFlash() {
  stopResultReadyTitleFlash();
  _resultReadyNotified = true;
  var tA = 'YOUR RESULT IS READY' + PAGE_TITLE_SUFFIX;
  var tB = '\u25B6 YOUR RESULT IS READY' + PAGE_TITLE_SUFFIX;
  var reducedMotion = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reducedMotion) {
    document.title = tA;
    return;
  }
  var phase = 0;
  document.title = tA;
  _resultReadyTitleTimer = window.setInterval(function() {
    phase = 1 - phase;
    document.title = phase ? tB : tA;
  }, 700);
}

(function initResultReadyTitleVisibility() {
  if (typeof document === 'undefined' || !document.addEventListener) {
    return;
  }
  document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
      return;
    }
    if (_resultReadyTitleTimer != null) {
      stopResultReadyTitleFlash();
      document.title = _loadingTitleRestore;
      return;
    }
    if (_resultReadyNotified) {
      _resultReadyNotified = false;
      document.title = _loadingTitleRestore;
    }
  });
})();

function stopLoadingTitleAnimation() {
  if (_loadingTitleTimer != null) {
    window.clearInterval(_loadingTitleTimer);
    _loadingTitleTimer = null;
  }
  stopResultReadyTitleFlash();
  document.title = _loadingTitleRestore;
  stopLoadingSnakeGame();
}

/**
 * Called from /results fetch finally: stop loading art; if the response was good and the tab
 * is hidden, flash \"YOUR RESULT IS READY\" until the user focuses this tab.
 */
function completeResultsPageTitle(loadSucceeded) {
  if (_loadingTitleTimer != null) {
    window.clearInterval(_loadingTitleTimer);
    _loadingTitleTimer = null;
  }
  stopLoadingSnakeGame();
  stopResultReadyTitleFlash();
  if (loadSucceeded && typeof document !== 'undefined' && document.hidden) {
    startResultReadyTitleFlash();
  } else {
    document.title = _loadingTitleRestore;
  }
}

/** Optional mini-game in place of loading dots; torn down when loading finishes (see ``stopLoadingTitleAnimation``). */
var _loadingSnakeCtl = null;

function stopLoadingSnakeGame() {
  if (_loadingSnakeCtl && typeof _loadingSnakeCtl.destroy === 'function') {
    try {
      _loadingSnakeCtl.destroy();
    } catch (e) {}
  }
  _loadingSnakeCtl = null;
}

function showResultsLoading(el) {
  stopLoadingSnakeGame();
  var reducedMotion = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (reducedMotion) {
    el.innerHTML = '<div class="loading-state" role="status" aria-live="polite"><span class="loading-label">Loading</span><span class="loading-dots" aria-hidden="true"><span>.</span><span>.</span><span>.</span></span></div>';
  } else {
    el.innerHTML = (
      '<div class="results-loading-stack" role="status" aria-live="polite">' +
      '<div class="loading-state loading-state--snake-hud">' +
      '<span class="loading-label">Loading</span>' +
      '<span class="loading-dots" aria-hidden="true"><span>.</span><span>.</span><span>.</span></span>' +
      '</div>' +
      '<canvas class="loading-snake-canvas" aria-hidden="true"></canvas>' +
      '</div>'
    );
    var wrap = el.querySelector('.results-loading-stack');
    var cv = el.querySelector('.loading-snake-canvas');
    if (wrap && cv) _loadingSnakeCtl = initLoadingSnakeMiniGame(wrap, cv);
  }
  startLoadingTitleAnimation();
}

/**
 * Snake in the results panel (below ``Back to search``); torn down when ``destroy`` runs.
 */
function initLoadingSnakeMiniGame(wrapEl, canvas) {
  var STEP_MS = 52;
  var doubleTapMs = 320;
  var doubleTapDist = 28;

  var ctx = canvas.getContext('2d', { alpha: false, desynchronized: true })
    || canvas.getContext('2d', { alpha: false });
  if (!ctx) {
    return {
      destroy: function() {},
    };
  }
  var dpr = typeof window.devicePixelRatio === 'number' && window.devicePixelRatio > 0 ? window.devicePixelRatio : 1;
  var GRID_W = 12;
  var GRID_H = 8;
  var CELL = 10;
  var cssW = 0;
  var cssH = 0;
  var ox = 0;
  var oy = 0;
  var swipeMin = 40;

  var started = false;
  var dir = 1;
  var nextDir = 1;
  var snake = [];
  var food = { x: 0, y: 0 };
  var raf = 0;
  var acc = 0;
  var lastTs = 0;
  var touchActive = false;
  var tx0 = 0;
  var ty0 = 0;
  var lastTap = 0;
  var lastTapX = 0;
  var lastTapY = 0;
  var alive = true;

  function updateLayout() {
    cssW = wrapEl.clientWidth || window.innerWidth || 300;
    cssH = wrapEl.clientHeight || window.innerHeight || 200;
    var tc = 10;
    GRID_W = Math.max(14, Math.floor(cssW / tc));
    GRID_H = Math.max(12, Math.floor(cssH / tc));
    CELL = Math.min(cssW / GRID_W, cssH / GRID_H);
    ox = (cssW - GRID_W * CELL) * 0.5;
    oy = (cssH - GRID_H * CELL) * 0.5;
    swipeMin = Math.max(36, Math.min(cssW, cssH) * 0.045);
    canvas.width = Math.max(1, Math.round(cssW * dpr));
    canvas.height = Math.max(1, Math.round(cssH * dpr));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  function randFood() {
    var taken = {};
    var i;
    for (i = 0; i < snake.length; i++) {
      taken[snake[i].x + ',' + snake[i].y] = 1;
    }
    var opts = [];
    for (var y = 0; y < GRID_H; y++) {
      for (var x = 0; x < GRID_W; x++) {
        if (!taken[x + ',' + y]) opts.push({ x: x, y: y });
      }
    }
    if (!opts.length) return;
    food = opts[Math.floor(Math.random() * opts.length)];
  }

  function resetGame() {
    var mx = (GRID_W / 2) | 0;
    var my = (GRID_H / 2) | 0;
    snake = [{ x: mx + 1, y: my }, { x: mx, y: my }, { x: mx - 1, y: my }];
    dir = 1;
    nextDir = 1;
    randFood();
  }

  function drawFrame() {
    var bg = typeof getComputedStyle !== 'undefined'
      ? (getComputedStyle(document.documentElement).getPropertyValue('--bg').trim()
        || '#1a1a1a')
      : '#1a1a1a';
    var fg = typeof getComputedStyle !== 'undefined'
      ? (getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#eee')
      : '#eee';
    var fd = typeof getComputedStyle !== 'undefined'
      ? (getComputedStyle(document.documentElement).getPropertyValue('--link').trim()
        || getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim()
        || '#888')
      : '#888';
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, cssW, cssH);
    if (!started) {
      return;
    }
    var pad = 0.5;
    var inner = CELL - 1;
    var i;
    ctx.fillStyle = fd;
    ctx.globalAlpha = 1;
    ctx.fillRect(ox + food.x * CELL + pad, oy + food.y * CELL + pad, inner, inner);
    for (i = 0; i < snake.length; i++) {
      ctx.fillStyle = fg;
      ctx.globalAlpha = i === 0 ? 1 : 0.88;
      ctx.fillRect(ox + snake[i].x * CELL + pad, oy + snake[i].y * CELL + pad, inner, inner);
    }
    ctx.globalAlpha = 1;
  }

  function step() {
    dir = nextDir;
    var head = snake[0];
    var nx = head.x + (dir === 1 ? 1 : dir === 3 ? -1 : 0);
    var ny = head.y + (dir === 0 ? -1 : dir === 2 ? 1 : 0);
    if (nx < 0) nx = GRID_W - 1;
    else if (nx >= GRID_W) nx = 0;
    if (ny < 0) ny = GRID_H - 1;
    else if (ny >= GRID_H) ny = 0;
    var j;
    for (j = 0; j < snake.length; j++) {
      if (snake[j].x === nx && snake[j].y === ny) {
        resetGame();
        return;
      }
    }
    snake.unshift({ x: nx, y: ny });
    if (nx === food.x && ny === food.y) {
      randFood();
    } else {
      snake.pop();
    }
  }

  function loop(ts) {
    if (!alive) return;
    if (!lastTs) lastTs = ts;
    if (started) {
      acc += ts - lastTs;
      lastTs = ts;
      while (acc >= STEP_MS) {
        acc -= STEP_MS;
        step();
      }
    } else {
      lastTs = ts;
    }
    drawFrame();
    if (alive) raf = window.requestAnimationFrame(loop);
  }

  function setDirFromDelta(dx, dy) {
    if (!started) return;
    var adx = Math.abs(dx);
    var ady = Math.abs(dy);
    if (adx < swipeMin && ady < swipeMin) return;
    if (adx > ady) {
      if (dx > 0 && dir !== 3) nextDir = 1;
      else if (dx < 0 && dir !== 1) nextDir = 3;
    } else {
      if (dy > 0 && dir !== 0) nextDir = 2;
      else if (dy < 0 && dir !== 2) nextDir = 0;
    }
  }

  function onKeyDown(ev) {
    if (!started) return;
    var t = ev.target;
    if (t && (t.closest && (t.closest('input, textarea, select, [contenteditable="true"]')))) return;
    var k = ev.key;
    var nd = nextDir;
    if (k === 'ArrowUp' || k === 'w' || k === 'W') nd = 0;
    else if (k === 'ArrowRight' || k === 'd' || k === 'D') nd = 1;
    else if (k === 'ArrowDown' || k === 's' || k === 'S') nd = 2;
    else if (k === 'ArrowLeft' || k === 'a' || k === 'A') nd = 3;
    else return;
    if (nd === 0 && dir === 2) return;
    if (nd === 2 && dir === 0) return;
    if (nd === 1 && dir === 3) return;
    if (nd === 3 && dir === 1) return;
    ev.preventDefault();
    ev.stopPropagation();
    nextDir = nd;
  }

  function startGame() {
    if (started) return;
    started = true;
    acc = 0;
    lastTs = 0;
    resetGame();
  }

  function onKeyDownBeforeStart(ev) {
    if (started) return;
    var t = ev.target;
    if (t && (t.closest && (t.closest('input, textarea, select, [contenteditable="true"]')))) return;
    var k = ev.key;
    if (k === 'ArrowUp' || k === 'ArrowDown' || k === 'ArrowLeft' || k === 'ArrowRight'
      || k === 'w' || k === 'W' || k === 'a' || k === 'A' || k === 's' || k === 'S' || k === 'd' || k === 'D') {
      ev.preventDefault();
      startGame();
    }
  }

  function onTouchStart(ev) {
    if (ev.touches.length !== 1) return;
    touchActive = true;
    tx0 = ev.touches[0].clientX;
    ty0 = ev.touches[0].clientY;
  }

  function onTouchEnd(ev) {
    if (!touchActive) return;
    touchActive = false;
    var ch = ev.changedTouches && ev.changedTouches[0];
    if (!ch) return;
    var x = ch.clientX;
    var y = ch.clientY;
    if (started) {
      setDirFromDelta(x - tx0, y - ty0);
      return;
    }
    var now = typeof performance !== 'undefined' ? performance.now() : Date.now();
    var dt = lastTap > 0 ? now - lastTap : doubleTapMs + 1;
    var dx = x - lastTapX;
    var dy = y - lastTapY;
    var dist = Math.sqrt(dx * dx + dy * dy);
    if (lastTap > 0 && dt < doubleTapMs && dt > 20 && dist < doubleTapDist) {
      if (ev.cancelable) ev.preventDefault();
      startGame();
    }
    lastTap = now;
    lastTapX = x;
    lastTapY = y;
  }

  function onTouchMove(ev) {
    if (!started) return;
    if (ev.cancelable) ev.preventDefault();
  }

  function onTouchCancel() {
    touchActive = false;
  }

  function onResize() {
    if (!alive) return;
    updateLayout();
    if (started) resetGame();
  }

  var touchOptPassive = { passive: true };
  var touchOptActive = { passive: false };

  updateLayout();
  window.addEventListener('resize', onResize, false);
  window.addEventListener('keydown', onKeyDownBeforeStart, false);
  window.addEventListener('keydown', onKeyDown, false);
  wrapEl.addEventListener('touchstart', onTouchStart, touchOptPassive);
  wrapEl.addEventListener('touchend', onTouchEnd, touchOptActive);
  wrapEl.addEventListener('touchcancel', onTouchCancel, touchOptPassive);
  wrapEl.addEventListener('touchmove', onTouchMove, touchOptActive);

  raf = window.requestAnimationFrame(loop);

  return {
    destroy: function() {
      alive = false;
      window.removeEventListener('resize', onResize, false);
      window.removeEventListener('keydown', onKeyDownBeforeStart, false);
      window.removeEventListener('keydown', onKeyDown, false);
      wrapEl.removeEventListener('touchstart', onTouchStart, touchOptPassive);
      wrapEl.removeEventListener('touchend', onTouchEnd, touchOptActive);
      wrapEl.removeEventListener('touchcancel', onTouchCancel, touchOptPassive);
      wrapEl.removeEventListener('touchmove', onTouchMove, touchOptActive);
      if (raf) cancelAnimationFrame(raf);
      raf = 0;
      started = false;
    },
  };
}

document.getElementById('frmChat').addEventListener('submit', function(e) {
  e.preventDefault();
  const form = e.target;
  var word = (form.word.value || '').trim();
  var steamid = (form.steamid.value || '').trim();
  var mapQuery = sanitizeMapQueryInput(form.map_query && form.map_query.value ? form.map_query.value : '');
  var dateFrom = (form.date_from && form.date_from.value) ? String(form.date_from.value).trim() : '';
  var dateTo = (form.date_to && form.date_to.value) ? String(form.date_to.value).trim() : '';
  if (word.length > 200) {
    document.getElementById('chatResults').innerHTML = '<span class="error">Search word is too long.</span>';
    return;
  }
  if (!steamid && word.length < 3) {
    document.getElementById('chatResults').innerHTML = '<span class="error">When Steam ID is empty, search word must be at least 3 characters.</span>';
    return;
  }
  if (dateFrom && dateTo && dateFrom > dateTo) {
    document.getElementById('chatResults').innerHTML = '<span class="error">Date from must be before or equal to date to.</span>';
    return;
  }
  var chatParams = new URLSearchParams({ steamid: steamid, word: word, date_from: dateFrom, date_to: dateTo, map_query: mapQuery });
  var chatState = buildSanitizedSearchState('chat', chatParams);
  if (chatState) persistSearchState(chatState);
  var params = new URLSearchParams({ mode: 'chat', steamid: steamid, word: word, date_from: dateFrom, date_to: dateTo, map_query: mapQuery });
  window.location.href = '/results?' + params.toString();
});

document.getElementById('frmLogmatch').addEventListener('submit', function(e) {
  e.preventDefault();
  var steamids = (e.target.steamids.value || '').trim();
  var mapQuery = sanitizeMapQueryInput(e.target.map_query && e.target.map_query.value ? e.target.map_query.value : '');
  if (!steamids) {
    document.getElementById('logmatchResults').innerHTML = '<span class="error">At least one Steam ID is required.</span>';
    return;
  }
  var lmParams = new URLSearchParams({ steamids: steamids, map_query: mapQuery });
  var lmState = buildSanitizedSearchState('logmatch', lmParams);
  if (lmState) persistSearchState(lmState);
  var params = new URLSearchParams({ mode: 'logmatch', steamids: steamids, map_query: mapQuery });
  window.location.href = '/results?' + params.toString();
});

document.getElementById('frmStats').addEventListener('submit', function(e) {
  e.preventDefault();
  const form = e.target;
  const classes = Array.from(form.querySelectorAll('input[name="classes"]:checked')).map(function(c) { return c.value; }).join(',');
  var steamid = (form.steamid.value || '').trim();
  var mapQuery = sanitizeMapQueryInput(form.map_query && form.map_query.value ? form.map_query.value : '');
  var dateFrom = (form.date_from && form.date_from.value) ? String(form.date_from.value).trim() : '';
  var dateTo = (form.date_to && form.date_to.value) ? String(form.date_to.value).trim() : '';
  if (!steamid) {
    document.getElementById('statsResults').innerHTML = '<span class="error">Steam ID is required.</span>';
    return;
  }
  if (dateFrom && dateTo && dateFrom > dateTo) {
    document.getElementById('statsResults').innerHTML = '<span class="error">Date from must be before or equal to date to.</span>';
    return;
  }
  var statsParams = new URLSearchParams({
    steamid: steamid,
    gamemode: form.gamemode.value || 'hl',
    classes: classes,
    date_from: dateFrom,
    date_to: dateTo,
    map_query: mapQuery,
  });
  var statsState = buildSanitizedSearchState('stats', statsParams);
  if (statsState) persistSearchState(statsState);
  var params = new URLSearchParams({
    mode: 'stats',
    steamid: steamid,
    gamemode: form.gamemode.value || 'hl',
    classes: classes,
    date_from: dateFrom,
    date_to: dateTo,
    map_query: mapQuery,
  });
  window.location.href = '/results?' + params.toString();
});

document.getElementById('frmCoplayers').addEventListener('submit', function(e) {
  e.preventDefault();
  var form = e.target;
  var steamid = (form.steamid && form.steamid.value ? form.steamid.value : '').trim();
  var mapQuery = sanitizeMapQueryInput(form.map_query && form.map_query.value ? form.map_query.value : '');
  var gamemode = sanitizeCoplayersGamemodeInput(form.gamemode && form.gamemode.value ? form.gamemode.value : '');
  if (!steamid) {
    document.getElementById('coplayers-results').innerHTML = '<span class="error">Steam ID is required.</span>';
    return;
  }
  var cpParams = new URLSearchParams({
    steamid: steamid,
    gamemode: gamemode,
    map_query: mapQuery,
  });
  var cpState = buildSanitizedSearchState('coplayers', cpParams);
  if (cpState) persistSearchState(cpState);
  var params = new URLSearchParams({
    mode: 'coplayers',
    steamid: steamid,
    gamemode: gamemode,
    map_query: mapQuery,
  });
  window.location.href = '/results?' + params.toString();
});

document.getElementById('frmProfile').addEventListener('submit', function(e) {
  e.preventDefault();
  var form = e.target;
  var steamid = (form.steamid && form.steamid.value ? form.steamid.value : '').trim();
  var mapQuery = sanitizeMapQueryInput(form.map_query && form.map_query.value ? form.map_query.value : '');
  var gamemode = sanitizeCoplayersGamemodeInput(form.gamemode && form.gamemode.value ? form.gamemode.value : '');
  var dateFrom = (form.date_from && form.date_from.value) ? String(form.date_from.value).trim() : '';
  var dateTo = (form.date_to && form.date_to.value) ? String(form.date_to.value).trim() : '';
  if (!steamid) {
    document.getElementById('profileResults').innerHTML = '<span class="error">Steam ID is required.</span>';
    return;
  }
  if (dateFrom && dateTo && dateFrom > dateTo) {
    document.getElementById('profileResults').innerHTML = '<span class="error">Date from must be before or equal to date to.</span>';
    return;
  }
  var profParams = new URLSearchParams({
    steamid: steamid,
    gamemode: gamemode,
    date_from: dateFrom,
    date_to: dateTo,
    map_query: mapQuery,
  });
  var profState = buildSanitizedSearchState('profile', profParams);
  if (profState) persistSearchState(profState);
  var params = new URLSearchParams({
    mode: 'profile',
    steamid: steamid,
    gamemode: gamemode,
    date_from: dateFrom,
    date_to: dateTo,
    map_query: mapQuery,
  });
  window.location.href = '/results?' + params.toString();
});

document.getElementById('frmPlayerName').addEventListener('submit', function(e) {
  e.preventDefault();
  var form = e.target;
  var q = (form.q && form.q.value ? String(form.q.value) : '').trim();
  q = q.replace(/[\u0000-\u001F\u007F]/g, '');
  if (q.length > MAX_PLAYER_NAME_QUERY_LEN) q = q.slice(0, MAX_PLAYER_NAME_QUERY_LEN);
  if (q.length < MIN_PLAYER_NAME_QUERY_LEN) {
    document.getElementById('playerNameResults').innerHTML = '<span class="error">Enter at least ' + MIN_PLAYER_NAME_QUERY_LEN + ' characters.</span>';
    return;
  }
  var pnParams = new URLSearchParams({ q: q });
  var pnState = buildSanitizedSearchState('playername', pnParams);
  if (pnState) persistSearchState(pnState);
  var params = new URLSearchParams({ mode: 'playername', q: q });
  window.location.href = '/results?' + params.toString();
});

(function initLeaderboardTypeStrip() {
  var form = document.getElementById('frmLeaderboard');
  if (!form) return;
  form.querySelectorAll('.js-lb-type-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var t = btn.getAttribute('data-lb-type');
      if (!t) return;
      t = sanitizeLbTypeInput(t);
      if (form.elements.lb_type) form.elements.lb_type.value = t;
      form.querySelectorAll('.js-lb-type-btn').forEach(function(b) {
        var on = b.getAttribute('data-lb-type') === t;
        b.classList.toggle('active', on);
        b.setAttribute('aria-selected', on ? 'true' : 'false');
      });
      syncLeaderboardClassSelectForMedicLeaderboards(form);
      syncLeaderboardStatScopeStrip(form);
    });
  });
  syncLeaderboardClassSelectForMedicLeaderboards(form);
  syncLeaderboardStatScopeStrip(form);
})();

(function initLeaderboardStatScopeStrips() {
  var form = document.getElementById('frmLeaderboard');
  if (!form) return;
  function bindScopeButtons(btnSelector, stripSelector) {
    form.querySelectorAll(btnSelector).forEach(function(btn) {
      btn.addEventListener('click', function() {
        var sc = btn.getAttribute('data-stat-scope');
        if (!sc) return;
        var lb = sanitizeLbTypeInput(form.elements.lb_type && form.elements.lb_type.value ? form.elements.lb_type.value : 'dpm');
        sc = sanitizeLeaderboardStatScopeInput(sc, lb);
        if (form.elements.stat_scope) form.elements.stat_scope.value = sc;
        var strip = form.querySelector(stripSelector);
        if (strip) {
          strip.querySelectorAll(btnSelector).forEach(function(b) {
            var on = b.getAttribute('data-stat-scope') === sc;
            b.classList.toggle('active', on);
            b.setAttribute('aria-selected', on ? 'true' : 'false');
          });
        }
      });
    });
  }
  bindScopeButtons('.js-lb-stat-scope-btn', '.js-lb-stat-scope-strip');
  bindScopeButtons('.js-lb-winrate-scope-btn', '.js-lb-winrate-scope-strip');
})();

var frmLb = document.getElementById('frmLeaderboard');
if (frmLb) {
  frmLb.addEventListener('submit', function(e) {
    e.preventDefault();
    var mapQuery = sanitizeMapQueryInput(frmLb.elements.map_query && frmLb.elements.map_query.value ? frmLb.elements.map_query.value : '');
    var gamemode = sanitizeCoplayersGamemodeInput(frmLb.gamemode && frmLb.gamemode.value ? frmLb.gamemode.value : '');
    var dateFrom = (frmLb.elements.date_from && frmLb.elements.date_from.value) ? String(frmLb.elements.date_from.value).trim() : '';
    var dateTo = (frmLb.elements.date_to && frmLb.elements.date_to.value) ? String(frmLb.elements.date_to.value).trim() : '';
    if (dateFrom && dateTo && dateFrom > dateTo) {
      document.getElementById('leaderboardResults').innerHTML = '<span class="error">Date from must be before or equal to date to.</span>';
      return;
    }
    var lbForClass = sanitizeLbTypeInput(frmLb.elements.lb_type && frmLb.elements.lb_type.value ? frmLb.elements.lb_type.value : 'dpm');
    var statScopeVal = sanitizeLeaderboardStatScopeInput(
      frmLb.elements.stat_scope && frmLb.elements.stat_scope.value ? frmLb.elements.stat_scope.value : (lbForClass === 'winrate' ? 'highest' : 'total'),
      lbForClass
    );
    var classFilterVal = sanitizeLeaderboardClassFilter(
      frmLb.elements.class_filter && frmLb.elements.class_filter.value ? frmLb.elements.class_filter.value : '',
      lbForClass
    );
    var lbParams = new URLSearchParams({
      lb_type: lbForClass,
      stat_scope: statScopeVal,
      gamemode: gamemode,
      class_filter: classFilterVal,
      map_query: mapQuery,
      date_from: dateFrom,
      date_to: dateTo,
      min_logs: sanitizeLeaderboardMinLogs(frmLb.elements.min_logs && frmLb.elements.min_logs.value ? frmLb.elements.min_logs.value : '10'),
    });
    var lbState = buildSanitizedSearchState('leaderboard', lbParams);
    if (lbState) persistSearchState(lbState);
    var params = new URLSearchParams({
      mode: 'leaderboard',
      lb_type: lbForClass,
      stat_scope: statScopeVal,
      gamemode: gamemode,
      class_filter: classFilterVal,
      map_query: mapQuery,
      date_from: dateFrom,
      date_to: dateTo,
      min_logs: sanitizeLeaderboardMinLogs(frmLb.elements.min_logs && frmLb.elements.min_logs.value ? frmLb.elements.min_logs.value : '10'),
    });
    window.location.href = '/results?' + params.toString();
  });
}

(function initResultsPage() {
  var pathname = window.location.pathname;
  if (pathname !== '/results' && pathname !== '/results/') return;
  var params = new URLSearchParams(window.location.search);
  var mode = params.get('mode');
  var homePage = document.getElementById('homePage');
  var resultsPage = document.getElementById('resultsPage');
  var resultsContent = document.getElementById('resultsContent');
  if (!homePage || !resultsPage || !resultsContent) return;
  homePage.style.display = 'none';
  resultsPage.style.display = 'block';
  if (!mode) {
    resultsContent.innerHTML = '<span class="error">Missing search parameters.</span>';
    return;
  }
  var persisted = buildSanitizedSearchState(mode, params);
  if (persisted) persistSearchState(persisted);
  var t0 = performance.now();
  showResultsLoading(resultsContent);
  var apiUrl = '';
  if (mode === 'chat') {
    apiUrl = '/api/search/chat?' + new URLSearchParams({
      steamid: params.get('steamid') || '',
      word: params.get('word') || '',
      date_from: params.get('date_from') || '',
      date_to: params.get('date_to') || '',
      map_query: params.get('map_query') || '',
    }).toString();
  } else if (mode === 'stats') {
    apiUrl = '/api/search/stats?' + new URLSearchParams({
      steamid: params.get('steamid') || '',
      gamemode: params.get('gamemode') || 'hl',
      classes: params.get('classes') || '',
      date_from: params.get('date_from') || '',
      date_to: params.get('date_to') || '',
      map_query: params.get('map_query') || '',
    }).toString();
  } else if (mode === 'logmatch') {
    apiUrl = '/api/search/logmatch?' + new URLSearchParams({
      steamids: params.get('steamids') || '',
      map_query: params.get('map_query') || '',
    }).toString();
  } else if (mode === 'coplayers') {
    var cpGm = sanitizeCoplayersGamemodeInput(params.get('gamemode'));
    apiUrl = '/api/search/coplayers?' + new URLSearchParams({
      steamid: params.get('steamid') || '',
      gamemode: cpGm,
      map_query: sanitizeMapQueryInput(params.get('map_query') || ''),
    }).toString();
  } else if (mode === 'profile') {
    var prGm = sanitizeCoplayersGamemodeInput(params.get('gamemode'));
    apiUrl = '/api/player/profile?' + new URLSearchParams({
      steamid: params.get('steamid') || '',
      gamemode: prGm,
      date_from: params.get('date_from') || '',
      date_to: params.get('date_to') || '',
      map_query: sanitizeMapQueryInput(params.get('map_query') || ''),
    }).toString();
  } else if (mode === 'leaderboard') {
    var lbApi = sanitizeLbTypeInput(params.get('lb_type'));
    var ssApi = sanitizeLeaderboardStatScopeInput(params.get('stat_scope'), lbApi);
    apiUrl = '/api/leaderboard?' + new URLSearchParams({
      lb_type: lbApi,
      stat_scope: ssApi,
      gamemode: sanitizeCoplayersGamemodeInput(params.get('gamemode')),
      class_filter: sanitizeLeaderboardClassFilter(params.get('class_filter'), lbApi),
      map_query: sanitizeMapQueryInput(params.get('map_query') || ''),
      date_from: params.get('date_from') || '',
      date_to: params.get('date_to') || '',
      min_logs: sanitizeLeaderboardMinLogs(params.get('min_logs')),
    }).toString();
  } else if (mode === 'playername') {
    var pnQ = (params.get('q') || '').trim().replace(/[\u0000-\u001F\u007F]/g, '');
    if (pnQ.length > MAX_PLAYER_NAME_QUERY_LEN) pnQ = pnQ.slice(0, MAX_PLAYER_NAME_QUERY_LEN);
    if (pnQ.length < MIN_PLAYER_NAME_QUERY_LEN) {
      resultsContent.innerHTML = '<span class="error">Enter a name query of at least ' + MIN_PLAYER_NAME_QUERY_LEN + ' characters.</span>';
      stopLoadingTitleAnimation();
      return;
    }
    apiUrl = '/api/search/player-name?' + new URLSearchParams({ q: pnQ }).toString();
  } else {
    resultsContent.innerHTML = '<span class="error">Unknown search mode.</span>';
    stopLoadingTitleAnimation();
    return;
  }
  var resultsLoadSucceeded = false;
  fetch(apiUrl)
    .then(function(r) {
      return r.text().then(function(text) {
        return { r: r, text: text };
      });
    })
    .then(function(bundle) {
      var ms = performance.now() - t0;
      var r = bundle.r;
      var text = bundle.text || '';
      if (!r.ok) {
        var errParsed = null;
        try {
          errParsed = JSON.parse(text);
        } catch (e) {}
        if (mode === 'profile' && r.status === 404) {
          resultsContent.innerHTML = '<p class="stats-summary-meta">' + escapeHtml('No profile data available for this player. Run the stats backfill to populate the stats database.') + '</p>' + requestTimingFooter(ms);
          return;
        }
        if (errParsed && errParsed.error) {
          resultsContent.innerHTML = '<span class="error">' + escapeHtml(String(errParsed.error)) + '</span>' + requestTimingFooter(ms);
          return;
        }
        var extra = '';
        if (r.status === 524) {
          extra = ' Origin timeout (e.g. Cloudflare) — the server took too long to respond. If player name search is still indexing, wait for the downloader log "Alias FTS: rebuild finished" or use a 3+ character query only after indexing completes.';
        } else if (r.status === 504 || r.status === 502) {
          extra = ' Gateway or upstream timeout — try again or use a shorter or more specific name.';
        } else if (r.status === 503) {
          extra = ' Service temporarily unavailable — often the chat database is locked or the name index is still building; retry shortly.';
        }
        resultsContent.innerHTML = '<span class="error">HTTP ' + escapeHtml(String(r.status)) + escapeHtml(extra) + '</span>' + requestTimingFooter(ms);
        return;
      }
      var ct = (r.headers.get('content-type') || '').toLowerCase();
      if (ct.indexOf('application/json') < 0 && text.trim().charAt(0) !== '{') {
        resultsContent.innerHTML = '<span class="error">The server returned a non-JSON page (often a proxy timeout). Try again in a moment.</span>' + requestTimingFooter(ms);
        return;
      }
      var data;
      try {
        data = JSON.parse(text);
      } catch (e) {
        resultsContent.innerHTML = '<span class="error">Invalid response from server (expected JSON).</span>' + requestTimingFooter(ms);
        return;
      }
      if (data.error) {
        resultsContent.innerHTML = '<span class="error">' + escapeHtml(data.error) + '</span>' + requestTimingFooter(ms);
        return;
      }
      resultsLoadSucceeded = true;
      if (mode === 'chat') {
        if (data && data.leaderboard) {
          renderChatLeaderboard(resultsContent, data, params.get('word'), ms);
        } else {
          renderChatResult(resultsContent, data, params.get('steamid'), params.get('word'), true, ms);
        }
      } else if (mode === 'stats') {
        var rows = data.rows || [];
        if (rows.length === 0) {
          resultsContent.innerHTML = 'No rows.' + requestTimingFooter(ms);
        } else {
          renderStatsTable(resultsContent, rows, ms);
        }
      } else if (mode === 'logmatch') {
        renderLogmatchResult(resultsContent, data, ms);
      } else if (mode === 'coplayers') {
        var cprows = data.rows || [];
        if (cprows.length === 0) {
          var emptyMsg = 'No co-players found.';
          if (data.logs_searched != null && Number.isFinite(Number(data.logs_searched))) {
            emptyMsg = coplayersSummaryLine(0, data.logs_searched);
          }
          resultsContent.innerHTML = '<p class="stats-summary-meta">' + escapeHtml(emptyMsg) + '</p>' + requestTimingFooter(ms);
        } else {
          var rs = data.resolved_steamid64 != null ? String(data.resolved_steamid64).trim() : '';
          var paramSid = (params.get('steamid') || '').trim();
          var searchedForCoplayers = /^\d{17}$/.test(rs) ? rs : (/^\d{17}$/.test(paramSid) ? paramSid : undefined);
          renderCoplayers(resultsContent, data, ms, searchedForCoplayers);
        }
      } else if (mode === 'profile') {
        renderProfileResult(resultsContent, data, ms);
      } else if (mode === 'leaderboard') {
        renderLeaderboard(resultsContent, data, ms);
      } else if (mode === 'playername') {
        var qShown = (params.get('q') || '').trim();
        if (qShown.length > MAX_PLAYER_NAME_QUERY_LEN) qShown = qShown.slice(0, MAX_PLAYER_NAME_QUERY_LEN);
        renderPlayerNameResult(resultsContent, data, qShown, ms);
      }
    })
    .catch(function(err) {
      resultsLoadSucceeded = false;
      var ms = performance.now() - t0;
      var msg = err && err.message ? String(err.message) : 'Request failed.';
      resultsContent.innerHTML = '<span class="error">' + escapeHtml(msg) + '</span>' + requestTimingFooter(ms);
    })
    .finally(function() {
      completeResultsPageTitle(resultsLoadSucceeded);
    });
})();
