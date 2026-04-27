
function fmtProgressNum(n) {
  if (n == null || typeof n !== 'number' || !Number.isFinite(n)) return '\u2014';
  try {
    return Math.trunc(n).toLocaleString();
  } catch (_) {
    return String(Math.trunc(n));
  }
}
var VALID_LB_TYPES = {
  dpm: 1,
  kdr: 1,
  winrate: 1,
  logs: 1,
  ubers: 1,
  drops: 1,
  damage_taken: 1,
  avg_deaths: 1,
};
function sanitizeLbTypeInput(v) {
  var s = (v == null ? '' : String(v)).trim().toLowerCase();
  return VALID_LB_TYPES[s] ? s : 'dpm';
}
function sanitizeLeaderboardClassFilter(v, lbType) {
  var s = (v == null ? '' : String(v)).trim().toLowerCase();
  if (!s) return '';
  if (!VALID_STATS_CLASSES[s]) return '';
  var t = arguments.length >= 2 ? sanitizeLbTypeInput(lbType) : 'dpm';
  if ((t === 'ubers' || t === 'drops') && s !== 'medic') return '';
  return s;
}

/** Disable non-Medic class options when Most Ubers / Most Drops is selected; reset invalid selection. */
function syncLeaderboardClassSelectForMedicLeaderboards(form) {
  if (!form || !form.elements.class_filter) return;
  var lb = sanitizeLbTypeInput(form.elements.lb_type && form.elements.lb_type.value ? form.elements.lb_type.value : 'dpm');
  var medicOnly = (lb === 'ubers' || lb === 'drops');
  var sel = form.elements.class_filter;
  var opts = sel.querySelectorAll('option');
  for (var i = 0; i < opts.length; i++) {
    var o = opts[i];
    var val = (o.value || '').trim().toLowerCase();
    if (!val) {
      o.disabled = false;
      continue;
    }
    o.disabled = medicOnly && val !== 'medic';
  }
  if (medicOnly) {
    var cur = (sel.value || '').trim().toLowerCase();
    if (cur && cur !== 'medic') sel.value = '';
  }
}

function sanitizeLeaderboardStatScopeInput(raw, lbType) {
  var s = (raw == null ? '' : String(raw)).trim().toLowerCase();
  var t = sanitizeLbTypeInput(lbType || 'dpm');
  if (t === 'ubers' || t === 'drops' || t === 'damage_taken') {
    if (s === 'total' || s === 'per_log') return s;
    return 'total';
  }
  if (t === 'winrate') {
    if (s === 'highest' || s === 'lowest') return s;
    return 'highest';
  }
  return 'total';
}

function escapeHtml(str) {
  if (str == null) return '';
  const s = String(str);
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/** Escape for double-quoted HTML attributes (e.g. title=""). */
function escapeAttr(str) {
  if (str == null) return '';
  return String(str).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

var _avatarCache = {};

function steamAvatarPlaceholder(steamid64) {
  if (!steamid64 || !/^\d{17}$/.test(String(steamid64))) return '';
  return '<span class="steam-avatar-wrap" data-steamid64="' + escapeAttr(steamid64) + '"></span>';
}

/** Same-app profile URL for player name links (replaces external Steam / logs.tf profile links). */
function internalProfileHref(steamid64) {
  var s = String(steamid64 || '').trim();
  return /^\d{17}$/.test(s) ? ('/?mode=profile&steamid=' + encodeURIComponent(s)) : '';
}

function loadAvatarsInContainer(container) {
  if (!container || !container.querySelectorAll) return;
  var wraps = container.querySelectorAll('.steam-avatar-wrap[data-steamid64]:not([data-avatar-loaded])');
  var bySid = {};
  for (var i = 0; i < wraps.length; i++) {
    var w = wraps[i];
    var sid = w.getAttribute('data-steamid64');
    if (!sid || !/^\d{17}$/.test(sid)) {
      w.setAttribute('data-avatar-loaded', '1');
      continue;
    }
    if (!bySid[sid]) bySid[sid] = [];
    bySid[sid].push(w);
  }
  var sids = Object.keys(bySid);
  if (!sids.length) return;

  function applySid(sid, url) {
    var list = bySid[sid];
    if (!list) return;
    for (var j = 0; j < list.length; j++) {
      var elw = list[j];
      if (url) {
        elw.innerHTML = '<img src="' + escapeAttr(url) + '" width="24" height="24" alt="" loading="lazy" style="border-radius:4px;vertical-align:middle;margin-right:0.35em" onerror="this.style.display=\'none\'">';
      }
      elw.setAttribute('data-avatar-loaded', '1');
    }
  }

  var uncached = [];
  sids.forEach(function(sid2) {
    if (Object.prototype.hasOwnProperty.call(_avatarCache, sid2)) {
      applySid(sid2, _avatarCache[sid2]);
    } else {
      uncached.push(sid2);
    }
  });

  if (!uncached.length) return;

  var AVATAR_BATCH_MAX = 100;
  var promises = [];
  for (var c = 0; c < uncached.length; c += AVATAR_BATCH_MAX) {
    (function(chunk) {
      var sp = new URLSearchParams();
      sp.set('steamids', chunk.join(','));
      promises.push(
        fetch('/api/avatars/batch?' + sp.toString())
          .then(function(r) { return r.json(); })
          .then(function(data) {
            var map = data && data.avatars && typeof data.avatars === 'object' ? data.avatars : {};
            chunk.forEach(function(sid) {
              var url = Object.prototype.hasOwnProperty.call(map, sid) ? map[sid] : null;
              if (url != null && typeof url !== 'string') url = null;
              _avatarCache[sid] = url;
              applySid(sid, url);
            });
          })
          .catch(function() {
            chunk.forEach(function(sid) {
              _avatarCache[sid] = null;
              applySid(sid, null);
            });
          })
      );
    })(uncached.slice(c, c + AVATAR_BATCH_MAX));
  }
  return Promise.all(promises);
}

function logmatchAliasTooltip(row) {
  var q = row.search_input != null ? String(row.search_input) : '';
  var sid = row.resolved_steamid64 != null ? String(row.resolved_steamid64) : '';
  if (q && sid) return 'Searched: ' + q + ' · ' + sid;
  if (q) return q;
  if (sid) return 'SteamID64: ' + sid;
  return '';
}

/** Minutes:seconds for class time tooltips (m:ss). */
function formatClassTimeMinSec(totalSec) {
  var n = Math.max(0, Math.floor(Number(totalSec) || 0));
  var m = Math.floor(n / 60);
  var s = n % 60;
  return m + ':' + (s < 10 ? '0' : '') + s;
}

var LOGMATCH_CLASS_ICON = {
  scout: '/static/class_scout.png',
  soldier: '/static/class_soldier.png',
  pyro: '/static/class_pyro.png',
  demoman: '/static/class_demoman.png',
  heavyweapons: '/static/class_heavy.png',
  engineer: '/static/class_engineer.png',
  medic: '/static/class_medic.png',
  sniper: '/static/class_sniper.png',
  spy: '/static/class_spy.png'
};
var LOGMATCH_CLASS_LABEL = {
  scout: 'Scout',
  soldier: 'Soldier',
  pyro: 'Pyro',
  demoman: 'Demoman',
  heavyweapons: 'Heavy',
  engineer: 'Engineer',
  medic: 'Medic',
  sniper: 'Sniper',
  spy: 'Spy'
};

function logmatchClassIconsHtml(row) {
  var arr = row.class_playtime;
  if (!arr || !Array.isArray(arr) || arr.length === 0) return '';
  var maxSec = 0;
  for (var i = 0; i < arr.length; i++) {
    var t = Number(arr[i].seconds);
    if (!Number.isNaN(t) && t > maxSec) maxSec = t;
  }
  var parts = [];
  for (var j = 0; j < arr.length; j++) {
    var p = arr[j];
    var cid = p && p.class;
    var src = cid && LOGMATCH_CLASS_ICON[cid];
    if (!src) continue;
    var sec = Math.max(0, Math.floor(Number(p.seconds) || 0));
    var opacity = maxSec > 0 ? (0.18 + 0.82 * (sec / maxSec)) : 1;
    if (opacity > 1) opacity = 1;
    if (opacity < 0.12) opacity = 0.12;
    var label = LOGMATCH_CLASS_LABEL[cid] || cid;
    var tip = label + ' — ' + formatClassTimeMinSec(sec) + ' (min:sec)';
    parts.push(
      '<img class="logmatch-class-icon has-tooltip" src="' + src + '" alt="" width="22" height="22" loading="lazy" ' +
      'style="opacity:' + opacity.toFixed(3) + '" data-tip="' + escapeAttr(tip) + '">'
    );
  }
  if (!parts.length) return '';
  return '<span class="logmatch-class-icons" role="img" aria-label="Classes played in this log">' + parts.join('') + '</span>';
}

var _tooltipNode = null;
var _tooltipTarget = null;
function ensureTooltipNode() {
  if (_tooltipNode) return _tooltipNode;
  var n = document.createElement('div');
  n.className = 'custom-tooltip';
  n.hidden = true;
  n.setAttribute('role', 'tooltip');
  document.body.appendChild(n);
  _tooltipNode = n;
  return n;
}
function hideTooltip() {
  if (_tooltipNode) _tooltipNode.hidden = true;
  _tooltipTarget = null;
}
function placeTooltip(target) {
  var n = ensureTooltipNode();
  var rect = target.getBoundingClientRect();
  var pad = 8;
  var top = rect.bottom + pad;
  var left = rect.left;
  n.style.top = '0px';
  n.style.left = '0px';
  var w = n.offsetWidth || 220;
  var h = n.offsetHeight || 24;
  if (left + w > window.innerWidth - pad) left = Math.max(pad, window.innerWidth - w - pad);
  if (top + h > window.innerHeight - pad) top = Math.max(pad, rect.top - h - pad);
  n.style.left = Math.round(left) + 'px';
  n.style.top = Math.round(top) + 'px';
}
function showTooltipFor(target) {
  var text = target.getAttribute('data-tip');
  if (!text) return;
  var n = ensureTooltipNode();
  n.textContent = text;
  n.hidden = false;
  _tooltipTarget = target;
  placeTooltip(target);
}
(function initCustomTooltips() {
  if (window._customTooltipsBound) return;
  window._customTooltipsBound = true;
  document.addEventListener('mouseover', function(ev) {
    var t = ev.target && ev.target.closest ? ev.target.closest('[data-tip]') : null;
    if (!t) return;
    showTooltipFor(t);
  });
  document.addEventListener('mouseout', function(ev) {
    if (!_tooltipTarget) return;
    var rel = ev.relatedTarget;
    if (rel && _tooltipTarget.contains(rel)) return;
    hideTooltip();
  });
  document.addEventListener('click', function(ev) {
    var t = ev.target && ev.target.closest ? ev.target.closest('[data-tip]') : null;
    if (!t) { hideTooltip(); return; }
    if (_tooltipTarget === t && _tooltipNode && !_tooltipNode.hidden) hideTooltip();
    else showTooltipFor(t);
  });
  window.addEventListener('scroll', hideTooltip, { passive: true });
  window.addEventListener('resize', function() {
    if (_tooltipTarget && _tooltipNode && !_tooltipNode.hidden) placeTooltip(_tooltipTarget);
  }, { passive: true });
})();

/** Escape string for use as a literal in a RegExp (security: no regex injection). */
function escapeRegexLiteral(str) {
  if (str == null || str === '') return '';
  return String(str).replace(/\\/g, '\\\\').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Return HTML-safe message with case-insensitive matches of word wrapped in <strong>.
 * If word is empty, returns escaped message with no highlighting.
 */
function highlightChatMatch(msg, word) {
  var safe = escapeHtml(msg);
  var term = (word || '').trim();
  if (term === '') return safe;
  try {
    var pattern = escapeRegexLiteral(term);
    if (pattern === '') return safe;
    var re = new RegExp(pattern, 'gi');
    return safe.replace(re, '<strong>$&</strong>');
  } catch (_) {
    return safe;
  }
}

function parseStatsDate(str) {
  if (!str || typeof str !== 'string') return NaN;
  const parts = str.trim().split(/\s+/);
  if (parts.length < 4) return NaN;
  const datePart = parts[parts.length - 1];
  const dateBits = datePart.split('/').map(Number);
  if (dateBits.length !== 3) return NaN;
  const [m, d, y] = dateBits;
  if (!y || !m || !d) return NaN;
  const timePart = parts[0];
  const ampm = (parts[1] || '').toUpperCase();
  const timeBits = timePart.split(':').map(Number);
  let hour = timeBits[0] || 0;
  const min = timeBits[1] || 0;
  const sec = timeBits[2] || 0;
  if (ampm === 'PM' && hour < 12) hour += 12;
  if (ampm === 'AM' && hour === 12) hour = 0;
  return new Date(y, m - 1, d, hour, min, sec).getTime();
}
function escapeCsvField(value) {
  if (value == null) return '';
  var s = String(value);
  if (/[",\r\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/** Prefix cells that could be interpreted as formulas when opened in Excel / similar. */
function statsValueForCsvCell(raw) {
  var s = raw == null ? '' : String(raw);
  if (/^[=+\-@]/.test(s)) s = "'" + s;
  return escapeCsvField(s);
}
function triggerCsvDownload(filename, text) {
  try {
    var blob = new Blob([text], { type: 'text/csv;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.rel = 'noopener';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function() { URL.revokeObjectURL(url); }, 0);
  } catch (e) {}
}
function formatUpdatedAt(isoString) {
  try {
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return isoString;
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'short', timeStyle: 'short', timeZone: tz }).format(date);
  } catch (_) {
    return new Date(isoString + 'Z').toLocaleString(undefined, { timeZone: 'UTC' }) || isoString;
  }
}

function formatEarliestLogDate(unixSeconds) {
  if (unixSeconds == null || typeof unixSeconds !== 'number') return '';
  const date = new Date(unixSeconds * 1000);
  if (Number.isNaN(date.getTime())) return '';
  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'short', timeStyle: 'short', timeZone: tz }).format(date);
  } catch (_) {
    return date.toLocaleString(undefined, { timeZone: 'UTC' });
  }
}

/** Logmatch / log timestamps: browser TZ, fallback to UTC (same pattern as progress dates). */
function formatUnixLogTimestamp(unixSeconds) {
  if (unixSeconds == null || unixSeconds === '') return '';
  var n = Number(unixSeconds);
  if (!Number.isFinite(n)) return '';
  var date = new Date(n * 1000);
  if (Number.isNaN(date.getTime())) return '';
  try {
    var tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
    return new Intl.DateTimeFormat(undefined, { dateStyle: 'short', timeStyle: 'short', timeZone: tz }).format(date);
  } catch (_) {
    return date.toLocaleString(undefined, { timeZone: 'UTC' }) + ' UTC';
  }
}

function requestTimingFooter(ms) {
  if (typeof ms !== 'number' || !Number.isFinite(ms) || ms < 0) return '';
  return '<p class="request-timing">Loaded in <b>' + escapeHtml(String(Math.round(ms))) + '</b> ms</p>';
}

function getSortValue(row, key, type) {
  const v = row[key];
  if (type === 'number') {
    const n = Number(v);
    return Number.isNaN(n) ? -Infinity : n;
  }
  if (type === 'date') {
    const t = parseStatsDate(v);
    return Number.isNaN(t) ? 0 : t;
  }
  return v != null ? String(v).toLowerCase() : '';
}
