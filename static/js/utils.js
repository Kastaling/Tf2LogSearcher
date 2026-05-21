
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
  avg_killstreak: 1,
  backstabs: 1,
  headshots: 1,
  heals: 1,
};
var KILLS_BY_CLASS_VICTIMS = [
  { id: 'scout', label: 'Scout' },
  { id: 'soldier', label: 'Soldier' },
  { id: 'pyro', label: 'Pyro' },
  { id: 'demoman', label: 'Demoman' },
  { id: 'heavyweapons', label: 'Heavy' },
  { id: 'engineer', label: 'Engineer' },
  { id: 'medic', label: 'Medic' },
  { id: 'sniper', label: 'Sniper' },
  { id: 'spy', label: 'Spy' },
];
KILLS_BY_CLASS_VICTIMS.forEach(function(c) {
  VALID_LB_TYPES['kills_' + c.id] = 1;
  VALID_LB_TYPES['kills_weapon_' + c.id] = 1;
});
function killsLbType(classId) {
  return 'kills_' + String(classId || '').trim().toLowerCase();
}
function killsWeaponLbType(classId) {
  return 'kills_weapon_' + String(classId || '').trim().toLowerCase();
}
function victimClassFromLbType(lbType) {
  var t = sanitizeLbTypeInput(lbType);
  if (weaponLbClassFromLbType(t)) return '';
  if (t.indexOf('kills_') !== 0) return '';
  var vc = t.slice(6);
  return VALID_LB_TYPES[t] ? vc : '';
}
function weaponLbClassFromLbType(lbType) {
  var t = sanitizeLbTypeInput(lbType);
  if (t.indexOf('kills_weapon_') !== 0) return '';
  var pc = t.slice(13);
  return VALID_LB_TYPES[t] ? pc : '';
}
var DEFAULT_WEAPON_BY_LB_CLASS = {
  scout: 'scattergun',
  soldier: 'tf_projectile_rocket',
  pyro: 'flamethrower',
  demoman: 'tf_projectile_pipe',
  heavyweapons: 'minigun',
  engineer: 'obj_sentrygun',
  medic: 'crusaders_crossbow',
  sniper: 'sniperrifle',
  spy: 'knife',
};
var _lbWeaponsCache = Object.create(null);
var _lbWeaponsPending = Object.create(null);
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
  if (
    t === 'dpm' || t === 'kdr' || t === 'ubers' || t === 'drops' || t === 'damage_taken'
    || t === 'backstabs' || t === 'headshots' || t === 'heals' || t === 'avg_deaths'
    || t === 'avg_killstreak' || victimClassFromLbType(t) || weaponLbClassFromLbType(t)
  ) {
    if (s === 'total' || s === 'per_log') return s;
    return 'total';
  }
  if (t === 'winrate') {
    if (s === 'highest' || s === 'lowest') return s;
    return 'highest';
  }
  return 'total';
}

/** Leaderboard dropdown visual indent (native ``<option>`` has no nested markup). */
function leaderboardStatIndentPrefix(level) {
  var n = Math.max(0, Math.min(3, Number(level) || 0));
  var em = '\u2003';
  var out = '';
  for (var i = 0; i < n; i++) out += em + em;
  return out;
}

function leaderboardStatIconStyle(indentLevel, iconClass) {
  if (!iconClass || !LOGMATCH_CLASS_ICON[iconClass]) return null;
  var pad = 0.35 + indentLevel * 1.25;
  return {
    backgroundImage: 'url(' + LOGMATCH_CLASS_ICON[iconClass] + ')',
    backgroundRepeat: 'no-repeat',
    backgroundPosition: pad + 'rem center',
    backgroundSize: '18px 18px',
    paddingLeft: (pad + 1.3) + 'rem',
  };
}

var LEADERBOARD_SCOPE_LABELS = { total: 1, per_log: 1, highest: 1, lowest: 1 };

/** Full path shown on the closed dropdown only (not in the open option list). */
function leaderboardStatSelectionLabel(catLabel, sectionHeading, subHeading, ent) {
  var parts = [catLabel];
  if (sectionHeading) parts.push(sectionHeading);
  if (subHeading) parts.push(subHeading);
  if (LEADERBOARD_SCOPE_LABELS[ent.scope]) {
    parts.push(ent.label);
  } else if (!sectionHeading) {
    parts.push(ent.label);
  }
  return parts.join(' - ');
}

function killsByClassMenuEntries() {
  var out = [{ kind: 'heading', label: 'Kills by Class' }];
  KILLS_BY_CLASS_VICTIMS.forEach(function(c) {
    out.push({ kind: 'heading', label: c.label, iconClass: c.id, indent: 1 });
    out.push({ kind: 'stat', lb: killsLbType(c.id), scope: 'total', label: 'Total', indent: 2, iconClass: c.id });
    out.push({ kind: 'stat', lb: killsLbType(c.id), scope: 'per_log', label: 'Per log', indent: 2, iconClass: c.id });
  });
  return out;
}

function killsByWeaponMenuEntries() {
  var out = [{ kind: 'heading', label: 'Kills by Weapon' }];
  KILLS_BY_CLASS_VICTIMS.forEach(function(c) {
    out.push({ kind: 'heading', label: c.label, iconClass: c.id, indent: 1 });
    out.push({ kind: 'stat', lb: killsWeaponLbType(c.id), scope: 'total', label: 'Total', indent: 2, iconClass: c.id });
    out.push({ kind: 'stat', lb: killsWeaponLbType(c.id), scope: 'per_log', label: 'Per log', indent: 2, iconClass: c.id });
  });
  return out;
}

function sanitizeLeaderboardWeaponInput(raw, lbType) {
  var w = (raw == null ? '' : String(raw)).trim().toLowerCase();
  if (!w) return '';
  var pc = weaponLbClassFromLbType(lbType);
  if (!pc) return '';
  var list = _lbWeaponsCache[killsWeaponLbType(pc)];
  if (!list || !list.length) return w;
  for (var i = 0; i < list.length; i++) {
    if (list[i].id === w) return w;
  }
  return '';
}

function fetchLeaderboardWeaponsForLbType(lbType, cb) {
  var lt = sanitizeLbTypeInput(lbType);
  var pc = weaponLbClassFromLbType(lt);
  if (!pc) {
    if (cb) cb([]);
    return;
  }
  if (_lbWeaponsCache[lt]) {
    if (cb) cb(_lbWeaponsCache[lt]);
    return;
  }
  if (_lbWeaponsPending[lt]) {
    _lbWeaponsPending[lt].push(cb);
    return;
  }
  _lbWeaponsPending[lt] = [cb];
  fetch('/api/leaderboard-weapons?lb_type=' + encodeURIComponent(lt))
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var weapons = (data && data.weapons) ? data.weapons : [];
      _lbWeaponsCache[lt] = weapons;
      var cbs = _lbWeaponsPending[lt] || [];
      delete _lbWeaponsPending[lt];
      cbs.forEach(function(fn) { if (fn) fn(weapons); });
    })
    .catch(function() {
      var cbs = _lbWeaponsPending[lt] || [];
      delete _lbWeaponsPending[lt];
      cbs.forEach(function(fn) { if (fn) fn([]); });
    });
}

function populateLeaderboardWeaponSelect(select, weapons, selectedId) {
  if (!select) return;
  select.innerHTML = '';
  var want = (selectedId == null ? '' : String(selectedId)).trim().toLowerCase();
  (weapons || []).forEach(function(w) {
    var opt = document.createElement('option');
    opt.value = w.id;
    opt.textContent = w.label || w.id;
    select.appendChild(opt);
  });
  if (want) select.value = want;
  if (!select.value && select.options.length) select.selectedIndex = 0;
}

function syncLeaderboardWeaponSelect(form, preferredWeapon) {
  if (!form) return;
  var wrap = form.querySelector('.js-lb-weapon-wrap');
  var sel = form.elements.lb_weapon;
  if (!wrap || !sel) return;
  var lb = sanitizeLbTypeInput(form.elements.lb_type && form.elements.lb_type.value ? form.elements.lb_type.value : 'dpm');
  var pc = weaponLbClassFromLbType(lb);
  if (!pc) {
    wrap.hidden = true;
    wrap.setAttribute('aria-hidden', 'true');
    sel.disabled = true;
    sel.setAttribute('aria-hidden', 'true');
    sel.value = '';
    return;
  }
  wrap.hidden = false;
  wrap.removeAttribute('aria-hidden');
  sel.disabled = false;
  sel.removeAttribute('aria-hidden');
  var pref = sanitizeLeaderboardWeaponInput(preferredWeapon, lb);
  if (!pref) pref = DEFAULT_WEAPON_BY_LB_CLASS[pc] || '';
  fetchLeaderboardWeaponsForLbType(lb, function(weapons) {
    populateLeaderboardWeaponSelect(sel, weapons, pref);
    if (!sel.value && weapons.length) sel.value = weapons[0].id;
  });
}

function leaderboardClassIconImg(classId) {
  if (!classId) return '';
  var key = String(classId).toLowerCase();
  var src = LOGMATCH_CLASS_ICON[key];
  if (!src) return '';
  var label = LOGMATCH_CLASS_LABEL[key] || classId;
  return '<img class="logmatch-class-icon lb-stat-class-icon" src="' + escapeAttr(src) + '" alt="" width="22" height="22" loading="lazy" title="' + escapeAttr(label) + '">';
}

/** Insert class icons beside Stats Sorter class checkboxes (once per form). */
function initStatsClassCheckboxIcons(form) {
  var frm = form || document.getElementById('frmStats');
  if (!frm || frm.dataset.statsClassIconsInit === '1') return;
  frm.dataset.statsClassIconsInit = '1';
  frm.querySelectorAll('label.stats-class-option').forEach(function(label) {
    var inp = label.querySelector('input[name="classes"]');
    if (!inp || label.querySelector('.stats-class-icon')) return;
    var cid = (inp.value || '').trim().toLowerCase();
    var iconHtml = leaderboardClassIconImg(cid);
    if (!iconHtml) return;
    var labelText = (LOGMATCH_CLASS_LABEL[cid] || cid);
    var wrap = document.createElement('span');
    wrap.className = 'stats-class-option-inner';
    wrap.appendChild(inp);
    var holder = document.createElement('span');
    holder.innerHTML = iconHtml;
    var img = holder.querySelector('.logmatch-class-icon');
    if (img) {
      img.classList.add('stats-class-icon');
      wrap.appendChild(img);
    }
    var textSpan = document.createElement('span');
    textSpan.className = 'stats-class-option-text';
    textSpan.textContent = labelText;
    wrap.appendChild(textSpan);
    label.textContent = '';
    label.appendChild(wrap);
  });
}

function syncLeaderboardStatIconPreview(form) {
  if (!form) return;
  var preview = form.querySelector('.js-lb-stat-icon-preview');
  if (!preview) return;
  var lb = form.elements.lb_type && form.elements.lb_type.value
    ? form.elements.lb_type.value
    : 'dpm';
  var vc = victimClassFromLbType(lb) || weaponLbClassFromLbType(lb);
  preview.innerHTML = vc ? leaderboardClassIconImg(vc) : '';
  preview.hidden = !vc;
}

function syncLeaderboardStatClosedDisplay(form) {
  if (!form) return;
  var sel = form.elements.lb_stat;
  var display = form.querySelector('.js-lb-stat-display');
  if (!sel || !display) return;
  var opt = sel.options[sel.selectedIndex];
  var full = opt && opt.getAttribute('data-full-label');
  display.textContent = full || '';
  display.hidden = !full;
}

function initLeaderboardStatSelectUi(form) {
  if (!form || !form.elements.lb_stat) return;
  if (form.dataset.lbStatSelectUiInit === '1') {
    syncLeaderboardStatClosedDisplay(form);
    return;
  }
  form.dataset.lbStatSelectUiInit = '1';
  var sel = form.elements.lb_stat;
  var inner = form.querySelector('.lb-stat-select-inner');
  if (inner) {
    sel.addEventListener('focus', function() {
      inner.classList.add('is-open');
    });
    sel.addEventListener('blur', function() {
      inner.classList.remove('is-open');
      syncLeaderboardStatClosedDisplay(form);
    });
  }
  syncLeaderboardStatClosedDisplay(form);
}

/** Leaderboard stat picker: optgroup categories, disabled sub-headings, indented leaf options. */
var LEADERBOARD_STAT_MENU = [
  {
    label: 'General',
    entries: [
      { kind: 'stat', lb: 'logs', scope: 'total', label: 'Most Logs' },
      { kind: 'heading', label: 'Win Rate' },
      { kind: 'stat', lb: 'winrate', scope: 'highest', label: 'Highest', indent: 1 },
      { kind: 'stat', lb: 'winrate', scope: 'lowest', label: 'Lowest', indent: 1 },
    ],
  },
  {
    label: 'Support',
    entries: [
      { kind: 'heading', label: 'Ubers' },
      { kind: 'stat', lb: 'ubers', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'ubers', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Drops' },
      { kind: 'stat', lb: 'drops', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'drops', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Heals' },
      { kind: 'stat', lb: 'heals', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'heals', scope: 'per_log', label: 'Per log', indent: 1 },
    ],
  },
  {
    label: 'Offensive',
    entries: [
      { kind: 'heading', label: 'DPM' },
      { kind: 'stat', lb: 'dpm', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'dpm', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Damage taken' },
      { kind: 'stat', lb: 'damage_taken', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'damage_taken', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'KDR' },
      { kind: 'stat', lb: 'kdr', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'kdr', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Deaths' },
      { kind: 'stat', lb: 'avg_deaths', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'avg_deaths', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Killstreak' },
      { kind: 'stat', lb: 'avg_killstreak', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'avg_killstreak', scope: 'per_log', label: 'Per log', indent: 1 },
    ].concat(killsByClassMenuEntries()).concat(killsByWeaponMenuEntries()).concat([
      { kind: 'heading', label: 'Headshots' },
      { kind: 'stat', lb: 'headshots', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'headshots', scope: 'per_log', label: 'Per log', indent: 1 },
      { kind: 'heading', label: 'Backstabs' },
      { kind: 'stat', lb: 'backstabs', scope: 'total', label: 'Total', indent: 1 },
      { kind: 'stat', lb: 'backstabs', scope: 'per_log', label: 'Per log', indent: 1 },
    ]),
  },
];

function leaderboardStatOptionValue(lbType, statScope) {
  var lb = sanitizeLbTypeInput(lbType);
  var scope = sanitizeLeaderboardStatScopeInput(statScope, lb);
  return lb + '|' + scope;
}

function parseLeaderboardStatOptionValue(raw) {
  var s = (raw == null ? '' : String(raw)).trim();
  var pipe = s.indexOf('|');
  var lbRaw = pipe >= 0 ? s.slice(0, pipe) : s;
  var scopeRaw = pipe >= 0 ? s.slice(pipe + 1) : '';
  var lb = sanitizeLbTypeInput(lbRaw);
  return {
    lb_type: lb,
    stat_scope: sanitizeLeaderboardStatScopeInput(scopeRaw, lb),
  };
}

function populateLeaderboardStatSelect(select) {
  if (!select) return;
  select.innerHTML = '';
  LEADERBOARD_STAT_MENU.forEach(function(cat) {
    var og = document.createElement('optgroup');
    og.label = cat.label;
    var sectionHeading = '';
    var subHeading = '';
    cat.entries.forEach(function(ent) {
      var opt = document.createElement('option');
      var indentLvl = ent.indent != null ? Number(ent.indent) : 0;
      var prefix = leaderboardStatIndentPrefix(indentLvl);
      var iconStyle = leaderboardStatIconStyle(indentLvl, ent.iconClass);
      if (ent.kind === 'heading') {
        if (indentLvl <= 0) {
          sectionHeading = ent.label;
          subHeading = '';
        } else if (indentLvl === 1) {
          subHeading = ent.label;
        }
        opt.disabled = true;
        opt.textContent = prefix + ent.label;
        opt.className = 'lb-stat-heading' + (indentLvl ? ' lb-stat-indent-' + indentLvl : '');
        if (iconStyle) {
          opt.style.backgroundImage = iconStyle.backgroundImage;
          opt.style.backgroundRepeat = iconStyle.backgroundRepeat;
          opt.style.backgroundPosition = iconStyle.backgroundPosition;
          opt.style.backgroundSize = iconStyle.backgroundSize;
          opt.style.paddingLeft = iconStyle.paddingLeft;
        }
      } else {
        var fullLabel = leaderboardStatSelectionLabel(
          cat.label, sectionHeading, subHeading, ent
        );
        opt.value = leaderboardStatOptionValue(ent.lb, ent.scope);
        opt.textContent = prefix + ent.label;
        opt.setAttribute('data-full-label', fullLabel);
        opt.title = fullLabel;
        if (indentLvl) opt.className = 'lb-stat-indent lb-stat-indent-' + indentLvl;
        if (iconStyle) {
          opt.style.backgroundImage = iconStyle.backgroundImage;
          opt.style.backgroundRepeat = iconStyle.backgroundRepeat;
          opt.style.backgroundPosition = iconStyle.backgroundPosition;
          opt.style.backgroundSize = iconStyle.backgroundSize;
          opt.style.paddingLeft = iconStyle.paddingLeft;
        }
      }
      og.appendChild(opt);
    });
    select.appendChild(og);
  });
}

function setLeaderboardStatSelectValue(select, lbType, statScope) {
  if (!select) return;
  if (!select.options.length) populateLeaderboardStatSelect(select);
  var want = leaderboardStatOptionValue(lbType, statScope);
  select.value = want;
  if (select.value !== want) {
    select.value = leaderboardStatOptionValue('dpm', 'total');
  }
  var form = select.closest && select.closest('form');
  if (form) syncLeaderboardStatClosedDisplay(form);
}

function applyLeaderboardStatSelectToForm(form) {
  if (!form) return { lb_type: 'dpm', stat_scope: 'total' };
  var sel = form.elements.lb_stat;
  if (!sel) {
    return {
      lb_type: sanitizeLbTypeInput(form.elements.lb_type && form.elements.lb_type.value),
      stat_scope: sanitizeLeaderboardStatScopeInput(
        form.elements.stat_scope && form.elements.stat_scope.value,
        form.elements.lb_type && form.elements.lb_type.value
      ),
    };
  }
  var parsed = parseLeaderboardStatOptionValue(sel.value);
  if (form.elements.lb_type) form.elements.lb_type.value = parsed.lb_type;
  if (form.elements.stat_scope) form.elements.stat_scope.value = parsed.stat_scope;
  syncLeaderboardClassSelectForMedicLeaderboards(form);
  syncLeaderboardWeaponSelect(form, form.elements.lb_weapon && form.elements.lb_weapon.value);
  syncLeaderboardStatIconPreview(form);
  syncLeaderboardStatClosedDisplay(form);
  return parsed;
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

/** Browser-only notification prefs (same-origin cookie; no PII). */
var NOTIFY_PREFS_COOKIE = 'tf2ls_notify_prefs';
var NOTIFY_PREFS_COOKIE_MAX_AGE = 31536000;
/** Default playback volume (0–1) for the results notification sound. */
var NOTIFY_SOUND_VOLUME_DEFAULT = 0.5;

function sanitizeNotifyVolume(raw) {
  var n = Number(raw);
  if (!Number.isFinite(n)) return NOTIFY_SOUND_VOLUME_DEFAULT;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return Math.round(n * 100) / 100;
}

function readNotifyPrefs() {
  var d = { soundEnabled: true, soundVolume: NOTIFY_SOUND_VOLUME_DEFAULT };
  try {
    var all = typeof document !== 'undefined' && document.cookie ? document.cookie : '';
    if (!all) return d;
    var prefix = NOTIFY_PREFS_COOKIE + '=';
    var idx = all.indexOf(prefix);
    if (idx < 0) return d;
    var start = idx + prefix.length;
    var end = all.indexOf(';', start);
    var raw = decodeURIComponent(end < 0 ? all.slice(start) : all.slice(start, end));
    var o = JSON.parse(raw);
    if (o && typeof o === 'object') {
      if (o.soundEnabled === false) d.soundEnabled = false;
      if (o.soundVolume != null) d.soundVolume = sanitizeNotifyVolume(o.soundVolume);
    }
  } catch (e) {}
  return d;
}

/** Remove a same-origin TF2LS preference cookie (path=/, SameSite=Lax). */
function clearTf2lsCookie(cookieName) {
  var name = (cookieName || '').trim();
  if (!name) return;
  try {
    document.cookie = name + '=;path=/;max-age=0;SameSite=Lax';
  } catch (e) {}
}

function writeNotifyPrefs(prefs) {
  try {
    var cur = readNotifyPrefs();
    var soundEnabled = prefs && prefs.soundEnabled !== undefined ? !!prefs.soundEnabled : cur.soundEnabled;
    var soundVolume = prefs && prefs.soundVolume !== undefined
      ? sanitizeNotifyVolume(prefs.soundVolume)
      : cur.soundVolume;
    var payload = encodeURIComponent(JSON.stringify({
      soundEnabled: soundEnabled,
      soundVolume: soundVolume
    }));
    if (payload.length > 256) return;
    document.cookie = NOTIFY_PREFS_COOKIE + '=' + payload + ';path=/;max-age=' + NOTIFY_PREFS_COOKIE_MAX_AGE + ';SameSite=Lax';
  } catch (e) {}
}

function isNotifySoundEnabled() {
  return !!readNotifyPrefs().soundEnabled;
}

function getNotifySoundVolume() {
  return readNotifyPrefs().soundVolume;
}
