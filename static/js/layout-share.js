/**
 * Import/export home + profile layout (order, visibility, profile collapse default).
 * Single source of truth for allowed IDs (keep in sync if endpoints change).
 */
(function (global) {
  'use strict';

  var LAYOUT_VERSION = 1;
  var HOME_LAYOUT_COOKIE = 'tf2ls_home_layout_v1';
  var PROFILE_LAYOUT_COOKIE = 'tf2ls_profile_layout_v1';
  var COOKIE_MAX_AGE = 31536000;

  var HOME_ENDPOINT_IDS = [
    'chat', 'profile', 'coplayers', 'logmatch', 'stats', 'leaderboard', 'playername', 'log_library',
  ];
  var PROFILE_SECTION_IDS = [
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

  global.TF2LS_HOME_ENDPOINT_IDS = HOME_ENDPOINT_IDS;
  global.TF2LS_PROFILE_SECTION_IDS = PROFILE_SECTION_IDS;

  function sanitizeHomeOrder(raw) {
    var seen = Object.create(null);
    var out = [];
    if (Array.isArray(raw)) {
      raw.forEach(function (id) {
        if (HOME_ENDPOINT_IDS.indexOf(id) >= 0 && !seen[id]) {
          seen[id] = true;
          out.push(id);
        }
      });
    }
    HOME_ENDPOINT_IDS.forEach(function (id) {
      if (!seen[id]) out.push(id);
    });
    return out;
  }

  function sanitizeHomeHidden(raw) {
    var h = Object.create(null);
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
      HOME_ENDPOINT_IDS.forEach(function (id) {
        if (raw[id] === true) h[id] = true;
      });
    } else if (Array.isArray(raw)) {
      raw.forEach(function (id) {
        if (HOME_ENDPOINT_IDS.indexOf(id) >= 0) h[id] = true;
      });
    }
    return h;
  }

  function sanitizeProfileOrder(raw) {
    var seen = Object.create(null);
    var out = [];
    if (Array.isArray(raw)) {
      raw.forEach(function (id) {
        if (PROFILE_SECTION_IDS.indexOf(id) >= 0 && !seen[id]) {
          seen[id] = true;
          out.push(id);
        }
      });
    }
    PROFILE_SECTION_IDS.forEach(function (id) {
      if (!seen[id]) out.push(id);
    });
    return out;
  }

  function readHomeFromCookie() {
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
        if (Array.isArray(o.order)) d.order = sanitizeHomeOrder(o.order);
        d.hidden = sanitizeHomeHidden(o.hidden);
      }
    } catch (e) {}
    return d;
  }

  function cookieValue(name) {
    try {
      var all = typeof document !== 'undefined' && document.cookie ? document.cookie : '';
      if (!all) return null;
      var parts = all.split(';');
      var prefix = name + '=';
      for (var i = 0; i < parts.length; i++) {
        var part = parts[i].trim();
        if (part.indexOf(prefix) === 0) return part.slice(prefix.length);
      }
    } catch (e) {}
    return null;
  }

  function writeLayoutCookie(name, payload) {
    try {
      if (payload.length > 3800) return false;
      document.cookie = name + '=' + payload + ';path=/;max-age=' + COOKIE_MAX_AGE + ';SameSite=Lax';
      return cookieValue(name) === payload;
    } catch (e) {
      return false;
    }
  }

  function writeHomeToCookie(settings) {
    try {
      var order = sanitizeHomeOrder(settings.order || []);
      var hidden = sanitizeHomeHidden(settings.hidden);
      var payload = encodeURIComponent(JSON.stringify({ order: order, hidden: hidden }));
      return writeLayoutCookie(HOME_LAYOUT_COOKIE, payload);
    } catch (e) {
      return false;
    }
  }

  function readProfileFromCookie() {
    var d = { order: PROFILE_SECTION_IDS.slice(), collapseDefault: false };
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
        if (Array.isArray(o.order)) d.order = sanitizeProfileOrder(o.order);
        if (typeof o.collapseDefault === 'boolean') d.collapseDefault = o.collapseDefault;
      }
    } catch (e) {}
    return d;
  }

  function writeProfileToCookie(settings) {
    try {
      var payload = encodeURIComponent(
        JSON.stringify({
          order: sanitizeProfileOrder(settings.order || []),
          collapseDefault: !!settings.collapseDefault,
        }),
      );
      return writeLayoutCookie(PROFILE_LAYOUT_COOKIE, payload);
    } catch (e) {
      return false;
    }
  }

  function utf8ToBase64Url(obj) {
    var json = JSON.stringify(obj);
    if (json.length > 16000) return '';
    var b64 = btoa(unescape(encodeURIComponent(json)));
    return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  }

  function base64UrlToUtf8(token) {
    var b64 = token.replace(/-/g, '+').replace(/_/g, '/');
    while (b64.length % 4) b64 += '=';
    return decodeURIComponent(escape(atob(b64)));
  }

  /**
   * Compact payload: { v, h: { o, x }, p: { o, c } }
   * h.x = hidden map; only keys that are true matter — we store list of hidden ids in x as object {id:true}
   */
  function defaultAccentId() {
    return global.TF2LS_ACCENT_DEFAULT_ID || 'teal';
  }

  function sanitizeAccentId(id) {
    if (typeof global.sanitizeAccentId === 'function') {
      return global.sanitizeAccentId(id);
    }
    var key = String(id || '').trim().toLowerCase();
    var presets = global.TF2LS_ACCENT_PRESETS;
    return presets && presets[key] ? key : defaultAccentId();
  }

  function readAccentForExport() {
    if (typeof global.readAccentPrefs !== 'function') return null;
    if (typeof global.accentExportToken === 'function') {
      return global.accentExportToken(global.readAccentPrefs());
    }
    var id = global.readAccentPrefs().id;
    return id && id !== defaultAccentId() ? id : null;
  }

  function parseAccentHexToken(part) {
    var t = String(part || '').trim().toLowerCase();
    if (/^[0-9a-f]{6}$/.test(t)) return '#' + t;
    if (/^#[0-9a-f]{6}$/.test(t)) return t;
    if (/^[0-9a-f]{3}$/.test(t)) {
      return '#' + t[0] + t[0] + t[1] + t[1] + t[2] + t[2];
    }
    return null;
  }

  /** Parse accent import token; works without accent-prefs.js (custom hex only). */
  function parseAccentImportToken(val) {
    if (typeof global.accentFromExportToken === 'function') {
      return global.accentFromExportToken(val);
    }
    var raw = String(val || '').trim();
    if (!raw) return null;
    if (raw.indexOf('custom ') === 0) {
      var parts = raw.slice(7).trim().split(/\s+/);
      if (parts.length < 2) return null;
      var light = parseAccentHexToken(parts[0]);
      var dark = parseAccentHexToken(parts[1]);
      if (!light || !dark) return null;
      return {
        id: 'custom',
        custom: { light: light, dark: dark },
      };
    }
    var id = sanitizeAccentId(raw);
    if (id === defaultAccentId() && raw.toLowerCase() !== defaultAccentId()) {
      return null;
    }
    return { id: id };
  }

  function packAccentField(token) {
    if (!token) return null;
    if (String(token).indexOf('custom ') === 0) {
      var parts = String(token).slice(7).trim().split(/\s+/);
      if (parts.length >= 2) return { c: [parts[0], parts[1]] };
      return null;
    }
    return sanitizeAccentId(token);
  }

  function pack(home, profile, accentId) {
    var ho = sanitizeHomeOrder(home.order || []);
    var hh = sanitizeHomeHidden(home.hidden);
    var po = sanitizeProfileOrder(profile.order || []);
    var pc = !!profile.collapseDefault;
    var x = {};
    HOME_ENDPOINT_IDS.forEach(function (id) {
      if (hh[id]) x[id] = true;
    });
    var out = { v: LAYOUT_VERSION, h: { o: ho, x: x }, p: { o: po, c: pc ? 1 : 0 } };
    var aid = accentId != null && accentId !== '' ? accentId : readAccentForExport();
    var packedAccent = packAccentField(aid);
    if (packedAccent) out.a = packedAccent;
    return out;
  }

  function unpack(obj) {
    if (!obj || typeof obj !== 'object') return null;
    var v = obj.v;
    if (v !== 1 && v !== undefined && v !== null) return null;
    var out = {
      home: { order: HOME_ENDPOINT_IDS.slice(), hidden: {} },
      profile: { order: PROFILE_SECTION_IDS.slice(), collapseDefault: false },
      accent: null,
    };
    var h = obj.h;
    if (h && typeof h === 'object') {
      if (Array.isArray(h.o)) out.home.order = sanitizeHomeOrder(h.o);
      out.home.hidden = sanitizeHomeHidden(h.x);
    }
    var p = obj.p;
    if (p && typeof p === 'object') {
      if (Array.isArray(p.o)) out.profile.order = sanitizeProfileOrder(p.o);
      if (p.c === 1 || p.c === true) out.profile.collapseDefault = true;
    }
    if (obj.a && typeof obj.a === 'object' && Array.isArray(obj.a.c) && obj.a.c.length >= 2) {
      out.accent = parseAccentImportToken('custom ' + obj.a.c[0] + ' ' + obj.a.c[1]);
    } else if (typeof obj.a === 'string' && obj.a.trim()) {
      out.accent = parseAccentImportToken(obj.a);
    }
    return out;
  }

  function decodeToken(token) {
    if (!token || typeof token !== 'string') return null;
    token = token.trim();
    if (!token) return null;
    if (token.length > 20000) return null;
    try {
      var obj = JSON.parse(base64UrlToUtf8(token));
      return unpack(obj);
    } catch (e) {
      return null;
    }
  }

  function decodeLayoutInput(text) {
    if (!text || typeof text !== 'string') return null;
    var trimmed = text.trim();
    if (!trimmed) return null;
    var decoded = importPlainText(trimmed);
    if (decoded) return decoded;
    try {
      var u = new URL(trimmed);
      var token = u.searchParams.get('layout');
      if (token) return decodeToken(token.trim());
    } catch (e) {}
    return decodeToken(trimmed);
  }

  function exportPlainText(home, profile) {
    var lines = [
      'tf2ls-layout-v' + LAYOUT_VERSION,
      'home.order ' + sanitizeHomeOrder(home.order || []).join(','),
      'home.hidden ' +
        HOME_ENDPOINT_IDS.filter(function (id) {
          return sanitizeHomeHidden(home.hidden)[id];
        }).join(','),
      'profile.order ' + sanitizeProfileOrder(profile.order || []).join(','),
      'profile.collapse ' + (profile.collapseDefault ? '1' : '0'),
    ];
    var accentToken = readAccentForExport();
    if (accentToken) lines.push('accent ' + accentToken);
    return lines.join('\n');
  }

  function importPlainText(text) {
    if (!text || typeof text !== 'string') return null;
    var home = { order: HOME_ENDPOINT_IDS.slice(), hidden: {} };
    var profile = { order: PROFILE_SECTION_IDS.slice(), collapseDefault: false };
    var accent = null;
    var lines = text.split(/\r?\n/);
    var okHeader = false;
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) continue;
      if (line.indexOf('tf2ls-layout-v') === 0) {
        okHeader = true;
        continue;
      }
      var sp = line.indexOf(' ');
      var key = sp >= 0 ? line.slice(0, sp) : line;
      var val = sp >= 0 ? line.slice(sp + 1).trim() : '';
      if (key === 'home.order') {
        home.order = sanitizeHomeOrder(
          val
            ? val.split(',').map(function (s) {
                return s.trim();
              })
            : [],
        );
      } else if (key === 'home.hidden') {
        var hid = {};
        if (val) {
          val.split(',').forEach(function (s) {
            s = s.trim();
            if (HOME_ENDPOINT_IDS.indexOf(s) >= 0) hid[s] = true;
          });
        }
        home.hidden = sanitizeHomeHidden(hid);
      } else if (key === 'profile.order') {
        profile.order = sanitizeProfileOrder(
          val
            ? val.split(',').map(function (s) {
                return s.trim();
              })
            : [],
        );
      } else if (key === 'profile.collapse') {
        profile.collapseDefault = val === '1' || val.toLowerCase() === 'true';
      } else if (key === 'accent') {
        if (val) accent = parseAccentImportToken(val);
      }
    }
    if (!okHeader) return null;
    return { home: home, profile: profile, accent: accent };
  }

  function applyAccentPacked(accent) {
    if (!accent || !accent.id) return true;
    if (typeof global.writeAccentPrefs !== 'function') return true;
    if (!global.writeAccentPrefs(accent)) return false;
    if (typeof global.applyAccentPrefs === 'function' && typeof global.readAccentPrefs === 'function') {
      global.applyAccentPrefs(global.readAccentPrefs());
    }
    if (typeof global.refreshAccentPickerUi === 'function') {
      global.refreshAccentPickerUi();
    }
    return true;
  }

  function applyPacked(data) {
    if (!data || !data.home || !data.profile) return false;
    var homeOk = writeHomeToCookie(data.home);
    var profileOk = writeProfileToCookie(data.profile);
    var accentOk = applyAccentPacked(data.accent);
    return homeOk && profileOk && accentOk;
  }

  function consumeLayoutQueryParam() {
    try {
      var u = new URL(window.location.href);
      var token = u.searchParams.get('layout');
      if (!token) return;
      var decoded = decodeToken(token.trim());
      if (!decoded) {
        u.searchParams.delete('layout');
        var q0 = u.searchParams.toString();
        history.replaceState(null, '', u.pathname + (q0 ? '?' + q0 : '') + u.hash);
        return;
      }
      if (applyPacked(decoded)) {
        u.searchParams.delete('layout');
        var qs = u.searchParams.toString();
        history.replaceState(null, '', u.pathname + (qs ? '?' + qs : '') + u.hash);
      }
    } catch (e) {}
  }

  function encodeTokenFromCurrentCookies() {
    return utf8ToBase64Url(pack(readHomeFromCookie(), readProfileFromCookie()));
  }

  function buildHomeShareUrl() {
    var token = encodeTokenFromCurrentCookies();
    if (!token) return '';
    try {
      var u = new URL(global.location.origin + '/');
      u.searchParams.set('layout', token);
      return u.toString();
    } catch (e) {
      return '';
    }
  }

  function setStatus(el, label, isError) {
    if (!el) return;
    el.textContent = label || '';
    el.className = 'layout-share-status stats-summary-meta' + (isError ? ' layout-share-status--error' : '');
  }

  function bindPanel(root, options) {
    if (!root) return;
    var ta = root.querySelector('.js-layout-share-text');
    var btnImport = root.querySelector('.js-layout-share-import');
    var btnExport = root.querySelector('.js-layout-share-export');
    var btnLinkPage = root.querySelector('.js-layout-share-link-page');
    var btnLinkHome = root.querySelector('.js-layout-share-link-home');
    var status = root.querySelector('.js-layout-share-status');

    function refreshTextareaFromCookies() {
      if (!ta) return;
      ta.value = exportPlainText(readHomeFromCookie(), readProfileFromCookie());
    }

    function selectTextareaValue(value) {
      if (!ta) return false;
      ta.value = value || '';
      try {
        ta.focus();
        ta.select();
        ta.setSelectionRange(0, ta.value.length);
        return true;
      } catch (e) {
        return false;
      }
    }

    function showManualCopy(value, label) {
      selectTextareaValue(value);
      setStatus(status, label || 'Copy blocked by the browser. The text is selected above; press Ctrl+C to copy it.', true);
    }

    function copyOrSelect(value, successLabel, fallbackLabel) {
      selectTextareaValue(value);
      if (global.navigator.clipboard && global.navigator.clipboard.writeText) {
        global.navigator.clipboard.writeText(value).then(
          function () {
            setStatus(status, successLabel, false);
          },
          function () {
            showManualCopy(value, fallbackLabel);
          },
        );
        return;
      }
      try {
        var ok = document.execCommand('copy');
        if (ok) {
          setStatus(status, successLabel, false);
        } else {
          showManualCopy(value, fallbackLabel);
        }
      } catch (e) {
        showManualCopy(value, fallbackLabel);
      }
    }

    if (btnExport) {
      btnExport.addEventListener('click', function () {
        var t = exportPlainText(readHomeFromCookie(), readProfileFromCookie());
        copyOrSelect(t, 'Copied text layout to clipboard.', 'Copy blocked by the browser. The layout text is selected above; press Ctrl+C to copy it.');
      });
    }

    if (btnImport && ta) {
      btnImport.addEventListener('click', function () {
        var parsed = decodeLayoutInput(ta.value);
        if (!parsed) {
          setStatus(status, 'Could not parse layout. Use exported text, a share link, or a layout= token.', true);
          return;
        }
        if (!applyPacked(parsed)) {
          setStatus(status, 'Could not save layout to this browser. Check cookie/storage settings or use the exported text again.', true);
          return;
        }
        if (typeof options.afterApply === 'function') options.afterApply(parsed);
        setStatus(status, 'Applied layout (saved to this browser).', false);
      });
    }

    if (btnLinkPage) {
      btnLinkPage.addEventListener('click', function () {
        var token = encodeTokenFromCurrentCookies();
        if (!token) {
          setStatus(status, 'Could not build link.', true);
          return;
        }
        try {
          var u = new URL(global.location.href);
          u.searchParams.set('layout', token);
          var url = u.toString();
          copyOrSelect(url, 'Copied link to this page with layout.', 'Copy blocked by the browser. The share link is selected above; press Ctrl+C to copy it.');
        } catch (e) {
          setStatus(status, 'Could not build URL.', true);
        }
      });
    }

    if (btnLinkHome) {
      btnLinkHome.addEventListener('click', function () {
        var url = buildHomeShareUrl();
        if (!url) {
          setStatus(status, 'Could not build link.', true);
          return;
        }
        copyOrSelect(url, 'Copied home page link with layout.', 'Copy blocked by the browser. The home share link is selected above; press Ctrl+C to copy it.');
      });
    }

    refreshTextareaFromCookies();
  }

  global.tf2lsLayoutShare = {
    consumeLayoutQueryParam: consumeLayoutQueryParam,
    decodeToken: decodeToken,
    encodeTokenFromCurrentCookies: encodeTokenFromCurrentCookies,
    exportPlainText: exportPlainText,
    importPlainText: importPlainText,
    applyPacked: applyPacked,
    buildHomeShareUrl: buildHomeShareUrl,
    readHomeFromCookie: readHomeFromCookie,
    readProfileFromCookie: readProfileFromCookie,
    bindPanel: bindPanel,
    pack: pack,
    unpack: unpack,
  };
  try {
    consumeLayoutQueryParam();
  } catch (e) {}
})(window);
