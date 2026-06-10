/**
 * Browser-only accent color (links, buttons, headings). Stored in tf2ls_accent_v1 cookie.
 * Preset suggestions + optional per-theme custom hex (validated, contrast-safe).
 */
(function (global) {
  'use strict';

  var ACCENT_PREFS_COOKIE = 'tf2ls_accent_v1';
  var ACCENT_PREFS_COOKIE_MAX_AGE = 31536000;
  var ACCENT_PREFS_COOKIE_MAX_LEN = 280;
  var DEFAULT_ACCENT_ID = 'teal';
  var CUSTOM_ACCENT_ID = 'custom';
  var MIN_LINK_CONTRAST = 3.0;
  var MIN_BUTTON_TEXT_CONTRAST = 4.5;
  var THEME_BG = { light: '#ffffff', dark: '#1a1a1a' };
  /** Keep in sync with THEME_KEY in static/js/search.js */
  var THEME_STORAGE_KEY = 'tf2log-theme';

  var ACCENT_PRESETS = {
    teal: {
      label: 'Teal',
      light: { link: '#2e6c80' },
      dark: { link: '#6eb5c0' },
    },
    blue: {
      label: 'Blue',
      light: { link: '#1d5f8a' },
      dark: { link: '#5ba4d9' },
    },
    green: {
      label: 'Green',
      light: { link: '#1f6b42' },
      dark: { link: '#5cb87a' },
    },
    purple: {
      label: 'Purple',
      light: { link: '#5b3f8c' },
      dark: { link: '#a78bfa' },
    },
    rose: {
      label: 'Rose',
      light: { link: '#9f3048' },
      dark: { link: '#f08ba0' },
    },
    amber: {
      label: 'Amber',
      light: { link: '#9a5f14' },
      dark: { link: '#e8b44a' },
    },
    slate: {
      label: 'Slate',
      light: { link: '#4a5568' },
      dark: { link: '#94a3b8' },
    },
  };

  var ACCENT_PRESET_IDS = ['teal', 'blue', 'green', 'purple', 'rose', 'amber', 'slate'];

  function sanitizeAccentId(id) {
    var key = String(id || '').trim().toLowerCase();
    if (key === CUSTOM_ACCENT_ID) return CUSTOM_ACCENT_ID;
    return ACCENT_PRESETS[key] ? key : DEFAULT_ACCENT_ID;
  }

  function parseHexColor(raw) {
    var t = String(raw || '').trim();
    if (/^#[0-9A-Fa-f]{3}$/.test(t)) {
      return ('#' + t[1] + t[1] + t[2] + t[2] + t[3] + t[3]).toLowerCase();
    }
    if (/^#[0-9A-Fa-f]{6}$/.test(t)) {
      return t.toLowerCase();
    }
    if (/^[0-9A-Fa-f]{6}$/.test(t)) {
      return ('#' + t).toLowerCase();
    }
    return null;
  }

  function hexToRgb(hex) {
    var h = parseHexColor(hex);
    if (!h) return null;
    return {
      r: parseInt(h.slice(1, 3), 16),
      g: parseInt(h.slice(3, 5), 16),
      b: parseInt(h.slice(5, 7), 16),
    };
  }

  function rgbToHex(r, g, b) {
    function clamp(n) {
      return Math.max(0, Math.min(255, Math.round(n)));
    }
    function pad(n) {
      var s = clamp(n).toString(16);
      return s.length === 1 ? '0' + s : s;
    }
    return '#' + pad(r) + pad(g) + pad(b);
  }

  function rgbToHsl(rgb) {
    var r = rgb.r / 255;
    var g = rgb.g / 255;
    var b = rgb.b / 255;
    var max = Math.max(r, g, b);
    var min = Math.min(r, g, b);
    var h = 0;
    var s = 0;
    var l = (max + min) / 2;
    if (max !== min) {
      var d = max - min;
      s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
      switch (max) {
        case r: h = (g - b) / d + (g < b ? 6 : 0); break;
        case g: h = (b - r) / d + 2; break;
        default: h = (r - g) / d + 4; break;
      }
      h /= 6;
    }
    return { h: h, s: s, l: l };
  }

  function hslToRgb(hsl) {
    var h = hsl.h;
    var s = hsl.s;
    var l = hsl.l;
    if (s === 0) {
      var g = Math.round(l * 255);
      return { r: g, g: g, b: g };
    }
    function hue2rgb(p, q, t) {
      if (t < 0) t += 1;
      if (t > 1) t -= 1;
      if (t < 1 / 6) return p + (q - p) * 6 * t;
      if (t < 1 / 2) return q;
      if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
      return p;
    }
    var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
    var p = 2 * l - q;
    return {
      r: Math.round(hue2rgb(p, q, h + 1 / 3) * 255),
      g: Math.round(hue2rgb(p, q, h) * 255),
      b: Math.round(hue2rgb(p, q, h - 1 / 3) * 255),
    };
  }

  function relativeLuminance(hex) {
    var c = hexToRgb(hex);
    if (!c) return 0;
    function lin(v) {
      v /= 255;
      return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
    }
    return 0.2126 * lin(c.r) + 0.7152 * lin(c.g) + 0.0722 * lin(c.b);
  }

  function contrastRatio(hex1, hex2) {
    var l1 = relativeLuminance(hex1);
    var l2 = relativeLuminance(hex2);
    var lighter = Math.max(l1, l2);
    var darker = Math.min(l1, l2);
    return (lighter + 0.05) / (darker + 0.05);
  }

  function accentTextForBackground(accentHex) {
    var white = contrastRatio(accentHex, '#ffffff');
    var dark = contrastRatio(accentHex, '#1a1a1a');
    if (white >= MIN_BUTTON_TEXT_CONTRAST && white >= dark) return '#ffffff';
    if (dark >= MIN_BUTTON_TEXT_CONTRAST) return '#1a1a1a';
    return white >= dark ? '#ffffff' : '#1a1a1a';
  }

  function nudgeAccentForTheme(hex, theme) {
    var parsed = parseHexColor(hex);
    if (!parsed) return null;
    var dark = theme === 'dark';
    var bg = dark ? THEME_BG.dark : THEME_BG.light;
    if (contrastRatio(parsed, bg) >= MIN_LINK_CONTRAST) {
      return { hex: parsed, adjusted: false };
    }
    var rgb = hexToRgb(parsed);
    if (!rgb) return null;
    var hsl = rgbToHsl(rgb);
    var original = parsed;
    for (var i = 0; i < 48; i++) {
      if (dark) {
        hsl.l = Math.min(0.94, hsl.l + 0.025);
        if (hsl.s < 0.08) hsl.s = 0.08;
      } else {
        hsl.l = Math.max(0.06, hsl.l - 0.025);
        if (hsl.s < 0.08) hsl.s = 0.08;
      }
      var rgbOut = hslToRgb(hsl);
      var next = rgbToHex(rgbOut.r, rgbOut.g, rgbOut.b);
      if (contrastRatio(next, bg) >= MIN_LINK_CONTRAST) {
        return { hex: next, adjusted: next !== original };
      }
    }
    return null;
  }

  function sanitizeCustomHex(raw, theme) {
    var parsed = parseHexColor(raw);
    if (!parsed) return null;
    return nudgeAccentForTheme(parsed, theme);
  }

  function defaultCustomColors() {
    return {
      light: ACCENT_PRESETS[DEFAULT_ACCENT_ID].light.link,
      dark: ACCENT_PRESETS[DEFAULT_ACCENT_ID].dark.link,
    };
  }

  function normalizeAccentPrefs(input) {
    if (typeof input === 'string') {
      return { id: sanitizeAccentId(input) };
    }
    if (!input || typeof input !== 'object') {
      return { id: DEFAULT_ACCENT_ID };
    }
    var id = sanitizeAccentId(input.id);
    if (id !== CUSTOM_ACCENT_ID) {
      return { id: id };
    }
    var base = input.custom && typeof input.custom === 'object' ? input.custom : {};
    var light = sanitizeCustomHex(base.light, 'light');
    var dark = sanitizeCustomHex(base.dark, 'dark');
    var defaults = defaultCustomColors();
    return {
      id: CUSTOM_ACCENT_ID,
      custom: {
        light: (light && light.hex) || defaults.light,
        dark: (dark && dark.hex) || defaults.dark,
      },
    };
  }

  function readAccentPrefs() {
    try {
      var all = typeof document !== 'undefined' && document.cookie ? document.cookie : '';
      if (!all) return { id: DEFAULT_ACCENT_ID };
      var prefix = ACCENT_PREFS_COOKIE + '=';
      var idx = all.indexOf(prefix);
      if (idx < 0) return { id: DEFAULT_ACCENT_ID };
      var start = idx + prefix.length;
      var end = all.indexOf(';', start);
      var raw = decodeURIComponent(end < 0 ? all.slice(start) : all.slice(start, end));
      if (raw.length > ACCENT_PREFS_COOKIE_MAX_LEN) return { id: DEFAULT_ACCENT_ID };
      var o = JSON.parse(raw);
      if (o && typeof o === 'object') {
        return normalizeAccentPrefs(o);
      }
    } catch (e) {}
    return { id: DEFAULT_ACCENT_ID };
  }

  function writeAccentPrefs(prefsOrId) {
    var normalized = normalizeAccentPrefs(prefsOrId);
    try {
      var payload = encodeURIComponent(JSON.stringify(normalized));
      if (payload.length > ACCENT_PREFS_COOKIE_MAX_LEN) return false;
      document.cookie = ACCENT_PREFS_COOKIE + '=' + payload + ';path=/;max-age=' + ACCENT_PREFS_COOKIE_MAX_AGE + ';SameSite=Lax';
      var stored = readAccentPrefs();
      if (stored.id !== normalized.id) return false;
      if (normalized.id === CUSTOM_ACCENT_ID) {
        return stored.custom.light === normalized.custom.light
          && stored.custom.dark === normalized.custom.dark;
      }
      return true;
    } catch (e) {
      return false;
    }
  }

  function writeAccentCustomForTheme(hex, theme) {
    var themeKey = theme === 'dark' ? 'dark' : 'light';
    var result = sanitizeCustomHex(hex, themeKey);
    if (!result) {
      return { ok: false, reason: 'That color is too close to the page background. Try a stronger hue or brightness.' };
    }
    var prefs = readAccentPrefs();
    var custom;
    if (prefs.id === CUSTOM_ACCENT_ID && prefs.custom) {
      custom = { light: prefs.custom.light, dark: prefs.custom.dark };
    } else {
      custom = {
        light: accentColorForTheme(prefs, 'light'),
        dark: accentColorForTheme(prefs, 'dark'),
      };
    }
    custom[themeKey] = result.hex;
    if (!writeAccentPrefs({ id: CUSTOM_ACCENT_ID, custom: custom })) {
      return { ok: false, reason: 'Could not save accent color to this browser.' };
    }
    applyAccentPrefs(readAccentPrefs());
    return {
      ok: true,
      hex: result.hex,
      adjusted: result.adjusted,
    };
  }

  function isDarkTheme() {
    return typeof document !== 'undefined'
      && document.documentElement.getAttribute('data-theme') === 'dark';
  }

  function followsSystemTheme() {
    try {
      if (typeof localStorage === 'undefined') return true;
      var stored = localStorage.getItem(THEME_STORAGE_KEY);
      return stored !== 'dark' && stored !== 'light';
    } catch (e) {
      return true;
    }
  }

  function systemPrefersDark() {
    return typeof matchMedia === 'function'
      && matchMedia('(prefers-color-scheme: dark)').matches;
  }

  /** Re-sync page theme from OS preference when the user has not pinned light/dark. */
  function applySystemPreferredTheme() {
    if (!followsSystemTheme() || typeof document === 'undefined') return false;
    var dark = systemPrefersDark();
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
    if (typeof global.tf2lsOnThemeApplied === 'function') {
      global.tf2lsOnThemeApplied();
    } else {
      applyAccentPrefs(readAccentPrefs());
      if (typeof global.refreshAccentPickerUi === 'function') {
        global.refreshAccentPickerUi();
      }
    }
    return true;
  }

  function initSystemThemeMediaListener() {
    if (typeof matchMedia !== 'function' || typeof document === 'undefined') return;
    var mq = matchMedia('(prefers-color-scheme: dark)');
    if (!mq) return;
    var onChange = function() {
      applySystemPreferredTheme();
    };
    if (typeof mq.addEventListener === 'function') {
      mq.addEventListener('change', onChange);
      return;
    }
    if (typeof mq.addListener === 'function') {
      mq.addListener(onChange);
    }
  }

  function resolveAccentLink(prefs) {
    var normalized = normalizeAccentPrefs(prefs);
    if (normalized.id === CUSTOM_ACCENT_ID) {
      return isDarkTheme() ? normalized.custom.dark : normalized.custom.light;
    }
    var preset = ACCENT_PRESETS[normalized.id];
    if (!preset) return null;
    return isDarkTheme() ? preset.dark.link : preset.light.link;
  }

  function clearAccentInlineStyles(root) {
    root.style.removeProperty('--link');
    root.style.removeProperty('--accent-hover');
    root.style.removeProperty('--accent');
    root.style.removeProperty('--accent-text');
  }

  function applyAccentPrefs(prefs) {
    if (typeof document === 'undefined') return;
    var normalized = normalizeAccentPrefs(prefs);
    var root = document.documentElement;
    root.dataset.accent = normalized.id;
    if (normalized.id === DEFAULT_ACCENT_ID) {
      clearAccentInlineStyles(root);
      return;
    }
    var link = resolveAccentLink(normalized);
    if (!link) {
      clearAccentInlineStyles(root);
      root.dataset.accent = DEFAULT_ACCENT_ID;
      return;
    }
    root.style.setProperty('--link', link);
    root.style.setProperty('--accent', link);
    root.style.setProperty('--accent-text', accentTextForBackground(link));
  }

  function applyDefaultAccentPrefs() {
    applyAccentPrefs({ id: DEFAULT_ACCENT_ID });
  }

  function accentColorForTheme(prefs, theme) {
    var normalized = normalizeAccentPrefs(prefs);
    if (normalized.id === CUSTOM_ACCENT_ID) {
      return theme === 'dark' ? normalized.custom.dark : normalized.custom.light;
    }
    var preset = ACCENT_PRESETS[normalized.id];
    if (!preset) return ACCENT_PRESETS[DEFAULT_ACCENT_ID].light.link;
    return theme === 'dark' ? preset.dark.link : preset.light.link;
  }

  function accentIsDefault(prefs) {
    var normalized = normalizeAccentPrefs(prefs);
    return normalized.id === DEFAULT_ACCENT_ID;
  }

  function accentExportToken(prefs) {
    var normalized = normalizeAccentPrefs(prefs);
    if (normalized.id === DEFAULT_ACCENT_ID) return null;
    if (normalized.id === CUSTOM_ACCENT_ID) {
      return 'custom ' + normalized.custom.light.slice(1) + ' ' + normalized.custom.dark.slice(1);
    }
    return normalized.id;
  }

  function accentFromExportToken(val) {
    var raw = String(val || '').trim();
    if (!raw) return null;
    if (raw.indexOf('custom ') === 0) {
      var parts = raw.slice(7).trim().split(/\s+/);
      if (parts.length < 2) return null;
      var light = sanitizeCustomHex(parts[0], 'light');
      var dark = sanitizeCustomHex(parts[1], 'dark');
      if (!light || !dark) return null;
      return {
        id: CUSTOM_ACCENT_ID,
        custom: { light: light.hex, dark: dark.hex },
      };
    }
    var key = raw.toLowerCase();
    if (key === CUSTOM_ACCENT_ID) {
      return { id: CUSTOM_ACCENT_ID, custom: defaultCustomColors() };
    }
    if (!ACCENT_PRESETS[key]) return null;
    return { id: key };
  }

  global.TF2LS_ACCENT_PREFS_COOKIE = ACCENT_PREFS_COOKIE;
  global.TF2LS_ACCENT_DEFAULT_ID = DEFAULT_ACCENT_ID;
  global.TF2LS_ACCENT_CUSTOM_ID = CUSTOM_ACCENT_ID;
  global.TF2LS_ACCENT_PRESET_IDS = ACCENT_PRESET_IDS;
  global.TF2LS_ACCENT_PRESETS = ACCENT_PRESETS;
  global.sanitizeAccentId = sanitizeAccentId;
  global.parseHexColor = parseHexColor;
  global.sanitizeCustomHex = sanitizeCustomHex;
  global.readAccentPrefs = readAccentPrefs;
  global.writeAccentPrefs = writeAccentPrefs;
  global.writeAccentCustomForTheme = writeAccentCustomForTheme;
  global.applyAccentPrefs = applyAccentPrefs;
  global.applyDefaultAccentPrefs = applyDefaultAccentPrefs;
  global.accentColorForTheme = accentColorForTheme;
  global.accentIsDefault = accentIsDefault;
  global.accentExportToken = accentExportToken;
  global.accentFromExportToken = accentFromExportToken;
  global.normalizeAccentPrefs = normalizeAccentPrefs;
  global.tf2lsApplySystemPreferredTheme = applySystemPreferredTheme;
  global.tf2lsFollowsSystemTheme = followsSystemTheme;

  if (typeof document !== 'undefined') {
    if (!applySystemPreferredTheme()) {
      applyAccentPrefs(readAccentPrefs());
    }
    initSystemThemeMediaListener();
  }
})(typeof window !== 'undefined' ? window : globalThis);
