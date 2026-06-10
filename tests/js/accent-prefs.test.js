'use strict';

const { loadAccentPrefs } = require('./setup-accent-prefs');

describe('accent-prefs', () => {
  test('parseHexColor accepts 3- and 6-digit hex', () => {
    const g = loadAccentPrefs();
    expect(g.parseHexColor('#abc')).toBe('#aabbcc');
    expect(g.parseHexColor('aabbcc')).toBe('#aabbcc');
    expect(g.parseHexColor('not-a-color')).toBeNull();
  });

  test('sanitizeCustomHex rejects near-background colors', () => {
    const g = loadAccentPrefs();
    expect(g.sanitizeCustomHex('#ffffff', 'light')).toBeNull();
    expect(g.sanitizeCustomHex('#1a1a1a', 'dark')).toBeNull();
  });

  test('sanitizeCustomHex nudges low-contrast colors', () => {
    const g = loadAccentPrefs();
    const result = g.sanitizeCustomHex('#f0f0f0', 'light');
    expect(result).not.toBeNull();
    expect(result.adjusted).toBe(true);
    expect(result.hex).not.toBe('#f0f0f0');
  });

  test('accentFromExportToken parses preset and custom tokens', () => {
    const g = loadAccentPrefs();
    expect(g.accentFromExportToken('purple')).toEqual({ id: 'purple' });
    expect(g.accentFromExportToken('custom 5b3f8c a78bfa')).toEqual({
      id: 'custom',
      custom: { light: '#5b3f8c', dark: '#a78bfa' },
    });
    expect(g.accentFromExportToken('not-a-preset')).toBeNull();
  });

  test('accentExportToken omits default teal', () => {
    const g = loadAccentPrefs();
    expect(g.accentExportToken({ id: 'teal' })).toBeNull();
    expect(g.accentExportToken({
      id: 'custom',
      custom: { light: '#112233', dark: '#aabbcc' },
    })).toBe('custom 112233 aabbcc');
  });

  test('writeAccentCustomForTheme seeds inactive theme from preset', () => {
    const g = loadAccentPrefs();
    g.writeAccentPrefs('purple');
    const purpleLight = g.TF2LS_ACCENT_PRESETS.purple.light.link;
    const result = g.writeAccentCustomForTheme('#112233', 'dark');
    expect(result.ok).toBe(true);
    const prefs = g.readAccentPrefs();
    expect(prefs.id).toBe('custom');
    expect(prefs.custom.light).toBe(purpleLight);
    expect(prefs.custom.dark).toBe('#112233');
  });

  test('applySystemPreferredTheme updates data-theme when following OS', () => {
    const g = loadAccentPrefs();
    g.localStorage = {
      getItem: () => null,
      setItem: () => {},
    };
    g.matchMedia = () => ({ matches: true, addEventListener: () => {} });
    g.document.documentElement.setAttribute('data-theme', 'light');
    g.writeAccentPrefs('purple');
    const applied = g.tf2lsApplySystemPreferredTheme();
    expect(applied).toBe(true);
    expect(g.document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('module init applies system preferred theme when following OS', () => {
    const attrs = { 'data-theme': 'light' };
    const mockGlobal = {
      document: {
        documentElement: {
          getAttribute(name) {
            return attrs[name] ?? null;
          },
          setAttribute(name, value) {
            attrs[name] = value;
          },
          style: { setProperty() {}, removeProperty() {} },
          dataset: {},
        },
        cookie: '',
      },
      localStorage: {
        getItem: () => null,
        setItem: () => {},
      },
      matchMedia: () => ({ matches: true, addEventListener() {} }),
    };
    mockGlobal.window = mockGlobal;
    const fs = require('fs');
    const path = require('path');
    const vm = require('vm');
    const src = fs.readFileSync(
      path.resolve(__dirname, '../../static/js/accent-prefs.js'),
      'utf8',
    );
    vm.createContext(mockGlobal);
    vm.runInContext(src, mockGlobal);
    expect(attrs['data-theme']).toBe('dark');
  });

  test('applySystemPreferredTheme is skipped when theme is pinned', () => {
    const g = loadAccentPrefs();
    g.localStorage = {
      getItem: (key) => (key === 'tf2log-theme' ? 'light' : null),
      setItem: () => {},
    };
    g.matchMedia = () => ({ matches: true, addEventListener: () => {} });
    g.document.documentElement.setAttribute('data-theme', 'light');
    const applied = g.tf2lsApplySystemPreferredTheme();
    expect(applied).toBe(false);
    expect(g.document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});
