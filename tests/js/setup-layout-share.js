/**
 * Loads layout-share.js into a mock global and returns tf2lsLayoutShare.
 * Call once per test file (or per describe block if you need different cookie state).
 *
 * Mocked globals:
 *   - window / global: same object (layout-share uses `global` as its IIFE parameter)
 *   - document.cookie: readable/writable string (simulates browser cookie jar)
 *   - window.location.href / origin: configurable per test
 *   - history.replaceState: jest.fn() (no-op, call args captured)
 *   - navigator.clipboard: not provided (tests that need copy use the execCommand path)
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadLayoutShare({ cookieJar = '', locationHref = 'http://localhost/', locationOrigin = 'http://localhost' } = {}) {
  // Build a minimal global mock
  let _cookie = cookieJar;
  const mockGlobal = {
    // layout-share uses (function(global){...})(window) — we pass mockGlobal as window
    location: {
      href: locationHref,
      origin: locationOrigin,
    },
    history: {
      replaceState: jest.fn(),
    },
    navigator: {},
    // btoa/atob are available in Node 16+
    btoa: (s) => Buffer.from(s, 'binary').toString('base64'),
    atob: (s) => Buffer.from(s, 'base64').toString('binary'),
    // URL is a global in Node
    URL: URL,
  };

  // document.cookie as a simple string jar (not full cookie spec — enough for tests)
  const docMock = {
    get cookie() { return _cookie; },
    set cookie(val) {
      // Parse "name=value;..." write and upsert into _cookie string
      const eqIdx = val.indexOf('=');
      if (eqIdx < 0) return;
      const name = val.slice(0, eqIdx).trim();
      const rest = val.slice(eqIdx + 1);
      const valueEnd = rest.indexOf(';');
      const value = valueEnd >= 0 ? rest.slice(0, valueEnd) : rest;
      // Remove existing entry with this name
      const parts = _cookie ? _cookie.split(';').map(p => p.trim()).filter(Boolean) : [];
      const filtered = parts.filter(p => !p.startsWith(name + '='));
      filtered.push(name + '=' + value);
      _cookie = filtered.join('; ');
    },
    execCommand: jest.fn(() => true),
  };

  // Attach document to mock global
  mockGlobal.document = docMock;
  // layout-share checks `typeof document !== 'undefined'`
  // and calls `global.location` — wire both
  mockGlobal.window = mockGlobal;

  const src = fs.readFileSync(
    path.resolve(__dirname, '../../static/js/layout-share.js'),
    'utf8'
  );

  // Execute in a VM context so the IIFE runs with our mocked global as `window`
  const script = new vm.Script(src);
  const ctx = vm.createContext(mockGlobal);
  script.runInContext(ctx);
  mockGlobal.history.replaceState.mockClear();

  return {
    api: mockGlobal.tf2lsLayoutShare,
    global: mockGlobal,
    doc: docMock,
    historyMock: mockGlobal.history,
    setLocationHref: (href) => { mockGlobal.location.href = href; },
    HOME_ENDPOINT_IDS: mockGlobal.TF2LS_HOME_ENDPOINT_IDS,
    PROFILE_SECTION_IDS: mockGlobal.TF2LS_PROFILE_SECTION_IDS,
  };
}

module.exports = { loadLayoutShare };
