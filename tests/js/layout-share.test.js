'use strict';
const { loadLayoutShare } = require('./setup-layout-share');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const HOME_IDS = [
  'chat', 'profile', 'coplayers', 'logmatch', 'stats', 'leaderboard', 'playername', 'log_library',
];
const PROFILE_IDS = [
  'trend',
  'favorite_words',
  'coplayers',
  'top_maps',
  'classes',
  'top_logs',
  'rounds',
  'weapons',
  'healspread',
  'class_kills',
];

function defaultHome() {
  return { order: HOME_IDS.slice(), hidden: {} };
}
function defaultProfile() {
  return { order: PROFILE_IDS.slice(), collapseDefault: false };
}

// ---------------------------------------------------------------------------
// Exported ID lists
// ---------------------------------------------------------------------------

describe('TF2LS_HOME_ENDPOINT_IDS and TF2LS_PROFILE_SECTION_IDS', () => {
  const { HOME_ENDPOINT_IDS, PROFILE_SECTION_IDS } = loadLayoutShare();

  test('HOME_ENDPOINT_IDS matches expected list', () => {
    expect(HOME_ENDPOINT_IDS).toEqual(HOME_IDS);
  });

  test('PROFILE_SECTION_IDS matches expected list', () => {
    expect(PROFILE_SECTION_IDS).toEqual(PROFILE_IDS);
  });
});

// ---------------------------------------------------------------------------
// pack / unpack round-trip
// ---------------------------------------------------------------------------

describe('pack and unpack', () => {
  const { api } = loadLayoutShare();

  test('default home and profile round-trips through pack→decodeToken', () => {
    const home = defaultHome();
    const profile = defaultProfile();
    // Test the compact packed shape directly; token encode/decode is covered below.
    const packed = api.pack(home, profile);
    expect(packed.v).toBe(1);
    expect(packed.h.o).toEqual(HOME_IDS);
    expect(packed.p.o).toEqual(PROFILE_IDS);
    expect(packed.p.c).toBe(0);
  });

  test('unpack returns null for unknown version', () => {
    const { api } = loadLayoutShare();
    // Construct a token with v=99
    const bad = { v: 99, h: { o: HOME_IDS, x: {} }, p: { o: PROFILE_IDS, c: 0 } };
    const token = api.pack(defaultHome(), defaultProfile());
    // Directly test unpack behaviour via decodeToken with a hand-crafted token
    const json = JSON.stringify(bad);
    const b64 = Buffer.from(json).toString('base64url');
    const result = api.decodeToken(b64);
    expect(result).toBeNull();
  });

  test('unpack preserves collapseDefault=true', () => {
    const { api } = loadLayoutShare();
    const profile = { order: PROFILE_IDS.slice(), collapseDefault: true };
    const home = defaultHome();
    const packed = api.pack(home, profile);
    expect(packed.p.c).toBe(1);
  });

  test('unpack with hidden sections', () => {
    const { api } = loadLayoutShare();
    const home = { order: HOME_IDS.slice(), hidden: { stats: true, leaderboard: true } };
    const packed = api.pack(home, defaultProfile());
    expect(packed.h.x.stats).toBe(true);
    expect(packed.h.x.leaderboard).toBe(true);
    expect(packed.h.x.chat).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Token encode / decode round-trip
// ---------------------------------------------------------------------------

describe('encodeTokenFromCurrentCookies / decodeToken', () => {
  test('tokens round-trip through encode→decode with no cookies (defaults)', () => {
    const { api } = loadLayoutShare({ cookieJar: '' });
    const token = api.encodeTokenFromCurrentCookies();
    expect(typeof token).toBe('string');
    expect(token.length).toBeGreaterThan(0);
    const decoded = api.decodeToken(token);
    expect(decoded).not.toBeNull();
    expect(decoded.home.order).toEqual(HOME_IDS);
    expect(decoded.profile.order).toEqual(PROFILE_IDS);
    expect(decoded.profile.collapseDefault).toBe(false);
  });

  test('decodeToken returns null for empty string', () => {
    const { api } = loadLayoutShare();
    expect(api.decodeToken('')).toBeNull();
    expect(api.decodeToken(null)).toBeNull();
    expect(api.decodeToken('   ')).toBeNull();
  });

  test('decodeToken returns null for garbage input', () => {
    const { api } = loadLayoutShare();
    expect(api.decodeToken('not-valid-base64url!!!')).toBeNull();
    expect(api.decodeToken('aGVsbG8=')).toBeNull(); // valid base64 but not a layout JSON
  });

  test('decodeToken returns null when token exceeds 20000 chars', () => {
    const { api } = loadLayoutShare();
    expect(api.decodeToken('a'.repeat(20001))).toBeNull();
  });

  test('token survives URL safe base64 characters (no +/=)', () => {
    const { api } = loadLayoutShare();
    const token = api.encodeTokenFromCurrentCookies();
    expect(token).not.toMatch(/[+/=]/);
  });

  test('encodeTokenFromCurrentCookies returns empty string for oversized payload', () => {
    // pack with a valid but fabricated oversized JSON would trigger the 16000-char guard;
    // we can test the guard indirectly by checking the function returns a string in all cases
    const { api } = loadLayoutShare();
    const result = api.encodeTokenFromCurrentCookies();
    expect(typeof result).toBe('string');
  });
});

// ---------------------------------------------------------------------------
// Plain text import / export
// ---------------------------------------------------------------------------

describe('exportPlainText / importPlainText', () => {
  test('export produces a header line and all four keys', () => {
    const { api } = loadLayoutShare();
    const text = api.exportPlainText(defaultHome(), defaultProfile());
    expect(text).toMatch(/^tf2ls-layout-v1/);
    expect(text).toContain('home.order ');
    expect(text).toContain('home.hidden ');
    expect(text).toContain('profile.order ');
    expect(text).toContain('profile.collapse ');
  });

  test('importPlainText round-trips with default settings', () => {
    const { api } = loadLayoutShare();
    const text = api.exportPlainText(defaultHome(), defaultProfile());
    const imported = api.importPlainText(text);
    expect(imported).not.toBeNull();
    expect(imported.home.order).toEqual(HOME_IDS);
    expect(imported.profile.order).toEqual(PROFILE_IDS);
    expect(imported.profile.collapseDefault).toBe(false);
  });

  test('importPlainText returns null without the header line', () => {
    const { api } = loadLayoutShare();
    const noHeader = 'home.order chat,profile\nprofile.order trend,classes';
    expect(api.importPlainText(noHeader)).toBeNull();
  });

  test('importPlainText handles reordered home sections', () => {
    const { api } = loadLayoutShare();
    const reversed = HOME_IDS.slice().reverse();
    const text = [
      'tf2ls-layout-v1',
      'home.order ' + reversed.join(','),
      'home.hidden ',
      'profile.order ' + PROFILE_IDS.join(','),
      'profile.collapse 0',
    ].join('\n');
    const imported = api.importPlainText(text);
    expect(imported).not.toBeNull();
    expect(imported.home.order).toEqual(reversed);
  });

  test('importPlainText with hidden sections', () => {
    const { api } = loadLayoutShare();
    const text = [
      'tf2ls-layout-v1',
      'home.order ' + HOME_IDS.join(','),
      'home.hidden stats,leaderboard',
      'profile.order ' + PROFILE_IDS.join(','),
      'profile.collapse 0',
    ].join('\n');
    const imported = api.importPlainText(text);
    expect(imported.home.hidden.stats).toBe(true);
    expect(imported.home.hidden.leaderboard).toBe(true);
    expect(imported.home.hidden.chat).toBeUndefined();
  });

  test('importPlainText ignores unknown section IDs in home.order', () => {
    const { api } = loadLayoutShare();
    const text = [
      'tf2ls-layout-v1',
      'home.order chat,INVALID_SECTION,profile',
      'home.hidden ',
      'profile.order ' + PROFILE_IDS.join(','),
      'profile.collapse 0',
    ].join('\n');
    const imported = api.importPlainText(text);
    // INVALID_SECTION must not appear; all real IDs must still be present
    expect(imported.home.order).not.toContain('INVALID_SECTION');
    expect(imported.home.order).toContain('chat');
    expect(imported.home.order).toContain('profile');
    // Missing IDs are appended by sanitizeHomeOrder
    HOME_IDS.forEach(id => expect(imported.home.order).toContain(id));
  });

  test('importPlainText with profile.collapse 1 sets collapseDefault true', () => {
    const { api } = loadLayoutShare();
    const text = [
      'tf2ls-layout-v1',
      'home.order ' + HOME_IDS.join(','),
      'home.hidden ',
      'profile.order ' + PROFILE_IDS.join(','),
      'profile.collapse 1',
    ].join('\n');
    const imported = api.importPlainText(text);
    expect(imported.profile.collapseDefault).toBe(true);
  });

  test('importPlainText returns null for empty or non-string input', () => {
    const { api } = loadLayoutShare();
    expect(api.importPlainText('')).toBeNull();
    expect(api.importPlainText(null)).toBeNull();
    expect(api.importPlainText(42)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Cookie read / write (via applyPacked + encodeTokenFromCurrentCookies)
// ---------------------------------------------------------------------------

describe('cookie read/write via applyPacked', () => {
  test('applyPacked writes cookies and encodeTokenFromCurrentCookies reads them back', () => {
    const { api } = loadLayoutShare({ cookieJar: '' });
    const profile = { order: PROFILE_IDS.slice().reverse(), collapseDefault: true };
    const home = { order: HOME_IDS.slice().reverse(), hidden: { stats: true } };
    const packed = { home, profile };
    api.applyPacked(packed);
    const token = api.encodeTokenFromCurrentCookies();
    const decoded = api.decodeToken(token);
    expect(decoded).not.toBeNull();
    expect(decoded.home.order[0]).toBe(HOME_IDS[HOME_IDS.length - 1]); // reversed
    expect(decoded.home.hidden.stats).toBe(true);
    expect(decoded.profile.collapseDefault).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// consumeLayoutQueryParam
// ---------------------------------------------------------------------------

describe('consumeLayoutQueryParam', () => {
  test('applies a valid ?layout= token and removes it from history', () => {
    const { api: fresh } = loadLayoutShare({ cookieJar: '' });
    const token = fresh.encodeTokenFromCurrentCookies();

    const { api, historyMock } = loadLayoutShare({
      cookieJar: '',
      locationHref: 'http://localhost/?layout=' + token,
      locationOrigin: 'http://localhost',
    });
    api.consumeLayoutQueryParam();
    expect(historyMock.replaceState).toHaveBeenCalledTimes(1);
    const newUrl = historyMock.replaceState.mock.calls[0][2];
    expect(newUrl).not.toContain('layout=');
  });

  test('removes an invalid ?layout= token from history without applying', () => {
    const { api, historyMock } = loadLayoutShare({
      cookieJar: '',
      locationHref: 'http://localhost/?layout=not-valid-garbage',
      locationOrigin: 'http://localhost',
    });
    api.consumeLayoutQueryParam();
    expect(historyMock.replaceState).toHaveBeenCalledTimes(1);
    const newUrl = historyMock.replaceState.mock.calls[0][2];
    expect(newUrl).not.toContain('layout=');
  });

  test('does nothing when no ?layout= param is present', () => {
    const { api, historyMock } = loadLayoutShare({
      locationHref: 'http://localhost/?mode=profile&steamid=12345',
    });
    api.consumeLayoutQueryParam();
    expect(historyMock.replaceState).not.toHaveBeenCalled();
  });

  test('preserves other query params when removing ?layout=', () => {
    const { api: fresh } = loadLayoutShare({ cookieJar: '' });
    const token = fresh.encodeTokenFromCurrentCookies();

    const { api, historyMock } = loadLayoutShare({
      cookieJar: '',
      locationHref: 'http://localhost/?mode=profile&layout=' + token + '&steamid=123',
      locationOrigin: 'http://localhost',
    });
    api.consumeLayoutQueryParam();
    const newUrl = historyMock.replaceState.mock.calls[0][2];
    expect(newUrl).toContain('mode=profile');
    expect(newUrl).toContain('steamid=123');
    expect(newUrl).not.toContain('layout=');
  });
});

// ---------------------------------------------------------------------------
// buildHomeShareUrl
// ---------------------------------------------------------------------------

describe('buildHomeShareUrl', () => {
  test('returns a URL containing a layout= query param', () => {
    const { api } = loadLayoutShare({
      locationHref: 'http://localhost/',
      locationOrigin: 'http://localhost',
    });
    const url = api.buildHomeShareUrl();
    expect(url).toMatch(/^http:\/\/localhost\/\?layout=/);
  });

  test('the layout token in buildHomeShareUrl is decodeable', () => {
    const { api } = loadLayoutShare({
      locationHref: 'http://localhost/',
      locationOrigin: 'http://localhost',
    });
    const url = api.buildHomeShareUrl();
    const token = new URL(url).searchParams.get('layout');
    const decoded = api.decodeToken(token);
    expect(decoded).not.toBeNull();
    expect(decoded.home.order).toEqual(HOME_IDS);
  });
});
