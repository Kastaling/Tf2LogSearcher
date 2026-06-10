const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadAccentPrefs() {
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
      _data: {},
      getItem(key) {
        return Object.prototype.hasOwnProperty.call(this._data, key) ? this._data[key] : null;
      },
      setItem(key, value) {
        this._data[key] = String(value);
      },
    },
    matchMedia: () => ({ matches: false, addEventListener() {} }),
  };
  mockGlobal.window = mockGlobal;
  const src = fs.readFileSync(
    path.resolve(__dirname, '../../static/js/accent-prefs.js'),
    'utf8',
  );
  vm.createContext(mockGlobal);
  vm.runInContext(src, mockGlobal);
  return mockGlobal;
}

module.exports = { loadAccentPrefs };
