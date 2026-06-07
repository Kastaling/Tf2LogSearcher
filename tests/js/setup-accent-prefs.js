const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadAccentPrefs() {
  const mockGlobal = {
    document: {
      documentElement: {
        getAttribute: () => 'light',
        style: { setProperty() {}, removeProperty() {} },
        dataset: {},
      },
      cookie: '',
    },
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
