const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadWordCloud() {
  const mockGlobal = {
    document: {
      body: { style: {} },
      createElement: () => ({ getContext: () => null })
    },
    getComputedStyle: () => ({ fontFamily: 'sans-serif' })
  };
  const src = fs.readFileSync(
    path.resolve(__dirname, '../../static/js/word-cloud.js'),
    'utf8'
  );
  vm.createContext(mockGlobal);
  vm.runInContext(src, mockGlobal);
  return mockGlobal;
}

describe('profileWordCloudItemTitle', () => {
  test('includes rank and capitalized word', () => {
    const g = loadWordCloud();
    expect(g.profileWordCloudItemTitle('have', 163, 4)).toBe('#4. Have: 163 uses');
  });
});

describe('profileWordCloudFontSize', () => {
  test('maps rank #1 and lowest count to font bounds', () => {
    const g = loadWordCloud();
    const minFont = 13;
    const maxFont = 52;
    expect(g.profileWordCloudFontSize(419, 99, 419, minFont, maxFont)).toBe(maxFont);
    expect(g.profileWordCloudFontSize(99, 99, 419, minFont, maxFont)).toBe(minFont);
    expect(g.profileWordCloudFontSize(436, 62, 436, minFont, maxFont)).toBe(maxFont);
    expect(g.profileWordCloudFontSize(62, 62, 436, minFont, maxFont)).toBe(minFont);
  });
});

describe('scheduleProfileWordCloudMount', () => {
  test('attaches ResizeObserver and disconnect cleans up', () => {
    const g = loadWordCloud();
    const observed = [];
    class MockResizeObserver {
      constructor(cb) {
        this._cb = cb;
      }
      observe(el) {
        observed.push(el);
      }
      disconnect() {
        observed.length = 0;
      }
    }
    function makeEl() {
      return {
        className: '',
        style: {},
        innerHTML: '',
        children: [],
        setAttribute() {},
        getAttribute() {
          return null;
        },
        appendChild(child) {
          this.children.push(child);
        },
        addEventListener() {},
        querySelectorAll() {
          return [];
        },
      };
    }
    g.ResizeObserver = MockResizeObserver;
    g.requestAnimationFrame = (fn) => {
      fn();
      return 1;
    };
    g.cancelAnimationFrame = () => {};
    g.document.createElement = () => makeEl();

    const host = makeEl();
    host.getBoundingClientRect = () => ({ width: 420 });
    host.clientWidth = 420;
    host.offsetWidth = 420;
    host.closest = () => null;

    g.scheduleProfileWordCloudMount(host, [{ word: 'gg', count: 10 }], {});
    expect(host._profileWordCloudResizeObserver).toBeDefined();
    expect(observed).toContain(host);

    g.disconnectProfileWordCloudMount(host);
    expect(host._profileWordCloudResizeObserver).toBeNull();
    expect(observed.length).toBe(0);
  });
});

describe('layoutProfileWordCloud', () => {
  test('spreads sizes from top rank to lowest in the set', () => {
    const g = loadWordCloud();
    const items = [
      { word: 'have', count: 419 },
      { word: 'what', count: 317 },
      { word: 'fuck', count: 99 }
    ];
    const layout = g.layoutProfileWordCloud(items, 420, 200);
    const byWord = Object.fromEntries(layout.placements.map((p) => [p.word, p.fontSize]));
    expect(byWord.have).toBeGreaterThan(byWord.what);
    expect(byWord.what).toBeGreaterThan(byWord.fuck);
    expect(byWord.have - byWord.fuck).toBeGreaterThanOrEqual(25);
  });

  test('tooltip rank follows count order when input is unsorted', () => {
    const g = loadWordCloud();
    const items = [
      { word: 'low', count: 50, rank: 1 },
      { word: 'top', count: 400, rank: 2 },
      { word: 'mid', count: 200, rank: 3 }
    ];
    const layout = g.layoutProfileWordCloud(items, 420, 200);
    const top = layout.placements.find((p) => p.word === 'top');
    expect(top).toBeDefined();
    expect(top.rank).toBe(1);
    expect(g.profileWordCloudItemTitle(top.word, top.count, top.rank)).toBe(
      '#1. Top: 400 uses'
    );
  });

  test('places words without overlap', () => {
    const g = loadWordCloud();
    const items = [
      { word: 'gg', count: 400 },
      { word: 'lol', count: 300 },
      { word: 'nice', count: 200 },
      { word: 'ping', count: 100 }
    ];
    const layout = g.layoutProfileWordCloud(items, 420, 200);
    expect(layout.placements.length).toBe(items.length);
    const pad = 6;
    for (let i = 0; i < layout.placements.length; i++) {
      const a = layout.placements[i];
      for (let j = i + 1; j < layout.placements.length; j++) {
        const b = layout.placements[j];
        const overlap = !(
          a.x + a.width + pad <= b.x ||
          b.x + b.width + pad <= a.x ||
          a.y + a.height + pad <= b.y ||
          b.y + b.height + pad <= a.y
        );
        expect(overlap).toBe(false);
      }
    }
  });
});
