const fs = require('fs');
const path = require('path');
const vm = require('vm');

function loadTrendDrillUtils() {
  const mockGlobal = {
    URLSearchParams,
  };
  const src = fs.readFileSync(
    path.resolve(__dirname, '../../static/js/utils.js'),
    'utf8'
  );
  vm.createContext(mockGlobal);
  vm.runInContext(src, mockGlobal);
  return mockGlobal;
}

describe('trend drill-down helpers', () => {
  const rows = [
    { date: '10:00:00 AM UTC 05/10/2022', dpm: 200 },
    { date: '11:00:00 AM UTC 05/16/2022', dpm: 305 },
    { date: '02:32:33 AM UTC 05/20/2022', dpm: 180 },
  ];

  test('trendRowDateToIsoDate parses UTC calendar date', () => {
    const g = loadTrendDrillUtils();
    expect(g.trendRowDateToIsoDate('02:32:33 AM UTC 05/16/2022')).toBe('2022-05-16');
    expect(g.trendRowDateToIsoDate('')).toBe('');
    expect(g.trendRowDateToIsoDate('not a date')).toBe('');
  });

  test('orderedTrendIsoDatesFromIndices orders indices and dates', () => {
    const g = loadTrendDrillUtils();
    expect(g.orderedTrendIsoDatesFromIndices(rows, 2, 0)).toEqual({
      date_from: '2022-05-10',
      date_to: '2022-05-20',
    });
    expect(g.orderedTrendIsoDatesFromIndices(rows, 1, 1)).toEqual({
      date_from: '2022-05-16',
      date_to: '2022-05-16',
    });
  });

  test('buildTrendStatsSorterResultsUrl builds same-origin stats link', () => {
    const g = loadTrendDrillUtils();
    const url = g.buildTrendStatsSorterResultsUrl('76561198000000000', rows, 0, 2, {
      gamemode: 'hl',
      map_query: 'process',
    });
    expect(url).toMatch(/^\/results\?/);
    const params = new URLSearchParams(url.split('?')[1]);
    expect(params.get('mode')).toBe('stats');
    expect(params.get('steamid')).toBe('76561198000000000');
    expect(params.get('date_from')).toBe('2022-05-10');
    expect(params.get('date_to')).toBe('2022-05-20');
    expect(params.get('gamemode')).toBe('hl');
    expect(params.get('map_query')).toBe('process');
  });

  test('buildTrendStatsSorterResultsUrl rejects missing steamid', () => {
    const g = loadTrendDrillUtils();
    expect(g.buildTrendStatsSorterResultsUrl('', rows, 0, 1)).toBeNull();
  });
});
