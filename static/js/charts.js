var statsTrendState = { chart: null };
var profileTrendState = { chart: null };
var statsTrendHost = null;
var statsTrendRows = null;
var statsTrendMetric = 'dpm';
var profileTrendHost = null;
var profileTrendRows = null;
var profileTrendMetric = 'dpm';
var chartJsLoadPromise = null;

function loadChartJsIfNeeded() {
  if (window.Chart) return Promise.resolve(window.Chart);
  if (chartJsLoadPromise) return chartJsLoadPromise;
  chartJsLoadPromise = new Promise(function(resolve, reject) {
    var s = document.createElement('script');
    s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js';
    s.async = true;
    s.onload = function() {
      if (window.Chart) resolve(window.Chart);
      else reject(new Error('Chart.js did not load.'));
    };
    s.onerror = function() { reject(new Error('Failed to load chart library.')); };
    document.head.appendChild(s);
  });
  return chartJsLoadPromise;
}

function statsTrendSortRows(rows) {
  return rows.slice().sort(function(a, b) {
    var ta = parseStatsDate(a && a.date);
    var tb = parseStatsDate(b && b.date);
    if (Number.isNaN(ta) && Number.isNaN(tb)) return 0;
    if (Number.isNaN(ta)) return 1;
    if (Number.isNaN(tb)) return -1;
    return ta - tb;
  });
}

function rollingAverage(values, windowSize) {
  var out = [];
  var q = [];
  for (var i = 0; i < values.length; i++) {
    var v = Number(values[i]);
    if (!Number.isFinite(v)) v = NaN;
    q.push(v);
    if (q.length > windowSize) q.shift();
    var sum = 0;
    var count = 0;
    for (var j = 0; j < q.length; j++) {
      if (!Number.isFinite(q[j])) continue;
      sum += q[j];
      count += 1;
    }
    out.push(count > 0 ? (sum / count) : NaN);
  }
  return out;
}

/** Bounds from 2nd–98th percentile; used to clamp plotted Y values (Chart.js always expands the axis to data). */
function computeTrendYBounds(rowsSorted, metric) {
  var vals = [];
  if (metric === 'dpm') {
    for (var i = 0; i < rowsSorted.length; i++) {
      var v = Number(rowsSorted[i] && rowsSorted[i].dpm);
      if (Number.isFinite(v)) vals.push(v);
    }
  } else if (metric === 'deaths') {
    for (var id = 0; id < rowsSorted.length; id++) {
      var vd = Number(rowsSorted[id] && rowsSorted[id].deaths);
      if (Number.isFinite(vd)) vals.push(vd);
    }
  } else {
    for (var j = 0; j < rowsSorted.length; j++) {
      var r = rowsSorted[j];
      var a = Number(r && r.kdr);
      var b = Number(r && r.kadr);
      if (Number.isFinite(a)) vals.push(a);
      if (Number.isFinite(b)) vals.push(b);
    }
  }
  if (vals.length < 4) return null;
  vals.sort(function(a, b) { return a - b; });
  var n = vals.length;
  var loI = Math.max(0, Math.floor(n * 0.02));
  var hiI = Math.min(n - 1, Math.ceil(n * 0.98) - 1);
  if (hiI <= loI) return null;
  var lo = vals[loI];
  var hi = vals[hiI];
  var span = hi - lo;
  if (!Number.isFinite(span) || span <= 0) {
    span = Math.max(Math.abs(hi), Math.abs(lo), 1e-9) * 0.15;
  }
  var pad = span * 0.08;
  lo = lo - pad;
  hi = hi + pad;
  if (metric === 'dpm' || metric === 'deaths') {
    lo = Math.max(0, lo);
  }
  if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi <= lo) return null;
  return { min: lo, max: hi };
}

function clampTrendY(v, bounds) {
  if (bounds == null || !Number.isFinite(v)) return v;
  return Math.min(bounds.max, Math.max(bounds.min, v));
}

function formatTrendTooltipValue(v) {
  if (!Number.isFinite(v)) return '\u2014';
  var a = Math.abs(v);
  if (a >= 1000) return String(Math.round(v));
  if (a >= 100) return String(Math.round(v));
  return (Math.round(v * 100) / 100).toString();
}

function colorVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function withAlpha(color, alpha) {
  if (!color) return 'rgba(128,128,128,' + alpha + ')';
  if (color.indexOf('rgb(') === 0) {
    var m = color.replace('rgb(', '').replace(')', '').split(',');
    if (m.length === 3) return 'rgba(' + m[0].trim() + ',' + m[1].trim() + ',' + m[2].trim() + ',' + alpha + ')';
  }
  if (/^#([0-9a-f]{3}){1,2}$/i.test(color)) {
    var c = color.slice(1);
    if (c.length === 3) c = c[0] + c[0] + c[1] + c[1] + c[2] + c[2];
    var r = parseInt(c.slice(0, 2), 16);
    var g = parseInt(c.slice(2, 4), 16);
    var b = parseInt(c.slice(4, 6), 16);
    return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
  }
  return color;
}

function buildTrendDatasets(rowsSorted, metric, yBounds) {
  var link = colorVar('--link') || '#2e6c80';
  var kdrBlue = '#547d8c';
  var kadrRed = '#a7584b';
  var useRolling = rowsSorted.length >= 5;
  var out = [];
  function addMetric(label, key, col) {
    var raw = rowsSorted.map(function(r) {
      var n = Number(r && r[key]);
      return Number.isFinite(n) ? n : NaN;
    });
    var rawPlot = yBounds ? raw.map(function(v) { return clampTrendY(v, yBounds); }) : raw;
    if (!useRolling) {
      out.push({
        label: label,
        data: rawPlot,
        _tooltipY: raw,
        showLine: false,
        pointRadius: 2.4,
        pointHoverRadius: 3,
        pointBackgroundColor: withAlpha(col, 0.55),
        pointBorderColor: withAlpha(col, 0.55),
        borderWidth: 0,
        order: 1
      });
      return;
    }
    var roll = rollingAverage(raw, 20);
    var rollPlot = yBounds ? roll.map(function(v) { return clampTrendY(v, yBounds); }) : roll;
    out.push({
      label: label + ' (raw)',
      data: rawPlot,
      _tooltipY: raw,
      showLine: false,
      pointRadius: 2,
      pointHoverRadius: 2,
      pointBackgroundColor: withAlpha(col, 0.22),
      pointBorderColor: withAlpha(col, 0.22),
      borderWidth: 0,
      order: 0
    });
    out.push({
      label: label + ' (20-game avg)',
      data: rollPlot,
      _tooltipY: roll,
      borderColor: col,
      backgroundColor: withAlpha(col, 0.18),
      pointRadius: 0,
      pointHoverRadius: 3,
      borderWidth: 2,
      tension: 0.2,
      spanGaps: true,
      order: 1
    });
  }
  if (metric === 'dpm') {
    addMetric('DPM', 'dpm', link);
  } else if (metric === 'deaths') {
    addMetric('Deaths', 'deaths', '#a86d5c');
  } else {
    addMetric('KDR', 'kdr', kdrBlue);
    addMetric('KADR', 'kadr', kadrRed);
  }
  return out;
}

function destroyTrendState(state) {
  if (!state || !state.chart) return;
  try { state.chart.destroy(); } catch (e) {}
  state.chart = null;
}

function destroyStatsTrendChart() {
  destroyTrendState(statsTrendState);
}

function destroyProfileTrendChart() {
  destroyTrendState(profileTrendState);
}

function renderTrendChartShared(state, host, rows, metric, shouldAbort) {
  if (!host || !rows || rows.length < 2) return;
  var canvas = host.querySelector('.js-trend-chart-canvas');
  if (!canvas) return;
  var sorted = statsTrendSortRows(rows);
  var labels = sorted.map(function(r) { return (r && r.date) ? String(r.date) : ''; });
  var yBounds = computeTrendYBounds(sorted, metric);
  var datasets = buildTrendDatasets(sorted, metric, yBounds);
  var showLegend = metric === 'kpair';
  destroyTrendState(state);
  loadChartJsIfNeeded().then(function() {
    if (shouldAbort()) return;
    var border = colorVar('--border') || '#ccc';
    var text = colorVar('--text') || '#222';
    var textMuted = colorVar('--text-muted') || '#666';
    state.chart = new window.Chart(canvas.getContext('2d'), {
      type: 'line',
      data: { labels: labels, datasets: datasets },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        normalized: true,
        plugins: {
          legend: { display: showLegend, labels: { color: text, boxWidth: 14, usePointStyle: true } },
          tooltip: {
            mode: 'index',
            intersect: false,
            callbacks: {
              label: function(ctx) {
                var ds = ctx.dataset;
                var arr = ds._tooltipY;
                var y = arr && arr[ctx.dataIndex];
                if (!Number.isFinite(y)) y = ctx.parsed.y;
                var lab = ds.label != null ? String(ds.label) : '';
                return lab ? (lab + ': ' + formatTrendTooltipValue(y)) : formatTrendTooltipValue(y);
              }
            }
          }
        },
        scales: {
          x: { ticks: { color: textMuted, maxRotation: 0, autoSkip: true, maxTicksLimit: 8 }, grid: { color: withAlpha(border, 0.35) } },
          y: { ticks: { color: textMuted }, grid: { color: withAlpha(border, 0.35) } }
        }
      }
    });
  }).catch(function() {});
}

function renderStatsTrendChart(host, rows, metric) {
  renderTrendChartShared(statsTrendState, host, rows, metric, function() {
    return statsTrendHost !== host;
  });
}

function renderProfileTrendChart(host, rows, metric) {
  renderTrendChartShared(profileTrendState, host, rows, metric, function() {
    return profileTrendHost !== host;
  });
}

function bindStatsTrendControls(container) {
  var trend = container.querySelector('.js-stats-trend');
  if (!trend) return;
  var btns = trend.querySelectorAll('.js-stats-trend-btn');
  btns.forEach(function(btn) {
    btn.addEventListener('click', function() {
      var metric = btn.getAttribute('data-metric');
      if (metric !== 'dpm' && metric !== 'kpair') return;
      statsTrendMetric = metric;
      btns.forEach(function(b) {
        var active = b.getAttribute('data-metric') === metric;
        b.classList.toggle('active', active);
        b.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      renderStatsTrendChart(trend, statsTrendRows, statsTrendMetric);
    });
  });
}

function refreshStatsTrendChart() {
  if (!statsTrendHost || !statsTrendRows || statsTrendRows.length < 2) return;
  renderStatsTrendChart(statsTrendHost, statsTrendRows, statsTrendMetric);
}

function refreshProfileTrendChart() {
  if (!profileTrendHost || !profileTrendRows || profileTrendRows.length < 2) return;
  renderProfileTrendChart(profileTrendHost, profileTrendRows, profileTrendMetric);
}

function bindProfileTrendControls(container) {
  var trend = container.querySelector('.js-profile-trend');
  if (!trend) return;
  var btns = trend.querySelectorAll('.js-profile-trend-btn');
  btns.forEach(function(btn) {
    btn.addEventListener('click', function() {
      var metric = btn.getAttribute('data-metric');
      if (metric !== 'dpm' && metric !== 'kpair' && metric !== 'deaths') return;
      profileTrendMetric = metric;
      btns.forEach(function(b) {
        var active = b.getAttribute('data-metric') === metric;
        b.classList.toggle('active', active);
        b.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      renderProfileTrendChart(trend, profileTrendRows, profileTrendMetric);
    });
  });
}
