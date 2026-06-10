var statsTrendState = { chart: null };
var profileTrendState = { chart: null };
var trendRangeSelectPluginInstalled = false;
var trendRangeSelectPluginId = 'tf2lsTrendRangeSelect';
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

function detachTrendRangeSelect(host) {
  if (host) {
    syncTrendRangeStatus(host, null, null, null);
    host.classList.remove('trend-range-selecting');
    host._trendRangeSorted = null;
  }
  if (!host || !host._trendRangeSelectHandlers) return;
  var h = host._trendRangeSelectHandlers;
  var wrap = host.querySelector('.stats-trend-canvas-wrap');
  var canvas = wrap && wrap.querySelector('.js-trend-chart-canvas');
  if (canvas) canvas.removeEventListener('click', h.onClick);
  if (wrap) wrap.removeEventListener('mousemove', h.onMove);
  if (h.onKey) document.removeEventListener('keydown', h.onKey);
  host._trendRangeSelectHandlers = null;
}

function ensureTrendRangeStatusEl(host) {
  if (!host) return null;
  var el = host.querySelector('.js-trend-range-status');
  if (el) return el;
  el = document.createElement('p');
  el.className = 'stats-trend-range-status js-trend-range-status';
  el.setAttribute('aria-live', 'polite');
  el.hidden = true;
  var note = host.querySelector('.stats-trend-note');
  if (note) host.insertBefore(el, note);
  else host.appendChild(el);
  return el;
}

function trendRowLabelAt(sorted, idx) {
  if (!sorted || idx == null || idx < 0 || idx >= sorted.length) return '';
  var r = sorted[idx];
  var d = r && r.date != null ? String(r.date).trim() : '';
  if (d.length > 96) d = d.slice(0, 96);
  return d;
}

function syncTrendRangeStatus(host, sorted, anchor, hover) {
  var el = ensureTrendRangeStatusEl(host);
  if (!el) return;
  if (anchor == null) {
    el.hidden = true;
    el.textContent = '';
    el.removeAttribute('aria-label');
    return;
  }
  var end = hover != null ? hover : anchor;
  var leftLabel = trendRowLabelAt(sorted, anchor);
  var rightLabel = trendRowLabelAt(sorted, end);
  if (!leftLabel && !rightLabel) {
    el.hidden = true;
    el.textContent = '';
    el.removeAttribute('aria-label');
    return;
  }
  el.hidden = false;
  el.textContent = '';
  var leftSpan = document.createElement('span');
  leftSpan.className = 'stats-trend-range-status-start';
  leftSpan.textContent = leftLabel || '\u2014';
  var midSpan = document.createElement('span');
  midSpan.className = 'stats-trend-range-status-mid';
  midSpan.setAttribute('aria-hidden', 'true');
  midSpan.textContent = '\u2192';
  var rightSpan = document.createElement('span');
  rightSpan.className = 'stats-trend-range-status-end';
  rightSpan.textContent = rightLabel || '\u2014';
  el.appendChild(leftSpan);
  el.appendChild(midSpan);
  el.appendChild(rightSpan);
  el.setAttribute('aria-label', 'Selected range from ' + leftLabel + ' to ' + rightLabel);
}

function setTrendChartTooltipEnabled(chart, enabled) {
  if (!chart || !chart.options || !chart.options.plugins) return;
  if (!chart.options.plugins.tooltip) chart.options.plugins.tooltip = {};
  chart.options.plugins.tooltip.enabled = !!enabled;
}

function clearTrendRangeSelection(host, chart) {
  if (chart && chart.$tf2lsRangeSelect) {
    chart.$tf2lsRangeSelect.anchor = null;
    chart.$tf2lsRangeSelect.hover = null;
  }
  if (host) {
    host.classList.remove('trend-range-selecting');
    syncTrendRangeStatus(host, null, null, null);
  }
  setTrendChartTooltipEnabled(chart, true);
  if (chart) chart.update('none');
}

function getTrendIndexFromEvent(chart, evt) {
  if (!chart || !evt) return -1;
  var pts = chart.getElementsAtEventForMode(evt, 'index', { intersect: false }, false);
  if (pts && pts.length) return pts[0].index;
  var xScale = chart.scales && chart.scales.x;
  if (!xScale || !chart.canvas) return -1;
  var rect = chart.canvas.getBoundingClientRect();
  var x = evt.clientX - rect.left;
  var n = (chart.data && chart.data.labels) ? chart.data.labels.length : 0;
  if (n < 1) return -1;
  var best = 0;
  var bestD = Infinity;
  for (var i = 0; i < n; i++) {
    var px = xScale.getPixelForValue(i);
    if (!Number.isFinite(px)) continue;
    var d = Math.abs(px - x);
    if (d < bestD) {
      bestD = d;
      best = i;
    }
  }
  return best;
}

function ensureTrendRangeSelectPlugin() {
  if (trendRangeSelectPluginInstalled || !window.Chart || !window.Chart.register) return;
  window.Chart.register({
    id: trendRangeSelectPluginId,
    afterDraw: function(chart) {
      var sel = chart.$tf2lsRangeSelect;
      if (!sel || sel.anchor == null) return;
      var end = sel.hover != null ? sel.hover : sel.anchor;
      var lo = Math.min(sel.anchor, end);
      var hi = Math.max(sel.anchor, end);
      var xScale = chart.scales.x;
      if (!xScale || !chart.chartArea) return;
      var x1 = xScale.getPixelForValue(lo);
      var x2 = xScale.getPixelForValue(hi);
      if (!Number.isFinite(x1) || !Number.isFinite(x2)) return;
      var left = Math.min(x1, x2);
      var width = Math.max(2, Math.abs(x2 - x1));
      var top = chart.chartArea.top;
      var height = chart.chartArea.bottom - chart.chartArea.top;
      var ctx = chart.ctx;
      var link = colorVar('--link') || '#2e6c80';
      ctx.save();
      ctx.fillStyle = withAlpha(link, 0.18);
      ctx.fillRect(left, top, width, height);
      ctx.strokeStyle = withAlpha(link, 0.55);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x1, top);
      ctx.lineTo(x1, chart.chartArea.bottom);
      if (hi !== lo) {
        ctx.moveTo(x2, top);
        ctx.lineTo(x2, chart.chartArea.bottom);
      }
      ctx.stroke();
      var text = colorVar('--text') || '#222';
      var anchorIdx = sel.anchor;
      var hoverIdx = sel.hover != null ? sel.hover : sel.anchor;
      var labels = chart.data && chart.data.labels;
      var anchorLabel = labels && labels[anchorIdx] != null ? String(labels[anchorIdx]) : '';
      var hoverLabel = labels && labels[hoverIdx] != null ? String(labels[hoverIdx]) : '';
      if (anchorLabel.length > 48) anchorLabel = anchorLabel.slice(0, 48) + '\u2026';
      if (hoverLabel.length > 48) hoverLabel = hoverLabel.slice(0, 48) + '\u2026';
      var fontSize = 11;
      ctx.font = fontSize + 'px sans-serif';
      ctx.fillStyle = text;
      ctx.textBaseline = 'bottom';
      var pad = 4;
      var y = chart.chartArea.bottom - 2;
      if (anchorLabel) {
        ctx.textAlign = 'left';
        var ax = xScale.getPixelForValue(anchorIdx);
        if (Number.isFinite(ax)) {
          var axClamped = Math.max(chart.chartArea.left + pad, Math.min(ax, chart.chartArea.right - pad));
          ctx.fillText(anchorLabel, axClamped, y);
        }
      }
      if (hoverLabel && hoverIdx !== anchorIdx) {
        ctx.textAlign = 'right';
        var hx = xScale.getPixelForValue(hoverIdx);
        if (Number.isFinite(hx)) {
          var hxClamped = Math.max(chart.chartArea.left + pad, Math.min(hx, chart.chartArea.right - pad));
          ctx.fillText(hoverLabel, hxClamped, y);
        }
      }
      ctx.restore();
    }
  });
  trendRangeSelectPluginInstalled = true;
}

function bindTrendRangeSelect(state, host, sorted, drillCtx) {
  detachTrendRangeSelect(host);
  if (!host || !drillCtx || !drillCtx.steamid || !sorted || sorted.length < 2) return;
  var wrap = host.querySelector('.stats-trend-canvas-wrap');
  var canvas = wrap && wrap.querySelector('.js-trend-chart-canvas');
  if (!wrap || !canvas) return;
  host._trendRangeSorted = sorted;

  var onClick = function(evt) {
    var chart = state.chart;
    if (!chart) return;
    var idx = getTrendIndexFromEvent(chart, evt);
    if (idx < 0) return;
    if (!chart.$tf2lsRangeSelect) chart.$tf2lsRangeSelect = { anchor: null, hover: null };
    var sel = chart.$tf2lsRangeSelect;
    if (sel.anchor == null) {
      sel.anchor = idx;
      sel.hover = idx;
      host.classList.add('trend-range-selecting');
      setTrendChartTooltipEnabled(chart, false);
      syncTrendRangeStatus(host, sorted, sel.anchor, sel.hover);
      chart.update('none');
      return;
    }
    var url = typeof buildTrendStatsSorterResultsUrl === 'function'
      ? buildTrendStatsSorterResultsUrl(drillCtx.steamid, sorted, sel.anchor, idx, {
        gamemode: drillCtx.gamemode,
        map_query: drillCtx.map_query,
        classes: drillCtx.classes
      })
      : null;
    clearTrendRangeSelection(host, chart);
    if (url && url.indexOf('/results?') === 0) {
      window.location.href = url;
    }
  };

  var onMove = function(evt) {
    var chart = state.chart;
    if (!chart || !chart.$tf2lsRangeSelect || chart.$tf2lsRangeSelect.anchor == null) return;
    var idx = getTrendIndexFromEvent(chart, evt);
    if (idx < 0 || chart.$tf2lsRangeSelect.hover === idx) return;
    chart.$tf2lsRangeSelect.hover = idx;
    syncTrendRangeStatus(host, sorted, chart.$tf2lsRangeSelect.anchor, idx);
    chart.update('none');
  };

  var onKey = function(evt) {
    if (evt.key !== 'Escape') return;
    var chart = state.chart;
    if (!chart || !chart.$tf2lsRangeSelect || chart.$tf2lsRangeSelect.anchor == null) return;
    clearTrendRangeSelection(host, chart);
  };

  canvas.addEventListener('click', onClick);
  wrap.addEventListener('mousemove', onMove);
  document.addEventListener('keydown', onKey);
  host._trendRangeSelectHandlers = { onClick: onClick, onMove: onMove, onKey: onKey };
}

function destroyStatsTrendChart() {
  if (statsTrendHost) detachTrendRangeSelect(statsTrendHost);
  destroyTrendState(statsTrendState);
}

function destroyProfileTrendChart() {
  if (profileTrendHost) detachTrendRangeSelect(profileTrendHost);
  destroyTrendState(profileTrendState);
}

function renderTrendChartShared(state, host, rows, metric, shouldAbort, drillCtx) {
  if (!host || !rows || rows.length < 2) return;
  var canvas = host.querySelector('.js-trend-chart-canvas');
  if (!canvas) return;
  var sorted = statsTrendSortRows(rows);
  var labels = sorted.map(function(r) { return (r && r.date) ? String(r.date) : ''; });
  var yBounds = computeTrendYBounds(sorted, metric);
  var datasets = buildTrendDatasets(sorted, metric, yBounds);
  var showLegend = metric === 'kpair';
  detachTrendRangeSelect(host);
  destroyTrendState(state);
  loadChartJsIfNeeded().then(function() {
    if (shouldAbort()) return;
    ensureTrendRangeSelectPlugin();
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
    if (shouldAbort()) {
      destroyTrendState(state);
      return;
    }
    bindTrendRangeSelect(state, host, sorted, drillCtx);
  }).catch(function() {});
}

function renderStatsTrendChart(host, rows, metric) {
  renderTrendChartShared(statsTrendState, host, rows, metric, function() {
    return statsTrendHost !== host;
  });
}

function renderProfileTrendChart(host, rows, metric) {
  var drillCtx = host && host._trendDrillContext ? host._trendDrillContext : null;
  renderTrendChartShared(profileTrendState, host, rows, metric, function() {
    return profileTrendHost !== host;
  }, drillCtx);
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
