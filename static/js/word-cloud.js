/**
 * Horizontal word-cloud layout for profile favorite words (no external deps).
 * Words are placed largest-first on a spiral; sizes reflect count only (order is visual).
 */

function profileWordCloudFontFamily() {
  try {
    if (typeof document !== 'undefined' && document.body) {
      return getComputedStyle(document.body).fontFamily || 'sans-serif';
    }
  } catch (e) {}
  return 'sans-serif';
}

var _profileWordCloudMeasureCtx = null;

function profileWordCloudMeasureCtx() {
  if (_profileWordCloudMeasureCtx) {
    return _profileWordCloudMeasureCtx;
  }
  if (typeof document === 'undefined') {
    return null;
  }
  var canvas = document.createElement('canvas');
  _profileWordCloudMeasureCtx = canvas.getContext('2d');
  return _profileWordCloudMeasureCtx;
}

/**
 * @param {string} text
 * @param {number} fontSizePx
 * @returns {{ width: number, height: number }}
 */
function profileWordCloudMeasure(text, fontSizePx) {
  var ctx = profileWordCloudMeasureCtx();
  var size = Math.max(10, fontSizePx);
  if (!ctx) {
    return { width: Math.max(8, String(text).length * size * 0.55), height: size * 1.2 };
  }
  ctx.font = '600 ' + size + 'px ' + profileWordCloudFontFamily();
  var m = ctx.measureText(text);
  var w = Math.ceil(m.width);
  var h = Math.ceil((m.actualBoundingBoxAscent + m.actualBoundingBoxDescent) || size * 1.15);
  return { width: w, height: h };
}

function profileWordCloudRectsOverlap(a, b, pad) {
  return !(
    a.x + a.w + pad <= b.x ||
    b.x + b.w + pad <= a.x ||
    a.y + a.h + pad <= b.y ||
    b.y + b.h + pad <= a.y
  );
}

function profileWordCloudFits(rect, placed, pad) {
  for (var i = 0; i < placed.length; i++) {
    if (profileWordCloudRectsOverlap(rect, placed[i], pad)) {
      return false;
    }
  }
  return true;
}

/**
 * Font size from rank spread: #1 count → max font, lowest shown count → min font.
 * @param {number} count
 * @param {number} minCount
 * @param {number} maxCount
 * @param {number} minFont
 * @param {number} maxFont
 * @returns {number}
 */
function profileWordCloudItemTitle(word, count, rank) {
  var w = String(word || '');
  var display = w ? w.charAt(0).toUpperCase() + w.slice(1) : w;
  var countStr = Number(count).toLocaleString();
  if (rank != null && rank > 0) {
    return '#' + rank + '. ' + display + ': ' + countStr + ' uses';
  }
  return display + ': ' + countStr + ' uses';
}

function profileWordCloudFontSize(count, minCount, maxCount, minFont, maxFont) {
  var c = Number(count);
  var minC = Number(minCount);
  var maxC = Number(maxCount);
  if (!Number.isFinite(c) || c < 0) {
    c = 0;
  }
  var lo = Math.min(minFont, maxFont);
  var hi = Math.max(minFont, maxFont);
  if (!Number.isFinite(minC) || !Number.isFinite(maxC) || maxC <= 0) {
    return Math.round((lo + hi) / 2);
  }
  if (maxC < minC) {
    var swap = maxC;
    maxC = minC;
    minC = swap;
  }
  var span = maxC - minC;
  if (span <= 0) {
    return Math.round((lo + hi) / 2);
  }
  var norm = Math.max(0, Math.min(1, (c - minC) / span));
  return Math.round(lo + norm * (hi - lo));
}

/**
 * @param {Array<{word: string, count: number}>} items — any order; sorted by count for placement
 * @param {number} boxWidth
 * @param {number} boxHeight
 * @returns {{ placements: Array<object>, width: number, height: number }}
 */
function layoutProfileWordCloud(items, boxWidth, boxHeight) {
  var pad = 6;
  var sorted = items.slice().sort(function(a, b) {
    return b.count - a.count;
  });
  var maxCount = Math.max(1, sorted[0] ? sorted[0].count : 1);
  var minCount = sorted.length ? sorted[sorted.length - 1].count : maxCount;
  var minFont = 13;
  var maxFont = Math.min(52, Math.max(28, Math.floor(boxWidth / 9)));

  var specs = sorted.map(function(w, i) {
    var fontSize = profileWordCloudFontSize(w.count, minCount, maxCount, minFont, maxFont);
    var dim = profileWordCloudMeasure(w.word, fontSize);
    return {
      word: w.word,
      count: w.count,
      rank: i + 1,
      fontSize: fontSize,
      width: dim.width,
      height: dim.height
    };
  });

  var cx = boxWidth / 2;
  var cy = boxHeight / 2;
  var placedRects = [];
  var placements = [];
  var maxAttempts = 2400;

  specs.forEach(function(spec) {
    var placed = false;
    for (var i = 0; i < maxAttempts; i++) {
      var angle = i * 0.38;
      var radius = 4 + Math.sqrt(i) * 8;
      var x = cx + radius * Math.cos(angle) - spec.width / 2;
      var y = cy + radius * Math.sin(angle) - spec.height / 2;
      var rect = { x: x, y: y, w: spec.width, h: spec.height };
      if (x >= -4 && y >= -4 && x + spec.width <= boxWidth + 4 && y + spec.height <= boxHeight + 4) {
        if (profileWordCloudFits(rect, placedRects, pad)) {
          placedRects.push(rect);
          placements.push({
            word: spec.word,
            count: spec.count,
            rank: spec.rank,
            fontSize: spec.fontSize,
            x: x,
            y: y,
            width: spec.width,
            height: spec.height
          });
          placed = true;
          break;
        }
      }
    }
    if (!placed) {
      var fallbackX = Math.max(0, cx - spec.width / 2);
      var fallbackY = boxHeight - spec.height - placedRects.length * 2;
      placements.push({
        word: spec.word,
        count: spec.count,
        rank: spec.rank,
        fontSize: spec.fontSize,
        x: fallbackX,
        y: Math.max(0, fallbackY),
        width: spec.width,
        height: spec.height
      });
      placedRects.push({
        x: fallbackX,
        y: Math.max(0, fallbackY),
        w: spec.width,
        h: spec.height
      });
    }
  });

  var padTop = 12;
  var padBottom = 40;
  var padSide = 16;
  var boundsW = boxWidth;
  var boundsH = Math.max(140, boxHeight);
  if (placements.length) {
    var minX = Infinity;
    var minY = Infinity;
    var maxX = 0;
    var maxY = 0;
    placements.forEach(function(p) {
      minX = Math.min(minX, p.x);
      minY = Math.min(minY, p.y);
      maxX = Math.max(maxX, p.x + p.width);
      maxY = Math.max(maxY, p.y + p.height);
    });
    var shiftX = padSide - minX;
    var shiftY = padTop - minY;
    placements.forEach(function(p) {
      p.x += shiftX;
      p.y += shiftY;
    });
    boundsW = Math.ceil(maxX - minX) + padSide * 2;
    boundsH = Math.max(140, Math.ceil(maxY - minY) + padTop + padBottom);
  }

  return { placements: placements, width: boundsW, height: boundsH };
}

/**
 * Build a positioned word cloud inside `host`. Calls `linkHover(word|null)` on hover changes.
 * @param {HTMLElement} host
 * @param {Array<{word: string, count: number, pct: number}>} items
 * @param {{ linkHover?: function(string|null): void }} options
 */
function mountProfileWordCloud(host, items, options) {
  if (!host || !items || !items.length) {
    return;
  }
  options = options || {};
  host.innerHTML = '';
  host.className = 'profile-word-cloud-host profile-word-cloud';
  host.setAttribute('role', 'img');
  host.setAttribute(
    'aria-label',
    'Word cloud of most-used chat words. Hover a word to highlight its row in the table below.'
  );

  var layoutWidth = Math.max(280, host.clientWidth || host.offsetWidth || 480);
  var layoutHeight = Math.max(180, Math.min(380, 110 + items.length * 12));
  var layout = layoutProfileWordCloud(items, layoutWidth, layoutHeight);

  host.style.width = '100%';
  host.style.minHeight = layout.height + 'px';
  host.style.maxHeight = 'none';

  var inner = document.createElement('div');
  inner.className = 'profile-word-cloud-inner';
  inner.style.width = layout.width + 'px';
  inner.style.height = layout.height + 'px';
  host.appendChild(inner);

  var activeWord = null;

  function setActive(word) {
    if (activeWord === word) {
      return;
    }
    activeWord = word;
    if (typeof options.linkHover === 'function') {
      options.linkHover(word);
    }
    inner.querySelectorAll('.profile-word-cloud-item').forEach(function(el) {
      var on = word && el.getAttribute('data-word') === word;
      el.classList.toggle('is-linked-hover', !!on);
      el.setAttribute('aria-current', on ? 'true' : 'false');
    });
  }

  layout.placements.forEach(function(p) {
    var el = document.createElement('span');
    el.className = 'profile-word-cloud-item';
    el.setAttribute('role', 'button');
    el.setAttribute('tabindex', '0');
    el.setAttribute('data-word', p.word);
    el.style.left = p.x + 'px';
    el.style.top = p.y + 'px';
    el.style.fontSize = p.fontSize + 'px';
    el.textContent = p.word;
    el.title = profileWordCloudItemTitle(p.word, p.count, p.rank);
    el.addEventListener('mouseenter', function() {
      setActive(p.word);
    });
    el.addEventListener('mouseleave', function() {
      setActive(null);
    });
    el.addEventListener('focus', function() {
      setActive(p.word);
    });
    el.addEventListener('blur', function() {
      setActive(null);
    });
    el.addEventListener('keydown', function(ev) {
      if (ev.key === 'Enter' || ev.key === ' ') {
        ev.preventDefault();
        setActive(p.word);
      }
    });
    inner.appendChild(el);
  });

  inner.addEventListener('mouseleave', function() {
    setActive(null);
  });

  host._profileWordCloudSetActive = setActive;
}

/**
 * Mount when the host has measurable width (e.g. after a collapsed profile section opens).
 * Re-layouts on resize via ResizeObserver (debounced).
 */
function scheduleProfileWordCloudMount(host, items, options) {
  if (!host || !items || !items.length) {
    return;
  }
  host._profileWordCloudWords = items;
  host._profileWordCloudOptions = options || {};

  function remountIfSized() {
    var w = host.getBoundingClientRect().width;
    if (w < 48) {
      return;
    }
    mountProfileWordCloud(host, host._profileWordCloudWords, host._profileWordCloudOptions);
  }

  if (host._profileWordCloudResizeObserver) {
    host._profileWordCloudResizeObserver.disconnect();
    host._profileWordCloudResizeObserver = null;
  }
  if (host._profileWordCloudResizeRaf) {
    cancelAnimationFrame(host._profileWordCloudResizeRaf);
    host._profileWordCloudResizeRaf = 0;
  }

  remountIfSized();

  if (typeof ResizeObserver !== 'undefined') {
    var lastWidth = 0;
    var ro = new ResizeObserver(function() {
      var w = host.getBoundingClientRect().width;
      if (w < 48) {
        return;
      }
      if (lastWidth > 0 && Math.abs(w - lastWidth) < 12) {
        return;
      }
      lastWidth = w;
      if (host._profileWordCloudResizeRaf) {
        cancelAnimationFrame(host._profileWordCloudResizeRaf);
      }
      host._profileWordCloudResizeRaf = requestAnimationFrame(function() {
        host._profileWordCloudResizeRaf = 0;
        remountIfSized();
      });
    });
    ro.observe(host);
    var panel = host.closest('.profile-section-panel');
    if (panel) {
      ro.observe(panel);
    }
    host._profileWordCloudResizeObserver = ro;
  } else if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
    var onResize = function() {
      remountIfSized();
    };
    if (host._profileWordCloudWindowResize) {
      window.removeEventListener('resize', host._profileWordCloudWindowResize);
    }
    host._profileWordCloudWindowResize = onResize;
    window.addEventListener('resize', onResize);
  } else if (typeof requestAnimationFrame === 'function') {
    requestAnimationFrame(remountIfSized);
  }
}
