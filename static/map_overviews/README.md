# Map overview images

Top-down PNGs for heatmaps and kill/death overlays. Served at:

`/static/map_overviews/<slug>.png`

Examples: `gullywash.png`, `ashville.png`, `process.png`. The **slug** is usually the canonical map name with the gamemode prefix removed (`cp_gullywash` → `gullywash`), matching `_map_canonical_key` in `app/search/search.py`.

`available_slugs.json` lists overview PNGs present in this directory.

## World coordinates → image pixels

Kill/death positions in **raw server logs** are Hammer world units `(x, y, z)` — see `app/raw_log_parser.py` (`victim_position`, `attacker_position`) and `raw_events.db` table `kill_events` (`victim_x`, `victim_y`, `victim_z`, etc.).

For a **top-down 2D heatmap**, use horizontal axes only (usually **x** and **y**; ignore **z** or filter by height band).

### 1. Empirical bounds (good first step)

For a given map, aggregate positions from indexed raw logs:

```sql
SELECT
  MIN(victim_x), MAX(victim_x),
  MIN(victim_y), MAX(victim_y)
FROM kill_events ke
JOIN raw_logs rl ON rl.log_id = ke.log_id
JOIN logs l ON l.log_id = ke.log_id   -- stats DB: filter by l.map
WHERE ke.victim_x IS NOT NULL
  AND l.map LIKE '%gullywash%';
```

Add ~5–10% padding, then linear map:

```text
px = (world_x - xmin) / (xmax - xmin) * image_width
py = (1 - (world_y - ymin) / (ymax - ymin)) * image_height   -- if Y is flipped vs image
```

You must **verify Y flip** once per map (matplotlib `imshow(..., extent=[xmin, xmax, ymin, ymax])` vs canvas coordinates differ).

### 2. Known overview extents (legacy calibration)

`legacy/analysis/heatmap.py` used fixed Hammer bounds with matching PNGs for some maps — see `bounds.example.json`. Treat these as starting points; confirm against your data.

### 3. Overview export metadata

Some overview tools document `scale`, `pos_x`, `pos_y` (see comment in `legacy/analysis/heatmap.py`). If you recover those for a slug, world → pixel is typically a linear transform from that metadata rather than from min/max of kills alone.

### 4. Control points

Pick two world locations you can identify on the image (e.g. cap point centers from map wiki + `capture_events.pos_x/y` in raw DB). Solve affine scale + offset (and possibly Y flip) from two or more `(world_x, world_y) ↔ (pixel_x, pixel_y)` pairs.

## Visual bounds calibration (recommended)

Build an offline bundle from your indexed raw logs and overview PNGs:

```bash
python tools/map_bounds_calibrator/build_bundle.py
# → tools/map_bounds_calibrator/bundle/ (+ optional map_bounds_calibration.zip)

cd tools/map_bounds_calibrator/bundle
python -m http.server 8765
# open http://localhost:8765/
```

The bundle includes up to **5 indexed logs per map** (with PNG), sample points (kills, captures, ubers), and a browser UI (`tools/map_bounds_calibrator/`) to:

- Toggle layers (victims, attackers, capture points, ubers) and per-log filters
- Adjust `xmin` / `xmax` / `ymin` / `ymax` and **Y flip** until points align with the overview
- **Download** `bounds.json` and copy it to `static/map_overviews/bounds.json`

Everything lives under `tools/map_bounds_calibrator/`; `build_bundle.py` copies the UI into `bundle/`.

## Suggested implementation path

1. API: given `log_id`, load kills from `raw_events.db` + map slug from log JSON / stats.
2. Resolve overview URL: `/static/map_overviews/{slug}.png` via `app/map_overviews.py` (`resolve_overview_slug`).
3. Load per-slug bounds from `static/map_overviews/bounds.json` (calibrated with the tool above).
4. Frontend: canvas/SVG overlay with normalized `[0,1]` coordinates; draw image underneath.
