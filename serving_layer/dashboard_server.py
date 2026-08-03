"""
Local serving/visualisation dashboard: merges the batch baseline
(serving_layer/batch_baseline.json, produced by
batch_layer/export_baseline_json.py) with the live speed layer view
(speed_view_latest.json, continuously rewritten by
speed_layer/speed_consumer.py) into a single ranked "deviation from
baseline" feed -- this is the actual Lambda-architecture insight the
project answers, not just two raw numbers shown side by side.

How the deviation score works:
  1. The batch layer gives each wiki's total edit count over the whole
     archived time span (serving_layer/batch_baseline.json also stores
     that span in minutes).
  2. That's converted into an *expected* edit count per current
     window (e.g. per 5 minutes) -- the wiki's normal, historical rate.
  3. The speed layer gives the *actual* live count in the current
     window.
  4. deviation = actual / expected. A wiki running at 1.0x is exactly
     normal; 3x means a real-time surge; 0.3x means unusually quiet.
     Wikis with live activity but no batch history are flagged "NEW"
     rather than given a misleading ratio.

Rows are ranked by deviation (surging wikis first), which is what
makes this a genuine anomaly/trend detector rather than a plain
leaderboard.

No external dependencies (stdlib only) -- deliberately simple so it's
guaranteed to run for the demo without a pip install failing at the
worst moment.

Usage:
    python dashboard_server.py --batch-baseline serving_layer/batch_baseline.json \
        --speed-view speed_view_latest.json --host 0.0.0.0 --port 8000

Then open http://<host>:<port> in a browser.
"""

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

BATCH_BASELINE_PATH = "serving_layer/batch_baseline.json"
SPEED_VIEW_PATH = "speed_view_latest.json"
# Set to an int to cap the number of rows returned/rendered; None shows
# every tracked wiki (there are ~800+, which a plain HTML table handles
# fine -- no pagination/virtualisation needed for a demo).
TOP_N = None
SURGE_THRESHOLD = 2.0
QUIET_THRESHOLD = 0.5

# A wiki's "expected" baseline can be a tiny fraction of an edit per
# window (e.g. 0.02) if it barely appears in the archive at all. Raw
# live_count / expected in that regime is not a meaningful signal --
# a single incidental edit against an expected 0.02 produces a ~50x
# "deviation" that looks like a dramatic surge but is really just
# sampling noise from an under-observed baseline. Below this expected
# threshold we don't trust the ratio enough to call it SURGE/QUIET/
# NORMAL, and label it LOW-DATA instead so the dashboard doesn't
# misrepresent statistical noise as a real anomaly.
MIN_EXPECTED_FOR_CONFIDENT_RATIO = 1.0

# Small additive smoothing constant applied to both live and expected
# counts before dividing. Standard technique for stabilising ratios of
# small counts (keeps a lone live edit against a borderline-confident
# baseline from swinging wildly) without changing the ratio much once
# counts are reasonably large.
DEVIATION_SMOOTHING = 0.5

PAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>WIKIDRIFT</title>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    background: #10091c; color: #e5e1f0; margin: 0; padding: 32px;
  }
  header { display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 26px; flex-wrap: wrap; gap: 10px; }
  .brand-row { display: flex; align-items: center; gap: 12px; }
  .brand { font-size: 22px; font-weight: 800; letter-spacing: -0.5px; color: #a78bfa; }
  .subtitle { font-size: 13px; color: #8b83a3; font-weight: 400; }
  .status { font-size: 12px; color: #a89fc2; display: flex; gap: 14px; align-items: center;
    background: #1a1229; padding: 6px 14px; border-radius: 999px; }
  .live-dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; background: #4ade80;
    margin-right: 4px; animation: pulse 1.6s ease-in-out infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }

  .cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 14px; margin-bottom: 26px; }
  .card { background: linear-gradient(160deg, #1a1229, #150e22); border-left: 3px solid var(--accent, #a78bfa);
    border-radius: 10px; padding: 16px 18px; }
  .card .label { font-size: 11px; color: #8b83a3; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
  .card .value { font-size: 28px; font-weight: 800; }
  .card .hint { font-size: 11px; color: #6f6889; margin-top: 4px; }
  .card.c1 { --accent: #a78bfa; } .card.c1 .value { color: #a78bfa; }
  .card.c2 { --accent: #fb7185; } .card.c2 .value { color: #fb7185; }
  .card.c3 { --accent: #38bdf8; } .card.c3 .value { color: #38bdf8; }
  .card.c4 { --accent: #4ade80; } .card.c4 .value { color: #4ade80; }
  .card.c5 { --accent: #6f6889; } .card.c5 .value { color: #a89fc2; }

  .panel { background: #150e22; border-radius: 10px; overflow: hidden; }
  .panel-head { display: flex; justify-content: space-between; align-items: center; padding: 14px 18px; }
  .panel-title { font-size: 13px; font-weight: 700; letter-spacing: 0.5px; color: #a78bfa; }
  .pill { font-size: 10px; padding: 3px 10px; border-radius: 999px; margin-left: 10px; font-weight: 600;
    background: rgba(167,139,250,0.15); color: #a78bfa; }
  .panel-count { font-size: 11px; color: #6f6889; }
  .legend { font-size: 11px; color: #6f6889; padding: 0 18px 14px; }
  .legend span { margin-right: 14px; }

  .filter-bar { display: flex; gap: 8px; padding: 0 18px 14px; flex-wrap: wrap; align-items: center; }
  .filter-btn { font-size: 11px; font-weight: 600; padding: 5px 12px; border-radius: 999px;
    border: 1px solid #241a38; background: #1a1229; color: #a89fc2; cursor: pointer; }
  .filter-btn:hover { border-color: #a78bfa; }
  .filter-btn.active { background: rgba(167,139,250,0.18); color: #a78bfa; border-color: #a78bfa; }
  .search-box { margin-left: auto; font-size: 12px; background: #1a1229; border: 1px solid #241a38;
    color: #e5e1f0; padding: 6px 12px; border-radius: 999px; outline: none; min-width: 200px; }
  .search-box::placeholder { color: #6f6889; }

  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 9px 18px; font-size: 13px; }
  th { color: #6f6889; font-weight: 600; font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px;
    border-bottom: 1px solid #241a38; padding-bottom: 10px; }
  td.rank { color: #4a4462; width: 20px; }
  td.wiki { font-weight: 600; }
  td.num { color: #a89fc2; }
  .deviation { font-weight: 700; font-size: 14px; }
  .dev-surge { color: #fb7185; } .dev-normal { color: #a89fc2; } .dev-quiet { color: #38bdf8; } .dev-new { color: #facc15; }
  .dev-lowdata { color: #6f6889; }
  .status-badge { font-size: 10px; font-weight: 700; padding: 3px 9px; border-radius: 999px; letter-spacing: 0.5px; }
  .badge-surge { background: rgba(251,113,133,0.15); color: #fb7185; }
  .badge-normal { background: rgba(168,159,194,0.12); color: #a89fc2; }
  .badge-quiet { background: rgba(56,189,248,0.15); color: #38bdf8; }
  .badge-new { background: rgba(250,204,21,0.15); color: #facc15; }
  .badge-lowdata { background: rgba(111,104,137,0.15); color: #8b83a3; }
  .empty { padding: 26px 18px; color: #6f6889; font-size: 13px; }
</style>
</head>
<body>
  <header>
    <div class="brand-row">
      <span class="brand">WIKIDRIFT</span>
      <span class="subtitle">WIKI activity deviation detector : live rate vs. historical baseline</span>
    </div>
    <div class="status" id="status"><span class="live-dot"></span>connecting...</div>
  </header>

  <div class="cards" id="cards"></div>

  <div class="panel">
    <div class="panel-head">
      <div><span class="panel-title">DEVIATION FEED</span><span class="pill" id="window-pill"></span></div>
      <div class="panel-count" id="row-count"></div>
    </div>
    <div class="legend">
      <span class="dev-surge">&#9679; surge (&ge;2x baseline)</span>
      <span class="dev-normal">&#9679; normal</span>
      <span class="dev-quiet">&#9679; quiet (&le;0.5x baseline)</span>
      <span class="dev-new">&#9679; new activity, no baseline yet</span>
      <span class="dev-lowdata">&#9679; low-data (baseline too small to trust a ratio)</span>
    </div>
    <div class="filter-bar" id="filter-bar">
      <button class="filter-btn active" data-status="ALL">All</button>
      <button class="filter-btn" data-status="SURGE">Surge</button>
      <button class="filter-btn" data-status="NORMAL">Normal</button>
      <button class="filter-btn" data-status="QUIET">Quiet</button>
      <button class="filter-btn" data-status="NEW">New</button>
      <button class="filter-btn" data-status="LOW-DATA">Low-data</button>
      <input class="search-box" id="search-input" type="text" placeholder="Search wiki...">
    </div>
    <table>
      <thead><tr><th></th><th>Wiki</th><th>Live (window)</th><th>Expected (baseline)</th><th>Deviation</th><th>Status</th></tr></thead>
      <tbody id="rows"></tbody>
    </table>
  </div>

<script>
function statusInfo(status) {
  const map = {
    SURGE:  { cls: 'dev-surge',  badge: 'badge-surge',  label: 'SURGE' },
    NORMAL: { cls: 'dev-normal', badge: 'badge-normal', label: 'NORMAL' },
    QUIET:  { cls: 'dev-quiet',  badge: 'badge-quiet',  label: 'QUIET' },
    NEW:    { cls: 'dev-new',    badge: 'badge-new',    label: 'NEW' },
    'LOW-DATA': { cls: 'dev-lowdata', badge: 'badge-lowdata', label: 'LOW-DATA' },
  };
  return map[status] || map.NORMAL;
}

function renderRows(rows) {
  const tbody = document.getElementById('rows');
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">no data yet</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r, i) => {
    const info = statusInfo(r.status);
    const expectedLabel = r.expected === null ? 'n/a' : r.expected.toFixed(1);
    const devLabel = r.deviation === null ? '--' : r.deviation.toFixed(1) + 'x';
    return `<tr>
      <td class="rank">${i + 1}</td>
      <td class="wiki">${r.wiki}</td>
      <td class="num">${r.live.toLocaleString()}</td>
      <td class="num">${expectedLabel}</td>
      <td class="deviation ${info.cls}">${devLabel}</td>
      <td><span class="status-badge ${info.badge}">${info.label}</span></td>
    </tr>`;
  }).join('');
}

let allRows = [];
let currentFilter = 'ALL';

function applyFilters() {
  const search = document.getElementById('search-input').value.trim().toLowerCase();
  let filtered = allRows;
  if (currentFilter !== 'ALL') {
    filtered = filtered.filter(r => r.status === currentFilter);
  }
  if (search) {
    filtered = filtered.filter(r => r.wiki.toLowerCase().includes(search));
  }
  document.getElementById('row-count').textContent =
    `${filtered.length} of ${allRows.length} wikis` + (currentFilter !== 'ALL' || search ? ' (filtered)' : '');
  renderRows(filtered);
}

document.getElementById('filter-bar').addEventListener('click', (e) => {
  const btn = e.target.closest('.filter-btn');
  if (!btn) return;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  currentFilter = btn.dataset.status;
  applyFilters();
});

document.getElementById('search-input').addEventListener('input', applyFilters);

async function refresh() {
  const resp = await fetch('/api/merged');
  const data = await resp.json();

  document.getElementById('status').innerHTML =
    `<span class="live-dot"></span>live &nbsp;|&nbsp; ${data.generated_at ? data.generated_at.split('.')[0].replace('T',' ') + ' UTC' : 'n/a'} &nbsp;|&nbsp; ${data.window_minutes}-min window`;
  document.getElementById('window-pill').textContent = data.window_minutes + '-min window';

  const s = data.summary;
  document.getElementById('cards').innerHTML = `
    <div class="card c1"><div class="label">Wikis tracked</div><div class="value">${s.total_wikis}</div><div class="hint">with live or historical data</div></div>
    <div class="card c2"><div class="label">Surging</div><div class="value">${s.surge_count}</div><div class="hint">&ge;2x normal rate</div></div>
    <div class="card c3"><div class="label">Quiet</div><div class="value">${s.quiet_count}</div><div class="hint">&le;0.5x normal rate</div></div>
    <div class="card c4"><div class="label">New activity</div><div class="value">${s.new_count}</div><div class="hint">no baseline yet</div></div>
    <div class="card c5"><div class="label">Low-data</div><div class="value">${s.low_data_count}</div><div class="hint">baseline too thin to trust</div></div>
  `;

  allRows = data.rows;
  applyFilters();
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path) as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default


def classify(deviation, has_baseline, low_data):
    if not has_baseline:
        return "NEW"
    if low_data:
        return "LOW-DATA"
    if deviation >= SURGE_THRESHOLD:
        return "SURGE"
    if deviation <= QUIET_THRESHOLD:
        return "QUIET"
    return "NORMAL"


# Rows are ranked so that statistically confident signals (SURGE, then
# NORMAL, then QUIET) always sit above rows we don't trust enough to
# classify (LOW-DATA, NEW) -- otherwise a LOW-DATA row with an inflated
# ratio could still crowd out a genuine, well-supported SURGE.
STATUS_GROUP_RANK = {"SURGE": 3, "NORMAL": 2, "QUIET": 1, "LOW-DATA": 0, "NEW": 0}


def build_merged_view(batch_path, speed_path):
    batch = load_json(batch_path, {"meta": {"span_minutes": None}, "wikis": {}})
    speed = load_json(speed_path, {
        "generated_at": None, "window_minutes": 5,
        "totals": {"edits_in_window": 0, "human_edits": 0, "bot_edits": 0, "bot_ratio_pct": 0.0},
        "wiki_stats": {},
    })

    window_minutes = speed.get("window_minutes", 5)
    span_minutes = batch.get("meta", {}).get("span_minutes")
    batch_wikis = batch.get("wikis", {})
    wiki_stats = speed.get("wiki_stats", {})

    all_wikis = set(batch_wikis.keys()) | set(wiki_stats.keys())

    rows = []
    for wiki in all_wikis:
        live_count = wiki_stats.get(wiki, {}).get("count", 0)
        batch_entry = batch_wikis.get(wiki)

        has_baseline = bool(batch_entry) and bool(span_minutes)
        expected = None
        deviation = None
        low_data = False

        if has_baseline:
            total_edits = batch_entry.get("edit_count", 0)
            expected = total_edits * (window_minutes / span_minutes)
            low_data = expected < MIN_EXPECTED_FOR_CONFIDENT_RATIO
            if not low_data:
                # Additive smoothing stabilises the ratio near the
                # confidence boundary; negligible effect once counts
                # are comfortably large.
                deviation = (live_count + DEVIATION_SMOOTHING) / (expected + DEVIATION_SMOOTHING)

        status = classify(deviation, has_baseline, low_data)

        # Rank confident rows (SURGE/NORMAL/QUIET) above LOW-DATA/NEW,
        # then order within each group by the most relevant metric:
        # deviation for confident rows, raw live count otherwise (so an
        # unclassifiable wiki with a lot of live activity still surfaces
        # above one with a single stray edit).
        secondary = deviation if deviation is not None else live_count
        sort_key = (STATUS_GROUP_RANK[status], secondary)

        rows.append({
            "wiki": wiki,
            "live": live_count,
            "expected": round(expected, 1) if expected is not None else None,
            "deviation": round(deviation, 2) if deviation is not None else None,
            "status": status,
            "_sort_key": sort_key,
        })

    rows.sort(key=lambda r: r["_sort_key"], reverse=True)
    for r in rows:
        del r["_sort_key"]

    # Compute the summary cards over ALL wikis, not just the top-N shown
    # in the table below. The table is sorted by deviation descending
    # (surging wikis first) and truncated to TOP_N, so counting status
    # after truncation would silently exclude QUIET/NEW wikis -- they
    # sort toward the bottom and rarely survive the cut, making those
    # cards look permanently stuck at 0 even when such wikis exist.
    summary = {
        "total_wikis": len(all_wikis),
        "surge_count": sum(1 for r in rows if r["status"] == "SURGE"),
        "quiet_count": sum(1 for r in rows if r["status"] == "QUIET"),
        "new_count": sum(1 for r in rows if r["status"] == "NEW"),
        "low_data_count": sum(1 for r in rows if r["status"] == "LOW-DATA"),
    }

    if TOP_N is not None:
        rows = rows[:TOP_N]

    return {
        "generated_at": speed.get("generated_at"),
        "window_minutes": window_minutes,
        "summary": summary,
        "rows": rows,
    }


def make_handler(batch_path, speed_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/merged":
                payload = json.dumps(build_merged_view(batch_path, speed_path)).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            else:
                payload = PAGE_TEMPLATE.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        def log_message(self, format, *args):
            pass  # keep the console quiet

    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve the local deviation-detection dashboard.")
    parser.add_argument("--batch-baseline", default=BATCH_BASELINE_PATH)
    parser.add_argument("--speed-view", default=SPEED_VIEW_PATH)
    parser.add_argument("--host", default="localhost", help="use 0.0.0.0 to accept connections from outside this machine (e.g. on EC2)")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), make_handler(args.batch_baseline, args.speed_view))
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print(f"Reading batch baseline from {args.batch_baseline}, speed view from {args.speed_view}")
    server.serve_forever()