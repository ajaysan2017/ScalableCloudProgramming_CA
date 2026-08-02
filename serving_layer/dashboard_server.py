"""
Local serving/visualisation dashboard: merges the batch baseline
(serving_layer/batch_baseline.json, produced by
batch_layer/export_baseline_json.py) with the live speed layer view
(speed_view_latest.json, continuously rewritten by
speed_layer/speed_consumer.py) and serves it as an auto-refreshing
HTML page.

Layout: a summary row (edits in window, human vs. bot split, bot
ratio) followed by two independently-ranked panels side by side --
"Speed layer" (top wikis by live 5-min count) and "Batch layer" (top
wikis by all-time count) -- each with its own bot% column. This is the
same "current vs. baseline, batch and speed shown separately" merge as
serving_layer/athena_setup.sql, just rendered visually instead of
queried via SQL, with the bot-activity breakdown added since both
layers already compute it.

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
TOP_N = 12

PAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Wiki-Lambda Dashboard</title>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: 'SF Mono', 'Menlo', 'Consolas', monospace;
    background: #0b0d10; color: #d7dbe0; margin: 0; padding: 28px;
  }
  header { display: flex; align-items: baseline; justify-content: space-between;
    border-bottom: 1px solid #1e2228; padding-bottom: 16px; margin-bottom: 24px; flex-wrap: wrap; gap: 8px; }
  .brand { font-size: 20px; font-weight: 700; letter-spacing: 2px; color: #4fd1ff; }
  .subtitle { font-size: 12px; color: #6b7280; margin-left: 12px; }
  .status { font-size: 12px; color: #9aa4b2; display: flex; gap: 16px; align-items: center; }
  .live-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #34d399;
    margin-right: 6px; box-shadow: 0 0 6px #34d399; }

  .cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 28px; }
  .card { background: #12151a; border: 1px solid #1e2228; border-radius: 6px; padding: 14px 16px; }
  .card .label { font-size: 11px; color: #6b7280; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
  .card .value { font-size: 26px; font-weight: 700; }
  .card .hint { font-size: 11px; color: #6b7280; margin-top: 4px; }
  .c-cyan { color: #4fd1ff; } .c-green { color: #34d399; } .c-red { color: #f87171; } .c-purple { color: #c084fc; }

  .panels { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media (max-width: 900px) { .panels { grid-template-columns: 1fr; } .cards { grid-template-columns: repeat(2, 1fr); } }
  .panel { background: #12151a; border: 1px solid #1e2228; border-radius: 6px; overflow: hidden; }
  .panel-head { display: flex; justify-content: space-between; align-items: center; padding: 12px 16px;
    border-bottom: 1px solid #1e2228; }
  .panel-title { font-size: 13px; font-weight: 700; letter-spacing: 1px; }
  .panel.speed .panel-title { color: #4fd1ff; }
  .panel.batch .panel-title { color: #fb923c; }
  .badge { font-size: 10px; padding: 2px 8px; border-radius: 10px; margin-left: 8px; letter-spacing: 0.5px; }
  .panel.speed .badge { background: rgba(79,209,255,0.12); color: #4fd1ff; }
  .panel.batch .badge { background: rgba(251,146,60,0.12); color: #fb923c; }
  .panel-count { font-size: 11px; color: #6b7280; }

  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 7px 16px; font-size: 13px; border-bottom: 1px solid #171a1f; }
  th { color: #6b7280; font-weight: 500; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; }
  td.rank { color: #4b5563; width: 24px; }
  td.wiki { font-weight: 600; }
  .bar-cell { position: relative; min-width: 140px; }
  .bar-track { background: #1a1e24; border-radius: 3px; height: 18px; position: relative; overflow: hidden; }
  .panel.speed .bar-fill { background: linear-gradient(90deg, #0891b2, #4fd1ff); }
  .panel.batch .bar-fill { background: linear-gradient(90deg, #c2410c, #fb923c); }
  .bar-fill { height: 100%; border-radius: 3px; }
  .bar-num { position: absolute; right: 8px; top: 0; bottom: 0; display: flex; align-items: center;
    font-size: 12px; font-weight: 600; }
  .bot-pct { font-size: 12px; }
  .bot-low { color: #34d399; } .bot-mid { color: #fbbf24; } .bot-high { color: #f87171; }
  .empty { padding: 24px 16px; color: #6b7280; font-size: 13px; }
</style>
</head>
<body>
  <header>
    <div><span class="brand">WIKI-LAMBDA</span><span class="subtitle">Lambda Architecture -- Scalable Cloud Programming CA</span></div>
    <div class="status" id="status"><span class="live-dot"></span>connecting...</div>
  </header>

  <div class="cards" id="cards"></div>

  <div class="panels">
    <div class="panel speed">
      <div class="panel-head">
        <div><span class="panel-title">SPEED LAYER</span><span class="badge">LIVE - 5-MIN WINDOW</span></div>
        <div class="panel-count" id="speed-count"></div>
      </div>
      <table>
        <thead><tr><th></th><th>Wiki</th><th>Edits</th><th>Bot%</th></tr></thead>
        <tbody id="speed-rows"></tbody>
      </table>
    </div>
    <div class="panel batch">
      <div class="panel-head">
        <div><span class="panel-title">BATCH LAYER</span><span class="badge">ALL-TIME - EMR/SPARK</span></div>
        <div class="panel-count" id="batch-count"></div>
      </div>
      <table>
        <thead><tr><th></th><th>Wiki</th><th>Edits</th><th>Bot%</th></tr></thead>
        <tbody id="batch-rows"></tbody>
      </table>
    </div>
  </div>

<script>
function botClass(pct) {
  if (pct < 20) return 'bot-low';
  if (pct < 60) return 'bot-mid';
  return 'bot-high';
}

function renderRows(tbodyId, rows, maxVal, panelClass) {
  const tbody = document.getElementById(tbodyId);
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty">no data yet</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r, i) => {
    const pct = maxVal > 0 ? (r.count / maxVal) * 100 : 0;
    return `<tr>
      <td class="rank">${i + 1}</td>
      <td class="wiki">${r.wiki}</td>
      <td class="bar-cell"><div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div><span class="bar-num">${r.count.toLocaleString()}</span></div></td>
      <td class="bot-pct ${botClass(r.bot_pct)}">${r.bot_pct.toFixed(1)}%</td>
    </tr>`;
  }).join('');
}

async function refresh() {
  const resp = await fetch('/api/merged');
  const data = await resp.json();

  document.getElementById('status').innerHTML =
    `<span class="live-dot"></span>LIVE &nbsp;|&nbsp; Updated: ${data.generated_at ? data.generated_at.split('.')[0].replace('T',' ') + ' UTC' : 'n/a'} &nbsp;|&nbsp; Window: ${data.window_minutes} min`;

  const t = data.totals;
  document.getElementById('cards').innerHTML = `
    <div class="card"><div class="label">Edits in window</div><div class="value c-cyan">${t.edits_in_window.toLocaleString()}</div><div class="hint">last ${data.window_minutes} minutes</div></div>
    <div class="card"><div class="label">Human edits</div><div class="value c-green">${t.human_edits.toLocaleString()}</div><div class="hint">real editors</div></div>
    <div class="card"><div class="label">Bot edits</div><div class="value c-red">${t.bot_edits.toLocaleString()}</div><div class="hint">automated</div></div>
    <div class="card"><div class="label">Bot ratio</div><div class="value c-purple">${t.bot_ratio_pct.toFixed(1)}%</div><div class="hint">of all edits</div></div>
  `;

  document.getElementById('speed-count').textContent = data.speed_rows.length + ' wikis';
  document.getElementById('batch-count').textContent = data.batch_rows.length + ' wikis';

  const speedMax = Math.max(1, ...data.speed_rows.map(r => r.count));
  const batchMax = Math.max(1, ...data.batch_rows.map(r => r.count));
  renderRows('speed-rows', data.speed_rows, speedMax, 'speed');
  renderRows('batch-rows', data.batch_rows, batchMax, 'batch');
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


def build_merged_view(batch_path, speed_path):
    batch = load_json(batch_path, {})
    speed = load_json(speed_path, {
        "generated_at": None, "window_minutes": 5,
        "totals": {"edits_in_window": 0, "human_edits": 0, "bot_edits": 0, "bot_ratio_pct": 0.0},
        "wiki_stats": {},
    })

    wiki_stats = speed.get("wiki_stats", {})
    speed_rows = []
    for wiki, stats in wiki_stats.items():
        count = stats.get("count", 0)
        bot_count = stats.get("bot_count", 0)
        bot_pct = (bot_count / count * 100) if count else 0.0
        speed_rows.append({"wiki": wiki, "count": count, "bot_pct": bot_pct})
    speed_rows.sort(key=lambda r: r["count"], reverse=True)
    speed_rows = speed_rows[:TOP_N]

    batch_rows = []
    for wiki, stats in batch.items():
        count = stats.get("edit_count", 0)
        bot_pct = stats.get("bot_edit_fraction", 0.0) * 100
        batch_rows.append({"wiki": wiki, "count": count, "bot_pct": bot_pct})
    batch_rows.sort(key=lambda r: r["count"], reverse=True)
    batch_rows = batch_rows[:TOP_N]

    return {
        "generated_at": speed.get("generated_at"),
        "window_minutes": speed.get("window_minutes", 5),
        "totals": speed.get("totals", {"edits_in_window": 0, "human_edits": 0, "bot_edits": 0, "bot_ratio_pct": 0.0}),
        "speed_rows": speed_rows,
        "batch_rows": batch_rows,
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
    parser = argparse.ArgumentParser(description="Serve the local batch+speed dashboard.")
    parser.add_argument("--batch-baseline", default=BATCH_BASELINE_PATH)
    parser.add_argument("--speed-view", default=SPEED_VIEW_PATH)
    parser.add_argument("--host", default="localhost", help="use 0.0.0.0 to accept connections from outside this machine (e.g. on EC2)")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), make_handler(args.batch_baseline, args.speed_view))
    print(f"Dashboard running at http://{args.host}:{args.port}")
    print(f"Reading batch baseline from {args.batch_baseline}, speed view from {args.speed_view}")
    server.serve_forever()