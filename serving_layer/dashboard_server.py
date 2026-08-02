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
wikis by all-time count, plus average bytes changed per edit) -- each
with its own bot% column. This is the same "current vs. baseline,
batch and speed shown separately" merge as
serving_layer/athena_setup.sql, just rendered visually instead of
queried via SQL.

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
<title>WIKI-INSIGHTS</title>
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

  .cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 26px; }
  .card { background: linear-gradient(160deg, #1a1229, #150e22); border-left: 3px solid var(--accent, #a78bfa);
    border-radius: 10px; padding: 16px 18px; }
  .card .label { font-size: 11px; color: #8b83a3; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 8px; }
  .card .value { font-size: 28px; font-weight: 800; }
  .card .hint { font-size: 11px; color: #6f6889; margin-top: 4px; }
  .card.c1 { --accent: #a78bfa; } .card.c1 .value { color: #a78bfa; }
  .card.c2 { --accent: #4ade80; } .card.c2 .value { color: #4ade80; }
  .card.c3 { --accent: #fb7185; } .card.c3 .value { color: #fb7185; }
  .card.c4 { --accent: #facc15; } .card.c4 .value { color: #facc15; }

  .panels { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
  @media (max-width: 900px) { .panels { grid-template-columns: 1fr; } .cards { grid-template-columns: repeat(2, 1fr); } }
  .panel { background: #150e22; border-radius: 10px; overflow: hidden; }
  .panel-head { display: flex; justify-content: space-between; align-items: center; padding: 14px 18px; }
  .panel-title { font-size: 13px; font-weight: 700; letter-spacing: 0.5px; }
  .panel.speed .panel-title { color: #a78bfa; }
  .panel.batch .panel-title { color: #fb7185; }
  .pill { font-size: 10px; padding: 3px 10px; border-radius: 999px; margin-left: 10px; font-weight: 600; }
  .panel.speed .pill { background: rgba(167,139,250,0.15); color: #a78bfa; }
  .panel.batch .pill { background: rgba(251,113,133,0.15); color: #fb7185; }
  .panel-count { font-size: 11px; color: #6f6889; }

  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 8px 18px; font-size: 13px; }
  th { color: #6f6889; font-weight: 600; font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px;
    border-bottom: 1px solid #241a38; padding-bottom: 10px; }
  td.rank { color: #4a4462; width: 20px; }
  td.wiki { font-weight: 600; }
  .bar-cell { position: relative; min-width: 130px; }
  .bar-track { background: #1f1633; border-radius: 999px; height: 16px; position: relative; overflow: hidden; }
  .panel.speed .bar-fill { background: linear-gradient(90deg, #7c3aed, #c4b5fd); }
  .panel.batch .bar-fill { background: linear-gradient(90deg, #be123c, #fda4af); }
  .bar-fill { height: 100%; border-radius: 999px; }
  .bar-num { position: absolute; right: 10px; top: 0; bottom: 0; display: flex; align-items: center;
    font-size: 11px; font-weight: 700; }
  .bot-pct { font-size: 12px; font-weight: 600; }
  .bot-low { color: #4ade80; } .bot-mid { color: #facc15; } .bot-high { color: #fb7185; }
  .bytes-col { color: #8b83a3; font-size: 12px; }
  .empty { padding: 26px 18px; color: #6f6889; font-size: 13px; }
</style>
</head>
<body>
  <header>
    <div class="brand-row">
      <span class="brand">WIKI-INSIGHTS</span>
      <span class="subtitle">real-time Wikipedia edit analytics -- batch + speed layers</span>
    </div>
    <div class="status" id="status"><span class="live-dot"></span>connecting...</div>
  </header>

  <div class="cards" id="cards"></div>

  <div class="panels">
    <div class="panel speed">
      <div class="panel-head">
        <div><span class="panel-title">SPEED LAYER</span><span class="pill">live &middot; 5-min window</span></div>
        <div class="panel-count" id="speed-count"></div>
      </div>
      <table>
        <thead><tr><th></th><th>Wiki</th><th>Edits</th><th>Bot%</th></tr></thead>
        <tbody id="speed-rows"></tbody>
      </table>
    </div>
    <div class="panel batch">
      <div class="panel-head">
        <div><span class="panel-title">BATCH LAYER</span><span class="pill">all-time &middot; EMR/Spark</span></div>
        <div class="panel-count" id="batch-count"></div>
      </div>
      <table>
        <thead><tr><th></th><th>Wiki</th><th>Edits</th><th>Bot%</th><th>Avg &Delta;bytes</th></tr></thead>
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

function renderSpeedRows(rows, maxVal) {
  const tbody = document.getElementById('speed-rows');
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

function renderBatchRows(rows, maxVal) {
  const tbody = document.getElementById('batch-rows');
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty">no data yet</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r, i) => {
    const pct = maxVal > 0 ? (r.count / maxVal) * 100 : 0;
    const bytesLabel = r.avg_bytes_changed === null ? 'n/a' : r.avg_bytes_changed.toFixed(1);
    return `<tr>
      <td class="rank">${i + 1}</td>
      <td class="wiki">${r.wiki}</td>
      <td class="bar-cell"><div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div><span class="bar-num">${r.count.toLocaleString()}</span></div></td>
      <td class="bot-pct ${botClass(r.bot_pct)}">${r.bot_pct.toFixed(1)}%</td>
      <td class="bytes-col">${bytesLabel}</td>
    </tr>`;
  }).join('');
}

async function refresh() {
  const resp = await fetch('/api/merged');
  const data = await resp.json();

  document.getElementById('status').innerHTML =
    `<span class="live-dot"></span>live &nbsp;|&nbsp; ${data.generated_at ? data.generated_at.split('.')[0].replace('T',' ') + ' UTC' : 'n/a'} &nbsp;|&nbsp; ${data.window_minutes}-min window`;

  const t = data.totals;
  document.getElementById('cards').innerHTML = `
    <div class="card c1"><div class="label">Edits in window</div><div class="value">${t.edits_in_window.toLocaleString()}</div><div class="hint">last ${data.window_minutes} minutes</div></div>
    <div class="card c2"><div class="label">Human edits</div><div class="value">${t.human_edits.toLocaleString()}</div><div class="hint">real editors</div></div>
    <div class="card c3"><div class="label">Bot edits</div><div class="value">${t.bot_edits.toLocaleString()}</div><div class="hint">automated</div></div>
    <div class="card c4"><div class="label">Bot ratio</div><div class="value">${t.bot_ratio_pct.toFixed(1)}%</div><div class="hint">of all edits</div></div>
  `;

  document.getElementById('speed-count').textContent = data.speed_rows.length + ' wikis';
  document.getElementById('batch-count').textContent = data.batch_rows.length + ' wikis';

  const speedMax = Math.max(1, ...data.speed_rows.map(r => r.count));
  const batchMax = Math.max(1, ...data.batch_rows.map(r => r.count));
  renderSpeedRows(data.speed_rows, speedMax);
  renderBatchRows(data.batch_rows, batchMax);
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
        batch_rows.append({
            "wiki": wiki,
            "count": count,
            "bot_pct": bot_pct,
            "avg_bytes_changed": stats.get("avg_bytes_changed"),
        })
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