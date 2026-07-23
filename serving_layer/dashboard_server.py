"""
Local serving/visualisation dashboard: merges the batch baseline
(serving_layer/batch_baseline.json, produced by
batch_layer/export_baseline_json.py) with the live speed layer view
(speed_view_latest.json, continuously rewritten by
speed_layer/speed_consumer.py) and serves it as an auto-refreshing
HTML page -- exactly the "current 5-min count next to all-time
average, ranked" merge that serving_layer/athena_setup.sql also
computes, just rendered visually and refreshed client-side instead of
queried via SQL.

This is the demo-facing "Visualised: results dashboard" piece of the
architecture diagram. Athena remains the AWS-native serving layer used
in the actual pipeline/report; this dashboard reads the same shape of
data locally so it can be shown live without depending on a running
Athena query for every refresh.

No external dependencies (stdlib only) -- deliberately simple so it's
guaranteed to run for the demo without a pip install failing at the
worst moment.

Usage:
    python dashboard_server.py --batch-baseline serving_layer/batch_baseline.json \
        --speed-view speed_view_latest.json --port 8000

Then open http://localhost:8000 in a browser.
"""

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

BATCH_BASELINE_PATH = "serving_layer/batch_baseline.json"
SPEED_VIEW_PATH = "speed_view_latest.json"

PAGE_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Wikimedia Lambda Dashboard</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #14161a; color: #e6e6e6; margin: 0; padding: 24px; }
  h1 { font-size: 20px; font-weight: 500; }
  .meta { color: #9a9a9a; font-size: 13px; margin-bottom: 16px; }
  table { border-collapse: collapse; width: 100%; max-width: 900px; }
  th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #2a2d33; font-size: 14px; }
  th { color: #9a9a9a; font-weight: 500; font-size: 12px; text-transform: uppercase; }
  .bar-cell { position: relative; }
  .bar { position: absolute; left: 0; top: 0; bottom: 0; background: #2b6cb0; opacity: 0.35; z-index: 0; }
  .bar-text { position: relative; z-index: 1; }
  .wiki { font-weight: 500; }
</style>
</head>
<body>
  <h1>Wikimedia edit activity -- last 5 min vs. all-time average</h1>
  <div class="meta" id="meta">loading...</div>
  <table>
    <thead>
      <tr><th>Wiki</th><th>Last 5 min count</th><th>All-time edit count</th><th>All-time avg bytes changed</th></tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

<script>
async function refresh() {
  const resp = await fetch('/api/merged');
  const data = await resp.json();
  document.getElementById('meta').textContent =
    'generated at ' + data.generated_at + ' | window: ' + data.window_minutes + ' min | refreshes every 5s';

  const maxCount = Math.max(1, ...data.rows.map(r => r.last_5_min_count));
  const rowsHtml = data.rows.map(r => {
    const pct = (r.last_5_min_count / maxCount) * 100;
    const avgBytes = r.avg_bytes_changed !== null ? r.avg_bytes_changed.toFixed(1) : 'n/a';
    return `<tr>
      <td class="wiki">${r.wiki}</td>
      <td class="bar-cell"><div class="bar" style="width:${pct}%"></div><span class="bar-text">${r.last_5_min_count}</span></td>
      <td>${r.all_time_edit_count}</td>
      <td>${avgBytes}</td>
    </tr>`;
  }).join('');
  document.getElementById('rows').innerHTML = rowsHtml;
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
    speed = load_json(speed_path, {"generated_at": None, "window_minutes": 5, "wiki_counts": {}})

    wiki_counts = speed.get("wiki_counts", {})
    rows = []
    for wiki, recent_count in wiki_counts.items():
        baseline = batch.get(wiki, {})
        rows.append({
            "wiki": wiki,
            "last_5_min_count": recent_count,
            "all_time_edit_count": baseline.get("edit_count", 0),
            "avg_bytes_changed": baseline.get("avg_bytes_changed"),
        })

    rows.sort(key=lambda r: r["last_5_min_count"], reverse=True)

    return {
        "generated_at": speed.get("generated_at"),
        "window_minutes": speed.get("window_minutes", 5),
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
    parser = argparse.ArgumentParser(description="Serve the local batch+speed dashboard.")
    parser.add_argument("--batch-baseline", default=BATCH_BASELINE_PATH)
    parser.add_argument("--speed-view", default=SPEED_VIEW_PATH)
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = HTTPServer(("localhost", args.port), make_handler(args.batch_baseline, args.speed_view))
    print(f"Dashboard running at http://localhost:{args.port}")
    print(f"Reading batch baseline from {args.batch_baseline}, speed view from {args.speed_view}")
    server.serve_forever()