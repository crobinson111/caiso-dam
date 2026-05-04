from flask import Flask, request, jsonify, render_template_string
import requests
import zipfile
import io
import csv
import time
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

PACIFIC = pytz.timezone("America/Los_Angeles")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/zip, application/octet-stream, */*",
}


def dt_to_utc_str(dt_str):
    local_dt = PACIFIC.localize(datetime.strptime(dt_str, "%Y-%m-%d"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y%m%dT%H:%M-0000")


def fetch_dam_day(date_str):
    """Fetch DAM LMP data for a single date. Returns (rows, error_string)."""
    base_url = "https://oasis.caiso.com/oasisapi/SingleZip"
    next_day = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "queryname": "PRC_LMP",
        "market_run_id": "DAM",
        "startdatetime": dt_to_utc_str(date_str),
        "enddatetime": dt_to_utc_str(next_day),
        "version": 1,
        "node": "ELAP_PACE_UAMPS_LOAD",
        "resultformat": 6,
    }

    print(f"[DAM] Fetching {date_str} | start={params['startdatetime']} end={params['enddatetime']}", flush=True)

    for attempt in range(3):
        try:
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=90)
            print(f"[DAM] HTTP {resp.status_code}, size={len(resp.content)}, ct={resp.headers.get('content-type', 'unknown')}", flush=True)

            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"[DAM] Rate limited — waiting {wait}s (attempt {attempt + 1})", flush=True)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                return [], f"HTTP {resp.status_code} from CAISO"

            # CAISO returns XML on error, ZIP on success
            if resp.content[:1] == b"<":
                xml_msg = resp.content[:800].decode("utf-8", errors="replace")
                print(f"[WARN] XML error response: {xml_msg}", flush=True)
                # Try to extract a readable message from the XML
                import re
                match = re.search(r"<err>(.*?)</err>|<message>(.*?)</message>|ERR-\d+.*?(?=<)", xml_msg, re.DOTALL)
                friendly = match.group(0)[:200] if match else xml_msg[:200]
                return [], f"CAISO API error: {friendly}"

            try:
                rows = []
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    print(f"[DAM] ZIP contains: {z.namelist()}", flush=True)
                    for name in z.namelist():
                        if name.endswith(".csv"):
                            with z.open(name) as f:
                                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                                file_rows = list(reader)
                                print(f"[DAM] {name}: {len(file_rows)} rows | cols={reader.fieldnames}", flush=True)
                                rows.extend(file_rows)
                return rows, None
            except zipfile.BadZipFile:
                snippet = resp.content[:300].decode("utf-8", errors="replace")
                print(f"[ERROR] Response is not a ZIP: {snippet}", flush=True)
                return [], f"Response was not a valid ZIP file: {snippet[:200]}"

        except Exception as e:
            print(f"[ERROR] fetch_dam_day attempt {attempt + 1}: {e}", flush=True)
            if attempt < 2:
                time.sleep(5)

    return [], "All 3 fetch attempts failed — CAISO may be unavailable"


def parse_dam_rows(rows):
    """Parse raw CSV rows into hourly LMP dicts."""
    if not rows:
        return []

    lmp_types = set(r.get("LMP_TYPE", "MISSING") for r in rows)
    print(f"[PARSE] {len(rows)} raw rows | LMP_TYPE values present: {lmp_types}", flush=True)

    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    print(f"[PARSE] After LMP_TYPE=LMP filter: {len(lmp_rows)} rows", flush=True)

    result = []
    for row in lmp_rows:
        try:
            interval_start = (
                row.get("INTERVALSTARTTIME_GMT")
                or row.get("INTERVAL_START_GMT")
                or ""
            )
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S")
                dt_utc = pytz.utc.localize(dt_utc)
                dt_pt = dt_utc.astimezone(PACIFIC)
                result.append({
                    "date": dt_pt.strftime("%Y-%m-%d"),
                    "hour": dt_pt.hour,
                    "lmp": round(mw, 4),
                })
        except Exception as e:
            print(f"[PARSE ERROR] {e} | row={row}", flush=True)

    result.sort(key=lambda x: (x["date"], x["hour"]))
    return result


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>CAISO DAM — ELAP_PACE_UAMPS_LOAD</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #fdf8ee;
    --surface: #ffffff;
    --surface2: #fef3d0;
    --border: #e8d08a;
    --border2: #f0dfa0;
    --text: #2a1f00;
    --muted: #7a6020;
    --accent: #b07800;
    --accent-dark: #8a5c00;
    --danger: #c0392b;
    --neg: #1a6b3a;
    --mono: 'IBM Plex Mono', monospace;
    --sans: 'IBM Plex Sans', sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); min-height: 100vh; }

  header {
    background: var(--surface); border-bottom: 2px solid var(--border);
    padding: 16px 32px; display: flex; align-items: center; gap: 14px;
  }
  .logo {
    width: 38px; height: 38px; background: var(--accent);
    display: grid; place-items: center;
    font-family: var(--mono); font-size: 11px; color: #fff; font-weight: 500;
  }
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: 1px; color: var(--text); }
  .header-right { margin-left: auto; text-align: right; }
  .header-right .node { font-family: var(--mono); font-size: 11px; color: var(--muted); }
  .header-right .last-updated { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 2px; }

  .main { max-width: 860px; margin: 0 auto; padding: 24px 20px; }

  .control-bar {
    background: var(--surface); border: 1px solid var(--border);
    padding: 16px 20px; margin-bottom: 20px;
    display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
  }
  .date-nav { display: flex; align-items: center; gap: 8px; }
  .nav-btn {
    background: transparent; border: 1px solid var(--border);
    color: var(--accent); font-family: var(--mono); font-size: 16px;
    width: 32px; height: 32px; cursor: pointer; display: grid; place-items: center;
    transition: background 0.15s;
  }
  .nav-btn:hover { background: var(--surface2); }
  input[type="date"] {
    font-family: var(--mono); font-size: 13px; color: var(--text);
    border: 1px solid var(--border); background: var(--surface2);
    padding: 5px 10px; height: 32px; cursor: pointer;
    outline: none;
  }
  input[type="date"]:focus { border-color: var(--accent); }
  .refresh-btn {
    background: var(--accent); border: none; color: #fff;
    font-family: var(--sans); font-size: 13px; font-weight: 600;
    padding: 7px 20px; height: 32px; cursor: pointer; margin-left: auto;
    transition: background 0.15s;
  }
  .refresh-btn:hover { background: var(--accent-dark); }
  .refresh-btn:disabled { background: var(--border); cursor: not-allowed; }

  .status-bar {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-bottom: 18px; min-height: 16px; display: flex; align-items: center; gap: 7px;
  }
  .status-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--border); flex-shrink: 0; }
  .status-dot.loading { background: var(--accent); animation: pulse 0.8s infinite; }
  .status-dot.ok { background: #2a7a2a; }
  .status-dot.err { background: var(--danger); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  .error-box {
    background: #fff5f5; border: 1px solid #f5c6c6;
    padding: 14px 18px; margin-bottom: 18px;
    font-family: var(--mono); font-size: 11px; color: var(--danger);
    word-break: break-word; display: none;
  }

  .stats-row { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }
  .stat-chip {
    background: var(--surface); border: 1px solid var(--border);
    padding: 8px 16px; font-family: var(--mono);
  }
  .stat-chip .s-label { font-size: 9px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; display: block; margin-bottom: 3px; }
  .stat-chip .s-val { font-size: 15px; color: var(--text); font-weight: 500; }

  .section-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px; border-left: 4px solid var(--accent); background: var(--surface2);
  }
  .market-label { font-size: 12px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; color: var(--accent); }
  .section-desc { font-size: 12px; color: var(--muted); }
  .section-count { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }

  .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-top: none; }
  table { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  thead tr { background: var(--surface2); }
  th {
    padding: 8px 16px; text-align: right;
    color: var(--muted); font-weight: 500; font-size: 11px;
    border-bottom: 1px solid var(--border); border-right: 1px solid var(--border2);
    text-transform: uppercase; letter-spacing: 1px;
  }
  th:first-child { text-align: left; }
  th:last-child { border-right: none; }
  td { padding: 6px 16px; border-bottom: 1px solid var(--border2); border-right: 1px solid var(--border2); text-align: right; }
  td:first-child { text-align: left; color: var(--muted); font-size: 11px; white-space: nowrap; }
  td:last-child { border-right: none; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: var(--surface2); }
  tr:hover td { background: #f5e8a0 !important; }
  .vpos { color: var(--danger); }
  .vneg { color: #1a6b3a; }
  .vneu { color: var(--text); }

  .avg-row td { font-weight: 600; background: var(--surface2) !important; border-top: 2px solid var(--border); }
  .avg-row td:first-child { color: var(--accent); }

  .empty-state {
    padding: 48px; text-align: center; font-family: var(--mono); font-size: 12px; color: var(--muted);
    background: var(--surface); border: 1px solid var(--border); border-top: none;
  }
</style>
</head>
<body>

<header>
  <div class="logo">DAM</div>
  <h1>CAISO Day-Ahead Market</h1>
  <div class="header-right">
    <div class="node">ELAP_PACE_UAMPS_LOAD</div>
    <div class="last-updated" id="lastUpdated"></div>
  </div>
</header>

<div class="main">

  <div class="control-bar">
    <div class="date-nav">
      <button class="nav-btn" onclick="changeDay(-1)" title="Previous day">&#8249;</button>
      <input type="date" id="datePicker" onchange="onDateChange()" />
      <button class="nav-btn" onclick="changeDay(1)" title="Next day">&#8250;</button>
    </div>
    <button class="refresh-btn" id="refreshBtn" onclick="loadData()">&#8635; Refresh</button>
  </div>

  <div class="status-bar">
    <div class="status-dot" id="statusDot"></div>
    <span id="statusMsg">Loading...</span>
  </div>

  <div class="error-box" id="errorBox"></div>

  <div id="statsRow" class="stats-row" style="display:none"></div>

  <div id="tableSection" style="display:none">
    <div class="section-header">
      <span class="market-label">DAM</span>
      <span class="section-desc">Day-Ahead Market &middot; Hourly LMP</span>
      <span class="section-count" id="rowCount"></span>
    </div>
    <div class="table-wrap"><div id="damTable"></div></div>
  </div>

</div>

<script>
const DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function todayPT() {
  // Approximate today in Pacific time using UTC offset
  const now = new Date();
  const ptOffset = -7 * 60; // PDT (use -8 for PST Nov-Mar)
  const ptMs = now.getTime() + (now.getTimezoneOffset() + ptOffset) * 60000;
  const pt = new Date(ptMs);
  return pt.toISOString().slice(0, 10);
}

let currentDate = todayPT();

function initDatePicker() {
  document.getElementById('datePicker').value = currentDate;
}

function onDateChange() {
  currentDate = document.getElementById('datePicker').value;
  loadData();
}

function setStatus(msg, state) {
  document.getElementById('statusMsg').textContent = msg;
  document.getElementById('statusDot').className = 'status-dot' + (state ? ' ' + state : '');
}

function showError(msg) {
  const box = document.getElementById('errorBox');
  if (msg) {
    box.textContent = msg;
    box.style.display = 'block';
  } else {
    box.style.display = 'none';
  }
}

function changeDay(delta) {
  const d = new Date(currentDate + 'T12:00:00');
  d.setDate(d.getDate() + delta);
  currentDate = d.toISOString().slice(0, 10);
  document.getElementById('datePicker').value = currentDate;
  loadData();
}

async function loadData() {
  const btn = document.getElementById('refreshBtn');
  btn.disabled = true;
  document.getElementById('tableSection').style.display = 'none';
  document.getElementById('statsRow').style.display = 'none';
  showError(null);
  setStatus('Fetching DAM data for ' + currentDate + '...', 'loading');

  try {
    const resp = await fetch('/data?date=' + currentDate);
    const data = await resp.json();

    if (data.error) {
      setStatus('No data — ' + currentDate, 'err');
      showError(data.error);
      btn.disabled = false;
      return;
    }

    if (!data.rows || data.rows.length === 0) {
      setStatus('No data returned for ' + currentDate + ' — DAM may not be published yet.', 'err');
      btn.disabled = false;
      return;
    }

    renderStats(data.rows);
    renderTable(data.rows);
    document.getElementById('lastUpdated').textContent = 'fetched ' + new Date().toLocaleTimeString();
    setStatus('Loaded ' + data.rows.length + ' hourly intervals for ' + currentDate + '.', 'ok');
  } catch (e) {
    setStatus('Fetch failed: ' + e.message, 'err');
    showError('Network or server error: ' + e.message);
  }
  btn.disabled = false;
}

function vc(v) { return v > 50 ? 'vpos' : v < 0 ? 'vneg' : 'vneu'; }
function fmt(v) { return '$' + v.toFixed(4); }
function fmtHour(h) {
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return String(h12).padStart(2, '0') + ':00 ' + ampm;
}

function renderStats(rows) {
  const vals = rows.map(r => r.lmp);
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
  const max = Math.max(...vals);
  const min = Math.min(...vals);
  const maxHour = rows[vals.indexOf(max)].hour;
  const minHour = rows[vals.indexOf(min)].hour;

  const statsRow = document.getElementById('statsRow');
  statsRow.style.display = 'flex';
  statsRow.innerHTML =
    chip('Daily Avg', '$' + avg.toFixed(4)) +
    chip('Peak LMP', '$' + max.toFixed(4) + ' <small style="font-size:10px;color:var(--muted)">hr ' + (maxHour + 1) + '</small>') +
    chip('Off-Peak LMP', '$' + min.toFixed(4) + ' <small style="font-size:10px;color:var(--muted)">hr ' + (minHour + 1) + '</small>') +
    chip('Hours', rows.length);
}

function chip(label, val) {
  return '<div class="stat-chip"><span class="s-label">' + label + '</span><span class="s-val">' + val + '</span></div>';
}

function renderTable(rows) {
  document.getElementById('tableSection').style.display = 'block';
  document.getElementById('rowCount').textContent = rows.length + ' hours';

  const vals = rows.map(r => r.lmp);
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length;

  const dt = new Date(rows[0].date + 'T12:00:00');
  const weekday = DAYS[dt.getDay()];

  let tbody = '';
  rows.forEach(r => {
    const endHour = (r.hour + 1) % 24;
    const endAmPm = endHour < 12 ? 'AM' : 'PM';
    const endH12 = endHour % 12 === 0 ? 12 : endHour % 12;
    const timeLabel = fmtHour(r.hour) + ' – ' + String(endH12).padStart(2, '0') + ':00 ' + endAmPm;
    tbody += '<tr>'
      + '<td>' + timeLabel + '</td>'
      + '<td style="text-align:right">HE ' + (r.hour + 1) + '</td>'
      + '<td class="' + vc(r.lmp) + '" style="font-weight:500">' + fmt(r.lmp) + '</td>'
      + '</tr>';
  });

  // Average row
  tbody += '<tr class="avg-row">'
    + '<td colspan="2">Daily Average</td>'
    + '<td class="' + vc(avg) + '">' + fmt(avg) + '</td>'
    + '</tr>';

  document.getElementById('damTable').innerHTML =
    '<table><thead><tr>' +
    '<th style="text-align:left">Hour (PT)</th>' +
    '<th>HE</th>' +
    '<th>LMP ($/MWh)</th>' +
    '</tr></thead><tbody>' + tbody + '</tbody></table>';
}

// Init
initDatePicker();
loadData();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/data")
def data():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "date parameter required"})
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format, use YYYY-MM-DD"})

    print(f"[DAM] Fetching {date_str}", flush=True)
    raw, err = fetch_dam_day(date_str)

    if err:
        return jsonify({"error": err, "rows": []})

    rows = parse_dam_rows(raw)

    if not rows and raw:
        return jsonify({
            "error": f"Retrieved {len(raw)} rows from CAISO but none matched LMP_TYPE=LMP. Check server logs for column names.",
            "rows": []
        })

    return jsonify({"rows": rows})


if __name__ == "__main__":
    app.run(debug=True, port=5003)
