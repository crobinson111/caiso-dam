from flask import Flask, request, jsonify, render_template_string
import os
import requests
import zipfile
import io
import csv
import time
import threading
import concurrent.futures
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

PACIFIC = pytz.timezone("America/Los_Angeles")
NODE = "ELAP_PACE-APND"
MAX_RANGE_DAYS = 92
CHUNK_DAYS_DAM = 31
CHUNK_DAYS_RTM = 14

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/zip, application/octet-stream, */*",
}

# Global rate limiter — max 1 CAISO request per 3 seconds across all threads
_caiso_lock = threading.Lock()
_last_caiso_call = [0.0]


def dt_to_utc_str(dt_str):
    local_dt = PACIFIC.localize(datetime.strptime(dt_str, "%Y-%m-%d"))
    return local_dt.astimezone(pytz.utc).strftime("%Y%m%dT%H:%M-0000")


def _fetch(label, params):
    """Rate-limited fetch from CAISO OASIS. Returns (rows, error_string)."""
    with _caiso_lock:
        wait = 3.0 - (time.time() - _last_caiso_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_caiso_call[0] = time.time()

    base_url = "https://oasis.caiso.com/oasisapi/SingleZip"
    print(f"[{label}] {params.get('startdatetime')} → {params.get('enddatetime')}", flush=True)

    for attempt in range(3):
        try:
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=90)
            print(f"[{label}] HTTP {resp.status_code}, size={len(resp.content)}", flush=True)

            if resp.status_code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            if resp.status_code != 200:
                return [], f"HTTP {resp.status_code} from CAISO"
            if resp.content[:1] == b"<":
                xml_msg = resp.content[:800].decode("utf-8", errors="replace")
                print(f"[{label}] XML error: {xml_msg}", flush=True)
                import re
                match = re.search(r"<err>(.*?)</err>|<message>(.*?)</message>", xml_msg, re.DOTALL)
                friendly = match.group(0)[:200] if match else xml_msg[:200]
                return [], f"CAISO API error: {friendly}"

            try:
                rows = []
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    print(f"[{label}] ZIP: {z.namelist()}", flush=True)
                    for name in z.namelist():
                        if name.endswith(".csv"):
                            with z.open(name) as f:
                                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                                file_rows = list(reader)
                                print(f"[{label}] {name}: {len(file_rows)} rows", flush=True)
                                rows.extend(file_rows)
                        elif name.endswith(".xml"):
                            with z.open(name) as f:
                                print(f"[{label}] XML in ZIP: {f.read(400).decode('utf-8', errors='replace')}", flush=True)
                return rows, None
            except zipfile.BadZipFile:
                snippet = resp.content[:300].decode("utf-8", errors="replace")
                return [], f"Not a valid ZIP: {snippet[:200]}"

        except Exception as e:
            print(f"[{label}] attempt {attempt + 1} error: {e}", flush=True)
            if attempt < 2:
                time.sleep(5)

    return [], "All 3 fetch attempts failed"


def date_chunks(start_date, end_date, chunk_days):
    chunks = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end_excl = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    while current < end_excl:
        chunk_end = min(current + timedelta(days=chunk_days), end_excl)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end
    return chunks


def fetch_dam_range(start_date, end_date):
    all_rows = []
    for cs, ce in date_chunks(start_date, end_date, CHUNK_DAYS_DAM):
        rows, err = _fetch("DAM", {
            "queryname": "PRC_LMP", "market_run_id": "DAM",
            "startdatetime": dt_to_utc_str(cs), "enddatetime": dt_to_utc_str(ce),
            "version": 1, "node": NODE, "resultformat": 6,
        })
        if err:
            return [], err
        all_rows.extend(rows)
    return all_rows, None


def fetch_rtm_range(start_date, end_date):
    all_rows, last_err = [], None
    for cs, ce in date_chunks(start_date, end_date, CHUNK_DAYS_RTM):
        rows, err = _fetch("RTM", {
            "queryname": "PRC_INTVL_LMP", "market_run_id": "RTM",
            "startdatetime": dt_to_utc_str(cs), "enddatetime": dt_to_utc_str(ce),
            "version": 1, "node": NODE, "resultformat": 6,
        })
        if err:
            last_err = err
            break
        all_rows.extend(rows)
    return all_rows, last_err


def parse_dam_rows(rows):
    if not rows:
        return []
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    print(f"[PARSE DAM] {len(rows)} raw → {len(lmp_rows)} LMP rows", flush=True)
    result = []
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = pytz.utc.localize(datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S"))
                dt_pt = dt_utc.astimezone(PACIFIC)
                result.append({"date": dt_pt.strftime("%Y-%m-%d"), "hour": dt_pt.hour, "lmp": round(mw, 4)})
        except Exception as e:
            print(f"[PARSE DAM ERROR] {e}", flush=True)
    result.sort(key=lambda x: (x["date"], x["hour"]))
    return result


def parse_rtm_to_hourly(rows):
    if not rows:
        return []
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    print(f"[PARSE RTM] {len(rows)} raw → {len(lmp_rows)} LMP rows", flush=True)
    buckets = defaultdict(list)
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = pytz.utc.localize(datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S"))
                dt_pt = dt_utc.astimezone(PACIFIC)
                buckets[(dt_pt.strftime("%Y-%m-%d"), dt_pt.hour)].append(mw)
        except Exception as e:
            print(f"[PARSE RTM ERROR] {e}", flush=True)
    result = [{"date": d, "hour": h, "lmp": round(sum(v) / len(v), 4)} for (d, h), v in sorted(buckets.items())]
    print(f"[PARSE RTM] {len(result)} hourly averages", flush=True)
    return result


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>CAISO DAM + RTM — ELAP_PACE-APND</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #fdf8ee; --surface: #ffffff; --surface2: #fef3d0;
    --border: #e8d08a; --border2: #f0dfa0;
    --text: #2a1f00; --muted: #7a6020;
    --accent: #b07800; --accent-dark: #8a5c00;
    --rtm: #2a7a8a; --danger: #c0392b;
    --mono: 'IBM Plex Mono', monospace; --sans: 'IBM Plex Sans', sans-serif;
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
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: 1px; }
  .header-right { margin-left: auto; text-align: right; }
  .header-right .node { font-family: var(--mono); font-size: 11px; color: var(--muted); }
  .header-right .last-updated { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 2px; }

  .main { max-width: 960px; margin: 0 auto; padding: 24px 20px; }

  .control-bar {
    background: var(--surface); border: 1px solid var(--border);
    padding: 16px 20px; margin-bottom: 20px;
    display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  }
  .date-group { display: flex; align-items: center; gap: 8px; }
  .date-label { font-family: var(--mono); font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
  input[type="date"] {
    font-family: var(--mono); font-size: 13px; color: var(--text);
    border: 1px solid var(--border); background: var(--surface2);
    padding: 5px 10px; height: 32px; cursor: pointer; outline: none;
  }
  input[type="date"]:focus { border-color: var(--accent); }
  .date-sep { font-family: var(--mono); color: var(--muted); font-size: 13px; padding: 0 4px; }
  .range-hint { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-left: 4px; }
  .load-btn {
    background: var(--accent); border: none; color: #fff;
    font-family: var(--sans); font-size: 13px; font-weight: 600;
    padding: 7px 22px; height: 32px; cursor: pointer; margin-left: auto;
    transition: background 0.15s;
  }
  .load-btn:hover { background: var(--accent-dark); }
  .load-btn:disabled { background: var(--border); cursor: not-allowed; }

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

  .chart-wrap {
    background: var(--surface); border: 1px solid var(--border);
    padding: 20px 24px 16px; margin-bottom: 16px;
  }

  .stats-row { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }
  .stat-chip {
    background: var(--surface); border: 1px solid var(--border);
    padding: 8px 16px; font-family: var(--mono); flex: 1; min-width: 130px;
  }
  .stat-chip .s-label { font-size: 9px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; display: block; margin-bottom: 3px; }
  .stat-chip .s-val { font-size: 15px; color: var(--text); font-weight: 500; }
  .stat-chip.dam-chip { border-top: 3px solid var(--accent); }
  .stat-chip.rtm-chip { border-top: 3px solid var(--rtm); }
  .stat-chip.diff-chip { border-top: 3px solid var(--muted); }

  .section-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px; border-left: 4px solid var(--accent); background: var(--surface2);
  }
  .market-label { font-size: 12px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; color: var(--accent); }
  .section-desc { font-size: 12px; color: var(--muted); }
  .section-count { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }

  .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-top: none; max-height: 520px; overflow-y: auto; }
  table { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  thead tr { background: var(--surface2); position: sticky; top: 0; z-index: 1; }
  th {
    padding: 8px 16px; text-align: right;
    color: var(--muted); font-weight: 500; font-size: 11px;
    border-bottom: 1px solid var(--border); border-right: 1px solid var(--border2);
    text-transform: uppercase; letter-spacing: 1px;
  }
  th:first-child { text-align: left; }
  th:last-child { border-right: none; }
  th.dam-col { color: var(--accent); }
  th.rtm-col { color: var(--rtm); }
  td { padding: 7px 16px; border-bottom: 1px solid var(--border2); border-right: 1px solid var(--border2); text-align: right; }
  td:first-child { text-align: left; color: var(--muted); font-size: 11px; white-space: nowrap; }
  td:last-child { border-right: none; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: var(--surface2); }
  tr:hover td { background: #f5e8a0 !important; }
  .vpos { color: var(--danger); }
  .vneg { color: #1a6b3a; }
  .vneu { color: var(--text); }
  .vna  { color: var(--border); }
  .avg-row td { font-weight: 600; background: var(--surface2) !important; border-top: 2px solid var(--border); }
  .avg-row td:first-child { color: var(--accent); }
</style>
</head>
<body>

<header>
  <div class="logo">LMP</div>
  <h1>CAISO Day-Ahead &amp; Real-Time</h1>
  <div class="header-right">
    <div class="node">ELAP_PACE-APND</div>
    <div class="last-updated" id="lastUpdated"></div>
  </div>
</header>

<div class="main">

  <div class="control-bar">
    <div class="date-group">
      <span class="date-label">From</span>
      <input type="date" id="startDate" onchange="onDateChange()" />
      <span class="date-sep">—</span>
      <span class="date-label">To</span>
      <input type="date" id="endDate" onchange="onDateChange()" />
      <span class="range-hint" id="rangeHint"></span>
    </div>
    <button class="load-btn" id="loadBtn" onclick="loadData()">&#8635; Load</button>
  </div>

  <div class="status-bar">
    <div class="status-dot" id="statusDot"></div>
    <span id="statusMsg">Select a date range and click Load.</span>
  </div>

  <div class="error-box" id="errorBox"></div>

  <div id="chartSection" style="display:none">
    <div class="chart-wrap">
      <canvas id="lmpChart"></canvas>
    </div>
  </div>

  <div id="statsRow" class="stats-row" style="display:none"></div>

  <div id="tableSection" style="display:none">
    <div class="section-header">
      <span class="market-label">DAM &amp; RTM</span>
      <span class="section-desc" id="tableDesc"></span>
      <span class="section-count" id="rowCount"></span>
    </div>
    <div class="table-wrap"><div id="dataTable"></div></div>
  </div>

</div>

<script>
const DAYS = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function todayPT() {
  const now = new Date();
  const ptMs = now.getTime() + (now.getTimezoneOffset() + (-7 * 60)) * 60000;
  return new Date(ptMs).toISOString().slice(0, 10);
}

function init() {
  const today = todayPT();
  document.getElementById('startDate').value = today;
  document.getElementById('endDate').value = today;
  updateRangeHint();
}

function onDateChange() {
  updateRangeHint();
}

function updateRangeHint() {
  const s = document.getElementById('startDate').value;
  const e = document.getElementById('endDate').value;
  const hint = document.getElementById('rangeHint');
  if (!s || !e) { hint.textContent = ''; return; }
  const days = Math.round((new Date(e + 'T12:00:00') - new Date(s + 'T12:00:00')) / 86400000) + 1;
  if (days < 1) { hint.textContent = '⚠ end before start'; hint.style.color = 'var(--danger)'; return; }
  if (days > 92) { hint.textContent = '⚠ max 92 days'; hint.style.color = 'var(--danger)'; return; }
  hint.textContent = days === 1 ? '1 day' : days + ' days';
  hint.style.color = 'var(--muted)';
}

function setStatus(msg, state) {
  document.getElementById('statusMsg').textContent = msg;
  document.getElementById('statusDot').className = 'status-dot' + (state ? ' ' + state : '');
}
function showError(msg) {
  const box = document.getElementById('errorBox');
  box.textContent = msg || '';
  box.style.display = msg ? 'block' : 'none';
}

async function loadData() {
  const start = document.getElementById('startDate').value;
  const end = document.getElementById('endDate').value;
  if (!start || !end) { setStatus('Please select a date range.', 'err'); return; }

  const days = Math.round((new Date(end + 'T12:00:00') - new Date(start + 'T12:00:00')) / 86400000) + 1;
  if (days < 1) { setStatus('End date must be on or after start date.', 'err'); return; }
  if (days > 92) { setStatus('Maximum range is 92 days (~3 months).', 'err'); return; }

  const btn = document.getElementById('loadBtn');
  btn.disabled = true;
  document.getElementById('chartSection').style.display = 'none';
  document.getElementById('statsRow').style.display = 'none';
  document.getElementById('tableSection').style.display = 'none';
  showError(null);

  const waitMsg = days > 7 ? ` — fetching ${days} days, please wait…` : '';
  setStatus('Loading data for ' + start + (days > 1 ? ' → ' + end : '') + waitMsg, 'loading');

  try {
    const resp = await fetch('/data?start_date=' + start + '&end_date=' + end);
    const data = await resp.json();

    if (data.error) {
      setStatus('Error', 'err');
      showError(data.error);
      btn.disabled = false;
      return;
    }
    if (!data.rows || data.rows.length === 0) {
      setStatus('No data returned — DAM may not be published yet.', 'err');
      btn.disabled = false;
      return;
    }

    const isSingleDay = start === end;
    renderChart(data.rows, isSingleDay, start, end);
    renderStats(data.rows);
    renderTable(data.rows, isSingleDay);
    document.getElementById('lastUpdated').textContent = 'fetched ' + new Date().toLocaleTimeString();
    const rtmCount = data.rows.filter(r => r.rtm !== null).length;
    const rtmNote = rtmCount < data.rows.length ? ` (RTM: ${rtmCount}/${data.rows.length} hrs)` : '';
    setStatus('Loaded ' + data.rows.length + ' hourly intervals across ' + days + ' day' + (days > 1 ? 's' : '') + rtmNote + '.', 'ok');
    if (data.rtm_error) showError('RTM note: ' + data.rtm_error);
  } catch (e) {
    setStatus('Fetch failed: ' + e.message, 'err');
    showError('Network or server error: ' + e.message);
  }
  btn.disabled = false;
}

// ── Chart ──────────────────────────────────────────────────────────────────

let lmpChart = null;

function renderChart(rows, isSingleDay, start, end) {
  document.getElementById('chartSection').style.display = 'block';

  const damData = rows.map(r => r.dam);
  const rtmData = rows.map(r => r.rtm);

  // For multi-day: label = "May 1" at midnight, blank for other hours
  // For single-day: label = "HE N"
  let labels;
  if (isSingleDay) {
    labels = rows.map(r => 'HE ' + (r.hour + 1));
  } else {
    labels = rows.map(r => r.hour === 0 ? fmtDateShort(r.date) : '');
  }

  const chartType = isSingleDay ? 'bar' : 'line';

  const damDataset = {
    label: 'DAM LMP',
    data: damData,
    backgroundColor: isSingleDay ? 'rgba(176,120,0,0.75)' : 'rgba(176,120,0,0.15)',
    borderColor: 'rgba(176,120,0,1)',
    borderWidth: isSingleDay ? 1 : 1.5,
    borderRadius: isSingleDay ? 2 : 0,
    pointRadius: 0,
    tension: 0.1,
    fill: false,
  };
  const rtmDataset = {
    label: 'RTM Avg LMP',
    data: rtmData,
    backgroundColor: isSingleDay ? 'rgba(42,122,138,0.65)' : 'rgba(42,122,138,0.1)',
    borderColor: 'rgba(42,122,138,1)',
    borderWidth: isSingleDay ? 1 : 1.5,
    borderRadius: isSingleDay ? 2 : 0,
    pointRadius: 0,
    tension: 0.1,
    fill: false,
    spanGaps: false,
  };

  const options = {
    responsive: true,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: {
        position: 'top', align: 'end',
        labels: { font: { family: "'IBM Plex Mono'", size: 11 }, boxWidth: 12, padding: 16 },
      },
      tooltip: {
        bodyFont: { family: "'IBM Plex Mono'", size: 11 },
        titleFont: { family: "'IBM Plex Mono'", size: 11 },
        callbacks: {
          title: ctx => {
            const r = rows[ctx[0].dataIndex];
            return fmtDateShort(r.date) + '  HE ' + (r.hour + 1);
          },
          label: ctx => {
            const v = ctx.parsed.y;
            return ' ' + ctx.dataset.label + ': ' + (v !== null ? '$' + v.toFixed(4) : '—');
          },
        },
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(232,208,138,0.4)' },
        ticks: {
          font: { family: "'IBM Plex Mono'", size: 10 }, color: '#7a6020',
          maxTicksLimit: isSingleDay ? 24 : 20,
          autoSkip: !isSingleDay,
          maxRotation: isSingleDay ? 0 : 45,
        },
      },
      y: {
        grid: { color: 'rgba(232,208,138,0.4)' },
        ticks: {
          font: { family: "'IBM Plex Mono'", size: 10 }, color: '#7a6020',
          callback: v => '$' + v.toFixed(2),
        },
        title: { display: true, text: '$/MWh', font: { family: "'IBM Plex Sans'", size: 11 }, color: '#7a6020' },
      },
    },
  };

  if (lmpChart) {
    lmpChart.destroy();
    lmpChart = null;
  }
  lmpChart = new Chart(document.getElementById('lmpChart'), {
    type: chartType,
    data: { labels, datasets: [damDataset, rtmDataset] },
    options,
  });
}

// ── Stats ──────────────────────────────────────────────────────────────────

function avg(arr) {
  const v = arr.filter(x => x !== null && x !== undefined);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
function vc(v) {
  if (v === null || v === undefined) return 'vna';
  return v > 50 ? 'vpos' : v < 0 ? 'vneg' : 'vneu';
}
function fmt(v) { return v === null || v === undefined ? '—' : '$' + v.toFixed(4); }
function fmtDiff(v) {
  if (v === null || v === undefined) return '—';
  return (v > 0 ? '+' : '') + v.toFixed(4);
}
function fmtDateShort(d) {
  const dt = new Date(d + 'T12:00:00');
  return MONTHS[dt.getMonth()] + ' ' + dt.getDate();
}

function renderStats(rows) {
  const damAvg = avg(rows.map(r => r.dam));
  const rtmAvg = avg(rows.map(r => r.rtm));
  const spread = damAvg !== null && rtmAvg !== null ? damAvg - rtmAvg : null;
  const rtmHrs = rows.filter(r => r.rtm !== null).length;

  document.getElementById('statsRow').style.display = 'flex';
  document.getElementById('statsRow').innerHTML =
    chip('DAM Avg', fmt(damAvg), 'dam-chip') +
    chip('RTM Avg', fmt(rtmAvg), 'rtm-chip') +
    chip('Avg Spread', fmtDiff(spread), 'diff-chip') +
    chip('RTM Hours', rtmHrs + ' / ' + rows.length, '');
}
function chip(label, val, cls) {
  return '<div class="stat-chip ' + cls + '"><span class="s-label">' + label + '</span><span class="s-val">' + val + '</span></div>';
}

// ── Table ──────────────────────────────────────────────────────────────────

function fmtHour(h) {
  const ap = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return String(h12).padStart(2,'0') + ':00 ' + ap;
}

function renderTable(rows, isSingleDay) {
  document.getElementById('tableSection').style.display = 'block';

  if (isSingleDay) {
    renderHourlyTable(rows);
  } else {
    renderDailySummaryTable(rows);
  }
}

function renderHourlyTable(rows) {
  document.getElementById('tableDesc').textContent = 'Hourly LMP ($/MWh)';
  document.getElementById('rowCount').textContent = rows.length + ' hours';

  const damAvg = avg(rows.map(r => r.dam));
  const rtmAvg = avg(rows.map(r => r.rtm));
  const spreadAvg = damAvg !== null && rtmAvg !== null ? damAvg - rtmAvg : null;

  let tbody = '';
  rows.forEach(r => {
    const diff = r.dam !== null && r.rtm !== null ? r.dam - r.rtm : null;
    const endH = (r.hour + 1) % 24;
    const label = fmtHour(r.hour) + ' – ' + String(endH % 12 === 0 ? 12 : endH % 12).padStart(2,'0') + ':00 ' + (endH < 12 ? 'AM' : 'PM');
    tbody += '<tr>'
      + '<td>' + label + '</td><td>HE ' + (r.hour + 1) + '</td>'
      + '<td class="' + vc(r.dam) + '">' + fmt(r.dam) + '</td>'
      + '<td class="' + vc(r.rtm) + '">' + fmt(r.rtm) + '</td>'
      + '<td class="' + vc(diff) + '">' + fmtDiff(diff) + '</td>'
      + '</tr>';
  });
  tbody += '<tr class="avg-row"><td colspan="2">Daily Average</td>'
    + '<td class="' + vc(damAvg) + '">' + fmt(damAvg) + '</td>'
    + '<td class="' + vc(rtmAvg) + '">' + fmt(rtmAvg) + '</td>'
    + '<td class="' + vc(spreadAvg) + '">' + fmtDiff(spreadAvg) + '</td></tr>';

  document.getElementById('dataTable').innerHTML =
    '<table><thead><tr>'
    + '<th style="text-align:left">Hour (PT)</th><th>HE</th>'
    + '<th class="dam-col">DAM LMP</th><th class="rtm-col">RTM Avg</th><th>Spread</th>'
    + '</tr></thead><tbody>' + tbody + '</tbody></table>';
}

function renderDailySummaryTable(rows) {
  // Group rows by date
  const byDate = {};
  rows.forEach(r => {
    if (!byDate[r.date]) byDate[r.date] = [];
    byDate[r.date].push(r);
  });
  const dates = Object.keys(byDate).sort();
  document.getElementById('tableDesc').textContent = 'Daily averages ($/MWh)';
  document.getElementById('rowCount').textContent = dates.length + ' days';

  let tbody = '';
  const allDam = [], allRtm = [];
  dates.forEach(d => {
    const hrs = byDate[d];
    const damDay = avg(hrs.map(r => r.dam));
    const rtmDay = avg(hrs.map(r => r.rtm));
    const spread = damDay !== null && rtmDay !== null ? damDay - rtmDay : null;
    const dt = new Date(d + 'T12:00:00');
    const weekday = DAYS[dt.getDay()];
    allDam.push(damDay);
    if (rtmDay !== null) allRtm.push(rtmDay);
    tbody += '<tr>'
      + '<td>' + d + '</td><td>' + weekday + '</td>'
      + '<td class="' + vc(damDay) + '">' + fmt(damDay) + '</td>'
      + '<td class="' + vc(rtmDay) + '">' + fmt(rtmDay) + '</td>'
      + '<td class="' + vc(spread) + '">' + fmtDiff(spread) + '</td>'
      + '<td>' + hrs.length + '</td>'
      + '</tr>';
  });

  const totalDam = avg(allDam);
  const totalRtm = allRtm.length ? avg(allRtm) : null;
  const totalSpread = totalDam !== null && totalRtm !== null ? totalDam - totalRtm : null;
  tbody += '<tr class="avg-row"><td colspan="2">Period Average</td>'
    + '<td class="' + vc(totalDam) + '">' + fmt(totalDam) + '</td>'
    + '<td class="' + vc(totalRtm) + '">' + fmt(totalRtm) + '</td>'
    + '<td class="' + vc(totalSpread) + '">' + fmtDiff(totalSpread) + '</td>'
    + '<td>' + rows.length + '</td></tr>';

  document.getElementById('dataTable').innerHTML =
    '<table><thead><tr>'
    + '<th style="text-align:left">Date</th><th style="text-align:left">Day</th>'
    + '<th class="dam-col">DAM Avg</th><th class="rtm-col">RTM Avg</th><th>Spread</th><th>Hrs</th>'
    + '</tr></thead><tbody>' + tbody + '</tbody></table>';
}

init();
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/data")
def data():
    start_date = request.args.get("start_date") or request.args.get("date")
    end_date = request.args.get("end_date") or start_date

    if not start_date:
        return jsonify({"error": "start_date parameter required"})
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format, use YYYY-MM-DD"})
    if end_dt < start_dt:
        return jsonify({"error": "end_date must be on or after start_date"})
    if (end_dt - start_dt).days > MAX_RANGE_DAYS:
        return jsonify({"error": f"Date range cannot exceed {MAX_RANGE_DAYS} days"})

    days = (end_dt - start_dt).days + 1
    print(f"[REQUEST] {start_date} → {end_date} ({days} days)", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        dam_f = executor.submit(fetch_dam_range, start_date, end_date)
        rtm_f = executor.submit(fetch_rtm_range, start_date, end_date)
        dam_raw, dam_err = dam_f.result()
        rtm_raw, rtm_err = rtm_f.result()

    if dam_err:
        return jsonify({"error": dam_err, "rows": []})

    dam_rows = parse_dam_rows(dam_raw)
    rtm_rows = parse_rtm_to_hourly(rtm_raw)

    if not dam_rows:
        return jsonify({"error": "No DAM data returned", "rows": [], "rtm_error": rtm_err})

    rtm_by_key = {(r["date"], r["hour"]): r["lmp"] for r in rtm_rows}
    combined = [
        {"date": r["date"], "hour": r["hour"], "dam": r["lmp"],
         "rtm": rtm_by_key.get((r["date"], r["hour"]), None)}
        for r in dam_rows
    ]

    return jsonify({"rows": combined, "rtm_error": rtm_err})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(debug=False, host="0.0.0.0", port=port)
