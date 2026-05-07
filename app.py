from flask import Flask, request, jsonify, render_template_string
import os
import requests
import zipfile
import io
import csv
import time
import concurrent.futures
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

PACIFIC = pytz.timezone("America/Los_Angeles")
NODE = "ELAP_PACE-APND"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/zip, application/octet-stream, */*",
}


def dt_to_utc_str(dt_str):
    local_dt = PACIFIC.localize(datetime.strptime(dt_str, "%Y-%m-%d"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y%m%dT%H:%M-0000")


def _fetch(label, params):
    """Shared fetch logic. Returns (rows, error_string)."""
    base_url = "https://oasis.caiso.com/oasisapi/SingleZip"
    print(f"[{label}] params={params}", flush=True)

    for attempt in range(3):
        try:
            resp = requests.get(base_url, params=params, headers=HEADERS, timeout=90)
            print(f"[{label}] HTTP {resp.status_code}, size={len(resp.content)}", flush=True)

            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f"[{label}] Rate limited — waiting {wait}s", flush=True)
                time.sleep(wait)
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
                                print(f"[{label}] {name}: {len(file_rows)} rows | cols={reader.fieldnames}", flush=True)
                                rows.extend(file_rows)
                        elif name.endswith(".xml"):
                            with z.open(name) as f:
                                xml_content = f.read(800).decode("utf-8", errors="replace")
                                print(f"[{label}] XML in ZIP ({name}): {xml_content}", flush=True)
                return rows, None
            except zipfile.BadZipFile:
                snippet = resp.content[:300].decode("utf-8", errors="replace")
                print(f"[{label}] Not a ZIP: {snippet}", flush=True)
                return [], f"Response was not a valid ZIP: {snippet[:200]}"

        except Exception as e:
            print(f"[{label}] attempt {attempt + 1} error: {e}", flush=True)
            if attempt < 2:
                time.sleep(5)

    return [], "All 3 fetch attempts failed"


def fetch_dam_day(date_str):
    next_day = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return _fetch("DAM", {
        "queryname": "PRC_LMP",
        "market_run_id": "DAM",
        "startdatetime": dt_to_utc_str(date_str),
        "enddatetime": dt_to_utc_str(next_day),
        "version": 1,
        "node": NODE,
        "resultformat": 6,
    })


def fetch_rtm_day(date_str):
    next_day = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return _fetch("RTM", {
        "queryname": "PRC_INTVL_LMP",
        "market_run_id": "RTM",
        "startdatetime": dt_to_utc_str(date_str),
        "enddatetime": dt_to_utc_str(next_day),
        "version": 1,
        "node": NODE,
        "resultformat": 6,
    })


def parse_dam_rows(rows):
    if not rows:
        return []
    lmp_types = set(r.get("LMP_TYPE", "MISSING") for r in rows)
    print(f"[PARSE DAM] {len(rows)} rows | LMP_TYPE values: {lmp_types}", flush=True)
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    result = []
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S")
                dt_utc = pytz.utc.localize(dt_utc)
                dt_pt = dt_utc.astimezone(PACIFIC)
                result.append({"date": dt_pt.strftime("%Y-%m-%d"), "hour": dt_pt.hour, "lmp": round(mw, 4)})
        except Exception as e:
            print(f"[PARSE DAM ERROR] {e}", flush=True)
    result.sort(key=lambda x: (x["date"], x["hour"]))
    return result


def parse_rtm_to_hourly(rows):
    """Average 5-min RTM intervals into hourly LMP."""
    if not rows:
        return []
    lmp_types = set(r.get("LMP_TYPE", "MISSING") for r in rows)
    print(f"[PARSE RTM] {len(rows)} rows | LMP_TYPE values: {lmp_types}", flush=True)
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    buckets = defaultdict(list)
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S")
                dt_utc = pytz.utc.localize(dt_utc)
                dt_pt = dt_utc.astimezone(PACIFIC)
                buckets[(dt_pt.strftime("%Y-%m-%d"), dt_pt.hour)].append(mw)
        except Exception as e:
            print(f"[PARSE RTM ERROR] {e}", flush=True)
    result = []
    for (date, hour), vals in sorted(buckets.items()):
        result.append({"date": date, "hour": hour, "lmp": round(sum(vals) / len(vals), 4)})
    print(f"[PARSE RTM] {len(result)} hourly averages from {len(lmp_rows)} intervals", flush=True)
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

  .main { max-width: 920px; margin: 0 auto; padding: 24px 20px; }

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
    padding: 5px 10px; height: 32px; cursor: pointer; outline: none;
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
    padding: 8px 16px; font-family: var(--mono); flex: 1; min-width: 120px;
  }
  .stat-chip .s-label { font-size: 9px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; display: block; margin-bottom: 3px; }
  .stat-chip .s-val { font-size: 15px; color: var(--text); font-weight: 500; }
  .stat-chip.dam-chip { border-top: 3px solid var(--accent); }
  .stat-chip.rtm-chip { border-top: 3px solid #2a7a8a; }
  .stat-chip.diff-chip { border-top: 3px solid var(--muted); }

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
  th.dam-col { color: var(--accent); }
  th.rtm-col { color: #2a7a8a; }
  th.diff-col { color: var(--muted); }
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

  .chart-wrap {
    background: var(--surface); border: 1px solid var(--border);
    padding: 20px 24px 16px; margin-bottom: 16px;
  }
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

  <div id="chartSection" style="display:none">
    <div class="chart-wrap">
      <canvas id="lmpChart"></canvas>
    </div>
  </div>

  <div id="statsRow" class="stats-row" style="display:none"></div>

  <div id="tableSection" style="display:none">
    <div class="section-header">
      <span class="market-label">DAM &amp; RTM</span>
      <span class="section-desc">Day-Ahead Hourly &amp; Real-Time 5-min Avg &middot; LMP ($/MWh)</span>
      <span class="section-count" id="rowCount"></span>
    </div>
    <div class="table-wrap"><div id="damTable"></div></div>
  </div>

</div>

<script>
const DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];

function todayPT() {
  const now = new Date();
  const ptOffset = -7 * 60;
  const ptMs = now.getTime() + (now.getTimezoneOffset() + ptOffset) * 60000;
  return new Date(ptMs).toISOString().slice(0, 10);
}

let currentDate = todayPT();
let lmpChart = null;

function renderChart(rows) {
  const labels = rows.map(r => 'HE ' + (r.hour + 1));
  const damData = rows.map(r => r.dam);
  const rtmData = rows.map(r => r.rtm);

  document.getElementById('chartSection').style.display = 'block';

  if (lmpChart) {
    lmpChart.data.labels = labels;
    lmpChart.data.datasets[0].data = damData;
    lmpChart.data.datasets[1].data = rtmData;
    lmpChart.update();
    return;
  }

  lmpChart = new Chart(document.getElementById('lmpChart'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'DAM LMP',
          data: damData,
          backgroundColor: 'rgba(176, 120, 0, 0.75)',
          borderColor: 'rgba(176, 120, 0, 1)',
          borderWidth: 1,
          borderRadius: 2,
        },
        {
          label: 'RTM Avg LMP',
          data: rtmData,
          backgroundColor: 'rgba(42, 122, 138, 0.65)',
          borderColor: 'rgba(42, 122, 138, 1)',
          borderWidth: 1,
          borderRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'top',
          align: 'end',
          labels: { font: { family: "'IBM Plex Mono'", size: 11 }, boxWidth: 12, padding: 16 },
        },
        tooltip: {
          bodyFont: { family: "'IBM Plex Mono'", size: 11 },
          titleFont: { family: "'IBM Plex Mono'", size: 11 },
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              return ' ' + ctx.dataset.label + ': ' + (v !== null ? '$' + v.toFixed(4) : '—');
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: 'rgba(232, 208, 138, 0.4)' },
          ticks: { font: { family: "'IBM Plex Mono'", size: 10 }, color: '#7a6020' },
        },
        y: {
          grid: { color: 'rgba(232, 208, 138, 0.4)' },
          ticks: {
            font: { family: "'IBM Plex Mono'", size: 10 },
            color: '#7a6020',
            callback: v => '$' + v.toFixed(2),
          },
          title: {
            display: true,
            text: '$/MWh',
            font: { family: "'IBM Plex Sans'", size: 11 },
            color: '#7a6020',
          },
        },
      },
    },
  });
}

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
  box.textContent = msg || '';
  box.style.display = msg ? 'block' : 'none';
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
  document.getElementById('chartSection').style.display = 'none';
  showError(null);
  setStatus('Fetching DAM + RTM data for ' + currentDate + '...', 'loading');

  try {
    const resp = await fetch('/data?date=' + currentDate);
    const data = await resp.json();

    if (data.error) {
      setStatus('Error — ' + currentDate, 'err');
      showError(data.error);
      btn.disabled = false;
      return;
    }

    if (!data.rows || data.rows.length === 0) {
      setStatus('No DAM data for ' + currentDate + ' — may not be published yet.', 'err');
      if (data.rtm_error) showError('RTM: ' + data.rtm_error);
      btn.disabled = false;
      return;
    }

    renderChart(data.rows);
    renderStats(data.rows);
    renderTable(data.rows);
    document.getElementById('lastUpdated').textContent = 'fetched ' + new Date().toLocaleTimeString();

    const rtmCount = data.rows.filter(r => r.rtm !== null).length;
    const note = rtmCount < data.rows.length ? ` (RTM available for ${rtmCount} hrs)` : '';
    setStatus('Loaded ' + data.rows.length + ' hours for ' + currentDate + note + '.', 'ok');
    if (data.rtm_error) showError('RTM note: ' + data.rtm_error);
  } catch (e) {
    setStatus('Fetch failed: ' + e.message, 'err');
    showError('Network or server error: ' + e.message);
  }
  btn.disabled = false;
}

function vc(v) {
  if (v === null || v === undefined) return 'vna';
  return v > 50 ? 'vpos' : v < 0 ? 'vneg' : 'vneu';
}
function fmt(v) {
  if (v === null || v === undefined) return '—';
  return '$' + v.toFixed(4);
}
function fmtDiff(v) {
  if (v === null || v === undefined) return '—';
  const sign = v > 0 ? '+' : '';
  return sign + v.toFixed(4);
}
function fmtHour(h) {
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return String(h12).padStart(2, '0') + ':00 ' + ampm;
}

function avg(arr) {
  const valid = arr.filter(v => v !== null && v !== undefined);
  return valid.length ? valid.reduce((a, b) => a + b, 0) / valid.length : null;
}

function renderStats(rows) {
  const damVals = rows.map(r => r.dam);
  const rtmVals = rows.map(r => r.rtm).filter(v => v !== null);
  const damAvg = avg(damVals);
  const rtmAvg = rtmVals.length ? avg(rtmVals) : null;
  const diffAvg = (damAvg !== null && rtmAvg !== null) ? damAvg - rtmAvg : null;

  const statsRow = document.getElementById('statsRow');
  statsRow.style.display = 'flex';
  statsRow.innerHTML =
    chip('DAM Daily Avg', fmt(damAvg), 'dam-chip') +
    chip('RTM Daily Avg', rtmAvg !== null ? fmt(rtmAvg) : '—', 'rtm-chip') +
    chip('Avg Spread (DAM−RTM)', diffAvg !== null ? fmtDiff(diffAvg) : '—', 'diff-chip') +
    chip('Hours w/ RTM', rtmVals.length + ' / ' + rows.length, '');
}

function chip(label, val, cls) {
  return '<div class="stat-chip ' + cls + '"><span class="s-label">' + label + '</span><span class="s-val">' + val + '</span></div>';
}

function renderTable(rows) {
  document.getElementById('tableSection').style.display = 'block';
  document.getElementById('rowCount').textContent = rows.length + ' hours';

  const damVals = rows.map(r => r.dam);
  const rtmVals = rows.map(r => r.rtm).filter(v => v !== null);
  const damAvg = avg(damVals);
  const rtmAvg = rtmVals.length ? avg(rtmVals) : null;
  const diffAvg = (damAvg !== null && rtmAvg !== null) ? damAvg - rtmAvg : null;

  let tbody = '';
  rows.forEach(r => {
    const diff = (r.dam !== null && r.rtm !== null) ? r.dam - r.rtm : null;
    const endHour = (r.hour + 1) % 24;
    const endAmPm = endHour < 12 ? 'AM' : 'PM';
    const endH12 = endHour % 12 === 0 ? 12 : endHour % 12;
    const timeLabel = fmtHour(r.hour) + ' – ' + String(endH12).padStart(2, '0') + ':00 ' + endAmPm;
    tbody += '<tr>'
      + '<td>' + timeLabel + '</td>'
      + '<td>HE ' + (r.hour + 1) + '</td>'
      + '<td class="' + vc(r.dam) + '">' + fmt(r.dam) + '</td>'
      + '<td class="' + vc(r.rtm) + '">' + fmt(r.rtm) + '</td>'
      + '<td class="' + vc(diff) + '">' + fmtDiff(diff) + '</td>'
      + '</tr>';
  });

  tbody += '<tr class="avg-row">'
    + '<td colspan="2">Daily Average</td>'
    + '<td class="' + vc(damAvg) + '">' + fmt(damAvg) + '</td>'
    + '<td class="' + vc(rtmAvg) + '">' + (rtmAvg !== null ? fmt(rtmAvg) : '—') + '</td>'
    + '<td class="' + vc(diffAvg) + '">' + (diffAvg !== null ? fmtDiff(diffAvg) : '—') + '</td>'
    + '</tr>';

  document.getElementById('damTable').innerHTML =
    '<table><thead><tr>'
    + '<th style="text-align:left">Hour (PT)</th>'
    + '<th>HE</th>'
    + '<th class="dam-col">DAM LMP</th>'
    + '<th class="rtm-col">RTM Avg LMP</th>'
    + '<th class="diff-col">Spread (DAM−RTM)</th>'
    + '</tr></thead><tbody>' + tbody + '</tbody></table>';
}

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

    print(f"[REQUEST] Fetching DAM + RTM for {date_str}", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        dam_future = executor.submit(fetch_dam_day, date_str)
        rtm_future = executor.submit(fetch_rtm_day, date_str)
        dam_raw, dam_err = dam_future.result()
        rtm_raw, rtm_err = rtm_future.result()

    if dam_err:
        return jsonify({"error": dam_err, "rows": []})

    dam_rows = parse_dam_rows(dam_raw)
    rtm_rows = parse_rtm_to_hourly(rtm_raw)

    if not dam_rows:
        return jsonify({"error": "No DAM data returned", "rows": [], "rtm_error": rtm_err})

    rtm_by_hour = {r["hour"]: r["lmp"] for r in rtm_rows}
    combined = []
    for r in dam_rows:
        combined.append({
            "date": r["date"],
            "hour": r["hour"],
            "dam": r["lmp"],
            "rtm": rtm_by_hour.get(r["hour"], None),
        })

    return jsonify({"rows": combined, "rtm_error": rtm_err})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5003))
    app.run(debug=False, host="0.0.0.0", port=port)
