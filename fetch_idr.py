#!/usr/bin/env python3
"""Fetch all IDR studies and export metadata (license, organism, DOI, etc.) to CSV, HTML, and Google Sheets."""

import csv
import html
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

import requests

BASE_URL = "https://idr.openmicroscopy.org"
OUT_DIR = Path(__file__).parent / "output"
CACHE_PATH = Path(__file__).parent / "idr_studies_cache.json"
CREDENTIALS_DIR = Path(__file__).parent / "credentials"
CLIENT_SECRETS_PATH = CREDENTIALS_DIR / "client_secrets.json"
TOKEN_PATH = CREDENTIALS_DIR / "sheets_token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Map annotation keys we care about
MAP_KEYS = {
    "Organism": "organism",
    "Study Type": "study_type",
    "Imaging Method": "imaging_method",
    "License": "license",
    "Publication Title": "pub_title",
    "Publication DOI": "pub_doi",
    "Data DOI": "data_doi",
    "Publication Authors": "authors",
    "Release Date": "release_date",
}

HEADERS = [
    "Study ID", "Sub-entries", "Type", "Organism", "Study Type",
    "Imaging Method", "Datasets / Plates", "Dataset Count", "License",
    "Publication Title", "Publication DOI", "Data DOI", "Authors", "Release Date",
    "OMERO Link",
]

LICENSE_COLORS_CSS = {
    "cc by 4.0": "#c6efce",
    "cc by-sa 3.0": "#c6efce",
    "cc0 1.0": "#c6efce",
    "cc by-nc 4.0": "#fff2cc",
    "cc by-nc-sa 3.0": "#fff2cc",
    "cc by-nc-nd 4.0": "#f4cccc",
}
DEFAULT_LICENSE_CSS = "#f4cccc"

# Chart colors matching license permissiveness
LICENSE_CHART_COLORS = {
    "CC BY 4.0": "#4caf50",
    "CC0 1.0": "#66bb6a",
    "CC BY-SA 3.0": "#81c784",
    "CC BY-NC 4.0": "#ffc107",
    "CC BY-NC-SA 3.0": "#ffb300",
    "CC BY-NC-ND 4.0": "#ef5350",
    "(no license)": "#9e9e9e",
}


def fetch_all_studies() -> list[dict]:
    """Fetch all projects and screens from the IDR API."""
    studies = []
    for obj_type in ("project", "screen"):
        url = f"{BASE_URL}/api/v0/m/{obj_type}s/?limit=500"
        print(f"Fetching {obj_type}s from {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        items = resp.json()["data"]
        print(f"  Found {len(items)} {obj_type}s")

        for item in items:
            study = {
                "id": item["@id"],
                "name": item["Name"],
                "type": obj_type.capitalize(),
            }
            m = re.match(r"(idr\d+)", item["Name"], re.IGNORECASE)
            study["study_id"] = m.group(1).lower() if m else ""
            studies.append(study)

    studies.sort(key=lambda s: s.get("study_id", ""))
    return studies


def fetch_study_metadata(obj_type: str, obj_id: int) -> dict:
    """Get map annotations (license, DOI, etc.) for a study."""
    url = f"{BASE_URL}/webclient/api/annotations/?type=map&{obj_type.lower()}={obj_id}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Warning: failed to fetch annotations for {obj_type} {obj_id}: {e}")
        return {}

    metadata = {}
    for ann in resp.json().get("annotations", []):
        for kv in ann.get("values", []):
            key, val = kv[0], kv[1]
            if key in MAP_KEYS:
                metadata[MAP_KEYS[key]] = val
    return metadata


def fetch_children(obj_type: str, obj_id: int) -> list[str]:
    """Get dataset names (for projects) or plate names (for screens)."""
    child_type = "datasets" if obj_type == "Project" else "plates"
    url = f"{BASE_URL}/api/v0/m/{obj_type.lower()}s/{obj_id}/{child_type}/?limit=500"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Warning: failed to fetch {child_type} for {obj_type} {obj_id}: {e}")
        return []

    return [item["Name"] for item in resp.json().get("data", [])]


def normalize_license(raw: str) -> str:
    """Extract just the license name from a raw license string."""
    if not raw:
        return ""
    raw_lower = raw.lower().strip()
    for prefix in ["cc by-nc-sa", "cc by-nc-nd", "cc by-nc", "cc by-sa", "cc by-nd", "cc by", "cc0"]:
        if prefix in raw_lower:
            m = re.search(rf"({re.escape(prefix)}\s*[\d.]*)", raw_lower)
            if m:
                return m.group(1).strip().upper()
            return prefix.upper()
    return raw.split("http")[0].strip() if "http" in raw else raw.strip()


def omero_url(s: dict) -> str:
    """Build the OMERO webclient URL for a study."""
    obj_type = s.get("type", "Project").lower()
    obj_id = s.get("id", "")
    return f"{BASE_URL}/webclient/?show={obj_type}-{obj_id}"


def consolidate_studies(raw_studies: list[dict]) -> list[dict]:
    """Group 248 raw entries into ~173 unique studies by study_id.

    Many IDR studies have multiple sub-entries (screenA/B, experimentA/B/C).
    This merges them into one row per study, combining children and picking
    the best metadata from whichever sub-entry has it.
    """
    from collections import OrderedDict

    grouped = OrderedDict()
    for s in raw_studies:
        sid = s.get("study_id", "") or s.get("name", "")
        if sid not in grouped:
            grouped[sid] = {
                "study_id": s.get("study_id", ""),
                "entries": [],       # sub-entries (screenA, experimentB, etc.)
                "children": [],      # all datasets/plates across sub-entries
                "types": set(),
                # metadata — fill from first entry that has it
                "organism": "", "study_type": "", "imaging_method": "",
                "license": "", "pub_title": "", "pub_doi": "", "data_doi": "",
                "authors": "", "release_date": "",
            }
        g = grouped[sid]
        g["entries"].append(s)
        g["types"].add(s.get("type", ""))
        g["children"].extend(s.get("children", []))

        # Fill metadata from first sub-entry that has each field
        for key in ["organism", "study_type", "imaging_method", "license",
                     "pub_title", "pub_doi", "data_doi", "authors", "release_date"]:
            if not g[key] and s.get(key):
                g[key] = s[key]

    result = []
    for sid, g in grouped.items():
        entry_names = [e["name"] for e in g["entries"]]
        # Use the first entry for the OMERO link
        first = g["entries"][0]
        types = sorted(g["types"])

        result.append({
            "study_id": g["study_id"],
            "name": sid,  # just the study ID as name
            "sub_entries": entry_names,
            "type": " + ".join(types) if len(types) > 1 else types[0] if types else "",
            "organism": g["organism"],
            "study_type": g["study_type"],
            "imaging_method": g["imaging_method"],
            "license": g["license"],
            "pub_title": g["pub_title"],
            "pub_doi": g["pub_doi"],
            "data_doi": g["data_doi"],
            "authors": g["authors"],
            "release_date": g["release_date"],
            "children": g["children"],
            # Keep first entry's id/type for OMERO link
            "id": first["id"],
            "_link_type": first["type"],
            "n_sub_entries": len(g["entries"]),
        })

    return result


def omero_url_consolidated(s: dict) -> str:
    """Build OMERO URL for a consolidated study (uses first sub-entry)."""
    obj_type = s.get("_link_type", s.get("type", "Project")).lower()
    obj_id = s.get("id", "")
    return f"{BASE_URL}/webclient/?show={obj_type}-{obj_id}"


def study_to_row(s: dict) -> list[str]:
    """Convert a consolidated study dict to a flat row for CSV/HTML."""
    children = s.get("children", [])
    if len(children) <= 20:
        children_str = ", ".join(children)
    else:
        children_str = ", ".join(children[:20]) + f" ... (+{len(children) - 20} more)"

    # Sub-entries column (screenA, experimentB, etc.)
    sub = s.get("sub_entries", [])
    if len(sub) == 1:
        sub_str = sub[0]
    elif len(sub) <= 5:
        sub_str = ", ".join(sub)
    else:
        sub_str = ", ".join(sub[:5]) + f" ... (+{len(sub) - 5} more)"

    license_norm = normalize_license(s.get("license", ""))
    link = omero_url_consolidated(s)
    return [
        s.get("study_id", ""),
        sub_str,
        s.get("type", ""),
        s.get("organism", ""),
        s.get("study_type", ""),
        s.get("imaging_method", ""),
        children_str,
        str(len(children)),
        license_norm or s.get("license", ""),
        s.get("pub_title", ""),
        s.get("pub_doi", ""),
        s.get("data_doi", ""),
        s.get("authors", ""),
        s.get("release_date", ""),
        link,
    ]


def export_csv(studies: list[dict], path: Path):
    """Export studies to CSV."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        for s in studies:
            w.writerow(study_to_row(s))
    print(f"CSV saved to {path}")


def license_color(license_str: str) -> str:
    """Get CSS background color for a license."""
    key = license_str.lower().strip()
    for pattern, color in LICENSE_COLORS_CSS.items():
        if pattern in key:
            return color
    return DEFAULT_LICENSE_CSS if license_str else "transparent"


def export_html(studies: list[dict], path: Path):
    """Export studies to a formatted HTML report with interactive charts."""
    license_counts = Counter(normalize_license(s.get("license", "")) or "(no license)" for s in studies)
    organism_counts = Counter(s.get("organism", "(unknown)") or "(unknown)" for s in studies)
    type_counts = Counter(s.get("type", "") for s in studies)

    top_organisms = organism_counts.most_common(15)

    lic_labels = json.dumps([k for k, _ in license_counts.most_common()])
    lic_values = json.dumps([v for _, v in license_counts.most_common()])
    lic_colors = json.dumps([LICENSE_CHART_COLORS.get(k, "#9e9e9e") for k, _ in license_counts.most_common()])

    org_labels = json.dumps([k for k, _ in top_organisms])
    org_values = json.dumps([v for _, v in top_organisms])

    total = len(studies)
    n_projects = type_counts.get("Project", 0)
    n_screens = type_counts.get("Screen", 0)

    # Build table rows with data attributes for filtering
    table_rows = []
    for i, s in enumerate(studies):
        row = study_to_row(s)
        lic = row[8]
        organism = s.get("organism", "(unknown)") or "(unknown)"
        bg = license_color(lic)
        row_class = "even" if i % 2 == 0 else "odd"
        link = omero_url_consolidated(s)

        cells = []
        for j, val in enumerate(row):
            escaped = html.escape(val)

            # Study ID — link to OMERO
            if j == 0 and val:
                escaped = f'<a href="{html.escape(link)}" target="_blank" title="Open in OMERO">{html.escape(val)}</a>'
            # Sub-entries — also link to OMERO
            elif j == 1 and val:
                escaped = f'<a href="{html.escape(link)}" target="_blank" title="Open in OMERO">{html.escape(val)}</a>'
            # DOIs clickable
            elif j in (10, 11) and val:
                doi = val.split()[0]
                if not doi.startswith("http"):
                    doi = f"https://doi.org/{doi}"
                escaped = f'<a href="{html.escape(doi)}" target="_blank">{html.escape(val.split()[0])}</a>'
            # OMERO Link column — icon link
            elif j == 14 and val:
                escaped = f'<a href="{html.escape(val)}" target="_blank" title="Open in OMERO">View</a>'

            if j == 8:
                if lic:
                    cells.append(f'<td style="background:{bg};font-weight:600">{escaped}</td>')
                else:
                    cells.append('<td style="background:#e53935;color:#fff;font-weight:700;text-align:center">NO LICENSE</td>')
            else:
                cells.append(f"<td>{escaped}</td>")

        lic_attr = html.escape(lic or "(no license)")
        org_attr = html.escape(organism)
        table_rows.append(
            f'<tr class="{row_class}" data-license="{lic_attr}" data-organism="{org_attr}">{"".join(cells)}</tr>'
        )

    table_html = "\n".join(table_rows)
    header_cells = "".join(f"<th>{h}</th>" for h in HEADERS)

    report = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IDR Study License Catalog</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; padding: 24px; }}
  h1 {{ font-size: 28px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; margin-bottom: 24px; font-size: 14px; }}
  .stats {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat-card {{ background: #fff; border-radius: 8px; padding: 16px 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; cursor: default; }}
  .stat-card .number {{ font-size: 32px; font-weight: 700; color: #1a3d7c; }}
  .stat-card .label {{ font-size: 13px; color: #666; margin-top: 4px; }}
  .charts {{ display: flex; gap: 24px; margin-bottom: 16px; flex-wrap: wrap; }}
  .chart-card {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; min-width: 340px; }}
  .chart-card h3 {{ margin-bottom: 12px; font-size: 16px; }}
  .filter-bar {{ display: flex; align-items: center; gap: 12px; margin-bottom: 16px; padding: 10px 16px; background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  .filter-bar .filter-label {{ font-size: 13px; color: #666; }}
  .filter-bar .filter-value {{ font-size: 14px; font-weight: 600; color: #1a3d7c; }}
  .filter-bar .clear-btn {{ background: #e8edf5; border: none; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; color: #283d6b; }}
  .filter-bar .clear-btn:hover {{ background: #d0d8eb; }}
  .filter-bar .match-count {{ font-size: 13px; color: #999; margin-left: auto; }}
  .filter-bar.hidden {{ display: none; }}
  .table-container {{ background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow-x: auto; max-height: 70vh; overflow-y: auto; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  th {{ background: #283d6b; color: #fff; padding: 10px 12px; text-align: left; position: sticky; top: 0; white-space: nowrap; z-index: 2; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #eee; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  tr.even {{ background: #fafbfd; }}
  tr.odd {{ background: #fff; }}
  tr:hover {{ background: #e8edf5 !important; }}
  tr.highlight {{ background: #fff9c4 !important; }}
  tr.dimmed {{ opacity: 0.25; }}
  a {{ color: #1a73e8; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .footer {{ text-align: center; color: #999; font-size: 12px; margin-top: 24px; }}
  canvas {{ cursor: pointer; }}
</style>
</head>
<body>
<div class="nav" style="margin-bottom:16px"><a href="../index.html" style="color:#1a73e8;font-size:14px">&larr; All Catalogs</a> &nbsp;|&nbsp; <a href="bia_studies.html" style="color:#1a73e8;font-size:14px">BioImage Archive Catalog &rarr;</a></div>
<h1>IDR Study License Catalog</h1>
<p class="subtitle">Image Data Resource &mdash; {total} studies ({n_projects} projects, {n_screens} screens) &mdash; generated {time.strftime("%Y-%m-%d %H:%M")} &mdash; click chart segments to filter</p>

<div class="stats">
  <div class="stat-card"><div class="number">{total}</div><div class="label">Total Studies</div></div>
  <div class="stat-card"><div class="number">{n_projects}</div><div class="label">Projects</div></div>
  <div class="stat-card"><div class="number">{n_screens}</div><div class="label">Screens</div></div>
  <div class="stat-card"><div class="number">{license_counts.get('CC BY 4.0', 0)}</div><div class="label">CC BY 4.0</div></div>
  <div class="stat-card"><div class="number">{len(organism_counts)}</div><div class="label">Unique Organisms</div></div>
</div>

<div class="charts">
  <div class="chart-card">
    <h3>License Distribution <span style="font-size:12px;color:#999;font-weight:normal">(click to filter)</span></h3>
    <canvas id="licenseChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Top Organisms <span style="font-size:12px;color:#999;font-weight:normal">(click to filter)</span></h3>
    <canvas id="organismChart"></canvas>
  </div>
</div>

<div class="filter-bar hidden" id="filterBar">
  <span class="filter-label">Filtered by:</span>
  <span class="filter-value" id="filterValue"></span>
  <button class="clear-btn" onclick="clearFilter()">Clear filter</button>
  <span class="match-count" id="matchCount"></span>
</div>

<div class="table-container">
<table id="studyTable">
<thead><tr>{header_cells}</tr></thead>
<tbody>
{table_html}
</tbody>
</table>
</div>

<p class="footer">Data fetched from idr.openmicroscopy.org OMERO API &mdash; click any Study ID or Name to open in OMERO</p>

<script>
// --- Filter state ---
let activeFilter = null; // {{ type: 'license'|'organism', value: string }}

function applyFilter(type, value) {{
  activeFilter = {{ type, value }};
  const rows = document.querySelectorAll('#studyTable tbody tr');
  let count = 0;
  rows.forEach(row => {{
    const attr = type === 'license' ? row.dataset.license : row.dataset.organism;
    if (attr === value) {{
      row.classList.add('highlight');
      row.classList.remove('dimmed');
      row.style.display = '';
      count++;
    }} else {{
      row.classList.remove('highlight');
      row.classList.add('dimmed');
      row.style.display = '';
    }}
  }});

  // Show filter bar
  document.getElementById('filterBar').classList.remove('hidden');
  document.getElementById('filterValue').textContent = value + ' (' + type + ')';
  document.getElementById('matchCount').textContent = count + ' of {total} studies';

  // Scroll table into view
  document.getElementById('studyTable').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
}}

function clearFilter() {{
  activeFilter = null;
  const rows = document.querySelectorAll('#studyTable tbody tr');
  rows.forEach(row => {{
    row.classList.remove('highlight', 'dimmed');
    row.style.display = '';
  }});
  document.getElementById('filterBar').classList.add('hidden');
}}

// --- License doughnut chart ---
const licenseChart = new Chart(document.getElementById('licenseChart'), {{
  type: 'doughnut',
  data: {{
    labels: {lic_labels},
    datasets: [{{ data: {lic_values}, backgroundColor: {lic_colors}, borderWidth: 2, borderColor: '#fff' }}]
  }},
  options: {{
    plugins: {{
      legend: {{
        position: 'right',
        labels: {{ font: {{ size: 12 }}, padding: 12 }},
        onClick: (e, legendItem, legend) => {{
          const label = legend.chart.data.labels[legendItem.index];
          if (activeFilter && activeFilter.type === 'license' && activeFilter.value === label) {{
            clearFilter();
          }} else {{
            applyFilter('license', label);
          }}
        }}
      }},
      tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + ctx.parsed + ' studies' }} }}
    }},
    onClick: (e, elements) => {{
      if (elements.length > 0) {{
        const idx = elements[0].index;
        const label = licenseChart.data.labels[idx];
        if (activeFilter && activeFilter.type === 'license' && activeFilter.value === label) {{
          clearFilter();
        }} else {{
          applyFilter('license', label);
        }}
      }}
    }}
  }}
}});

// --- Organism bar chart ---
const organismChart = new Chart(document.getElementById('organismChart'), {{
  type: 'bar',
  data: {{
    labels: {org_labels},
    datasets: [{{ label: 'Studies', data: {org_values}, backgroundColor: '#5c7cba', hoverBackgroundColor: '#3d5a99' }}]
  }},
  options: {{
    indexAxis: 'y',
    plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: ctx => ctx.parsed.x + ' studies' }} }} }},
    scales: {{ x: {{ beginAtZero: true, ticks: {{ stepSize: 10 }} }} }},
    onClick: (e, elements) => {{
      if (elements.length > 0) {{
        const idx = elements[0].index;
        const label = organismChart.data.labels[idx];
        if (activeFilter && activeFilter.type === 'organism' && activeFilter.value === label) {{
          clearFilter();
        }} else {{
          applyFilter('organism', label);
        }}
      }}
    }}
  }}
}});
</script>
</body>
</html>"""

    path.write_text(report)
    print(f"HTML report saved to {path}")


LICENSE_COLORS_SHEETS = {
    "cc by 4.0": {"red": 0.78, "green": 0.94, "blue": 0.81},
    "cc by-sa 3.0": {"red": 0.78, "green": 0.94, "blue": 0.81},
    "cc0 1.0": {"red": 0.78, "green": 0.94, "blue": 0.81},
    "cc by-nc 4.0": {"red": 1.0, "green": 0.95, "blue": 0.8},
    "cc by-nc-sa 3.0": {"red": 1.0, "green": 0.95, "blue": 0.8},
    "cc by-nc-nd 4.0": {"red": 0.96, "green": 0.8, "blue": 0.8},
}
DEFAULT_LICENSE_SHEETS = {"red": 0.96, "green": 0.8, "blue": 0.8}


def authenticate_sheets():
    """Authenticate with Google Sheets using OAuth."""
    import gspread
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRETS_PATH.exists():
                print(f"\nError: {CLIENT_SECRETS_PATH} not found.")
                print("Download OAuth client credentials from Google Cloud Console:")
                print("  APIs & Services > Credentials > OAuth 2.0 Client IDs > Download JSON")
                print(f"Save to: {CLIENT_SECRETS_PATH}")
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
        TOKEN_PATH.write_text(creds.to_json())
        print(f"Token saved to {TOKEN_PATH}")

    return gspread.authorize(creds)


def export_to_sheet(studies: list[dict], sheet_url: str):
    """Write formatted study data to a Google Sheet with hyperlinks, colors, and summary."""
    gc = authenticate_sheets()
    sh = gc.open_by_url(sheet_url)
    ws = sh.sheet1

    # Build rows
    rows = [HEADERS]
    license_counts = Counter()
    for s in studies:
        row = study_to_row(s)
        license_counts[row[8] or "(no license)"] += 1
        rows.append(row)

    # Summary section
    rows.append([""] * len(HEADERS))
    rows.append(["LICENSE SUMMARY", "Count"] + [""] * (len(HEADERS) - 2))
    for lic, count in sorted(license_counts.items(), key=lambda x: -x[1]):
        rows.append([lic, str(count)] + [""] * (len(HEADERS) - 2))

    print(f"Writing {len(rows)} rows to sheet...")
    ws.clear()
    ws.update(rows, value_input_option="RAW")

    # --- Formatting via batch_update (Sheets API) ---
    print("Applying formatting...")
    total_cols = len(HEADERS)
    data_rows = len(studies)
    sheet_id = ws.id

    reqs = []

    # 1. Header row: bold white text on dark blue
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                       "startColumnIndex": 0, "endColumnIndex": total_cols},
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.16, "green": 0.24, "blue": 0.46},
                    "textFormat": {"bold": True, "fontSize": 10,
                                   "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })

    # 2. Freeze header row
    reqs.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount",
        }
    })

    # 3. Alternating row colors
    reqs.append({
        "addBanding": {
            "bandedRange": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": data_rows + 1,
                           "startColumnIndex": 0, "endColumnIndex": total_cols},
                "rowProperties": {
                    "firstBandColor": {"red": 1, "green": 1, "blue": 1},
                    "secondBandColor": {"red": 0.95, "green": 0.95, "blue": 0.97},
                },
            }
        }
    })

    # 4. License column color coding (col I = index 8)
    for i, s in enumerate(studies):
        lic = normalize_license(s.get("license", "")).lower()
        color = DEFAULT_LICENSE_SHEETS
        for pattern, c in LICENSE_COLORS_SHEETS.items():
            if pattern in lic:
                color = c
                break
        if lic:
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": i + 1, "endRowIndex": i + 2,
                               "startColumnIndex": 8, "endColumnIndex": 9},
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": color,
                        "textFormat": {"bold": True},
                    }},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            })

    # 5. Summary header row
    summary_row = data_rows + 2
    reqs.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": summary_row, "endRowIndex": summary_row + 1,
                       "startColumnIndex": 0, "endColumnIndex": total_cols},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
                "textFormat": {"bold": True, "fontSize": 11},
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    })

    # 6. Auto-resize columns
    reqs.append({
        "autoResizeDimensions": {
            "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS",
                           "startIndex": 0, "endIndex": total_cols}
        }
    })

    # 7. Cap wide columns (Datasets/Plates=6, PubTitle=9, Authors=12) at 300px
    for col_idx in [6, 9, 12]:
        reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                          "startIndex": col_idx, "endIndex": col_idx + 1},
                "properties": {"pixelSize": 300},
                "fields": "pixelSize",
            }
        })

    sh.batch_update({"requests": reqs})
    print("Formatting applied.")

    # --- Hyperlinks: Study ID -> OMERO, DOIs -> doi.org ---
    # gspread doesn't have a direct hyperlink API, so we use the Sheets API directly
    print("Adding hyperlinks...")
    link_reqs = []
    for i, s in enumerate(studies):
        row_idx = i + 1  # 0-indexed, skip header
        link = omero_url_consolidated(s)

        # Col A (Study ID) -> OMERO link
        link_reqs.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"formulaValue":
                    f'=HYPERLINK("{link}", "{s.get("study_id", "")}")'
                }}]}],
                "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 0},
                "fields": "userEnteredValue",
            }
        })

        # Col B (Sub-entries) -> OMERO link
        sub = s.get("sub_entries", [])
        if len(sub) == 1:
            sub_label = sub[0]
        elif len(sub) <= 3:
            sub_label = ", ".join(sub)
        else:
            sub_label = f"{sub[0]} ... (+{len(sub)-1} more)"
        sub_label = sub_label.replace('"', '""')
        link_reqs.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"formulaValue":
                    f'=HYPERLINK("{link}", "{sub_label}")'
                }}]}],
                "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 1},
                "fields": "userEnteredValue",
            }
        })

        # Col O (OMERO Link) -> clickable
        link_reqs.append({
            "updateCells": {
                "rows": [{"values": [{"userEnteredValue": {"formulaValue":
                    f'=HYPERLINK("{link}", "View in OMERO")'
                }}]}],
                "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 14},
                "fields": "userEnteredValue",
            }
        })

        # Col K (Publication DOI)
        pub_doi = s.get("pub_doi", "")
        if pub_doi:
            doi_id = pub_doi.split()[0]
            doi_url = doi_id if doi_id.startswith("http") else f"https://doi.org/{doi_id}"
            link_reqs.append({
                "updateCells": {
                    "rows": [{"values": [{"userEnteredValue": {"formulaValue":
                        f'=HYPERLINK("{doi_url}", "{doi_id}")'
                    }}]}],
                    "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 10},
                    "fields": "userEnteredValue",
                }
            })

        # Col L (Data DOI)
        data_doi = s.get("data_doi", "")
        if data_doi:
            doi_id = data_doi.split()[0]
            doi_url = doi_id if doi_id.startswith("http") else f"https://doi.org/{doi_id}"
            link_reqs.append({
                "updateCells": {
                    "rows": [{"values": [{"userEnteredValue": {"formulaValue":
                        f'=HYPERLINK("{doi_url}", "{doi_id}")'
                    }}]}],
                    "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 11},
                    "fields": "userEnteredValue",
                }
            })

        # Send in batches of 500 to stay under API limits
        if len(link_reqs) >= 500:
            sh.batch_update({"requests": link_reqs})
            link_reqs = []

    if link_reqs:
        sh.batch_update({"requests": link_reqs})

    print(f"\nDone! Sheet updated: {sheet_url}")
    print(f"\nLicense summary:")
    for lic, count in sorted(license_counts.items(), key=lambda x: -x[1]):
        print(f"  {lic}: {count}")


def fetch_and_cache() -> list[dict]:
    """Fetch all studies with metadata from IDR, cache to disk."""
    print("=" * 60)
    print("Phase 1: Fetching IDR study list")
    print("=" * 60)
    studies = fetch_all_studies()
    print(f"\nTotal studies: {len(studies)}")

    print("\n" + "=" * 60)
    print("Phase 2: Fetching metadata for each study")
    print("=" * 60)
    for i, study in enumerate(studies):
        pct = (i + 1) / len(studies) * 100
        print(f"  [{i+1}/{len(studies)} {pct:.0f}%] {study['name']}")
        meta = fetch_study_metadata(study["type"], study["id"])
        study.update(meta)
        children = fetch_children(study["type"], study["id"])
        study["children"] = children
        time.sleep(0.2)

    CACHE_PATH.write_text(json.dumps(studies, indent=2))
    print(f"\nCached {len(studies)} studies to {CACHE_PATH}")
    return studies


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch IDR study metadata and export to CSV, HTML, and Google Sheets")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch from IDR API (otherwise use cache)")
    parser.add_argument("--output", default=str(OUT_DIR), help="Output directory (default: ./output)")
    parser.add_argument("--sheet", help="Google Sheet URL to export to")
    args = parser.parse_args()

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    # Fetch or load cache
    if args.refresh or not CACHE_PATH.exists():
        raw_studies = fetch_and_cache()
    else:
        print(f"Loading cached data from {CACHE_PATH}")
        raw_studies = json.loads(CACHE_PATH.read_text())
        print(f"  {len(raw_studies)} raw entries loaded")

    # Consolidate: 248 raw entries -> ~173 unique studies
    studies = consolidate_studies(raw_studies)
    print(f"  Consolidated to {len(studies)} unique studies")

    # Local exports
    print("\n" + "=" * 60)
    print("Exporting locally")
    print("=" * 60)
    export_csv(studies, out / "idr_studies.csv")
    export_html(studies, out / "idr_studies.html")

    # Google Sheets export
    if args.sheet:
        print("\n" + "=" * 60)
        print("Exporting to Google Sheets")
        print("=" * 60)
        export_to_sheet(studies, args.sheet)

    # Summary
    license_counts = Counter(normalize_license(s.get("license", "")) or "(no license)" for s in studies)
    print(f"\nLicense summary:")
    for lic, count in license_counts.most_common():
        print(f"  {lic}: {count}")

    print(f"\nOpen {out / 'idr_studies.html'} in your browser to view the report.")


if __name__ == "__main__":
    main()
