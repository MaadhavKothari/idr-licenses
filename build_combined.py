#!/usr/bin/env python3
"""Build a combined HTML view of IDR + BIA license data (summary charts only, no full table)."""

import json
import time
from collections import Counter
from pathlib import Path

IDR_CACHE = Path(__file__).parent / "idr_studies_cache.json"
BIA_CACHE = Path(__file__).parent / "bia_studies_cache.json"
OUT = Path(__file__).parent / "output" / "combined_studies.html"

# Reuse normalizers
from fetch_idr import normalize_license as idr_normalize, consolidate_studies
from fetch_bia import normalize_license as bia_normalize


def main():
    # Load IDR
    idr_raw = json.loads(IDR_CACHE.read_text())
    idr_studies = consolidate_studies(idr_raw)
    idr_licenses = Counter(idr_normalize(s.get("license", "")) or "(no license)" for s in idr_studies)

    # Load BIA
    bia_studies = json.loads(BIA_CACHE.read_text())
    bia_licenses = Counter(bia_normalize(s.get("license", "")) or "(no license)" for s in bia_studies)

    # Combined
    all_licenses = Counter()
    all_licenses.update(idr_licenses)
    all_licenses.update(bia_licenses)

    # Organisms
    idr_organisms = Counter(s.get("organism", "(unknown)") or "(unknown)" for s in idr_studies)
    bia_organisms = Counter(s.get("organism", "(unknown)") or "(unknown)" for s in bia_studies)
    all_organisms = Counter()
    all_organisms.update(idr_organisms)
    all_organisms.update(bia_organisms)
    top_organisms = all_organisms.most_common(20)

    n_idr = len(idr_studies)
    n_bia = len(bia_studies)
    n_total = n_idr + n_bia
    n_idr_licensed = sum(1 for s in idr_studies if s.get("license"))
    n_bia_licensed = sum(1 for s in bia_studies if s.get("license"))

    # Chart data
    lic_labels = json.dumps([k for k, _ in all_licenses.most_common()])
    lic_values = json.dumps([v for _, v in all_licenses.most_common()])

    colors_map = {
        "CC BY 4.0": "#4caf50", "CC0 1.0": "#66bb6a", "CC0": "#66bb6a",
        "CC BY-SA 3.0": "#81c784", "CC BY-NC 4.0": "#ffc107",
        "CC BY-NC-SA 3.0": "#ffb300", "CC BY-NC-SA": "#ffb300",
        "CC BY-NC-ND 4.0": "#ef5350", "(no license)": "#9e9e9e",
    }
    lic_colors = json.dumps([colors_map.get(k, "#78909c") for k, _ in all_licenses.most_common()])

    org_labels = json.dumps([k for k, _ in top_organisms])
    org_values = json.dumps([v for _, v in top_organisms])

    # Per-repo license comparison data
    idr_lic_items = sorted(set(idr_licenses.keys()) | set(bia_licenses.keys()))
    # Filter to ones with > 0 in either
    comp_labels = json.dumps(idr_lic_items)
    comp_idr = json.dumps([idr_licenses.get(k, 0) for k in idr_lic_items])
    comp_bia = json.dumps([bia_licenses.get(k, 0) for k in idr_lic_items])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Combined Bioimaging License Overview</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; padding: 24px; }}
  h1 {{ font-size: 28px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; margin-bottom: 24px; font-size: 14px; }}
  .stats {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat-card {{ background: #fff; border-radius: 8px; padding: 16px 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); text-align: center; }}
  .stat-card .number {{ font-size: 32px; font-weight: 700; color: #1a3d7c; }}
  .stat-card .label {{ font-size: 13px; color: #666; margin-top: 4px; }}
  .charts {{ display: flex; gap: 24px; margin-bottom: 24px; flex-wrap: wrap; }}
  .chart-card {{ background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); flex: 1; min-width: 340px; }}
  .chart-card h3 {{ margin-bottom: 12px; font-size: 16px; }}
  .section {{ margin-bottom: 24px; }}
  .footer {{ text-align: center; color: #999; font-size: 12px; margin-top: 24px; }}
</style>
</head>
<body>
<h1>Combined Bioimaging License Overview</h1>
<p class="subtitle">IDR + BioImage Archive &mdash; {n_total:,} total studies &mdash; generated {time.strftime("%Y-%m-%d %H:%M")}</p>

<div class="stats">
  <div class="stat-card"><div class="number">{n_total:,}</div><div class="label">Total Studies</div></div>
  <div class="stat-card"><div class="number">{n_idr}</div><div class="label">IDR Studies</div></div>
  <div class="stat-card"><div class="number">{n_bia:,}</div><div class="label">BIA Studies</div></div>
  <div class="stat-card"><div class="number">{n_idr_licensed + n_bia_licensed:,}</div><div class="label">Have License</div></div>
  <div class="stat-card"><div class="number">{n_total - n_idr_licensed - n_bia_licensed:,}</div><div class="label">No License</div></div>
  <div class="stat-card"><div class="number">{len(all_organisms)}</div><div class="label">Unique Organisms</div></div>
</div>

<div class="charts">
  <div class="chart-card">
    <h3>Overall License Distribution</h3>
    <canvas id="licenseChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Top Organisms (Combined)</h3>
    <canvas id="organismChart"></canvas>
  </div>
</div>

<div class="charts">
  <div class="chart-card" style="min-width:700px">
    <h3>License Comparison: IDR vs BIA</h3>
    <canvas id="compChart"></canvas>
  </div>
</div>

<p class="footer">Data from idr.openmicroscopy.org and ebi.ac.uk/bioimage-archive &mdash; use the IDR and BIA tabs for full study tables</p>

<script>
new Chart(document.getElementById('licenseChart'), {{
  type: 'doughnut',
  data: {{
    labels: {lic_labels},
    datasets: [{{ data: {lic_values}, backgroundColor: {lic_colors}, borderWidth: 2, borderColor: '#fff' }}]
  }},
  options: {{
    plugins: {{
      legend: {{ position: 'right', labels: {{ font: {{ size: 12 }}, padding: 10 }} }},
      tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + ctx.parsed.toLocaleString() + ' studies' }} }}
    }}
  }}
}});

new Chart(document.getElementById('organismChart'), {{
  type: 'bar',
  data: {{
    labels: {org_labels},
    datasets: [{{ label: 'Studies', data: {org_values}, backgroundColor: '#5c7cba' }}]
  }},
  options: {{
    indexAxis: 'y',
    plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true }} }}
  }}
}});

new Chart(document.getElementById('compChart'), {{
  type: 'bar',
  data: {{
    labels: {comp_labels},
    datasets: [
      {{ label: 'IDR', data: {comp_idr}, backgroundColor: '#4caf50' }},
      {{ label: 'BIA', data: {comp_bia}, backgroundColor: '#5c7cba' }}
    ]
  }},
  options: {{
    plugins: {{ legend: {{ position: 'top' }} }},
    scales: {{ y: {{ beginAtZero: true, type: 'logarithmic' }} }}
  }}
}});
</script>
</body>
</html>"""

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html)
    print(f"Combined report saved to {OUT}")


if __name__ == "__main__":
    main()
