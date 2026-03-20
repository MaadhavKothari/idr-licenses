#!/usr/bin/env python3
"""Fetch all BioImage Archive studies and export metadata (license, organism, DOI, etc.) to CSV + HTML."""

import csv
import html as html_mod
import json
import re
import time
from collections import Counter
from pathlib import Path

import requests

BASE_URL = "https://www.ebi.ac.uk/biostudies/api/v1"
OUT_DIR = Path(__file__).parent / "output"
CACHE_PATH = Path(__file__).parent / "bia_studies_cache.json"

HEADERS = [
    "Accession", "Title", "Collection", "Organism", "Imaging Method",
    "Keywords", "Files", "License", "Publication Title", "Publication DOI",
    "Authors", "Release Date", "BIA Link",
]

LICENSE_COLORS_CSS = {
    "cc by 4.0": "#c6efce",
    "cc by-sa": "#c6efce",
    "cc0": "#c6efce",
    "cc by-nc 4.0": "#fff2cc",
    "cc by-nc-sa": "#fff2cc",
    "cc by-nc-nd": "#f4cccc",
}
DEFAULT_LICENSE_CSS = "#f4cccc"

LICENSE_CHART_COLORS = {
    "CC BY 4.0": "#4caf50",
    "CC0": "#66bb6a",
    "CC BY-SA 4.0": "#81c784",
    "CC BY-NC 4.0": "#ffc107",
    "CC BY-NC-SA 4.0": "#ffb300",
    "CC BY-NC-ND 4.0": "#ef5350",
    "(no license)": "#9e9e9e",
}


def fetch_all_accessions() -> list[dict]:
    """Fetch all S-BIAD study accessions from BioImage Archive search API."""
    all_hits = []
    page = 1
    while True:
        # Use collection=BioImages to get only S-BIAD studies (not EMPIAR/S-BSST)
        url = f"{BASE_URL}/search"
        params = {"collection": "BioImages", "pageSize": 100, "page": page}
        print(f"  Fetching page {page}...", end=" ", flush=True)
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", [])
        if not hits:
            print("done.")
            break
        all_hits.extend(hits)
        total = data.get("totalHits", "?")
        print(f"{len(hits)} hits (total: {total})")
        page += 1
        time.sleep(0.1)
    return all_hits


def parse_study_metadata(study: dict) -> dict:
    """Extract structured metadata from a BioStudies study detail response."""
    meta = {
        "accession": study.get("accno", ""),
        "title": "",
        "description": "",
        "license": "",
        "license_url": "",
        "release_date": "",
        "doi": "",
        "organism": "",
        "imaging_method": "",
        "keywords": [],
        "pub_title": "",
        "pub_doi": "",
        "pub_authors": "",
        "authors": [],
    }

    # Top-level attributes (DOI, ReleaseDate, Template)
    for attr in study.get("attributes", []):
        name = attr.get("name", "")
        val = attr.get("value", "")
        if name == "DOI":
            meta["doi"] = val
        elif name == "ReleaseDate":
            meta["release_date"] = val
        elif name == "Title":
            meta["title"] = val

    section = study.get("section", {})

    # Section-level attributes (Title, Description, License, Keywords)
    for attr in section.get("attributes", []):
        name = attr.get("name", "")
        val = attr.get("value", "")
        if name == "Title" and not meta["title"]:
            meta["title"] = val
        elif name == "Description":
            meta["description"] = val
        elif name == "License":
            meta["license"] = val
            valqual = attr.get("valqual", [])
            if valqual:
                meta["license_url"] = valqual[0].get("value", "")
        elif name == "ReleaseDate" and not meta["release_date"]:
            meta["release_date"] = val
        elif name == "Keywords":
            meta["keywords"].append(val)
        # Older schema fields
        elif name == "Study Organism":
            meta["organism"] = val

    # Subsections (Biosample, Image acquisition, Publication, Author)
    for sub in section.get("subsections", []):
        # Handle both dict and list subsections
        if isinstance(sub, list):
            for item in sub:
                _parse_subsection(item, meta)
        else:
            _parse_subsection(sub, meta)

    return meta


def _parse_subsection(sub: dict, meta: dict):
    """Parse a single subsection and update metadata."""
    sub_type = sub.get("type", "")
    attrs = {a.get("name", ""): a.get("value", "") for a in sub.get("attributes", [])}

    if sub_type == "Biosample" and not meta["organism"]:
        meta["organism"] = attrs.get("Organism", "")
    elif sub_type == "Image acquisition" and not meta["imaging_method"]:
        meta["imaging_method"] = attrs.get("Imaging method", "")
    elif sub_type == "Publication":
        if not meta["pub_title"]:
            meta["pub_title"] = attrs.get("Title", "")
        if not meta["pub_doi"]:
            meta["pub_doi"] = attrs.get("DOI", "")
        if not meta["pub_authors"]:
            meta["pub_authors"] = attrs.get("Authors", "")
    elif sub_type == "Author":
        name = attrs.get("Name", "")
        if name:
            meta["authors"].append(name)

    # Recurse into nested subsections
    for nested in sub.get("subsections", []):
        if isinstance(nested, list):
            for item in nested:
                _parse_subsection(item, meta)
        else:
            _parse_subsection(nested, meta)


def normalize_license(raw: str) -> str:
    """Extract just the license name, handling BIA's inconsistent formats."""
    if not raw:
        return ""
    # Normalize hyphens: "CC-BY-NC-SA" -> "CC BY-NC-SA", "CC-BY 4.0" -> "CC BY 4.0"
    cleaned = raw.strip().upper().replace("CC-BY", "CC BY").replace("CC-0", "CC0")
    cleaned_lower = cleaned.lower()
    for prefix in ["cc by-nc-sa", "cc by-nc-nd", "cc by-nc", "cc by-sa", "cc by-nd", "cc by", "cc0"]:
        if prefix in cleaned_lower:
            m = re.search(rf"({re.escape(prefix)}\s*[\d.]*)", cleaned_lower)
            if m:
                return m.group(1).strip().upper()
            return prefix.upper()
    return raw.strip()


def bia_url(accession: str) -> str:
    """Build the BioImage Archive URL for a study."""
    return f"https://www.ebi.ac.uk/biostudies/BioImages/studies/{accession}"


def collection_type(accession: str) -> str:
    """Determine collection from accession prefix."""
    if accession.startswith("S-BIAD"):
        return "BioImage Archive"
    elif accession.startswith("EMPIAR"):
        return "EMPIAR"
    elif accession.startswith("S-BSST"):
        return "BioStudies"
    return "Other"


def study_to_row(s: dict) -> list[str]:
    """Convert a study dict to a flat row."""
    lic = normalize_license(s.get("license", ""))
    authors = s.get("pub_authors", "") or ", ".join(s.get("authors", [])[:10])
    keywords = ", ".join(s.get("keywords", [])[:5])
    return [
        s.get("accession", ""),
        s.get("title", ""),
        collection_type(s.get("accession", "")),
        s.get("organism", ""),
        s.get("imaging_method", ""),
        keywords,
        str(s.get("files", 0)),
        lic,
        s.get("pub_title", ""),
        s.get("pub_doi", ""),
        authors,
        s.get("release_date", ""),
        bia_url(s.get("accession", "")),
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
    key = license_str.lower().strip()
    for pattern, color in LICENSE_COLORS_CSS.items():
        if pattern in key:
            return color
    return DEFAULT_LICENSE_CSS if license_str else "transparent"


def export_html(studies: list[dict], path: Path):
    """Export studies to a formatted HTML report with interactive charts."""
    license_counts = Counter(normalize_license(s.get("license", "")) or "(no license)" for s in studies)
    organism_counts = Counter(s.get("organism", "(unknown)") or "(unknown)" for s in studies)
    collection_counts = Counter(collection_type(s.get("accession", "")) for s in studies)

    top_organisms = organism_counts.most_common(15)

    lic_labels = json.dumps([k for k, _ in license_counts.most_common()])
    lic_values = json.dumps([v for _, v in license_counts.most_common()])
    lic_colors = json.dumps([LICENSE_CHART_COLORS.get(k, "#9e9e9e") for k, _ in license_counts.most_common()])

    org_labels = json.dumps([k for k, _ in top_organisms])
    org_values = json.dumps([v for _, v in top_organisms])

    total = len(studies)
    n_licensed = sum(1 for s in studies if s.get("license"))
    n_with_organism = sum(1 for s in studies if s.get("organism"))

    # Build table rows
    table_rows = []
    for i, s in enumerate(studies):
        row = study_to_row(s)
        lic = row[7]  # License is col index 7
        bg = license_color(lic)
        row_class = "even" if i % 2 == 0 else "odd"
        link = bia_url(s.get("accession", ""))
        organism = s.get("organism", "(unknown)") or "(unknown)"

        cells = []
        for j, val in enumerate(row):
            escaped = html_mod.escape(val)

            # Accession — link to BIA
            if j == 0 and val:
                escaped = f'<a href="{html_mod.escape(link)}" target="_blank" title="Open in BioImage Archive">{html_mod.escape(val)}</a>'
            # Title — also link
            elif j == 1 and val:
                escaped = f'<a href="{html_mod.escape(link)}" target="_blank">{html_mod.escape(val)}</a>'
            # DOI clickable
            elif j == 9 and val:
                doi = val.split()[0]
                if not doi.startswith("http"):
                    doi = f"https://doi.org/{doi}"
                escaped = f'<a href="{html_mod.escape(doi)}" target="_blank">{html_mod.escape(val.split()[0])}</a>'
            # BIA Link column
            elif j == 12 and val:
                escaped = f'<a href="{html_mod.escape(val)}" target="_blank">View</a>'

            if j == 7:
                if lic:
                    cells.append(f'<td style="background:{bg};font-weight:600">{escaped}</td>')
                else:
                    cells.append('<td style="background:#e53935;color:#fff;font-weight:700;text-align:center">NO LICENSE</td>')
            else:
                cells.append(f"<td>{escaped}</td>")

        lic_attr = html_mod.escape(lic or "(no license)")
        org_attr = html_mod.escape(organism)
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
<title>BioImage Archive License Catalog</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; color: #333; padding: 24px; }}
  h1 {{ font-size: 28px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; margin-bottom: 24px; font-size: 14px; }}
  .nav {{ margin-bottom: 16px; }}
  .nav a {{ color: #1a73e8; font-size: 14px; }}
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
<div class="nav"><a href="index.html">&larr; Back to IDR Catalog</a></div>
<h1>BioImage Archive License Catalog</h1>
<p class="subtitle">EMBL-EBI BioImage Archive (S-BIAD collection) &mdash; {total} studies &mdash; generated {time.strftime("%Y-%m-%d %H:%M")} &mdash; click chart segments to filter</p>

<div class="stats">
  <div class="stat-card"><div class="number">{total}</div><div class="label">Total Studies</div></div>
  <div class="stat-card"><div class="number">{n_licensed}</div><div class="label">Have License</div></div>
  <div class="stat-card"><div class="number">{total - n_licensed}</div><div class="label">No License</div></div>
  <div class="stat-card"><div class="number">{n_with_organism}</div><div class="label">Have Organism</div></div>
  <div class="stat-card"><div class="number">{len(organism_counts) - (1 if '(unknown)' in organism_counts else 0)}</div><div class="label">Unique Organisms</div></div>
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

<p class="footer">Data fetched from EMBL-EBI BioStudies API &mdash; click any Accession or Title to open in BioImage Archive</p>

<script>
let activeFilter = null;

function applyFilter(type, value) {{
  activeFilter = {{ type, value }};
  const rows = document.querySelectorAll('#studyTable tbody tr');
  let count = 0;
  rows.forEach(row => {{
    const attr = type === 'license' ? row.dataset.license : row.dataset.organism;
    if (attr === value) {{
      row.classList.add('highlight');
      row.classList.remove('dimmed');
      count++;
    }} else {{
      row.classList.remove('highlight');
      row.classList.add('dimmed');
    }}
  }});
  document.getElementById('filterBar').classList.remove('hidden');
  document.getElementById('filterValue').textContent = value + ' (' + type + ')';
  document.getElementById('matchCount').textContent = count + ' of {total} studies';
  document.getElementById('studyTable').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
}}

function clearFilter() {{
  activeFilter = null;
  document.querySelectorAll('#studyTable tbody tr').forEach(row => {{
    row.classList.remove('highlight', 'dimmed');
  }});
  document.getElementById('filterBar').classList.add('hidden');
}}

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
          if (activeFilter && activeFilter.type === 'license' && activeFilter.value === label) clearFilter();
          else applyFilter('license', label);
        }}
      }},
      tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + ctx.parsed + ' studies' }} }}
    }},
    onClick: (e, elements) => {{
      if (elements.length > 0) {{
        const label = licenseChart.data.labels[elements[0].index];
        if (activeFilter && activeFilter.type === 'license' && activeFilter.value === label) clearFilter();
        else applyFilter('license', label);
      }}
    }}
  }}
}});

const organismChart = new Chart(document.getElementById('organismChart'), {{
  type: 'bar',
  data: {{
    labels: {org_labels},
    datasets: [{{ label: 'Studies', data: {org_values}, backgroundColor: '#5c7cba', hoverBackgroundColor: '#3d5a99' }}]
  }},
  options: {{
    indexAxis: 'y',
    plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: ctx => ctx.parsed.x + ' studies' }} }} }},
    scales: {{ x: {{ beginAtZero: true }} }},
    onClick: (e, elements) => {{
      if (elements.length > 0) {{
        const label = organismChart.data.labels[elements[0].index];
        if (activeFilter && activeFilter.type === 'organism' && activeFilter.value === label) clearFilter();
        else applyFilter('organism', label);
      }}
    }}
  }}
}});
</script>
</body>
</html>"""

    path.write_text(report)
    print(f"HTML report saved to {path}")


def fetch_and_cache() -> list[dict]:
    """Fetch all BIA studies with metadata, cache to disk."""
    print("=" * 60)
    print("Phase 1: Fetching BIA study list")
    print("=" * 60)
    hits = fetch_all_accessions()
    print(f"\nTotal accessions: {len(hits)}")

    print("\n" + "=" * 60)
    print("Phase 2: Fetching metadata for each study")
    print("=" * 60)
    studies = []
    for i, hit in enumerate(hits):
        acc = hit["accession"]
        pct = (i + 1) / len(hits) * 100
        if (i + 1) % 50 == 0 or i == 0:
            print(f"  [{i+1}/{len(hits)} {pct:.0f}%] {acc}", flush=True)

        try:
            resp = requests.get(f"{BASE_URL}/studies/{acc}", timeout=30)
            resp.raise_for_status()
            meta = parse_study_metadata(resp.json())
        except requests.RequestException as e:
            print(f"  Warning: failed to fetch {acc}: {e}")
            meta = {"accession": acc, "title": hit.get("title", "")}

        # Merge search-level fields
        meta["files"] = hit.get("files", 0)
        if not meta.get("title"):
            meta["title"] = hit.get("title", "")
        if not meta.get("release_date"):
            meta["release_date"] = hit.get("release_date", "")

        studies.append(meta)
        time.sleep(0.05)

    CACHE_PATH.write_text(json.dumps(studies, indent=2))
    print(f"\nCached {len(studies)} studies to {CACHE_PATH}")
    return studies


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch BioImage Archive study metadata and export to CSV + HTML")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch from BIA API (otherwise use cache)")
    parser.add_argument("--output", default=str(OUT_DIR), help="Output directory (default: ./output)")
    args = parser.parse_args()

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    if args.refresh or not CACHE_PATH.exists():
        studies = fetch_and_cache()
    else:
        print(f"Loading cached data from {CACHE_PATH}")
        studies = json.loads(CACHE_PATH.read_text())
        print(f"  {len(studies)} studies loaded")

    print("\n" + "=" * 60)
    print("Exporting")
    print("=" * 60)
    export_csv(studies, out / "bia_studies.csv")
    export_html(studies, out / "bia_studies.html")

    license_counts = Counter(normalize_license(s.get("license", "")) or "(no license)" for s in studies)
    print(f"\nLicense summary:")
    for lic, count in license_counts.most_common():
        print(f"  {lic}: {count}")

    print(f"\nOpen {out / 'bia_studies.html'} in your browser to view the report.")


if __name__ == "__main__":
    main()
