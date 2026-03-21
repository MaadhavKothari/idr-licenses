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
    "Data DOI", "Authors", "Release Date", "BIA Link",
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

    # Section-level attributes (Title, Description, License, Keywords, Organism)
    # Handles all schema versions: v3/v4/v5 + legacy
    for attr in section.get("attributes", []):
        name = attr.get("name", "")
        name_lower = name.lower()
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
        elif name_lower == "releasedate" and not meta["release_date"]:
            meta["release_date"] = val
        elif name_lower in ("keywords", "keyword"):  # v5 uses "Keywords", v4 uses "Keyword"
            if "," in val and val.count(",") > 2:
                # MIFA.v1: comma-separated keywords in one attribute
                meta["keywords"].extend(k.strip() for k in val.split(",") if k.strip())
            else:
                meta["keywords"].append(val)
        elif name_lower in ("organism", "study organism") and not meta["organism"]:
            meta["organism"] = val

    # Links (some studies store publication DOI as a link)
    for link in section.get("links", []):
        if not meta["pub_doi"]:
            url = link.get("url", "") if isinstance(link, dict) else ""
            link_attrs = {a.get("name", ""): a.get("value", "") for a in link.get("attributes", [])} if isinstance(link, dict) else {}
            desc = link_attrs.get("Description", "").lower()
            if desc == "publication" and "doi.org/" in url:
                meta["pub_doi"] = url

    # Subsections (Biosample, Image acquisition, Publication, Author, etc.)
    # Handles all schema versions with case-insensitive matching
    for sub in section.get("subsections", []):
        if isinstance(sub, list):
            for item in sub:
                _parse_subsection(item, meta)
        else:
            _parse_subsection(sub, meta)

    return meta


def _parse_subsection(sub: dict, meta: dict):
    """Parse a single subsection and update metadata.

    Handles all BIA schema versions (v3, v4, v5, legacy IDR imports)
    with case-insensitive type matching and multiple attribute name variants.
    """
    sub_type = sub.get("type", "")
    sub_type_lower = sub_type.lower()
    attrs = {a.get("name", ""): a.get("value", "") for a in sub.get("attributes", [])}
    # Case-insensitive attribute lookup helper
    attrs_lower = {k.lower(): v for k, v in attrs.items()}

    # --- Organism ---
    # v5: Biosample.Organism attr; v4: Biosample -> nested Organism subsection
    # v3: section-level "Organism" attr (handled above); legacy: standalone "organism" subsection
    if sub_type_lower == "biosample" and not meta["organism"]:
        meta["organism"] = attrs_lower.get("organism", "")
    elif sub_type_lower == "organism" and not meta["organism"]:
        # v4 nested Organism inside Biosample, or standalone organism subsection
        meta["organism"] = (
            attrs_lower.get("scientific name", "")
            or attrs_lower.get("scientific_name", "")
            or attrs_lower.get("organism", "")
            or attrs_lower.get("value", "")
        )
        # Append common name if present
        common = attrs_lower.get("common name", "") or attrs_lower.get("common_name", "")
        if common and meta["organism"] and f"({common.lower()})" not in meta["organism"].lower():
            meta["organism"] = f"{meta['organism']} ({common})"

    # --- Imaging Method ---
    # v5: "Image acquisition" -> "Imaging method" attr
    # v4: "Image Acquisition" -> nested "Imaging Method" subsection with "Ontology Value"
    # Legacy: standalone "imaging_method" subsection with "value" attr
    elif sub_type_lower == "image acquisition" and not meta["imaging_method"]:
        meta["imaging_method"] = (
            attrs_lower.get("imaging method", "")
            or attrs_lower.get("imaging_method", "")
        )
    elif sub_type_lower == "imaging method" and not meta["imaging_method"]:
        # v4: nested Imaging Method inside Image Acquisition
        meta["imaging_method"] = (
            attrs_lower.get("ontology value", "")
            or attrs_lower.get("imaging method", "")
            or attrs_lower.get("value", "")
        )
    elif sub_type_lower == "imaging_method" and not meta["imaging_method"]:
        # Legacy IDR import: standalone imaging_method subsection
        meta["imaging_method"] = attrs_lower.get("value", "")

    # --- Study Component (fallback for imaging method) ---
    elif sub_type_lower == "study component" and not meta["imaging_method"]:
        meta["imaging_method"] = (
            attrs_lower.get("imaging method", "")
            or attrs_lower.get("imaging_method", "")
        )

    # --- Experiment N (oldest schema, fallback for imaging method) ---
    elif sub_type_lower.startswith("experiment") and not meta["imaging_method"]:
        meta["imaging_method"] = attrs_lower.get("experiment imaging method", "")

    # --- Publication ---
    # v5: "Publication"; older: "Publications" (plural)
    elif sub_type_lower in ("publication", "publications"):
        if not meta["pub_title"]:
            meta["pub_title"] = attrs_lower.get("title", "")
        if not meta["pub_doi"]:
            meta["pub_doi"] = attrs_lower.get("doi", "")
        if not meta["pub_authors"]:
            meta["pub_authors"] = attrs_lower.get("authors", "")

    # --- Author ---
    # v5: "Author" (capital A); v3/v4: "author" (lowercase)
    elif sub_type_lower == "author":
        name = attrs_lower.get("name", "")
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
    # Also handle "CC-BY-4.0" -> "CC BY 4.0"
    cleaned = raw.strip().upper()
    cleaned = cleaned.replace("CC-BY", "CC BY").replace("CC-0", "CC0")
    # Fix missing hyphen: "CC BY NC" -> "CC BY-NC", "CC BY SA" -> "CC BY-SA", "CC BY ND" -> "CC BY-ND"
    cleaned = re.sub(r"CC BY (NC|SA|ND)", r"CC BY-\1", cleaned)
    # Remove stray hyphens between version digits: "CC BY-4.0" -> "CC BY 4.0"
    cleaned = re.sub(r"CC BY-(\d)", r"CC BY \1", cleaned)
    cleaned_lower = cleaned.lower()
    for prefix in ["cc by-nc-sa", "cc by-nc-nd", "cc by-nc", "cc by-sa", "cc by-nd", "cc by", "cc0"]:
        if prefix in cleaned_lower:
            m = re.search(rf"({re.escape(prefix)}\s*[\d.]*)", cleaned_lower)
            if m:
                return m.group(1).strip().upper()
            return prefix.upper()
    return raw.strip()


ORGANISM_CANONICAL = {
    "homo sapiens": "Homo sapiens (human)",
    "homo sapiens (human)": "Homo sapiens (human)",
    "mus musculus": "Mus musculus (mouse)",
    "mus musculus (mouse)": "Mus musculus (mouse)",
    "arabidopsis thaliana": "Arabidopsis thaliana (thale cress)",
    "arabidopsis thaliana (thale cress)": "Arabidopsis thaliana (thale cress)",
    "danio rerio": "Danio rerio (zebrafish)",
    "danio rerio (zebrafish)": "Danio rerio (zebrafish)",
    "saccharomyces cerevisiae": "Saccharomyces cerevisiae (brewer's yeast)",
    "saccharomyces cerevisiae (brewer's yeast)": "Saccharomyces cerevisiae (brewer's yeast)",
    "drosophila melanogaster": "Drosophila melanogaster (fruit fly)",
    "drosophila melanogaster (fruit fly)": "Drosophila melanogaster (fruit fly)",
    "caenorhabditis elegans": "Caenorhabditis elegans",
    "rattus norvegicus": "Rattus norvegicus (brown rat)",
    "rattus norvegicus (brown rat)": "Rattus norvegicus (brown rat)",
    "escherichia coli": "Escherichia coli",
    "e. coli": "Escherichia coli",
    "nicotiana benthamiana": "Nicotiana benthamiana",
    "plasmodium falciparum": "Plasmodium falciparum",
}


def normalize_organism(raw: str) -> str:
    """Normalize organism names to a canonical form."""
    if not raw:
        return ""
    stripped = raw.strip()
    key = stripped.lower()
    if key in ORGANISM_CANONICAL:
        return ORGANISM_CANONICAL[key]
    # Fix case for genus-species names not in the map (capitalize first word only)
    if stripped[0].islower() and " " in stripped:
        return stripped[0].upper() + stripped[1:]
    return stripped


def normalize_pub_doi(raw: str) -> str:
    """Clean up publication DOI — strip whitespace and remove placeholder values."""
    if not raw:
        return ""
    val = raw.strip()
    if val.upper() in ("TBD", "N/A", "NA", "NONE"):
        return ""
    return val


# Patterns to infer organism from title/description/keywords when not in metadata.
# Order matters: more specific patterns first to avoid false matches.
_ORGANISM_INFER_PATTERNS = [
    (re.compile(r"\b(?:mus musculus|mice|mouse)\b", re.I), "Mus musculus (mouse)"),
    (re.compile(r"\b(?:homo sapiens|human)\b", re.I), "Homo sapiens (human)"),
    (re.compile(r"\b(?:danio rerio|zebrafish)\b", re.I), "Danio rerio (zebrafish)"),
    (re.compile(r"\b(?:drosophila melanogaster|drosophila|fruit fly)\b", re.I), "Drosophila melanogaster (fruit fly)"),
    (re.compile(r"\b(?:arabidopsis thaliana|arabidopsis)\b", re.I), "Arabidopsis thaliana (thale cress)"),
    (re.compile(r"\b(?:saccharomyces cerevisiae|yeast)\b", re.I), "Saccharomyces cerevisiae (brewer's yeast)"),
    (re.compile(r"\b(?:escherichia coli|e\.\s*coli)\b", re.I), "Escherichia coli"),
    (re.compile(r"\b(?:caenorhabditis elegans|c\.\s*elegans)\b", re.I), "Caenorhabditis elegans"),
    (re.compile(r"\b(?:rattus norvegicus)\b", re.I), "Rattus norvegicus (brown rat)"),
]


def infer_organism(study: dict) -> str:
    """Try to infer organism from title, description, and keywords."""
    text = " ".join([
        study.get("title", ""),
        study.get("description", ""),
        " ".join(study.get("keywords", [])),
    ])
    for pattern, species in _ORGANISM_INFER_PATTERNS:
        if pattern.search(text):
            return species
    return ""


def clean_studies(studies: list[dict]) -> list[dict]:
    """Clean and normalize all study metadata in-place."""
    for s in studies:
        # 1. Backfill missing DOIs from accession pattern (10.6019/{accession})
        if not s.get("doi") and s.get("accession"):
            s["doi"] = f"10.6019/{s['accession']}"

        # 2. Normalize organism names
        s["organism"] = normalize_organism(s.get("organism", ""))

        # 3. Infer organism from title/description/keywords if still empty
        if not s.get("organism"):
            s["organism"] = infer_organism(s)

        # 4. Normalize pub_doi
        s["pub_doi"] = normalize_pub_doi(s.get("pub_doi", ""))

        # 5. Strip whitespace from keywords
        if s.get("keywords"):
            s["keywords"] = [k.strip() for k in s["keywords"] if k.strip()]

        # 6. Remove placeholder imaging methods
        if s.get("imaging_method", "").strip().lower() in ("imaging method", ""):
            s["imaging_method"] = ""

        # 7. Backfill missing licenses with CC0
        # The BIA website applies CC0 to all BioImages studies without an
        # explicit license (see detail.min.js injection logic), per EMBL-EBI's
        # policy of adopting CC0 as most in line with their Terms of Use.
        if not s.get("license"):
            s["license"] = "CC0"

    return studies


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
        s.get("doi", ""),
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
            # DOI clickable (Publication DOI = 9, Data DOI = 10)
            elif j in (9, 10) and val:
                doi = val.split()[0]
                if not doi.startswith("http"):
                    doi = f"https://doi.org/{doi}"
                escaped = f'<a href="{html_mod.escape(doi)}" target="_blank">{html_mod.escape(val.split()[0])}</a>'
            # BIA Link column
            elif j == 13 and val:
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
    print("Cleaning & normalizing")
    print("=" * 60)
    studies = clean_studies(studies)
    n_doi = sum(1 for s in studies if s.get("doi"))
    n_org = sum(1 for s in studies if s.get("organism"))
    print(f"  DOIs: {n_doi}/{len(studies)}")
    print(f"  Organisms: {n_org}/{len(studies)}")

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
