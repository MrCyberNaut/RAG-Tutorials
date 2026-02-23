"""
Post-Scraper Tasks
==================
1. Parse scraper_run.log → generate a failure/success report (output/scraper_report.md)
2. Rename all PDFs in output/pdfs/ based on their text content instead of random hashes
"""

import os
import re
import csv
import shutil
from pathlib import Path

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_DIR   = Path("./output")
PDF_DIR      = OUTPUT_DIR / "pdfs"
TEXT_DIR     = OUTPUT_DIR / "text"
LOG_FILE     = Path("./scraper_run.log")
REPORT_FILE  = OUTPUT_DIR / "scraper_report.md"
RENAME_LOG   = OUTPUT_DIR / "rename_log.csv"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1: Parse log and generate failure/success report
# ─────────────────────────────────────────────────────────────────────────────

def parse_log_and_generate_report():
    print("=" * 60)
    print("  TASK 1: Generating Scraper Report")
    print("=" * 60)

    if not LOG_FILE.exists():
        print(f"  ⚠  Log file not found: {LOG_FILE}")
        return

    # Try multiple encodings
    text = ""
    for enc in ["utf-16", "utf-8", "latin-1"]:
        try:
            text = LOG_FILE.read_text(encoding=enc, errors="replace")
            break
        except Exception:
            continue

    lines = text.splitlines()
    print(f"  Log lines: {len(lines)}")

    # Categorize
    failed_items = []       # (url, document_name, error_reason)
    saved_items = []        # (filename, size_info)
    existing_items = []     # (filename,)
    fallback_hits = []      # (description,)
    fallback_misses = []    # (description,)
    html_skips = []         # URLs that returned HTML instead of PDF
    ssl_errors = []         # SSL certificate errors

    current_doc_name = ""
    current_url = ""

    for i, line in enumerate(lines):
        l = line.strip()
        if not l:
            continue

        # Track current document being processed
        m = re.search(r'\[[\d/]+\]\s+(.+)', l)
        if m:
            current_doc_name = m.group(1).strip()

        # Failed URLs
        m = re.search(r'failed for URL:\s*(https?://\S+)', l)
        if m:
            url = m.group(1)
            reason = "Connection/HTTP error"
            # Look backwards for more context
            for j in range(max(0, i-5), i):
                ctx = lines[j].strip()
                if '404' in ctx:
                    reason = "HTTP 404 — Not Found"
                elif '403' in ctx:
                    reason = "HTTP 403 — Forbidden"
                elif '410' in ctx:
                    reason = "HTTP 410 — Gone"
                elif 'SSL' in ctx or 'certificate' in ctx.lower():
                    reason = "SSL Certificate Error"
                elif 'timeout' in ctx.lower() or 'Timeout' in ctx:
                    reason = "Connection Timeout"
                elif 'ConnectionError' in ctx:
                    reason = "Connection Error"
            failed_items.append((url, current_doc_name, reason))

        # Saved
        m = re.search(r'Saved:\s+(\S+)\s+\((.+?)\)', l)
        if m:
            saved_items.append((m.group(1), m.group(2)))

        # Already exists
        m = re.search(r'Already exists:\s+(\S+)', l)
        if m:
            existing_items.append(m.group(1))

        # HTML response skip
        if 'HTML response' in l or 'not PDF' in l:
            m2 = re.search(r'skipping:\s*(https?://\S+)', l)
            url = m2.group(1) if m2 else "unknown"
            html_skips.append((url, current_doc_name))

        # File too small
        if 'too small' in l.lower():
            html_skips.append(("(file too small)", current_doc_name))

        # Fallback hits
        if 'Fallback found' in l:
            fallback_hits.append(l[:200])

        # Fallback misses
        if 'No PDF links found' in l or 'No valid alternative' in l:
            fallback_misses.append(l[:200])

        # SSL errors
        if 'SSL' in l or 'certificate' in l.lower():
            ssl_errors.append(l[:200])

    # De-duplicate failed URLs
    seen = set()
    unique_failures = []
    for url, name, reason in failed_items:
        if url not in seen:
            seen.add(url)
            unique_failures.append((url, name, reason))

    # ── Write the report ──────────────────────────────────────────────────

    report_lines = []
    report_lines.append("# Scraper Run Report")
    report_lines.append("")
    report_lines.append("## Summary")
    report_lines.append("")
    report_lines.append(f"| Metric | Count |")
    report_lines.append(f"|---|---|")
    report_lines.append(f"| **New PDFs saved** | {len(saved_items)} |")
    report_lines.append(f"| **Already existing (skipped)** | {len(existing_items)} |")
    report_lines.append(f"| **Failed URLs (unique)** | {len(unique_failures)} |")
    report_lines.append(f"| **HTML instead of PDF (skipped)** | {len(html_skips)} |")
    report_lines.append(f"| **DuckDuckGo fallback hits** | {len(fallback_hits)} |")
    report_lines.append(f"| **DuckDuckGo fallback misses** | {len(fallback_misses)} |")
    report_lines.append(f"| **SSL errors** | {len(ssl_errors)} |")
    report_lines.append("")

    # ── Successfully saved ────────────────────────────────────────────────
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## ✅ Successfully Downloaded PDFs")
    report_lines.append("")
    if saved_items:
        report_lines.append("| # | Filename | Size |")
        report_lines.append("|---|---|---|")
        for idx, (fname, size_info) in enumerate(saved_items, 1):
            report_lines.append(f"| {idx} | `{fname}` | {size_info} |")
    else:
        report_lines.append("_No new PDFs saved in this run (all already existed)._")
    report_lines.append("")

    # ── Already existing ──────────────────────────────────────────────────
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## ⏭ Already Existing (Skipped)")
    report_lines.append("")
    if existing_items:
        for idx, fname in enumerate(existing_items, 1):
            report_lines.append(f"{idx}. `{fname}`")
    else:
        report_lines.append("_None._")
    report_lines.append("")

    # ── Failed URLs ───────────────────────────────────────────────────────
    report_lines.append("---")
    report_lines.append("")
    report_lines.append("## ❌ Failed Downloads")
    report_lines.append("")
    if unique_failures:
        report_lines.append("| # | Document | URL | Failure Reason |")
        report_lines.append("|---|---|---|---|")
        for idx, (url, name, reason) in enumerate(unique_failures, 1):
            safe_url = url.replace("|", "\\|")
            safe_name = name.replace("|", "\\|") if name else "—"
            report_lines.append(f"| {idx} | {safe_name} | `{safe_url}` | {reason} |")
    else:
        report_lines.append("_No failures!_")
    report_lines.append("")

    # ── HTML skips ────────────────────────────────────────────────────────
    if html_skips:
        report_lines.append("---")
        report_lines.append("")
        report_lines.append("## ⚠ Skipped (HTML response instead of PDF)")
        report_lines.append("")
        for idx, (url, name) in enumerate(html_skips, 1):
            report_lines.append(f"{idx}. **{name}** — `{url}`")
        report_lines.append("")

    # ── Fallback results ──────────────────────────────────────────────────
    if fallback_hits:
        report_lines.append("---")
        report_lines.append("")
        report_lines.append("## 🔍 DuckDuckGo Fallback — Hits")
        report_lines.append("")
        for h in fallback_hits:
            report_lines.append(f"- {h}")
        report_lines.append("")

    if fallback_misses:
        report_lines.append("---")
        report_lines.append("")
        report_lines.append("## 🔍 DuckDuckGo Fallback — Misses")
        report_lines.append("")
        for m in fallback_misses[:30]:
            report_lines.append(f"- {m}")
        if len(fallback_misses) > 30:
            report_lines.append(f"- _...and {len(fallback_misses) - 30} more_")
        report_lines.append("")

    REPORT_FILE.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"  ✅  Report saved: {REPORT_FILE}")
    print(f"      Failures: {len(unique_failures)}, Saved: {len(saved_items)}, Existing: {len(existing_items)}")
    return unique_failures


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2: Rename PDFs based on their content
# ─────────────────────────────────────────────────────────────────────────────

def extract_title_from_pdf(pdf_path: Path) -> str:
    """Try to extract a good title from a PDF's metadata or first page."""
    title = ""

    if fitz is None:
        return ""

    try:
        doc = fitz.open(pdf_path)

        # 1. Try PDF metadata title
        meta_title = doc.metadata.get("title", "").strip()
        if meta_title and len(meta_title) > 5 and not meta_title.startswith("Microsoft"):
            title = meta_title

        # 2. If no good metadata title, extract from first page text
        if not title and len(doc) > 0:
            first_page_text = doc[0].get_text()
            # Take the first non-empty line that looks like a title
            for line in first_page_text.splitlines():
                cleaned = line.strip()
                # Skip very short lines, page numbers, dates, headers like "GAZETTE"
                if len(cleaned) < 8:
                    continue
                if cleaned.isdigit():
                    continue
                if re.match(r'^(page\s*\d|chapter|section|\d+\.\d+)', cleaned, re.IGNORECASE):
                    continue
                # This looks like a title
                title = cleaned
                break

        doc.close()
    except Exception as e:
        print(f"  ⚠  Could not read {pdf_path.name}: {e}")

    return title


def sanitize_filename(name: str, max_len: int = 80) -> str:
    """Convert a string to a clean, safe filename."""
    name = re.sub(r'[^\w\s\-]', '', name)
    name = re.sub(r'\s+', '_', name.strip())
    name = name.strip('_')
    return name[:max_len] if name else ""


def rename_pdfs():
    print()
    print("=" * 60)
    print("  TASK 2: Renaming PDFs Based on Content")
    print("=" * 60)

    if not PDF_DIR.exists():
        print("  ⚠  PDF directory not found!")
        return

    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    print(f"  Found {len(pdfs)} PDFs to process")

    rename_log = []
    renamed_count = 0
    skipped_count = 0

    # Also try to read metadata.csv for name hints
    name_hints = {}
    meta_csv = OUTPUT_DIR / "metadata.csv"
    if meta_csv.exists():
        try:
            with open(meta_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    fname = row.get("filename", "")
                    doc_name = row.get("name", "")
                    if fname and doc_name:
                        name_hints[fname] = doc_name
        except Exception:
            pass

    for pdf_path in pdfs:
        old_name = pdf_path.name
        old_stem = pdf_path.stem

        # Check if it already has a clean name (no hash suffix)
        # Hashed names look like: Something_Something_a1b2c3.pdf
        has_hash = bool(re.search(r'_[0-9a-f]{6}$', old_stem))

        if not has_hash:
            # Already has a clean name
            skipped_count += 1
            rename_log.append({
                "old_name": old_name,
                "new_name": old_name,
                "source": "already_clean",
                "title_extracted": ""
            })
            continue

        # Try to get name from metadata CSV first
        new_name_base = ""
        source = ""

        if old_name in name_hints:
            new_name_base = sanitize_filename(name_hints[old_name])
            source = "metadata_csv"

        # If no name from CSV, try PDF content
        if not new_name_base:
            title = extract_title_from_pdf(pdf_path)
            if title:
                new_name_base = sanitize_filename(title)
                source = "pdf_content"

        # If still no good name, try the text file
        if not new_name_base:
            text_file = TEXT_DIR / (old_stem + ".txt")
            if text_file.exists():
                try:
                    text_content = text_file.read_text(encoding="utf-8", errors="replace")
                    for line in text_content.splitlines():
                        cleaned = line.strip()
                        if len(cleaned) > 8 and not cleaned.isdigit():
                            new_name_base = sanitize_filename(cleaned)
                            source = "text_file"
                            break
                except Exception:
                    pass

        # If we still have nothing, skip
        if not new_name_base or len(new_name_base) < 5:
            skipped_count += 1
            rename_log.append({
                "old_name": old_name,
                "new_name": old_name,
                "source": "no_title_found",
                "title_extracted": ""
            })
            continue

        # Build new filename
        new_filename = new_name_base + ".pdf"

        # Handle collisions
        if (PDF_DIR / new_filename).exists() and new_filename != old_name:
            counter = 2
            while (PDF_DIR / f"{new_name_base}_{counter}.pdf").exists():
                counter += 1
            new_filename = f"{new_name_base}_{counter}.pdf"

        if new_filename == old_name:
            skipped_count += 1
            rename_log.append({
                "old_name": old_name,
                "new_name": old_name,
                "source": "same_name",
                "title_extracted": new_name_base
            })
            continue

        # Rename the PDF
        new_path = PDF_DIR / new_filename
        try:
            pdf_path.rename(new_path)
            print(f"  ✅  {old_name}")
            print(f"      → {new_filename}  (from {source})")

            # Also rename the corresponding text file
            old_text = TEXT_DIR / (old_stem + ".txt")
            if old_text.exists():
                new_text = TEXT_DIR / (Path(new_filename).stem + ".txt")
                old_text.rename(new_text)

            rename_log.append({
                "old_name": old_name,
                "new_name": new_filename,
                "source": source,
                "title_extracted": new_name_base
            })
            renamed_count += 1

        except Exception as e:
            print(f"  ❌  Failed to rename {old_name}: {e}")
            rename_log.append({
                "old_name": old_name,
                "new_name": old_name,
                "source": f"error: {e}",
                "title_extracted": new_name_base
            })

    # Save rename log
    if rename_log:
        with open(RENAME_LOG, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["old_name", "new_name", "source", "title_extracted"])
            writer.writeheader()
            writer.writerows(rename_log)

    print()
    print(f"  ✅  Renamed: {renamed_count}")
    print(f"  ⏭  Skipped: {skipped_count}")
    print(f"  📋  Rename log: {RENAME_LOG}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    parse_log_and_generate_report()
    print()
    rename_pdfs()
    print()
    print("=" * 60)
    print("  ALL POST-SCRAPER TASKS COMPLETE")
    print("=" * 60)
