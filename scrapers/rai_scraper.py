"""
RAI Advocacy Updates Scraper  v2
=================================
Scrapes ALL advocacy updates from https://www.rai.net.in/advocacy-updates.php

Strategy (discovered via browser inspection):
  1. The main page has 25 pages of content: advocacy-updates.php?page=1..25
  2. Each entry links to a detail page: single.php?id=N
  3. Detail pages contain qrs.ly short-URL download links (not direct .pdf hrefs)
  4. We follow the qrs.ly redirect to get the actual PDF URL and download it

Requirements:
    pip install requests beautifulsoup4 PyMuPDF tqdm loguru
"""

import os
import re
import csv
import time
import hashlib
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime

import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from tqdm import tqdm
from loguru import logger

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL     = "https://www.rai.net.in/advocacy-updates.php"
SITE_ROOT    = "https://www.rai.net.in/"
OUTPUT_DIR   = Path("./output")
PDF_DIR      = OUTPUT_DIR / "pdfs"
TEXT_DIR     = OUTPUT_DIR / "text"
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.rai.net.in/",
}

DELAY          = 1.5   # seconds between requests
TOTAL_PAGES    = 25    # discovered via browser inspection
METADATA_ROWS  = []
SEEN_URLS      = set()
FAILED_ITEMS   = []


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def safe_filename(name, max_len=80):
    name = re.sub(r'[^\w\s\-]', '', name)
    name = re.sub(r'\s+', '_', name.strip())
    return name[:max_len]


def fetch_page(url):
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20, verify=False)
            resp.raise_for_status()
            return resp
        except Exception as e:
            logger.warning(f"  Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2 * (attempt + 1))
    return None


def resolve_redirect(short_url):
    """Follow a short URL (like qrs.ly) to find the final destination."""
    try:
        resp = requests.head(short_url, headers=HEADERS, timeout=15, allow_redirects=True, verify=False)
        final_url = resp.url
        if final_url != short_url:
            return final_url
        # Sometimes HEAD doesn't follow, try GET
        resp = requests.get(short_url, headers=HEADERS, timeout=15, allow_redirects=True, stream=True, verify=False)
        return resp.url
    except Exception as e:
        logger.warning(f"  Could not resolve {short_url}: {e}")
        return short_url


def extract_text_from_pdf(pdf_path):
    text_parts = []
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
    except Exception as e:
        logger.warning(f"Text extraction failed for {pdf_path}: {e}")
    return "\n".join(text_parts)


def download_file(url, name, category="RAI Advocacy", sub_category="Advocacy Update"):
    """Download a file (PDF or otherwise), extract text if PDF."""
    if url in SEEN_URLS:
        return False
    SEEN_URLS.add(url)

    suffix = hashlib.md5(url.encode()).hexdigest()[:6]
    filename = safe_filename(name) + "_" + suffix + ".pdf"
    dest = PDF_DIR / filename

    if dest.exists():
        logger.info(f"  Already exists: {dest.name}")
        return True

    try:
        logger.info(f"  Downloading: {name[:60]}")
        resp = requests.get(url, headers=HEADERS, timeout=30, stream=True, verify=False)
        resp.raise_for_status()

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        # Check if it's actually a PDF
        with open(dest, "rb") as f:
            header = f.read(5)
        
        if header != b'%PDF-':
            logger.warning(f"  Not a PDF file (header: {header}), removing")
            dest.unlink(missing_ok=True)
            return False

        if dest.stat().st_size < 500:
            logger.warning(f"  File too small ({dest.stat().st_size}B), removing")
            dest.unlink(missing_ok=True)
            return False

        # Extract text
        text = extract_text_from_pdf(dest)
        text_path = TEXT_DIR / (dest.stem + ".txt")
        text_path.write_text(text, encoding="utf-8")

        try:
            page_count = len(fitz.open(dest))
        except Exception:
            page_count = 0

        METADATA_ROWS.append({
            "category": category,
            "sub_category": sub_category,
            "name": name,
            "source": "rai.net.in",
            "url": url,
            "filename": dest.name,
            "text_file": text_path.name,
            "pages": page_count,
            "size_kb": round(dest.stat().st_size / 1024, 1),
            "word_count": len(text.split()),
            "downloaded_at": datetime.now().isoformat(),
        })

        logger.success(f"  Saved: {dest.name}  ({round(dest.stat().st_size/1024)}KB, {page_count}pp)")
        return True

    except Exception as e:
        logger.error(f"  Failed to download {url}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPING LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def scrape_list_page(page_num):
    """Scrape one list page and return list of (title, date, detail_url)."""
    url = f"{BASE_URL}?page={page_num}" if page_num > 1 else BASE_URL
    resp = fetch_page(url)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    entries = []

    # Find all "Read More" or detail links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "single.php" in href and "id=" in href:
            detail_url = urljoin(SITE_ROOT, href)
            # Try to get title from nearby text
            parent = a.parent
            title = ""
            # Walk up to find a heading or strong text
            for tag in (parent, parent.parent if parent else None):
                if tag:
                    heading = tag.find(["h2", "h3", "h4", "h5", "strong", "b"])
                    if heading:
                        title = heading.get_text(strip=True)
                        break
            if not title:
                # Try previous siblings
                for sib in a.find_previous_siblings(["h2", "h3", "h4", "p", "strong"]):
                    t = sib.get_text(strip=True)
                    if len(t) > 10:
                        title = t
                        break
            if not title:
                title = a.get_text(strip=True) or f"Entry_{href}"

            if detail_url not in [e[2] for e in entries]:
                entries.append((title, "", detail_url))

    # Also collect any direct PDF or qrs.ly links on the list page itself
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "qrs.ly" in href or href.lower().endswith(".pdf"):
            full_url = urljoin(SITE_ROOT, href)
            link_text = a.get_text(strip=True) or "Document"
            entries.append((link_text, "", full_url))

    return entries


def scrape_detail_page(detail_url, title):
    """Scrape a detail page for download links (qrs.ly, direct PDFs, etc.)."""
    resp = fetch_page(detail_url)
    if not resp:
        FAILED_ITEMS.append({"title": title, "url": detail_url, "reason": "Could not fetch detail page"})
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    download_links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        link_text = a.get_text(strip=True)

        # qrs.ly short links
        if "qrs.ly" in href:
            download_links.append(("qrs_redirect", href, link_text or title))

        # Direct PDF links
        elif href.lower().endswith(".pdf"):
            full_url = urljoin(detail_url, href)
            download_links.append(("direct_pdf", full_url, link_text or title))

        # Google Drive links
        elif "drive.google.com" in href:
            download_links.append(("gdrive", href, link_text or title))

        # Other document links
        elif any(ext in href.lower() for ext in [".doc", ".docx", ".xls", ".xlsx"]):
            full_url = urljoin(detail_url, href)
            download_links.append(("document", full_url, link_text or title))

    # Also extract the full text content of the detail page itself
    content_div = soup.find("div", class_=re.compile(r"content|article|post|entry|body", re.I))
    if not content_div:
        content_div = soup.find("main") or soup.find("article") or soup
    page_text = content_div.get_text(strip=True) if content_div else ""

    # Save the page text even if no PDF is found (the text IS the content)
    if page_text and len(page_text) > 100:
        text_filename = safe_filename(title) + "_webpage.txt"
        text_path = TEXT_DIR / text_filename
        if not text_path.exists():
            text_path.write_text(page_text, encoding="utf-8")
            METADATA_ROWS.append({
                "category": "RAI Advocacy",
                "sub_category": "Webpage Content",
                "name": title,
                "source": "rai.net.in",
                "url": detail_url,
                "filename": "",
                "text_file": text_filename,
                "pages": 0,
                "size_kb": round(len(page_text.encode("utf-8")) / 1024, 1),
                "word_count": len(page_text.split()),
                "downloaded_at": datetime.now().isoformat(),
            })

    return download_links


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("  RAI ADVOCACY UPDATES SCRAPER  v2")
    logger.info("  Source: https://www.rai.net.in/advocacy-updates.php")
    logger.info("  Pages to crawl: 25")
    logger.info("=" * 65)

    # ── STEP 1: Crawl all 25 list pages ──────────────────────────────────
    logger.info(f"\n STEP 1: Crawling {TOTAL_PAGES} list pages...\n")

    all_entries = []
    for page_num in tqdm(range(1, TOTAL_PAGES + 1), desc="List pages"):
        entries = scrape_list_page(page_num)
        all_entries.extend(entries)
        logger.info(f"  Page {page_num}: {len(entries)} entries")
        time.sleep(DELAY)

    # Deduplicate by URL
    seen_detail = set()
    unique_entries = []
    for title, date, url in all_entries:
        if url not in seen_detail:
            seen_detail.add(url)
            unique_entries.append((title, date, url))

    detail_entries = [(t, d, u) for t, d, u in unique_entries if "single.php" in u]
    direct_links = [(t, d, u) for t, d, u in unique_entries if "single.php" not in u]

    logger.info(f"\n  Total unique entries: {len(unique_entries)}")
    logger.info(f"  Detail pages: {len(detail_entries)}")
    logger.info(f"  Direct links: {len(direct_links)}")

    # ── STEP 2: Visit each detail page ───────────────────────────────────
    logger.info(f"\n STEP 2: Visiting {len(detail_entries)} detail pages...\n")

    all_downloads = []
    for title, date, detail_url in tqdm(detail_entries, desc="Detail pages"):
        downloads = scrape_detail_page(detail_url, title)
        for dtype, durl, dname in downloads:
            all_downloads.append((dtype, durl, dname, title))
        time.sleep(DELAY)

    # Add direct links from list pages
    for title, date, url in direct_links:
        if "qrs.ly" in url:
            all_downloads.append(("qrs_redirect", url, title, title))
        elif url.lower().endswith(".pdf"):
            all_downloads.append(("direct_pdf", url, title, title))

    logger.info(f"\n  Total download candidates: {len(all_downloads)}")

    # ── STEP 3: Resolve redirects and download ───────────────────────────
    logger.info(f"\n STEP 3: Resolving and downloading...\n")

    success_count = 0
    for dtype, durl, dname, parent_title in tqdm(all_downloads, desc="Downloads"):
        actual_url = durl

        if dtype == "qrs_redirect":
            logger.info(f"  Resolving qrs.ly: {durl}")
            actual_url = resolve_redirect(durl)
            logger.info(f"    -> {actual_url[:80]}")

        if dtype == "gdrive":
            # Try to convert Google Drive view link to direct download
            m = re.search(r'/d/([a-zA-Z0-9_-]+)', durl)
            if m:
                file_id = m.group(1)
                actual_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                logger.info(f"  Google Drive direct: {actual_url}")

        # Download
        doc_name = dname if dname and len(dname) > 3 else parent_title
        if download_file(actual_url, doc_name):
            success_count += 1

        time.sleep(DELAY)

    # ── STEP 4: Save metadata ────────────────────────────────────────────
    if METADATA_ROWS:
        csv_path = OUTPUT_DIR / "rai_metadata.csv"
        fieldnames = [
            "category", "sub_category", "name", "source", "url",
            "filename", "text_file", "pages", "size_kb", "word_count", "downloaded_at"
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(METADATA_ROWS)
        logger.success(f"\nMetadata CSV: {csv_path}  ({len(METADATA_ROWS)} items)")

    # ── STEP 5: Save failure log ─────────────────────────────────────────
    if FAILED_ITEMS:
        fail_path = OUTPUT_DIR / "rai_failures.csv"
        with open(fail_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["title", "url", "reason"])
            writer.writeheader()
            writer.writerows(FAILED_ITEMS)
        logger.warning(f"Failures logged: {fail_path}  ({len(FAILED_ITEMS)} items)")

    # ── Summary ──────────────────────────────────────────────────────────
    pdf_count = len(list(PDF_DIR.glob("*.pdf")))
    txt_count = len(list(TEXT_DIR.glob("*.txt")))

    logger.info("\n" + "=" * 65)
    logger.info("  DONE!")
    logger.info(f"  Detail pages scraped : {len(detail_entries)}")
    logger.info(f"  Downloads attempted  : {len(all_downloads)}")
    logger.info(f"  Files saved (new)    : {success_count}")
    logger.info(f"  Total PDFs on disk   : {pdf_count}")
    logger.info(f"  Total text files     : {txt_count}")
    logger.info(f"  Metadata CSV         : {OUTPUT_DIR / 'rai_metadata.csv'}")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
