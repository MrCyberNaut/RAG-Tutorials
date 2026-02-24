"""
Smart PDF Renamer - Uses text analysis to generate proper names.
Writes the list to a file so the AI can review and fix names.
"""
import os
import json
from pathlib import Path

try:
    import fitz
except ImportError:
    fitz = None

PDF_DIR = Path("output/pdfs")
TEXT_DIR = Path("output/text")

pdfs = sorted(PDF_DIR.glob("*.pdf"))
print(f"Total PDFs: {len(pdfs)}")

entries = []
for i, pdf in enumerate(pdfs):
    stem = pdf.stem
    size_kb = round(pdf.stat().st_size / 1024)
    
    # Get text
    text = ""
    text_file = TEXT_DIR / (stem + ".txt")
    if text_file.exists():
        try:
            text = text_file.read_text(encoding="utf-8", errors="replace")
        except:
            pass
    
    if not text and fitz:
        try:
            doc = fitz.open(pdf)
            for j, page in enumerate(doc):
                if j >= 2:
                    break
                text += page.get_text()
            doc.close()
        except:
            pass
    
    # Take first 300 words
    snippet = " ".join(text.split()[:300]) if text else ""
    
    entries.append({
        "index": i,
        "current_name": pdf.name,
        "size_kb": size_kb,
        "snippet": snippet[:1500]
    })

# Write to file
out_file = Path("output/_pdf_contents.json")
with open(out_file, "w", encoding="utf-8") as f:
    json.dump(entries, f, indent=2, ensure_ascii=False)

print(f"Written to {out_file}")
print()
# Also print a readable summary
for e in entries:
    print(f"[{e['index']}] {e['current_name']} ({e['size_kb']}KB)")
    if e["snippet"]:
        print(f"    {e['snippet'][:200]}")
    else:
        print(f"    (no text content)")
    print()
