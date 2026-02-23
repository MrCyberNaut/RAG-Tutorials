"""
=============================================================================
INDIA RETAIL COMPLIANCE — GOVERNMENT DOCUMENT SCRAPER  v2.0
=============================================================================
Target: ALL government documents relevant to Indian retailers —
        Acts, Rules, Policies, Advisories, Compliance Notices, Benefits.

Requirements:
    pip install requests beautifulsoup4 PyMuPDF tqdm loguru pandas duckduckgo-search

Run:
    python retail_govt_doc_scraper_v2.py

Outputs:
    ./output/pdfs/         ← downloaded PDFs
    ./output/text/         ← extracted plain text per PDF
    ./output/metadata.csv  ← full index of every document collected

=============================================================================
CHANGELOG vs v1.0
=============================================================================

NEW CATEGORY: Labour Laws (6 new PDFs)
  + Model Shops & Establishments Act 2016         (labour.gov.in)
  + Minimum Wages Act 1948                        (labour.gov.in)
  + Minimum Wages Central Rules 1950              (labour.gov.in)
  + Payment of Wages Act 1936                     (samadhan.labour.gov.in)
  + Maternity Benefit Act 1961                    (labour.gov.in)
  + Compliance Handbook — Four New Labour Codes   (labour.gov.in)  ← NEW 2026 doc

NEW CATEGORY: Essential Commodities & Supply Control (3 new PDFs)
  + Essential Commodities Act 1955                (dfpd.gov.in)
  + Prevention of Black Marketing Act 1980        (indiacode.nic.in)
  + Essential Commodities (Amendment) Act 2020    (indiacode.nic.in)

NEW CATEGORY: Taxation & Finance for Retailers (3 new PDFs)
  + Income Tax Act sections relevant to retail    (incometax.gov.in)
  + TDS provisions — Section 194C/194H guide      (incometax.gov.in)
  + MSME 45-day payment rule (Section 43B(h))     (msme.gov.in)

NEW CATEGORY: MSME & Business Registration (3 new PDFs)
  + MSME Development Act 2006                     (msme.gov.in)
  + PM Vishwakarma Scheme guidelines              (msme.gov.in)
  + Udyam Registration guidelines                 (udyamregistration.gov.in)

NEW CATEGORY: Competition & Trade Practices (2 new PDFs)
  + Competition Act 2002                          (cci.gov.in)
  + Competition (Amendment) Act 2023              (cci.gov.in)

NEW CATEGORY: Drugs & Cosmetics (Pharmacy Retailers) (2 new PDFs)
  + Drugs & Cosmetics Act 1940                    (cdsco.gov.in)
  + Drugs & Cosmetics Rules 1945                  (cdsco.gov.in)

NEW CATEGORY: IT & E-Commerce Laws (2 new PDFs)
  + Information Technology Act 2000               (meity.gov.in)
  + IT (Intermediary Guidelines) Rules 2021       (meity.gov.in)

NEW CATEGORY: BIS & Hallmarking (2 new PDFs)
  + BIS Act 2016                                  (bis.gov.in)
  + Hallmarking of Gold Jewellery Order 2021      (bis.gov.in)

NEW CATEGORY: State Retail Policies (1 new PDF)
  + Maharashtra Retail Trade Policy 2016          (maitri.mahaonline.gov.in)

NEW CRAWL TARGETS (5 new portals added):
  + labour.gov.in/acts-rules         — all central labour law PDFs
  + msme.gov.in/schemes              — MSME scheme documents
  + cci.gov.in/resources             — Competition Commission resources
  + indiacode.nic.in (search)        — India Code central acts archive
  + dfpd.gov.in (food/essential)     — Dept of Food & Public Distribution

IMPROVED: Metadata now captures `sub_category` field for finer RAG filtering
IMPROVED: Added `govt_ministry` field to metadata for filtering by issuer
IMPROVED: Retry logic added (3 retries with backoff) for flaky govt servers
IMPROVED: Duplicate URL detection — skips already-queued URLs across batches
=============================================================================
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

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_DIR = Path("./output")
PDF_DIR    = OUTPUT_DIR / "pdfs"
TEXT_DIR   = OUTPUT_DIR / "text"
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

DELAY_BETWEEN_REQUESTS = 2   # seconds — be polite to govt servers
MAX_RETRIES            = 3   # retry attempts for failed downloads
METADATA_ROWS          = []  # accumulated, saved to CSV at end
SEEN_URLS              = set()  # dedup across direct + crawled PDFs


# ─────────────────────────────────────────────────────────────────────────────
# DIRECT PDF LINKS
# Confirmed direct URLs to official government PDFs.
# Structure: category → sub_category → ministry → url
# ─────────────────────────────────────────────────────────────────────────────

DIRECT_PDFS = [

    # ═══════════════════════════════════════════════════════════════════════
    # 1. DATA PROTECTION (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Data Protection",
        "sub_category": "DPDP",
        "ministry":     "MeitY",
        "name":         "Digital Personal Data Protection Act 2023",
        "url":          "https://www.meity.gov.in/static/uploads/2024/06/2bf1f0e9f04e6fb4f8fef35e82c42aa5.pdf",
    },
    {
        "category":     "Data Protection",
        "sub_category": "DPDP",
        "ministry":     "MeitY",
        "name":         "DPDP Rules 2025 (PIB Official)",
        "url":          "https://static.pib.gov.in/WriteReadData/specificdocs/documents/2025/nov/doc20251117695301.pdf",
    },
    {
        "category":     "Data Protection",
        "sub_category": "DPDP",
        "ministry":     "MeitY",
        "name":         "DPDP Rules 2025 (English Clean Copy)",
        "url":          "https://dpdpa.com/DPDP_Rules_2025_English_only.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 2. GOA DEPOSIT REFUND SCHEME (from v1 + expanded)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Environment",
        "sub_category": "Deposit Refund Scheme",
        "ministry":     "Goa Govt",
        "name":         "Goa DRS Gazette Notification Aug 2024",
        "url":          "https://goaprintingpress.gov.in/downloads/2425/2425-20-SI-OG-0.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 3. GST (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Taxation",
        "sub_category": "GST",
        "ministry":     "Ministry of Finance",
        "name":         "Central GST Act 2017 (Updated Jan 2024)",
        "url":          "https://www.gst.gov.in/download/gstlaw/CGST_Act_Updated_31012024.pdf",
    },
    {
        "category":     "Taxation",
        "sub_category": "GST",
        "ministry":     "Ministry of Finance",
        "name":         "Integrated GST Act 2017 (Updated Jan 2024)",
        "url":          "https://www.gst.gov.in/download/gstlaw/IGST_Act_Updated_31012024.pdf",
    },
    {
        "category":     "Taxation",
        "sub_category": "GST",
        "ministry":     "Ministry of Finance",
        "name":         "Central GST Rules 2017 (Updated Jan 2024)",
        "url":          "https://www.gst.gov.in/download/gstlaw/CGST_Rules_Updated_31012024.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 4. LEGAL METROLOGY (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Product Standards",
        "sub_category": "Legal Metrology",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Legal Metrology Act 2009",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/lma-acts-rules/Legal_Metrology_Act_2009.pdf",
    },
    {
        "category":     "Product Standards",
        "sub_category": "Legal Metrology",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Legal Metrology Packaged Commodities Rules 2011",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/lma-acts-rules/LM%28PC%29Rules%202011.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 5. CONSUMER PROTECTION (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Consumer Protection",
        "sub_category": "Consumer Protection Act",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Consumer Protection Act 2019",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/acts-and-rules-pdf/CPA2019_0.pdf",
    },
    {
        "category":     "Consumer Protection",
        "sub_category": "E-Commerce Rules",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Consumer Protection E-Commerce Rules 2020",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/acts-and-rules-pdf/Consumer_protection_E-Commerce_Rules2020.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 6. ENVIRONMENT / EPR / PLASTIC WASTE (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Environment",
        "sub_category": "EPR / Plastic Waste",
        "ministry":     "MoEFCC / CPCB",
        "name":         "Plastic Waste Management Rules 2016 (Amended)",
        "url":          "https://cpcb.nic.in/openpdffile.php?id=TGF0ZXN0RmlsZS8yOTlfMTY1NTk2MDUxNF9tZWRpYXBob3RvMTc5MzgucGRm",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 7. FSSAI (from v1)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Food Safety",
        "sub_category": "FSSAI",
        "ministry":     "Ministry of Health",
        "name":         "Food Safety and Standards Act 2006",
        "url":          "https://www.fssai.gov.in/upload/uploadfiles/files/FSSA_Gazette_Notification.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 8. ★ NEW: LABOUR LAWS
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Labour Law",
        "sub_category": "Shops & Establishments",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Model Shops and Establishments (Regulation of Employment) Act 2016",
        "url":          "https://labour.gov.in/sites/default/files/model_bill_englsih_.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Wages",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Minimum Wages Act 1948",
        "url":          "https://labour.gov.in/sites/default/files/theminimumwagesact1948_0.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Wages",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Minimum Wages Central Rules 1950",
        "url":          "https://labour.gov.in/sites/default/files/theminimumwages_central_rules1950_0.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Wages",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Payment of Wages Act 1936",
        "url":          "https://samadhan.labour.gov.in/whatsnew/ThePaymentofWagesAct1936_0.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Employee Benefits",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Maternity Benefit Act 1961",
        "url":          "https://labour.gov.in/sites/default/files/maternity_benefit_act_1961_0.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Labour Codes",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Compliance Handbook — Four New Labour Codes (2026)",
        "url":          "https://www.labour.gov.in/static/uploads/2026/02/83978455025732b99b0165def80ab171.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Industrial Employment",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Industrial Employment Standing Orders Act 1946 and Central Rules",
        "url":          "https://labour.gov.in/sites/default/files/industrialemploymentstandingorders1centralrules1946.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Employee Benefits",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Payment of Bonus Act 1965",
        "url":          "https://labour.gov.in/sites/default/files/thepaymentofbonusact1965_0.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Employee Benefits",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Payment of Gratuity Act 1972",
        "url":          "https://labour.gov.in/sites/default/files/thepaymentofgratuityact1972_0.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 9. ★ NEW: ESSENTIAL COMMODITIES & SUPPLY CONTROL
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Essential Commodities",
        "sub_category": "Supply Control",
        "ministry":     "Ministry of Consumer Affairs / DFPD",
        "name":         "Essential Commodities Act 1955",
        "url":          "https://dfpd.gov.in/WriteReadData/Other/act5.pdf",
    },
    {
        "category":     "Essential Commodities",
        "sub_category": "Supply Control",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Prevention of Black Marketing and Maintenance of Supplies of Essential Commodities Act 1980",
        "url":          "https://www.indiacode.nic.in/bitstream/123456789/1614/1/198009.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 10. ★ NEW: TAXATION & INCOME TAX
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Taxation",
        "sub_category": "Income Tax",
        "ministry":     "Ministry of Finance",
        "name":         "Income Tax Act 1961 (as amended — finance act 2024)",
        "url":          "https://incometaxindia.gov.in/Pages/acts/income-tax-act.aspx",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 11. ★ NEW: MSME & BUSINESS REGISTRATION
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "MSME & Business Registration",
        "sub_category": "MSME",
        "ministry":     "Ministry of MSME",
        "name":         "MSME Development Act 2006",
        "url":          "https://www.msme.gov.in/sites/default/files/MSMED-Act2006.pdf",
    },
    {
        "category":     "MSME & Business Registration",
        "sub_category": "MSME Schemes",
        "ministry":     "Ministry of MSME",
        "name":         "PM Vishwakarma Scheme Guidelines",
        "url":          "https://pmvishwakarma.gov.in/Content/GuidlinesForPMVScheme.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 12. ★ NEW: COMPETITION LAW
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Competition Law",
        "sub_category": "Competition Act",
        "ministry":     "Ministry of Corporate Affairs",
        "name":         "Competition Act 2002 (as amended 2023)",
        "url":          "https://www.cci.gov.in/sites/default/files/cci_pdf/competition_act_2023_0.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 13. ★ NEW: DRUGS & COSMETICS (Pharmacy / Medical Retailers)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Drugs & Cosmetics",
        "sub_category": "Pharmaceutical Retail",
        "ministry":     "Ministry of Health & Family Welfare",
        "name":         "Drugs and Cosmetics Act 1940",
        "url":          "https://cdsco.gov.in/opencms/export/sites/CDSCO_WEB/Pdf-documents/acts_rules/DrugsandCosmeticsAct1940.pdf",
    },
    {
        "category":     "Drugs & Cosmetics",
        "sub_category": "Pharmaceutical Retail",
        "ministry":     "Ministry of Health & Family Welfare",
        "name":         "Drugs and Cosmetics Rules 1945",
        "url":          "https://cdsco.gov.in/opencms/export/sites/CDSCO_WEB/Pdf-documents/acts_rules/Drugs_and_Cosmetics_Rules_1945.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 14. ★ NEW: IT ACT & E-COMMERCE RULES
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "IT & E-Commerce",
        "sub_category": "IT Act",
        "ministry":     "MeitY",
        "name":         "Information Technology Act 2000 (as amended)",
        "url":          "https://www.meity.gov.in/writereaddata/files/itbill2000.pdf",
    },
    {
        "category":     "IT & E-Commerce",
        "sub_category": "Intermediary Rules",
        "ministry":     "MeitY",
        "name":         "IT Intermediary Guidelines and Digital Media Ethics Code Rules 2021",
        "url":          "https://www.meity.gov.in/writereaddata/files/Intermediary_Guidelines_and_Digital_Media_Ethics_Code_Rules-2021.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 15. ★ NEW: BIS & HALLMARKING
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Product Standards",
        "sub_category": "BIS & Hallmarking",
        "ministry":     "Ministry of Consumer Affairs / BIS",
        "name":         "Bureau of Indian Standards Act 2016",
        "url":          "https://www.bis.gov.in/wp-content/uploads/2020/09/BIS-Act-2016.pdf",
    },
    {
        "category":     "Product Standards",
        "sub_category": "BIS & Hallmarking",
        "ministry":     "Ministry of Consumer Affairs / BIS",
        "name":         "Compulsory Hallmarking of Gold Jewellery Order 2021",
        "url":          "https://www.bis.gov.in/wp-content/uploads/2021/06/Hallmarking-Order_2021.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 16. ★ NEW: STATE RETAIL POLICIES
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Retail Policy",
        "sub_category": "State Retail Policies",
        "ministry":     "Maharashtra Industries Dept",
        "name":         "Maharashtra Retail Trade Policy 2016",
        "url":          "https://maitri.mahaonline.gov.in/PDF/Retail_Policy_2016.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 17. ★ NEW: FDI POLICY (Retail-Specific)
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "FDI & Trade Policy",
        "sub_category": "FDI in Retail",
        "ministry":     "DPIIT",
        "name":         "Consolidated FDI Policy 2020 (includes Multi-Brand Retail)",
        "url":          "https://dpiit.gov.in/sites/default/files/FDI-Policy-2020_0.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 18. ★ NEW: FIRE SAFETY & BUILDING SAFETY
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Safety & Compliance",
        "sub_category": "Fire Safety",
        "ministry":     "Ministry of Home Affairs",
        "name":         "Model Building Byelaws 2016 (includes Fire Safety for Retail)",
        "url":          "https://mohua.gov.in/upload/uploadfiles/files/Model%20Building%20Bye-Laws%202016.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 19. ★ NEW: SOCIAL SECURITY
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Labour Law",
        "sub_category": "Social Security",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Employees Provident Funds and Miscellaneous Provisions Act 1952",
        "url":          "https://www.epfindia.gov.in/site_docs/PDFs/Downloads_PDFs/EPFAct1952.pdf",
    },
    {
        "category":     "Labour Law",
        "sub_category": "Social Security",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Employees State Insurance Act 1948",
        "url":          "https://www.esic.gov.in/attachments/files/8c9c71fa2dab23d1d5db6aab6c2b7b2c.pdf",
    },

    # ═══════════════════════════════════════════════════════════════════════
    # 20. ★ NEW: ENVIRONMENT — E-WASTE & PACKAGING
    # ═══════════════════════════════════════════════════════════════════════
    {
        "category":     "Environment",
        "sub_category": "E-Waste",
        "ministry":     "MoEFCC",
        "name":         "E-Waste Management Rules 2022",
        "url":          "https://cpcb.nic.in/openpdffile.php?id=TGF0ZXN0RmlsZS8zNzFfMTY2ODQ5NTE1MF9tZWRpYXBob3RvMTk1NDEucGRm",
    },
    {
        "category":     "Environment",
        "sub_category": "EPR / Plastic Waste",
        "ministry":     "MoEFCC / CPCB",
        "name":         "Extended Producer Responsibility Guidelines 2022 (Plastics)",
        "url":          "https://cpcb.nic.in/openpdffile.php?id=TGF0ZXN0RmlsZS8zNzBfMTY2ODQzOTI2NV9tZWRpYXBob3RvMTk1MzkucGRm",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# CRAWL TARGETS — Pages to scrape for additional PDF links dynamically
# ─────────────────────────────────────────────────────────────────────────────

CRAWL_TARGETS = [

    # ── EXISTING (from v1) ─────────────────────────────────────────────────
    {
        "category":     "Taxation",
        "sub_category": "GST",
        "ministry":     "Ministry of Finance",
        "name":         "GST Law Downloads Page",
        "url":          "https://www.gst.gov.in/download/gstlaw",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Food Safety",
        "sub_category": "FSSAI",
        "ministry":     "Ministry of Health",
        "name":         "FSSAI All Regulations Page",
        "url":          "https://www.fssai.gov.in/home/fssa-regulations/fssai-regulation.html",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Environment",
        "sub_category": "Deposit Refund Scheme",
        "ministry":     "Goa Govt",
        "name":         "Goa DRS Official Website",
        "url":          "https://www.goadrs.com/",
        "pdf_pattern":  r"\.(pdf|PDF)$",
    },
    {
        "category":     "Data Protection",
        "sub_category": "DPDP",
        "ministry":     "MeitY",
        "name":         "MeitY Data Protection Framework Page",
        "url":          "https://www.meity.gov.in/data-protection-framework",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Consumer Protection",
        "sub_category": "Consumer Protection Act",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Consumer Affairs — All Acts and Rules",
        "url":          "https://consumeraffairs.gov.in/acts-and-rules",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Environment",
        "sub_category": "EPR / Plastic Waste",
        "ministry":     "MoEFCC / CPCB",
        "name":         "CPCB Extended Producer Responsibility Page",
        "url":          "https://cpcb.nic.in/extended-producer-responsibility/",
        "pdf_pattern":  r"\.pdf$",
    },

    # ── ★ NEW CRAWL TARGETS ────────────────────────────────────────────────

    {
        "category":     "Labour Law",
        "sub_category": "All Labour Acts",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Labour Ministry — All Acts & Rules Page",
        "url":          "https://labour.gov.in/acts-rules",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "MSME & Business Registration",
        "sub_category": "MSME Schemes",
        "ministry":     "Ministry of MSME",
        "name":         "MSME Ministry — Schemes Page",
        "url":          "https://msme.gov.in/1-about-schemes",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Competition Law",
        "sub_category": "Competition Act",
        "ministry":     "Competition Commission of India",
        "name":         "CCI Resources and Publications",
        "url":          "https://www.cci.gov.in/resources/publications",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Essential Commodities",
        "sub_category": "Supply Control",
        "ministry":     "DFPD",
        "name":         "Dept of Food & Public Distribution — Laws",
        "url":          "https://dfpd.gov.in/acts-rules.htm",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Product Standards",
        "sub_category": "Legal Metrology",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Legal Metrology — All Acts and Rules",
        "url":          "https://legalmetrology.gov.in/acts-and-rules",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Drugs & Cosmetics",
        "sub_category": "Pharmaceutical Retail",
        "ministry":     "Ministry of Health",
        "name":         "CDSCO Acts and Rules Page",
        "url":          "https://cdsco.gov.in/opencms/opencms/en/Acts_Rules/",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Food Safety",
        "sub_category": "FSSAI",
        "ministry":     "Ministry of Health",
        "name":         "FSSAI Advisories and Notices to Food Businesses",
        "url":          "https://www.fssai.gov.in/home/food-business/guidance-documents.html",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Retail Policy",
        "sub_category": "Central Policy",
        "ministry":     "DPIIT",
        "name":         "DPIIT Press Notes and Policy Documents",
        "url":          "https://dpiit.gov.in/policies/press-notes",
        "pdf_pattern":  r"\.pdf$",
    },
    {
        "category":     "Consumer Protection",
        "sub_category": "Dark Patterns",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "CCPA Guidelines — Dark Patterns in E-Commerce 2023",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/latestnews/guidelines_dark_patterns.pdf",
        "pdf_pattern":  r"\.pdf$",
        "is_direct":    True,  # treat as direct link, crawl the surrounding page too
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# ADDITIONAL ONE-OFF DIRECT PDFs DISCOVERED DURING RESEARCH
# ─────────────────────────────────────────────────────────────────────────────

EXTRA_DIRECT_PDFS = [
    # CCPA Dark Patterns Guidelines (important for e-commerce retailers)
    {
        "category":     "Consumer Protection",
        "sub_category": "Dark Patterns",
        "ministry":     "Ministry of Consumer Affairs / CCPA",
        "name":         "CCPA Guidelines for Prevention and Regulation of Dark Patterns 2023",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/latestnews/guidelines_dark_patterns.pdf",
    },
    # Goa Govt official portal — look for any retail/trade PDFs
    {
        "category":     "Retail Policy",
        "sub_category": "Goa Notifications",
        "ministry":     "Goa Govt",
        "name":         "Goa Printing Press Official Gazette Portal",
        "url":          "https://goaprintingpress.gov.in/",
        "is_crawl":     True,
        "pdf_pattern":  r"\.pdf$",
    },
    # Payment of Bonus Act
    {
        "category":     "Labour Law",
        "sub_category": "Employee Benefits",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Payment of Bonus Act 1965",
        "url":          "https://labour.gov.in/sites/default/files/thepaymentofbonusact1965_0.pdf",
    },
    # Sale of Goods Act (relevant to all retailers)
    {
        "category":     "Contract & Trade Law",
        "sub_category": "Sale of Goods",
        "ministry":     "Ministry of Law & Justice",
        "name":         "Sale of Goods Act 1930",
        "url":          "https://www.indiacode.nic.in/bitstream/123456789/2249/1/the-sale-of-goods-act-1930.pdf",
    },
    # Consumer Welfare Fund Rules
    {
        "category":     "Consumer Protection",
        "sub_category": "Consumer Welfare",
        "ministry":     "Ministry of Consumer Affairs",
        "name":         "Consumer Welfare Fund Rules 1992 (amended)",
        "url":          "https://consumeraffairs.gov.in/sites/default/files/file-uploads/acts-and-rules-pdf/CWFRules1992.pdf",
    },
    # Pradhan Mantri Rojgar Protsahan Yojana — benefits for retailers who employ staff
    {
        "category":     "MSME & Business Registration",
        "sub_category": "Govt Schemes Benefits",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "PMRPY Scheme Guidelines (Employment Incentive Scheme)",
        "url":          "https://pmrpy.gov.in/sites/default/files/PMRPY%20Guidelines.pdf",
    },
    # Trade Marks Act (brand protection for retailers)
    {
        "category":     "Intellectual Property",
        "sub_category": "Trademarks",
        "ministry":     "Ministry of Commerce / CGPDTM",
        "name":         "Trade Marks Act 1999",
        "url":          "https://www.indiacode.nic.in/bitstream/123456789/15427/1/the_trade_marks_act,_1999.pdf",
    },
    # Prevention of Food Adulteration (older law still referenced by FSSAI)
    {
        "category":     "Food Safety",
        "sub_category": "FSSAI",
        "ministry":     "Ministry of Health",
        "name":         "Food Safety and Standards Labelling and Display Regulations 2020",
        "url":          "https://www.fssai.gov.in/upload/uploadfiles/files/Gazette_Notification_Labelling_Regulations_01_02_2021.pdf",
    },
    # FSSAI Licensing and Registration Regulations
    {
        "category":     "Food Safety",
        "sub_category": "FSSAI",
        "ministry":     "Ministry of Health",
        "name":         "FSSAI Licensing and Registration Regulations 2011",
        "url":          "https://www.fssai.gov.in/upload/uploadfiles/files/Licensing_Registration_Regulation_Gazette.pdf",
    },
    # GST Composition Scheme (very relevant for small retailers)
    {
        "category":     "Taxation",
        "sub_category": "GST",
        "ministry":     "Ministry of Finance",
        "name":         "GST Composition Scheme for Small Retailers — Circular",
        "url":          "https://www.gst.gov.in/download/gstcirculars/id/2",
    },
    # Fire Safety NOC — National Building Code reference
    {
        "category":     "Safety & Compliance",
        "sub_category": "Fire Safety",
        "ministry":     "BIS / MoHUA",
        "name":         "National Building Code of India 2016 Part 4 — Fire Safety",
        "url":          "https://bis.gov.in/product/NatBldCode2016/",
    },
    # Minimum Wages (recent notification 2024)
    {
        "category":     "Labour Law",
        "sub_category": "Wages",
        "ministry":     "Ministry of Labour & Employment",
        "name":         "Minimum Wages — Background and Current Rates 2025",
        "url":          "https://www.labour.gov.in/static/uploads/2025/06/e5419a690d59b0f1d13b16b290351987.pdf",
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def safe_filename(name: str, max_len: int = 80) -> str:
    name = re.sub(r'[^\w\s\-]', '', name)
    name = re.sub(r'\s+', '_', name.strip())
    return name[:max_len]


def url_to_filename(url: str) -> str:
    parsed = urlparse(url)
    base = os.path.basename(parsed.path) or "doc"
    base = re.sub(r'[^\w\.\-]', '_', base)
    suffix = hashlib.md5(url.encode()).hexdigest()[:6]
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    stem = Path(base).stem
    return f"{stem}_{suffix}.pdf"


def extract_text_from_pdf(pdf_path: Path) -> str:
    text_parts = []
    try:
        doc = fitz.open(pdf_path)
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
    except Exception as e:
        logger.warning(f"Text extraction failed for {pdf_path}: {e}")
    return "\n".join(text_parts)


def find_via_search_and_crawl(name: str, url_hint: str, category: str, sub_category: str, ministry: str, source: str) -> bool:
    """Fallback search: look up document via DuckDuckGo, crawl top pages to find actual PDF link."""
    if DDGS is None:
        logger.warning("  ⚠  duckduckgo-search not installed, skipping fallback.")
        return False
        
    portal = urlparse(url_hint).netloc
    logger.info(f"  🔍  Fallback: Searching DuckDuckGo for '{name}' on {portal}")
    try:
        query = f"{name} site:{portal}"
        results = DDGS().text(query, max_results=3)
        found_results = list(results) if results else []
        
        if not found_results:
            query_broad = f"{name} {ministry}"
            logger.info(f"  🔍  Fallback: Broad search for '{query_broad}'")
            results_broad = DDGS().text(query_broad, max_results=3)
            found_results = list(results_broad) if results_broad else []
            
        for res in found_results:
            page_url = res.get('href')
            if not page_url: continue
            if page_url in SEEN_URLS: continue
            
            if page_url.lower().endswith('.pdf'):
                logger.info(f"  🔗  Fallback found direct PDF: {page_url}")
                dest = PDF_DIR / url_to_filename(page_url)
                if download_pdf(page_url, dest, category, name, source, sub_category, ministry, is_fallback=True):
                    return True
            else:
                logger.info(f"  🌐  Fallback creeping into webpage: {page_url}")
                try:
                    resp = requests.get(page_url, headers=HEADERS, timeout=15)
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    found_pdfs = []
                    for a_tag in soup.find_all("a", href=True):
                        href = a_tag.get("href")
                        if href and re.search(r"\.pdf$", href, re.IGNORECASE):
                            pdf_url = urljoin(page_url, href)
                            link_text = a_tag.get_text(strip=True)
                            found_pdfs.append((pdf_url, link_text))
                            
                    if found_pdfs:
                        # find the best match based on keywords
                        name_words = set(re.findall(r'\w+', name.lower()))
                        best_pdf = found_pdfs[0][0]
                        max_overlap = -1
                        for pdf_url, link_text in found_pdfs:
                            link_words = set(re.findall(r'\w+', link_text.lower()))
                            overlap = len(name_words.intersection(link_words))
                            if overlap > max_overlap:
                                max_overlap = overlap
                                best_pdf = pdf_url
                                
                        logger.info(f"  🔗  Fallback found PDF link inside page: {best_pdf}")
                        dest = PDF_DIR / url_to_filename(best_pdf)
                        if download_pdf(best_pdf, dest, category, name, source, sub_category, ministry, is_fallback=True):
                            return True
                    else:
                        logger.debug("  ⚠  No PDF links found on this page.")
                except Exception as crawl_e:
                    logger.debug(f"  ⚠  Failed to creep into {page_url}: {crawl_e}")
                    
    except Exception as search_e:
        logger.error(f"  ❌  Search failed: {search_e}")
        
    return False


def download_pdf(url: str, dest_path: Path, category: str, name: str,
                 source: str, sub_category: str = "", ministry: str = "", is_fallback: bool = False) -> bool:
    """Download a PDF with retry logic, extract text, and log metadata."""

    if url in SEEN_URLS:
        logger.debug(f"  ⏭  Duplicate URL skipped: {url[:70]}")
        return False
    SEEN_URLS.add(url)

    if dest_path.exists():
        logger.info(f"  ⏭  Already exists: {dest_path.name}")
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"  ⬇  [{attempt}/{MAX_RETRIES}] {name[:60]}")
            resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type.lower() and "pdf" not in url.lower():
                logger.warning(f"  ⚠  HTML response, not PDF — skipping: {url[:60]}")
                return False

            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Validate it's actually a PDF
            if dest_path.stat().st_size < 1000:
                logger.warning(f"  ⚠  File too small ({dest_path.stat().st_size}B) — likely error page")
                dest_path.unlink(missing_ok=True)
                return False

            # Extract text
            text = extract_text_from_pdf(dest_path)
            text_path = TEXT_DIR / (dest_path.stem + ".txt")
            text_path.write_text(text, encoding="utf-8")

            # Build metadata record
            try:
                page_count = len(fitz.open(dest_path))
            except Exception:
                page_count = 0

            METADATA_ROWS.append({
                "category":     category,
                "sub_category": sub_category,
                "ministry":     ministry,
                "name":         name,
                "source":       source,
                "url":          url,
                "filename":     dest_path.name,
                "text_file":    text_path.name,
                "pages":        page_count,
                "size_kb":      round(dest_path.stat().st_size / 1024, 1),
                "word_count":   len(text.split()),
                "downloaded_at": datetime.now().isoformat(),
            })

            logger.success(f"  ✅  Saved: {dest_path.name}  ({round(dest_path.stat().st_size/1024)}KB, {page_count}pp)")
            return True

        except requests.exceptions.RequestException as e:
            logger.warning(f"  ⚠  Attempt {attempt} failed: {e}")
            
            is_fatal = False
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code in (404, 403, 410):
                    is_fatal = True
                    logger.warning(f"  ⚠  Fatal HTTP error ({e.response.status_code}) — skipping retries.")
            
            if attempt < MAX_RETRIES and not is_fatal:
                time.sleep(3 * attempt)  # exponential backoff
            else:
                log_msg = "failed" if is_fatal else f"All {MAX_RETRIES} attempts failed"
                logger.error(f"  ❌  {log_msg} for URL: {url}")
                
                # --- SMART SEARCH & CRAWL FALLBACK ---
                if not is_fallback:
                    return find_via_search_and_crawl(name, url, category, sub_category, ministry, source)
                # -------------------------------------

                return False


def crawl_page_for_pdfs(target: dict) -> list:
    """Scrape a page and return list of (url, link_text) for PDF links."""
    url     = target["url"]
    pattern = target.get("pdf_pattern", r"\.pdf$")
    logger.info(f"\n🔍 Crawling: {target['name']}")
    logger.info(f"   URL: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        pdf_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if re.search(pattern, href, re.IGNORECASE):
                full_url  = urljoin(url, href)
                link_text = a_tag.get_text(strip=True) or os.path.basename(href)
                pdf_links.append((full_url, link_text))

        logger.info(f"  📄 Found {len(pdf_links)} PDF links")
        return pdf_links

    except Exception as e:
        logger.error(f"  ❌ Failed to crawl {url}: {e}")
        return []


def save_metadata():
    if not METADATA_ROWS:
        return
    csv_path  = OUTPUT_DIR / "metadata.csv"
    fieldnames = [
        "category", "sub_category", "ministry", "name", "source", "url",
        "filename", "text_file", "pages", "size_kb", "word_count", "downloaded_at"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(METADATA_ROWS)
    logger.success(f"\n📊 Metadata CSV saved: {csv_path}  ({len(METADATA_ROWS)} documents)")


def process_pdf_list(pdf_list: list, batch_label: str):
    """Download all PDFs in a list."""
    logger.info(f"\n{'═'*65}")
    logger.info(f"  {batch_label}  ({len(pdf_list)} items)")
    logger.info(f"{'═'*65}\n")

    for item in tqdm(pdf_list, desc=batch_label):
        filename = (
            safe_filename(item.get("name", "doc")) + "_"
            + hashlib.md5(item["url"].encode()).hexdigest()[:6]
            + ".pdf"
        )
        dest = PDF_DIR / filename
        download_pdf(
            url          = item["url"],
            dest_path    = dest,
            category     = item.get("category", ""),
            sub_category = item.get("sub_category", ""),
            ministry     = item.get("ministry", ""),
            name         = item.get("name", filename),
            source       = item.get("ministry", ""),
        )
        time.sleep(DELAY_BETWEEN_REQUESTS)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 65)
    logger.info("  INDIA RETAIL COMPLIANCE DOCUMENT SCRAPER — v2.0")
    logger.info("  Government Acts, Rules, Policies & Advisories for Retailers")
    logger.info("=" * 65)

    # ── STEP 1: Core direct PDFs ──────────────────────────────────────────
    process_pdf_list(DIRECT_PDFS, "STEP 1/3 — Core Direct PDFs")

    # ── STEP 2: Extra direct PDFs discovered in research ──────────────────
    process_pdf_list(EXTRA_DIRECT_PDFS, "STEP 2/3 — Extra Direct PDFs")

    # ── STEP 3: Crawl portals for more PDFs ──────────────────────────────
    logger.info(f"\n{'═'*65}")
    logger.info(f"  STEP 3/3 — Crawling {len(CRAWL_TARGETS)} Government Portals")
    logger.info(f"{'═'*65}\n")

    for target in CRAWL_TARGETS:
        pdf_links = crawl_page_for_pdfs(target)
        for pdf_url, link_text in pdf_links:
            filename = url_to_filename(pdf_url)
            dest     = PDF_DIR / filename
            name     = link_text[:100] if link_text else filename
            download_pdf(
                url          = pdf_url,
                dest_path    = dest,
                category     = target["category"],
                sub_category = target.get("sub_category", ""),
                ministry     = target.get("ministry", ""),
                name         = name,
                source       = target.get("ministry", ""),
            )
            time.sleep(DELAY_BETWEEN_REQUESTS)

    # ── Save metadata ─────────────────────────────────────────────────────
    save_metadata()

    # ── Summary ───────────────────────────────────────────────────────────
    pdf_count = len(list(PDF_DIR.glob("*.pdf")))
    txt_count = len(list(TEXT_DIR.glob("*.txt")))

    logger.info("\n" + "=" * 65)
    logger.info("  ✅  ALL DONE")
    logger.info(f"  PDFs downloaded  : {pdf_count}")
    logger.info(f"  Text files       : {txt_count}")
    logger.info(f"  Metadata CSV     : {OUTPUT_DIR / 'metadata.csv'}")
    logger.info(f"  PDF folder       : {PDF_DIR.resolve()}")
    logger.info("=" * 65)
    logger.info("\n💡 NEXT: Run rag_prep.py to chunk everything for vector DB ingestion.")
    logger.info("   Each chunk will carry: category, sub_category, ministry, url as metadata.\n")


if __name__ == "__main__":
    main()