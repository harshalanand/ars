"""Generate ARS Listing Process SOP as a DOCX document."""
from docx import Document
from docx.shared import Inches, Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.section import WD_ORIENT
from docx.oxml.ns import qn, nsdecls
from docx.oxml import parse_xml
import os

doc = Document()

# ── Page setup (A4 landscape for wide tables) ──
for section in doc.sections:
    section.orientation = WD_ORIENT.PORTRAIT
    section.page_width = Cm(21)
    section.page_height = Cm(29.7)
    section.top_margin = Cm(1.5)
    section.bottom_margin = Cm(1.5)
    section.left_margin = Cm(1.8)
    section.right_margin = Cm(1.8)

# ── Styles ──
style = doc.styles["Normal"]
font = style.font
font.name = "Calibri"
font.size = Pt(10)
style.paragraph_format.space_after = Pt(4)
style.paragraph_format.space_before = Pt(2)

for level in range(1, 4):
    hs = doc.styles[f"Heading {level}"]
    hs.font.color.rgb = RGBColor(0x1B, 0x3A, 0x5C)
    hs.font.name = "Calibri"

def add_table(doc, headers, rows, col_widths=None):
    """Add a formatted table."""
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.LEFT
    # Header row
    hdr = table.rows[0]
    for i, h in enumerate(headers):
        cell = hdr.cells[i]
        cell.text = h
        for p in cell.paragraphs:
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            for r in p.runs:
                r.bold = True
                r.font.size = Pt(9)
                r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="1B3A5C"/>')
        cell._tc.get_or_add_tcPr().append(shading)
    # Data rows
    for ri, row in enumerate(rows):
        for ci, val in enumerate(row):
            cell = table.rows[ri + 1].cells[ci]
            cell.text = str(val)
            for p in cell.paragraphs:
                for r in p.runs:
                    r.font.size = Pt(9)
            if ri % 2 == 1:
                shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="EDF2F7"/>')
                cell._tc.get_or_add_tcPr().append(shading)
    if col_widths:
        for i, w in enumerate(col_widths):
            for row in table.rows:
                row.cells[i].width = Cm(w)
    return table

def add_code_block(doc, text):
    """Add a code block with monospace font and grey background."""
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.left_indent = Cm(0.5)
    run = p.add_run(text)
    run.font.name = "Consolas"
    run.font.size = Pt(8.5)
    run.font.color.rgb = RGBColor(0x1A, 0x1A, 0x2E)
    # Background shading on paragraph
    pPr = p._p.get_or_add_pPr()
    shading = parse_xml(f'<w:shd {nsdecls("w")} w:val="clear" w:fill="F0F0F0"/>')
    pPr.append(shading)

def bold_text(paragraph, text):
    run = paragraph.add_run(text)
    run.bold = True
    return run

def add_bullet(doc, text, level=0):
    p = doc.add_paragraph(text, style="List Bullet")
    p.paragraph_format.left_indent = Cm(1.2 + level * 0.8)
    for r in p.runs:
        r.font.size = Pt(10)
    return p


# ═══════════════════════════════════════════════════════════════════
# TITLE PAGE
# ═══════════════════════════════════════════════════════════════════
doc.add_paragraph()
doc.add_paragraph()
title = doc.add_paragraph()
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = title.add_run("ARS Listing Process")
run.font.size = Pt(28)
run.font.color.rgb = RGBColor(0x1B, 0x3A, 0x5C)
run.bold = True

subtitle = doc.add_paragraph()
subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = subtitle.add_run("Step-by-Step SOP & Technical Review")
run.font.size = Pt(16)
run.font.color.rgb = RGBColor(0x4A, 0x6E, 0x8C)

doc.add_paragraph()
meta = doc.add_paragraph()
meta.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = meta.add_run("V2 Retail Auto Replenishment System\n"
                    "Version 2.0 — April 2026\n\n"
                    "Owner: Akash Agarwal, Director V2 Retail\n"
                    "Repository: github.com/harshalanand/ars")
run.font.size = Pt(11)
run.font.color.rgb = RGBColor(0x55, 0x55, 0x55)

doc.add_page_break()


# ═══════════════════════════════════════════════════════════════════
# TABLE OF CONTENTS (manual)
# ═══════════════════════════════════════════════════════════════════
doc.add_heading("Table of Contents", level=1)
toc_items = [
    "1. Overview",
    "2. Output Tables",
    "3. Phase 0: Configuration & Settings",
    "4. Phase 1: Pre-Grid Calculations (Full Pipeline)",
    "   4.1 Track 1: MAJ_CAT Level (ARS_CALC_ST_MAJ_CAT)",
    "   4.2 Track M: Master Sale (MASTER_GEN_ART_SALE)",
    "   4.3 Track 2: Article Level (ARS_CALC_ST_ART)",
    "   4.4 Grid Execution",
    "5. Phase 2: Listing Generation (Core)",
    "   5.1 RDC Mode Logic",
    "   5.2 Part 1: Grid Data (Existing Stock)",
    "   5.3 Part 2: MSA Missing Options",
    "   5.4 Part 2.5: Index Creation",
    "   5.5 Part 3.5: Data Enrichment",
    "   5.6 Part 3.55: MSA Quantities + Variant Counts",
    "   5.7 Part 3.6: OPT_TYPE Classification (4-Way Tagging)",
    "   5.8 Part 3.7: MIX Aggregation",
    "   5.9 Part 4: Grid Joins",
    "   5.10 Part 4b: PER_OPT_SALE",
    "   5.11 Part 4c: OPT_MBQ + OPT_REQ",
    "   5.12 Part 4d: ART_EXCESS",
    "   5.13 Part 4e: Per-Grid REQ with Excess Deduction",
    "6. Phase 3: Store Ranking",
    "7. Phase 4: Working Table (ARS_LISTING_WORKING)",
    "   7.1 Filtered Copy",
    "   7.2 Grid Hierarchy Columns",
    "   7.3 ALLOC_FLAG",
    "8. Phase 5: Allocation Table (ARS_ALLOC_WORKING)",
    "   8.1 Join Working x Variant Articles",
    "   8.2 Variant-Level Stock",
    "   8.3 Size Contribution (CONT)",
    "   8.4 Waterfall Allocation (ALLOC_QTY)",
    "   8.5 Reflect Back to Working Table",
    "9. Phase 6: Output & API Endpoints",
    "10. Data Flow Diagram",
    "11. Review Notes & Observations",
]
for item in toc_items:
    p = doc.add_paragraph(item)
    p.paragraph_format.space_after = Pt(1)
    for r in p.runs:
        r.font.size = Pt(10)

doc.add_page_break()


# ═══════════════════════════════════════════════════════════════════
# 1. OVERVIEW
# ═══════════════════════════════════════════════════════════════════
doc.add_heading("1. Overview", level=1)
doc.add_paragraph(
    "The Listing module is the core data preparation engine of the ARS (Auto Replenishment System). "
    "It combines two primary inputs — existing store stock (from Grid tables) and MSA recommendations "
    "(what needs to be replenished) — and produces a unified master listing with calculated quantities, "
    "store rankings, and size-level allocation."
)
doc.add_paragraph(
    "The process runs as a single API call (POST /listing/generate) and executes 15+ sequential steps "
    "within a single database connection. When run in 'full' mode, it additionally triggers pre-grid "
    "calculations and parallel grid execution before the listing pipeline."
)
p = doc.add_paragraph()
bold_text(p, "Entry Point: ")
p.add_run("POST /listing/generate")
p = doc.add_paragraph()
bold_text(p, "Source File: ")
p.add_run("backend/app/api/v1/endpoints/listing.py (~2,100 lines)")
p = doc.add_paragraph()
bold_text(p, "Pre-Grid Calculations: ")
p.add_run("backend/app/services/grid_calculations.py (~1,115 lines)")


# ═══════════════════════════════════════════════════════════════════
# 2. OUTPUT TABLES
# ═══════════════════════════════════════════════════════════════════
doc.add_heading("2. Output Tables", level=1)
doc.add_paragraph(
    "OPT (Option) = MAJ_CAT × GEN_ART_NUMBER × CLR. "
    "This is the fundamental unit throughout the listing process."
)
doc.add_paragraph("The listing process creates four output tables:")
add_table(doc,
    ["Table Name", "Grain (Row = 1 per...)", "Purpose"],
    [
        ["ARS_LISTING", "Store × OPT", "Master listing — all options (existing + new)"],
        ["ARS_STORE_RANKING", "Store × MAJ_CAT", "Store prioritization per category"],
        ["ARS_LISTING_WORKING", "Store × OPT (filtered)", "Clean extract for allocation (eligible options only)"],
        ["ARS_ALLOC_WORKING", "Store × OPT × VAR_ART × SZ", "Size-level allocation quantities (waterfall)"],
    ],
    col_widths=[4.5, 6, 7]
)
doc.add_paragraph(
    "OPT = MAJ_CAT × GEN_ART_NUMBER × CLR"
)


# ═══════════════════════════════════════════════════════════════════
# 3. PHASE 0: CONFIGURATION
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("3. Phase 0: Configuration & Settings", level=1)
doc.add_paragraph(
    "Before generation, the UI calls GET /listing/config to load available options and saved settings."
)
doc.add_heading("What /config Returns", level=2)
add_bullet(doc, "RDCs — from Master_ALC_INPUT_ST_MASTER (column: RDC / WAREHOUSE / HUB / WH_CD)")
add_bullet(doc, "Active Stores — filtered by LISTING column (excludes stores marked 0 or N)")
add_bullet(doc, "MAJ_CATs — distinct values from ARS_MSA_GEN_ART")
add_bullet(doc, "Store-RDC Map — for auto-detecting which RDC a store belongs to")
add_bullet(doc, "Saved Settings — from AppSettings table (prefix listing.*)")

doc.add_heading("Configurable Variables", level=2)
add_table(doc,
    ["Variable", "Default", "Purpose"],
    [
        ["stock_threshold_pct", "0.6 (60%)", "RL threshold: STK >= X% of DPN = adequate stock"],
        ["excess_multiplier", "2.0", "Excess: STK > X × OPT_MBQ = excess stock"],
        ["hold_days", "0", "Extra days added to SAL_D for new options (IS_NEW=1) only"],
        ["age_threshold", "15", "OPTs with effective AGE < X use PER_OPT_SALE (IS_NEW=1 always 0)"],
        ["mix_mode", "st_maj_rng", "MIX aggregation: always at Store × MAJ_CAT (each = no agg)"],
        ["rdc_mode", "all", "RDC filter: all / own / cross"],
        ["req_weight", "0.4", "Store ranking: weight for requirement rank"],
        ["fill_weight", "0.6", "Store ranking: weight for fill rate rank"],
    ],
    col_widths=[4, 2.5, 11]
)
doc.add_paragraph(
    "Settings are auto-saved to the AppSettings table on each generation and loaded on the next /config call."
)


# ═══════════════════════════════════════════════════════════════════
# 4. PHASE 1: PRE-GRID CALCULATIONS
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("4. Phase 1: Pre-Grid Calculations (Full Pipeline)", level=1)
doc.add_paragraph(
    'Triggered when run_mode = "full". The system runs pre-grid calculations and all active grids '
    "before proceeding to listing generation. This is optional — if run_mode = 'listing', this phase "
    "is skipped and existing calc/grid tables are reused."
)
doc.add_paragraph(
    "The pre-grid calculation runs a two-track pipeline via calculate_per_day_sale() in grid_calculations.py."
)

# Track 1: MAJ_CAT
doc.add_heading("4.1 Track 1: MAJ_CAT Level (ARS_CALC_ST_MAJ_CAT)", level=2)
doc.add_paragraph(
    "Creates and populates the store × MAJ_CAT calculation table using a CO-base → ST-overlay cascade."
)

add_table(doc,
    ["Step", "Function", "What It Does"],
    [
        ["1", "_step_create_calc", "CO_MAJ_CAT × all stores → base table. Fallback: copy ST_MAJ_CAT directly."],
        ["1b", "_step_fill_co_gaps", "Insert missing (store × MAJ_CAT) combos from CO × ST_MASTER cross-join."],
        ["2", "_step_overlay_st_values", "ST_MAJ_CAT overrides CO values per-store. Non-null/non-blank ST wins."],
        ["3", "_step_defaults", "LISTING blank/Y/null → 1; I_ROD null/0 → 1; Growth rates null/0 → 1; MANUAL_MBQ ≤0 → 0."],
        ["4", "_step_sal_d", "SAL_D = INT_DAYS + PRD_DAYS + SL_CVR. Priority: ST_MAJ > CO_MAJ > ST_MASTER."],
        ["5", "_step_sal_pd", "SAL_PD = per-day sale using CM/NM sale quantities and remaining days."],
    ],
    col_widths=[1.2, 4, 12]
)

doc.add_heading("SAL_D Priority Cascade", level=3)
add_table(doc,
    ["Priority", "Source", "Rule"],
    [
        ["1 (Highest)", "ST_MAJ_CAT own SL_CVR", "If the store has its own SL_CVR for that MAJ_CAT → use it"],
        ["2", "CO_MAJ_CAT SL_CVR", "If company-level SL_CVR exists for the MAJ_CAT → override"],
        ["3 (Lowest)", "ST_MASTER", "Base: INT_DAYS + PRD_DAYS + SL_CVR from store master"],
    ],
    col_widths=[2.5, 4.5, 10.5]
)

doc.add_heading("SAL_PD Formula", level=3)
add_code_block(doc,
    "IF CM_REM_D = 0                    → 0\n"
    "IF CM_REM_D >= SAL_D               → CM_SAL_Q / CM_REM_D\n"
    "IF NM_REM_D = 0                    → CM_SAL_Q / CM_REM_D\n"
    "ELSE → (CM_SAL_Q + (NM_SAL_Q / NM_REM_D) × (SAL_D - CM_REM_D)) / SAL_D"
)
doc.add_paragraph(
    "Where: CM_SAL_Q = Current Month Sale Qty, CM_REM_D = Current Month Remaining Days, "
    "NM_SAL_Q = Next Month Sale Qty, NM_REM_D = Next Month Remaining Days."
)

# Track M
doc.add_heading("4.2 Track M: Master Sale (MASTER_GEN_ART_SALE)", level=2)
add_table(doc,
    ["Step", "Function", "What It Does"],
    [
        ["M1", "_step_ensure_sale_maj_cat", "Add MAJ_CAT column to MASTER_GEN_ART_SALE from vw_master_product (if missing)."],
        ["M2", "_step_master_sale_sal_pd", "Compute SAL_PD in-place on ~21 lakh rows. Same formula as MAJ_CAT. REM_D/SAL_D sourced from ARS_CALC_ST_MAJ_CAT."],
    ],
    col_widths=[1.2, 5.5, 11]
)
doc.add_paragraph(
    "This gives the listing full option-level sales coverage (AUTO_GEN_ART_SALE) at store × article × color grain."
)

# Track 2: Article
doc.add_heading("4.3 Track 2: Article Level (ARS_CALC_ST_ART)", level=2)
doc.add_paragraph("Mirrors the MAJ_CAT flow exactly, but at article grain:")
add_table(doc,
    ["Step", "Function", "What It Does"],
    [
        ["A1", "_step_create_calc_art", "CO_ART × stores → base table. Fallback: copy ST_ART. Drops CORE, AUTO, HH_ART."],
        ["A1b", "_step_fill_co_art_gaps", "Fill missing (store × article) combos from CO_ART × ST_MASTER."],
        ["A2", "_step_overlay_st_art", "ST_ART overrides CO values. Same cascade logic as MAJ_CAT."],
        ["A3", "_step_art_defaults", "Same defaults as MAJ_CAT + FOCUS_W_CAP/FOCUS_WO_CAP: Y → 1, else → 0."],
        ["A4", "_step_art_sal_d", "SAL_D pulled from ARS_CALC_ST_MAJ_CAT (article has no own SL_CVR)."],
        ["A5", "_step_art_sal_pd", "SAL_PD from MASTER_GEN_ART_SALE (CM_SAL_Q) + ST_MAJ for REM_D values."],
    ],
    col_widths=[1.2, 4, 12]
)

# Grid Execution
doc.add_heading("4.4 Grid Execution", level=2)
add_bullet(doc, "Reads ARS_GRID_BUILDER where status = 'ACTIVE'")
add_bullet(doc, "Runs each grid in parallel (ThreadPoolExecutor, max_workers=4)")
add_bullet(doc, "Grids produce stock pivot tables like ARS_GRID_MJ_GEN_ART, ARS_GRID_MJ_VAR_ART")
add_bullet(doc, "Each grid pivots SLOC-level stock into store × hierarchy × article rows")


# ═══════════════════════════════════════════════════════════════════
# 5. PHASE 2: LISTING GENERATION
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("5. Phase 2: Listing Generation (Core Pipeline)", level=1)
doc.add_paragraph(
    "This is the main pipeline that builds the ARS_LISTING table. All steps run within a single "
    "database connection. The table is dropped and recreated on each run."
)

# RDC Mode
doc.add_heading("5.1 RDC Mode Logic", level=2)
add_table(doc,
    ["Mode", "Stores Included", "MSA Options Included"],
    [
        ["All", "All active stores (LISTING ≠ 0/N)", "All MSA options, no RDC filter"],
        ["Own RDC", "Stores tagged to selected RDC(s) only", "MSA options from same RDC(s) only + RDC join enforced"],
        ["Cross RDC", "Stores from 'cross_to' RDC(s)", "MSA options from 'cross_from' RDC(s)"],
    ],
    col_widths=[2.5, 6.5, 8.5]
)

# Part 1
doc.add_heading("5.2 Part 1: Grid Data (Existing Stock) → IS_NEW = 0", level=2)
doc.add_paragraph(
    "Copies all existing store stock from ARS_GRID_MJ_GEN_ART into ARS_LISTING. "
    "These are articles the store already has in stock."
)
add_code_block(doc,
    "INSERT INTO ARS_LISTING (...)\n"
    "SELECT\n"
    "    G.WERKS, S.RDC, G.MAJ_CAT, G.GEN_ART_NUMBER, G.CLR,\n"
    "    <all SLOC stock columns>,\n"
    "    SUM(all SLOC) AS STK_TTL,\n"
    "    0 AS IS_NEW,              -- existing stock\n"
    "    NULL AS OPT_TYPE\n"
    "FROM ARS_GRID_MJ_GEN_ART G\n"
    "INNER JOIN (stores) S ON G.WERKS = S.ST_CD"
)
add_bullet(doc, "IS_NEW = 0 marks these as existing store stock")
add_bullet(doc, "STK_TTL = sum of all individual SLOC stock columns")

# Part 2
doc.add_heading("5.3 Part 2: MSA Missing Options → IS_NEW = 1", level=2)
doc.add_paragraph(
    "Adds MSA-recommended articles that the store does NOT currently have. Stock = 0."
)
add_code_block(doc,
    "INSERT INTO ARS_LISTING (...)\n"
    "SELECT\n"
    "    S.ST_CD AS WERKS, S.RDC, M.MAJ_CAT, M.GEN_ART_NUMBER, M.CLR,\n"
    "    <all zeros for stock>,\n"
    "    0 AS STK_TTL,\n"
    "    1 AS IS_NEW,              -- new MSA recommendation\n"
    "    NULL AS OPT_TYPE\n"
    "FROM (MSA unique options) M\n"
    "CROSS JOIN (stores) S\n"
    "WHERE NOT EXISTS (already in listing)"
)
add_bullet(doc, "IS_NEW = 1 marks these as new recommendations from MSA")
add_bullet(doc, "NOT EXISTS prevents duplicates with grid data")
add_bullet(doc, "In 'own' RDC mode, MSA RDC must match store RDC")

# Part 2.5
doc.add_heading("5.4 Part 2.5: Index Creation", level=2)
add_bullet(doc, "Only if listing >= 5,000 rows (skip for small listings to avoid overhead)")
add_bullet(doc, "Creates NONCLUSTERED INDEX on (WERKS, MAJ_CAT) with INCLUDE (GEN_ART_NUMBER, CLR, STK_TTL)")
add_bullet(doc, "Creates NONCLUSTERED INDEX on (GEN_ART_NUMBER) with INCLUDE (WERKS, MAJ_CAT, CLR)")
add_bullet(doc, "Dramatically speeds up all subsequent UPDATE joins in Part 3-4")

# Part 3.5
doc.add_heading("5.5 Part 3.5: Data Enrichment", level=2)
doc.add_paragraph("Adds calculated columns from upstream tables via UPDATE...INNER JOIN:")
add_table(doc,
    ["Column Added", "Source Table", "Join Key", "Purpose"],
    [
        ["DPN", "ARS_CALC_ST_MAJ_CAT", "WERKS = ST_CD + MAJ_CAT", "Display Norm (target stock level)"],
        ["SAL_D", "ARS_CALC_ST_MAJ_CAT", "WERKS = ST_CD + MAJ_CAT", "Total Sale Days (coverage period)"],
        ["AUTO_GEN_ART_SALE", "MASTER_GEN_ART_SALE.SAL_PD", "WERKS + MAJ_CAT + GEN_ART + CLR", "Per-day sale rate at option level"],
        ["AGE", "MASTER_GEN_ART_AGE", "WERKS + MAJ_CAT + GEN_ART + CLR", "Option age in days (store-level)"],
    ],
    col_widths=[3.5, 4.5, 5, 4.5]
)

# Part 3.55
doc.add_heading("5.6 Part 3.55: MSA Quantities + Variant Counts", level=2)
add_table(doc,
    ["Column", "Source", "Logic"],
    [
        ["MSA_FNL_Q", "ARS_MSA_GEN_ART", "SUM(FNL_Q) grouped by MAJ_CAT + GEN_ART + CLR [+ RDC in own mode]"],
        ["VAR_COUNT", "ARS_MSA_VAR_ART", "COUNT(*) of variant articles per option"],
        ["VAR_FNL_COUNT", "ARS_MSA_VAR_ART", "COUNT of variants where FNL_Q > 0 (available colors/sizes)"],
    ],
    col_widths=[3, 4, 10.5]
)
doc.add_paragraph("These values are needed for OPT_TYPE classification in the next step.")

# Part 3.6
doc.add_page_break()
doc.add_heading("5.7 Part 3.6: OPT_TYPE Classification (4-Way Tagging)", level=2)
doc.add_paragraph(
    "Every row is classified into one of four types. Rules are evaluated top-to-bottom — first match wins."
)
doc.add_paragraph("First, GEN_ART_DESC is populated from vw_master_product (article description).")

add_table(doc,
    ["OPT_TYPE", "Rule", "Meaning"],
    [
        ["MIX (a)", "STK_TTL < 60% × DPN  AND  MSA_FNL_Q = 0",
         "Low stock + no MSA backup → OPT is not viable for replenishment"],
        ["MIX (b)", "VAR_FNL_COUNT / VAR_COUNT < 60%",
         "Poor color/size availability → not enough variants available (applies to ALL rows)"],
        ["RL", "STK_TTL >= 60% × DPN",
         "Adequate stock → Running Line (no replenishment needed)"],
        ["TBC", "0 < STK < 60% × DPN  AND  MSA_FNL_Q > 0",
         "Low stock but MSA available → To Be Checked (manual review)"],
        ["TBL", "STK_TTL <= 0  AND  MSA_FNL_Q > 0",
         "Zero stock + MSA available → To Be Listed (new listing)"],
    ],
    col_widths=[2, 6.5, 9]
)

doc.add_paragraph(
    "Note: MIX(b) applies to ALL rows (both IS_NEW=0 and IS_NEW=1). "
    "The 60% threshold is configurable via stock_threshold_pct. "
    "No separate VAR ratio override is needed — MIX(b) in the CASE statement "
    "catches all poor-color rows, and the RL rule naturally handles adequate-stock rows."
)

# Part 3.7
doc.add_heading("5.8 Part 3.7: MIX Aggregation", level=2)
doc.add_paragraph(
    "ALL MIX-tagged rows (both IS_NEW=0 and IS_NEW=1) are aggregated into exactly "
    "1 MIX row per Store × MAJ_CAT. This enforces the rule: max 1 MIX per store × MAJ_CAT."
)
doc.add_paragraph(
    'If mix_mode = "each", MIX rows are kept as individual lines (no aggregation). '
    "Otherwise MIX always groups at (WERKS, MAJ_CAT) level regardless of mix_mode setting."
)

doc.add_heading("Aggregation Logic", level=3)
add_bullet(doc, "ALL MIX rows are aggregated (both IS_NEW=0 and IS_NEW=1)")
add_bullet(doc, "Grouping: always WERKS + MAJ_CAT (max 1 MIX row per store × MAJ_CAT)")
add_bullet(doc, "Numeric columns → SUM (stock, quantities, etc.)")
add_bullet(doc, "DPN, SAL_D → Fetched fresh from ARS_CALC_ST_MAJ_CAT (NOT summed)")
add_bullet(doc, "GEN_ART_NUMBER → 0, CLR → 'MIX', GEN_ART_DESC → 'MIX', IS_NEW → 0")
add_bullet(doc, "Uses temp table #mix_agg → delete ALL original MIX rows → insert aggregated rows")
add_bullet(doc, "Post-aggregation verification: logs WARNING if any store × MAJ_CAT has > 1 MIX")

# Part 4
doc.add_heading("5.9 Part 4: Grid Joins (CONT, MBQ, OPT_CNT, DISP_Q)", level=2)
doc.add_paragraph(
    "For each active grid in ARS_GRID_BUILDER (non-pivot_only), joins grid output onto the listing "
    "to fetch stock, contribution, MBQ, option count, and display quantity at each hierarchy level."
)
doc.add_heading("Pre-resolve Step", level=3)
doc.add_paragraph(
    "Before grid joins, vw_master_product hierarchy columns (MACRO_MVGR, MICRO_MVGR, FAB, "
    "M_VND_CD, RNG_SEG, etc.) are resolved onto the listing ONCE. This avoids re-joining "
    "the ~5M-row MP view for each grid — a major performance optimization."
)
doc.add_heading("Grid Column Mapping", level=3)
doc.add_paragraph("Each grid adds prefixed columns to the listing:")
add_code_block(doc,
    "Grid MJ       → MJ_STK_TTL, MJ_CONT, MJ_MBQ, MJ_OPT_CNT, MJ_DISP_Q\n"
    "Grid MJ_CLR   → CLR_STK_TTL, CLR_CONT, CLR_MBQ, CLR_OPT_CNT, CLR_DISP_Q\n"
    "Grid RNG_SEG  → RNG_SEG_STK_TTL, RNG_SEG_CONT, RNG_SEG_MBQ, ..."
)

# Part 4b
doc.add_heading("5.10 Part 4b: PER_OPT_SALE", level=2)
doc.add_paragraph("Calculated from the grid flagged use_for_opt_sale = 1:")
add_code_block(doc,
    "PER_OPT_SALE = ((OPT_MBQ - DISP_Q) / DISP_Q × DPN) / SAL_D\n\n"
    "Where:\n"
    "  OPT_MBQ = grid-level MBQ for that option\n"
    "  DISP_Q  = grid-level display quantity (pre-computed as DISP_Q × CONT)"
)

# Part 4c
doc.add_heading("5.11 Part 4c: OPT_MBQ + OPT_REQ (Core Formulas)", level=2)

doc.add_heading("Effective AGE (before rate selection)", level=3)
doc.add_paragraph(
    "AGE is adjusted before rate selection to handle edge cases:"
)
add_code_block(doc,
    "Effective AGE:\n"
    "  IS_NEW = 1 (any AGE)     → 0   (new MSA OPT always treated as new article)\n"
    "  IS_NEW = 0, AGE = NULL   → 0   (unknown = treat as new)\n"
    "  IS_NEW = 0, AGE = blank  → 0   (blank = treat as new)\n"
    "  IS_NEW = 0, AGE = 0      → 0   (zero = treat as new)\n"
    "  IS_NEW = 0, AGE = 25     → 25  (known existing article)"
)

doc.add_heading("Sale Rate Selection (based on effective AGE)", level=3)
add_table(doc,
    ["Condition", "Rate Used"],
    [
        ["Effective AGE < age_threshold (default 15)", "MAX(PER_OPT_SALE, L-7 Sale/7, AUTO_GEN_ART_SALE)"],
        ["Effective AGE >= age_threshold", "MAX(L-7 Sale/7, AUTO_GEN_ART_SALE)"],
    ],
    col_widths=[5.5, 12]
)
doc.add_paragraph(
    "IS_NEW=1 rows ALWAYS use the new-article rate (includes PER_OPT_SALE) because "
    "their effective AGE is forced to 0. AGE = NULL/blank/0 also uses the new-article rate."
)

doc.add_heading("Core Formulas", level=3)
add_code_block(doc,
    "OPT_MBQ       = DPN + rate × SAL_D\n"
    "OPT_REQ       = MAX(0, OPT_MBQ - STK_TTL)\n"
    "\n"
    "OPT_MBQ_WH    = DPN + rate × (SAL_D + HOLD_DAYS)\n"
    "                ↑ HOLD_DAYS only applies to IS_NEW=1 (new OPTs)\n"
    "                  For IS_NEW=0: OPT_MBQ_WH = OPT_MBQ\n"
    "\n"
    "OPT_REQ_WH    = MAX(0, OPT_MBQ_WH - STK_TTL)\n"
    "\n"
    "MAX_DAILY_SALE = MAX(L-7 Sale/7, AUTO_GEN_ART_SALE)"
)
doc.add_paragraph(
    "Where: DPN = Display Norm (target stock), SAL_D = Sale Days (coverage period), "
    "rate = daily sale rate selected based on effective AGE."
)

# Part 4d
doc.add_heading("5.12 Part 4d: ART_EXCESS", level=2)
add_code_block(doc,
    "ART_EXCESS = MAX(0, STK_TTL - excess_multiplier × OPT_MBQ)\n"
    "           = MAX(0, STK_TTL - 2.0 × OPT_MBQ)   [default]\n"
    "\n"
    "Skip MIX rows (excess = 0 for MIX).\n"
    "EXCESS_STK = ART_EXCESS (visible in output)."
)
doc.add_paragraph(
    "Articles with stock exceeding 2× their MBQ are marked as having excess. "
    "This excess is deducted at the grid level in Part 4e."
)

# Part 4e
doc.add_heading("5.13 Part 4e: Per-Grid REQ with Excess Deduction", level=2)
doc.add_paragraph("For each grid hierarchy, the REQ is recalculated accounting for aggregated excess:")
add_code_block(doc,
    "aggregated_excess = SUM(ART_EXCESS) GROUP BY grid hierarchy columns\n"
    "\n"
    "{PREFIX}_REQ = MAX(0, {PREFIX}_MBQ - ({PREFIX}_STK_TTL - aggregated_excess))\n"
    "\n"
    "Example: MJ_REQ = MAX(0, MJ_MBQ - (MJ_STK_TTL - agg_excess_by_WERKS_MAJ_CAT))"
)
doc.add_paragraph(
    "After calculation, the internal ART_EXCESS column is dropped (only needed for this aggregation)."
)


# ═══════════════════════════════════════════════════════════════════
# 6. PHASE 3: STORE RANKING
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("6. Phase 3: Store Ranking (ARS_STORE_RANKING)", level=1)
doc.add_paragraph(
    "Creates a ranking table that prioritizes stores within each MAJ_CAT. "
    "The ranking determines allocation order in Phase 5."
)
doc.add_heading("Calculation", level=2)
add_code_block(doc,
    "Per (MAJ_CAT, WERKS):\n"
    "  MJ_REQ    = MAX(MJ_REQ) for that store-category\n"
    "  FILL_RATE = MJ_STK_TTL / MJ_MBQ\n"
    "\n"
    "  REQ_RANK  = ROW_NUMBER() OVER (PARTITION BY MAJ_CAT ORDER BY MJ_REQ ASC)\n"
    "  FILL_RANK = ROW_NUMBER() OVER (PARTITION BY MAJ_CAT ORDER BY FILL_RATE DESC)\n"
    "\n"
    "  W_SCORE   = REQ_RANK × req_weight + FILL_RANK × fill_weight\n"
    "            = REQ_RANK × 0.4       + FILL_RANK × 0.6     [default]\n"
    "\n"
    "  ST_RANK   = ROW_NUMBER() OVER (PARTITION BY MAJ_CAT ORDER BY W_SCORE DESC)"
)

doc.add_heading("Interpretation", level=2)
add_bullet(doc, "Lower MJ_REQ (less unfulfilled demand) → better REQ_RANK (lower number)")
add_bullet(doc, "Higher FILL_RATE (better inventory efficiency) → better FILL_RANK (lower number)")
add_bullet(doc, "Combined W_SCORE balances both: stores with low demand gaps AND high fill rates rank highest")
add_bullet(doc, "ST_RANK = 1 is the highest priority store for that MAJ_CAT → gets stock first in allocation")
doc.add_paragraph(
    "ST_RANK is written back into ARS_LISTING for use in the working table and allocation."
)


# ═══════════════════════════════════════════════════════════════════
# 7. PHASE 4: WORKING TABLE
# ═══════════════════════════════════════════════════════════════════
doc.add_heading("7. Phase 4: Working Table (ARS_LISTING_WORKING)", level=1)

doc.add_heading("7.1 Filtered Copy", level=2)
doc.add_paragraph("A clean extract from ARS_LISTING with strict filters:")
add_code_block(doc,
    "SELECT <identity + calculated columns only>\n"
    "INTO ARS_LISTING_WORKING\n"
    "FROM ARS_LISTING\n"
    "WHERE MSA_FNL_Q > 0                              -- must have MSA recommendation\n"
    "  AND OPT_REQ_WH >= 1                            -- must have demand ≥ 1 unit\n"
    "  AND (VAR_FNL_COUNT / VAR_COUNT >= 0.6)         -- color availability ≥ 60%"
)
doc.add_paragraph("Columns DROPPED: All SLOC stock columns, all grid-prefix columns (MJ_CONT, RNG_SEG_MBQ, etc.)")
doc.add_paragraph(
    "Columns KEPT: WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, STK_TTL, IS_NEW, OPT_TYPE, "
    "DPN, SAL_D, OPT_MBQ, OPT_REQ, OPT_MBQ_WH, OPT_REQ_WH, ST_RANK, EXCESS_STK, "
    "MAX_DAILY_SALE, and all *_REQ columns."
)

doc.add_heading("7.2 Grid Hierarchy Columns", level=2)
doc.add_paragraph("From ARS_GRID_HIERARCHY table, additional columns are added:")
add_table(doc,
    ["Column Pattern", "Meaning"],
    [
        ["GH_{hierarchy}", "Raw hierarchy match (0/1) — does this option belong to that grid dimension?"],
        ["H_{hierarchy}", "Refined = GH × (REQ > 0) — option belongs to dimension AND has demand"],
        ["PRI_CT%", "Primary grid coverage = SUM(H_Primary) / SUM(GH_Primary) × 100"],
        ["SEC_CT%", "Secondary grid coverage = SUM(H_Secondary) / SUM(GH_Secondary) × 100"],
    ],
    col_widths=[3.5, 14]
)

doc.add_heading("7.3 ALLOC_FLAG", level=2)
add_code_block(doc,
    "ALLOC_FLAG = CASE WHEN PRI_CT% >= 100 THEN 1 ELSE 0 END\n"
    "\n"
    "1 = Eligible for size-level allocation (all primary grids have demand)\n"
    "0 = Not eligible (some primary grids have no demand for this option)"
)


# ═══════════════════════════════════════════════════════════════════
# 8. PHASE 5: ALLOCATION TABLE
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("8. Phase 5: Allocation Table (ARS_ALLOC_WORKING)", level=1)
doc.add_paragraph(
    "This is the final output — size-level allocation quantities per store, respecting shared warehouse stock."
)

doc.add_heading("8.1 Join Working × Variant Articles", level=2)
add_code_block(doc,
    "SELECT W.*, V.VAR_ART, V.SZ, V.FNL_Q, V.MRP, V.PAK_SZ, ...\n"
    "INTO ARS_ALLOC_WORKING\n"
    "FROM ARS_LISTING_WORKING W\n"
    "INNER JOIN ARS_MSA_VAR_ART V\n"
    "  ON W.MAJ_CAT = V.MAJ_CAT\n"
    "  AND W.GEN_ART_NUMBER = V.GEN_ART_NUMBER\n"
    "  AND W.CLR = V.CLR\n"
    "  AND W.RDC = V.RDC\n"
    "WHERE W.ALLOC_FLAG = 1           -- eligible options only\n"
    "  AND V.FNL_Q > 0                -- variant has warehouse stock"
)

doc.add_heading("8.2 Variant-Level Stock (STK_TTL)", level=2)
doc.add_paragraph(
    "STK_TTL is populated from ARS_GRID_MJ_VAR_ART (variant-article-level grid). "
    "This is fresh variant-level stock, NOT the option-level STK_TTL. "
    "Rows with no match → STK_TTL = 0."
)

doc.add_heading("8.3 Size Contribution (CONT)", level=2)
doc.add_paragraph("From Master_CONT_SZ with a two-level cascade:")
add_bullet(doc, "Step 1: Store-level CONT (join on WERKS + MAJ_CAT + SZ)")
add_bullet(doc, "Step 2: CO-level fallback (ST_CD = 'CO') for rows where store-level CONT is NULL")
add_code_block(doc,
    "SZ_MBQ = OPT_MBQ × CONT       -- size-level MBQ\n"
    "SZ_REQ = ROUND(MAX(0, SZ_MBQ - STK_TTL), 0)"
)

doc.add_heading("8.4 Waterfall Allocation (ALLOC_QTY) — Critical Algorithm", level=2)
doc.add_paragraph(
    "This is the most important calculation. FNL_Q is a SHARED pool per "
    "(RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ) — every store with that RDC competes "
    "for the same warehouse stock. Naive MIN(FNL_Q, SZ_REQ) would double-count."
)
doc.add_heading("Algorithm", level=3)
add_code_block(doc,
    "Pool = (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)\n"
    "Order: ST_RANK ASC (best-ranked store first)\n"
    "\n"
    "For each store in rank order:\n"
    "  prev_demand = SUM(SZ_REQ) of all preceding stores in the pool\n"
    "  remaining   = FNL_Q - prev_demand\n"
    "  ALLOC_QTY   = MIN(SZ_REQ, remaining)     -- floored at 0\n"
    "  FNL_Q_REM   = remaining - ALLOC_QTY"
)
doc.add_heading("Worked Example", level=3)
add_code_block(doc,
    "FNL_Q = 100 units for (RDC=HYD, MAJ_CAT=SHOES, VAR_ART=123, SZ=8)\n"
    "\n"
    "Store A (ST_RANK=1, SZ_REQ=40): ALLOC=40,  remaining=60\n"
    "Store B (ST_RANK=2, SZ_REQ=50): ALLOC=50,  remaining=10\n"
    "Store C (ST_RANK=3, SZ_REQ=30): ALLOC=10,  remaining=0  ← partially served\n"
    "Store D (ST_RANK=4, SZ_REQ=20): ALLOC=0                 ← starved (pool exhausted)"
)
doc.add_paragraph(
    "Implementation uses SQL window functions (SUM...ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) "
    "to compute cumulative demand efficiently in a single pass."
)

doc.add_heading("8.5 Reflect Back to Working Table", level=2)
doc.add_paragraph(
    "Option-level ALLOC_QTY = SUM(size-level ALLOC_QTY) grouped by "
    "(WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR) is written back to ARS_LISTING_WORKING."
)


# ═══════════════════════════════════════════════════════════════════
# 9. PHASE 6: OUTPUT & ENDPOINTS
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("9. Phase 6: Output & API Endpoints", level=1)
add_table(doc,
    ["Endpoint", "Table", "Sort Order", "Purpose"],
    [
        ["GET /listing/config", "Multiple", "N/A", "Load RDCs, stores, MAJ_CATs, settings"],
        ["POST /listing/generate", "All 4 tables", "N/A", "Execute full listing pipeline"],
        ["POST /listing/settings", "AppSettings", "N/A", "Save listing variables"],
        ["GET /listing/preview?table=listing", "ARS_LISTING", "WERKS, MAJ_CAT, GEN_ART, CLR", "Browse raw listing"],
        ["GET /listing/preview?table=working", "ARS_LISTING_WORKING", "ST_RANK ASC, OPT_TYPE, SEC_CT% DESC", "Browse working table"],
        ["GET /listing/preview?table=alloc", "ARS_ALLOC_WORKING", "ST_RANK ASC, MAJ_CAT, SZ", "Browse allocation table"],
        ["GET /listing/store-ranking", "ARS_STORE_RANKING", "MAJ_CAT, ST_RANK DESC", "Browse store rankings"],
        ["GET /listing/summary", "ARS_LISTING", "N/A", "Stats: by RDC, MAJ_CAT, OPT_TYPE"],
        ["POST /listing/create-final", "ARS_LISTING_WORKING", "N/A", "Re-create working with custom filters"],
        ["GET /listing/final/preview", "ARS_LISTING_WORKING", "WERKS, MAJ_CAT, GEN_ART, CLR", "Browse final table"],
        ["GET /listing/alloc-preview", "ARS_ALLOC_WORKING", "ST_RANK ASC, MAJ_CAT, SZ", "Browse alloc table"],
    ],
    col_widths=[4.5, 3.5, 4.5, 5]
)


# ═══════════════════════════════════════════════════════════════════
# 10. DATA FLOW DIAGRAM
# ═══════════════════════════════════════════════════════════════════
doc.add_heading("10. Data Flow Diagram", level=1)
add_code_block(doc,
    "  Master Tables                    MSA Output              Grid Output\n"
    "  ┌──────────────┐              ┌──────────────┐        ┌──────────────────┐\n"
    "  │ ST_MASTER    │              │ ARS_MSA_     │        │ ARS_GRID_MJ_     │\n"
    "  │ CO_MAJ_CAT   │              │ GEN_ART      │        │ GEN_ART          │\n"
    "  │ ST_MAJ_CAT   │              │ VAR_ART      │        │ VAR_ART          │\n"
    "  └──────┬───────┘              └──────┬───────┘        └────────┬─────────┘\n"
    "         │                             │                         │\n"
    "    ┌────▼────────┐                    │                         │\n"
    "    │ Pre-Grid    │                    │                         │\n"
    "    │ Calculations│                    │                         │\n"
    "    │ (Phase 1)   │                    │                         │\n"
    "    └────┬────────┘                    │                         │\n"
    "         │                             │                         │\n"
    "    ┌────▼────────┐               ┌────▼─────────────────────────▼──┐\n"
    "    │ ARS_CALC_   │               │         ARS_LISTING              │\n"
    "    │ ST_MAJ_CAT  │──DPN,SAL_D──> │  Part 1: Grid stock (IS_NEW=0)  │\n"
    "    │ ST_ART      │               │  Part 2: MSA new    (IS_NEW=1)  │\n"
    "    └─────────────┘               │  Part 3: Enrich + Tag + MIX     │\n"
    "                                  │  Part 4: Grid joins + OPT_MBQ   │\n"
    "                                  └──────────────┬──────────────────┘\n"
    "                                                 │\n"
    "                          ┌──────────────────────┼───────────────────┐\n"
    "                          │                      │                   │\n"
    "                  ┌───────▼───────┐    ┌─────────▼──────┐   ┌───────▼────────┐\n"
    "                  │ ARS_STORE_    │    │ ARS_LISTING_   │   │ ARS_ALLOC_     │\n"
    "                  │ RANKING       │──> │ WORKING        │──>│ WORKING        │\n"
    "                  │ (Phase 3)     │    │ (Phase 4)      │   │ (Phase 5)      │\n"
    "                  └───────────────┘    └────────────────┘   └────────────────┘"
)


# ═══════════════════════════════════════════════════════════════════
# 11. REVIEW NOTES
# ═══════════════════════════════════════════════════════════════════
doc.add_page_break()
doc.add_heading("11. Review Notes & Observations", level=1)

doc.add_heading("11.1 SQL Injection Risk (Low)", level=2)
doc.add_paragraph(
    "Filter values from GenerateRequest (e.g., maj_cat_values, store_codes) are interpolated "
    "directly into SQL strings via f-strings at listing.py:348-357. The _build_filter_where helper "
    "uses parameterized queries, but the core generation logic builds IN (...) clauses via string "
    "concatenation. Since these come from authenticated API calls (not public input), the risk is low "
    "but worth noting for a future security hardening pass."
)

doc.add_heading("11.2 Waterfall Allocation — Correctly Implemented", level=2)
doc.add_paragraph(
    "The CTE-based window function approach at listing.py:1698-1736 properly handles shared pool "
    "consumption using ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING. This prevents "
    "double-counting and ensures stores are served in ST_RANK order."
)

doc.add_heading("11.3 Performance Optimization", level=2)
doc.add_paragraph(
    "The pre-resolve MP step (Part 4) that materializes vw_master_product columns onto the listing "
    "once is a significant optimization. Without it, each grid would re-join the ~5M-row MP view "
    "separately. Index creation is also gated (>= 5,000 rows) to avoid overhead on small listings."
)

doc.add_heading("11.4 MIX Rules", level=2)
doc.add_paragraph(
    "MIX(b) applies to ALL rows (both IS_NEW=0 and IS_NEW=1) — any OPT with "
    "VAR_FNL_COUNT / VAR_COUNT < 60% is tagged MIX. MIX aggregation always groups at "
    "(WERKS, MAJ_CAT) level, enforcing max 1 MIX row per store × MAJ_CAT. "
    "ALL MIX-tagged rows (IS_NEW=0 and IS_NEW=1) are aggregated together."
)

doc.add_heading("11.5 AGE Handling for Rate Selection", level=2)
doc.add_paragraph(
    "IS_NEW=1 rows always get effective AGE = 0 (new MSA OPTs are always 'new', "
    "even if master data carries a stale AGE value). AGE = NULL, blank, or 0 also "
    "uses the new-article rate (includes PER_OPT_SALE). Only IS_NEW=0 rows with "
    "AGE >= age_threshold use the default rate (excludes PER_OPT_SALE)."
)

doc.add_heading("11.6 HOLD_DAYS Only for IS_NEW=1", level=2)
doc.add_paragraph(
    "HOLD_DAYS is correctly applied only to new MSA-recommended OPTs (IS_NEW=1). "
    "Existing store OPTs get OPT_MBQ_WH = OPT_MBQ (no extra buffer). "
    "This is by design — new OPTs need transit time; existing stock is already in-store."
)

doc.add_heading("11.7 Error Resilience", level=2)
doc.add_paragraph(
    "Most steps are wrapped in try-except blocks. Partial failures are logged but don't block "
    "the pipeline. This means the listing may complete with some columns as NULL if upstream "
    "tables are missing. The comprehensive step timings logged at the end help diagnose issues."
)


# ═══════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ARS_Listing_Process_SOP.docx")
doc.save(output_path)
print(f"Document saved to: {output_path}")
