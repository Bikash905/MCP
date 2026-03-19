CREATE OR REPLACE PROCEDURE NAABO_DEV.RR_REPORTING_WRITEBACK.SP_GENERATE_ROPES_REPAIR()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','pandas','reportlab','Pillow')
HANDLER = 'run'
EXECUTE AS CALLER
AS '
import os
import re
from io import BytesIO
from datetime import datetime  # <-- used for report dates
import pandas as pd
from snowflake.snowpark import Session
from reportlab.platypus import (
  BaseDocTemplate, PageTemplate, Frame,
  Table, TableStyle, Paragraph, Spacer, PageBreak, NextPageTemplate
)
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.utils import ImageReader
from PIL import Image
from reportlab.lib.enums import TA_CENTER
from reportlab.pdfgen import canvas as _rl_canvas
from reportlab.lib.colors import Color


class NumberedCanvas(_rl_canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []
        # Style defaults
        self._page_fmt  = "Page {page} "  
        self._font_name = "Helvetica"
        self._font_size = 14
        self._color     = Color(0.3, 0.3, 0.3)

    def showPage(self):
        # First pass: just stash the page state; DO NOT finalize the page.
        self._saved_page_states.append(dict(self.__dict__))
        # Start a fresh (blank) page object without emitting the current one
        self._startPage()  # <-- this avoids duplicating the entire document

    def save(self):
        # Second pass: we now know how many pages there are
        total_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_page_number_text(total_pages)
            # Now actually emit this page
            _rl_canvas.Canvas.showPage(self)
        _rl_canvas.Canvas.save(self)

    def _draw_page_number_text(self, total_pages: int):
        show_it = getattr(self, "_show_page_number", True)
        if not show_it:
            return

        page_width, page_height = self._pagesize
        # Use margins if provided by onPage; provide safe fallbacks
        right_margin  = getattr(self, "_doc_right_margin", 36)  # ~0.5"
        bottom_margin = getattr(self, "_doc_bottom_margin", 30) # you set bottomMargin=30

        # Bottom-right, inside margins
        x = page_width  - right_margin - 2
        y = max(10, bottom_margin * 0.6)

        self.setFont(self._font_name, self._font_size)
        self.setFillColor(self._color)
        label = self._page_fmt.format(page=self._pageNumber, total=total_pages)
        self.drawRightString(x, y, label)

# ---------------------------------------------------------------------------------
# Stage config (logo source and PDF destination)
# ---------------------------------------------------------------------------------
STAGE = ''@RR_REPORTING.MY_PY_STAGE''
DEST=''@RR_REPORTING.ROPES_REPAIR/''
LOGO_STAGE_PATH = f"{STAGE}/otis_logo/otis_image.jpg"   # adjust if your path differs
SRC = f"{STAGE}/Ropes_Repair/"                         # target folder for PDFs

adls_name = ''qliksaaswhqprodadls''
sas_token = ''?sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2036-01-09T15:18:41Z&st=2026-01-09T07:03:41Z&spr=https&sig=1w%2ByNg%2FXu1WI1YB6tFpPV17EQE%2BnQaPd7vFBXf9lA60%3D''

def _get_logo_from_stage(session) -> ImageReader:
    """Download logo from internal stage to /tmp and return an ImageReader."""
    session.sql(f"REMOVE {SRC}").collect()
    session.sql(f"REMOVE {DEST}").collect()
    local_dir = ''/tmp/rr_assets''
    os.makedirs(local_dir, exist_ok=True)
    # Download the logo to /tmp
    session.file.get(LOGO_STAGE_PATH, local_dir)
    local_logo = os.path.join(local_dir, os.path.basename(LOGO_STAGE_PATH))
    return ImageReader(local_logo)

def _upload_pdf_to_stage(session, file_name: str, pdf_bytes: bytes):
    """Upload PDF bytes to @MY_PY_STAGE/Ropes_Repair/ as a real .pdf (no gzip)."""
    tmp_path = os.path.join(''/tmp'', file_name)
    with open(tmp_path, ''wb'') as f:
        f.write(pdf_bytes)
    session.file.put(tmp_path, SRC , overwrite=True, auto_compress=False)

    copy_sql = f"""
    COPY FILES
    INTO {DEST}
    FROM {SRC}
    DETAILED_OUTPUT = TRUE
    """
    copy_results = session.sql(copy_sql).collect()

    try:
        os.remove(tmp_path)
    except Exception:
        pass

def draw_report_start(canvas, doc):
    canvas.saveState()
    page_w, page_h = doc.pagesize
    canvas.setFont("Helvetica-Bold", 20)
    canvas.drawCentredString(page_w / 2.0, page_h / 2.0, "== REPORT START ==")
    canvas.restoreState()
    canvas._draw_page_number = True

def draw_report_end(canvas, doc):
    canvas.saveState()
    page_w, page_h = doc.pagesize
    canvas.setFont("Helvetica-Bold", 20)
    canvas.drawCentredString(page_w / 2.0, page_h / 2.0, "== REPORT END ==")
    canvas.restoreState()
    canvas._draw_page_number = True

def get_centered_style():
    """Centered paragraph style used for START/END pages."""
    styles = getSampleStyleSheet()
    return ParagraphStyle(
        name="CenteredText",
        parent=styles["Normal"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=20,
        leading=18,
        spaceBefore=0,
        spaceAfter=0,
    )

def build_center_page_elements(text, frame_height, style=None):
    """
    Returns flowables to place `text` roughly center of the frame.
    frame_height = page_h - topMargin - bottomMargin
    """
    if style is None:
        style = get_centered_style()
    # Approximate vertical centering:
    # push down by half of frame height minus half of text leading
    top_pad = max(0, (frame_height - style.leading) / 2.0)
    return [Spacer(1, top_pad), Paragraph(text, style)]

def ensure_page_break(elements):
    """Append a PageBreak only if the last element is not already a PageBreak."""
    if not elements or not isinstance(elements[-1], PageBreak):
        elements.append(PageBreak())

def send_email(session, rcpnt):
    # Configuration
    container_path = ''ropesrepair/report''  # Adjusted to match stage folder structure
    
    # 1. List files in the stage (adjust @stage_name to your actual stage)
    # Using PATTERN to filter for only .pdf files in the specific subfolder
    list_files_query = f"LIST {DEST} "
    files_df = session.sql(list_files_query).collect()
    
    # 2. Iterate through each file found
    for row in files_df:
        # Full URL from LIST: azure://qliksaaswhqprodadls.blob.core.windows.net/ropesrepair/report/file.pdf
        full_url = row[''name'']

        stage_url = f''azure://{adls_name}.blob.core.windows.net/''
        # 2. Extract relative path: ropesrepair/report/file.pdf
        # We replace the base stage URL with an empty string
        relative_path = full_url.replace(stage_url, "")
        
        # 3. Construct the email call
        file_nm = relative_path.split(''/'')[-1]
        msg_subject = f"Report: {file_nm}"
        msg_body = f"Please find the attached report for {file_nm}"
        
        send_mail_sql = """
            SELECT DATA_PLATFORM_DQ.EMAIL_FUNCTION.NAA_EXTERNAL_EMAIL_ALERT(
                ''{subj}'', ''{body}'', ''noreplyropesNrepair@otis.com'', ''{to}'', '''', '''', 
                ''{adls}||{path}||{sas}''
            )
        """.format(
            subj=msg_subject,
            body=msg_body,
            to=rcpnt,
            adls=adls_name,
            path=relative_path, # Now contains ''ropesrepair/report/...''
            sas=sas_token
        )
        
        session.sql(send_mail_sql).collect()

# ---------------------------------------------------------------------------------
# Global for header logo (populated in run())
# ---------------------------------------------------------------------------------
LOGO_READER = None

# ---------------------------------------------------------------------------------
# Header (preserved, now uses global LOGO_READER)
# ---------------------------------------------------------------------------------
def header(canvas, doc):
    canvas.saveState()
    page_width, page_height = landscape(A3)
    left_margin, right_margin, top_margin = doc.leftMargin, doc.rightMargin, doc.topMargin

    # Title
    canvas.setFont(TITLE_FONT, TITLE_SIZE)
    text_w = canvas.stringWidth(TITLE_TEXT, TITLE_FONT, TITLE_SIZE)
    title_x = (page_width - text_w) / 2.0
    title_y = page_height - top_margin + 40
    canvas.drawString(title_x, title_y, TITLE_TEXT)

    # Rule
    line_y = title_y - 26
    canvas.setStrokeColor(colors.black)
    canvas.setLineWidth(5)
    canvas.line(left_margin, line_y, page_width - right_margin, line_y)

    # Logo
    image_w, image_h = 60, 50
    logo_x = page_width - right_margin - image_w
    logo_y = page_height - top_margin + 20
    try:
        if LOGO_READER is not None:
            canvas.drawImage(LOGO_READER, logo_x, logo_y, width=image_w, height=image_h, mask=''auto'')
    except Exception as e:
        print("Logo error:", e)

    canvas.restoreState()
    canvas._draw_page_number = True

# ---------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------

# NBSP helper to preserve runs of spaces in Paragraph text
NBSP = "\\u00A0"

def preserve_inner_spaces(text: str) -> str:
    """
    Prevent ReportLab Paragraph from collapsing runs of spaces by converting
    runs of 2+ normal spaces to NBSPs. Single spaces remain normal.
    """
    if text is None:
        return ""
    s = str(text)
    return re.sub(r" {2,}", lambda m: NBSP * len(m.group(0)), s)

def dict_to_rows(d, use_wrap_for_value=False):
    rows = []
    for k, v in d.items():
        key_p = Paragraph(f"<b>{k}</b>", wrap_style)
        val_style = body_wrap_style if use_wrap_for_value else wrap_style
        # ✅ preserve inner spaces for values
        val_text = "" if v is None else preserve_inner_spaces(v)
        val_p = Paragraph(val_text, val_style)
        rows.append([key_p, val_p])
    return rows

# Panel builders
def make_panel(title, data_dict, panel_width, key_col_ratio=0.45, wrap_values=False):
    """Two-column panel with a blue header bar and inner grid rows."""
    key_w = panel_width * key_col_ratio
    val_w = panel_width * (1 - key_col_ratio)

    data = [[Paragraph(title, section_header_style), ""]]
    data.extend(dict_to_rows(data_dict, use_wrap_for_value=wrap_values))

    panel = Table(data, colWidths=[key_w, val_w])
    panel.setStyle(TableStyle([
        ("SPAN", (0, 0), (1, 0)),
        ("BACKGROUND", (0, 0), (1, 0), colors.HexColor("#081F4A")),
        ("TEXTCOLOR", (0, 0), (1, 0), colors.white),
        ("ALIGN", (0, 0), (1, 0), "CENTER"),
        ("TOPPADDING", (0, 0), (1, 0), 6),
        ("BOTTOMPADDING", (0, 0), (1, 0), 6),

        # Stronger inner grid + more padding
        ("INNERGRID", (0, 1), (1, -1), INNER_LINE, colors.black),
        ("VALIGN", (0, 1), (1, -1), "TOP"),

        ("LEFTPADDING", (0, 1), (0, -1), KEY_LEFT_PADDING),
        ("RIGHTPADDING", (0, 1), (0, -1), KEY_RIGHT_PADDING),
        ("LEFTPADDING", (1, 1), (1, -1), VAL_LEFT_PADDING),
        ("RIGHTPADDING", (1, 1), (1, -1), VAL_RIGHT_PADDING),
        ("TOPPADDING", (0, 1), (1, -1), BODY_TOP_PADDING),
        ("BOTTOMPADDING", (0, 1), (1, -1), BODY_BOTTOM_PADDING),

        ("BACKGROUND", (0, 1), (1, -1), colors.white),

        # Heavier outer box
        ("BOX", (0, 0), (1, -1), PANEL_BORDER, colors.black),
    ]))
    panel.keepTogether = True
    panel.splitByRow = 1
    return panel

def make_name_desc_list(rows, panel_width, name_col_ratio=0.30, boxed=True):
    name_w = panel_width * name_col_ratio
    desc_w = panel_width * (1 - name_col_ratio)

    data = [[Paragraph("NAME", section_header_style),
             Paragraph("DESCRIPTION", section_header_style)]]

    for name, desc in rows:
        data.append([
            Paragraph(str(name or ""), wrap_style),
            # ✅ preserve inner spaces in DESCRIPTION
            Paragraph("" if desc is None else preserve_inner_spaces(desc), body_wrap_style)
        ])

    panel = Table(data, colWidths=[name_w, desc_w])

    style_cmds = [
        # Header
        ("BACKGROUND", (0,0), (1,0), colors.HexColor("#081F4A")),
        ("TEXTCOLOR", (0,0), (1,0), colors.white),
        ("ALIGN", (0,0), (1,0), "CENTER"),
        ("TOPPADDING", (0,0), (1,0), 6),
        ("BOTTOMPADDING",(0,0),(1,0), 6),

        # Body rows — NO grid
        ("VALIGN", (0,1), (1,-1), "TOP"),
        ("LEFTPADDING", (0,1), (0,-1), KEY_LEFT_PADDING),
        ("RIGHTPADDING", (0,1), (0,-1), KEY_RIGHT_PADDING),
        ("LEFTPADDING", (1,1), (1,-1), VAL_LEFT_PADDING),
        ("RIGHTPADDING", (1,1), (1,-1), VAL_RIGHT_PADDING),
        ("TOPPADDING", (0,1), (1,-1), BODY_TOP_PADDING),
        ("BOTTOMPADDING",(0,1), (1,-1), BODY_BOTTOM_PADDING),

        ("BACKGROUND", (0,1), (1,-1), colors.white),
    ]
    if boxed:
        style_cmds.append(("BOX", (0,0), (1,-1), PANEL_BORDER, colors.black))

    panel.setStyle(TableStyle(style_cmds))
    panel.keepTogether = True
    panel.splitByRow = 1
    return panel

def make_events_list_4col(events_rows, panel_width,
                          col_ratios=(0.52, 0.16, 0.18, 0.14),
                          boxed=True):
    """Right panel with DESCRIPTION / DATE / TIME / TIME ZONE columns."""
    w_desc = panel_width * col_ratios[0]
    w_date = panel_width * col_ratios[1]
    w_time = panel_width * col_ratios[2]
    w_tz   = panel_width * col_ratios[3]

    data = [[Paragraph("DESCRIPTION", section_header_style),
             Paragraph("DATE", section_header_style),
             Paragraph("TIME", section_header_style),
             Paragraph("TIME ZONE", section_header_style)]]

    for row in events_rows or []:
        padded = list(row) + ["", "", "", ""]
        desc, date, time, tz = padded[:4]
        data.append([
            Paragraph(preserve_inner_spaces(desc or ""), body_wrap_style),
            Paragraph(preserve_inner_spaces(date or ""), wrap_style),
            Paragraph(preserve_inner_spaces(time or ""), wrap_style),
            Paragraph(preserve_inner_spaces(tz or ""), wrap_style),
        ])

    panel = Table(data, colWidths=[w_desc, w_date, w_time, w_tz])

    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#081F4A")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, 0), 6),
        ("BOTTOMPADDING",(0,0),(-1,0), 6),

        ("VALIGN", (0, 1), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 1), (-1, -1), VAL_LEFT_PADDING),
        ("RIGHTPADDING", (0, 1), (-1, -1), VAL_RIGHT_PADDING),
        ("TOPPADDING", (0, 1), (-1, -1), BODY_TOP_PADDING),
        ("BOTTOMPADDING",(0, 1), (-1, -1), BODY_BOTTOM_PADDING),

        ("BACKGROUND", (0, 1), (-1, -1), colors.white),
    ]
    if boxed:
        style_cmds += [
            ("INNERGRID", (0,1), (-1,-1), INNER_LINE, colors.black),
            ("BOX", (0,0), (-1,-1), PANEL_BORDER, colors.black),
        ]

    panel.setStyle(TableStyle(style_cmds))
    panel.keepTogether = True
    panel.splitByRow = 1
    return panel

# --- Navy "NAME / DESCRIPTION" table built from C-fields (left section)
def make_section_table(rows, panel_width):
    name_w = panel_width * 0.33  # slightly more space for DESCRIPTION
    desc_w = panel_width * 0.67
    header_row = [
        Paragraph("NAME", section_header_style),
        Paragraph("DESCRIPTION", section_header_style),
    ]
    data = [header_row]
    for field, value in rows or []:
        data.append([
            Paragraph(str(field), styles["Normal"]),
            # (Optional) Preserve inner spaces if needed here as well:
            Paragraph("" if value is None else preserve_inner_spaces(value), body_wrap_style),
        ])
    tbl = Table(data, colWidths=[name_w, desc_w])
    tbl.setStyle(TableStyle([
        # Header bar
        (''BACKGROUND'', (0,0), (-1,0), colors.HexColor("#081F4A")),
        (''TEXTCOLOR'', (0,0), (-1,0), colors.white),
        (''FONTNAME'', (0,0), (-1,0), ''Helvetica-Bold''),
        (''ALIGN'', (0,0), (-1,0), ''CENTER''),
        (''TOPPADDING'', (0,0), (-1,0), 6),
        (''BOTTOMPADDING'',(0,0),(-1,0), 6),

        # Body cell alignment & padding
        (''BACKGROUND'', (0,1), (-1,-1), colors.white),
        (''VALIGN'', (0,1), (-1,-1), ''TOP''),
        (''LEFTPADDING'', (0,1), (0,-1), KEY_LEFT_PADDING),
        (''RIGHTPADDING'', (0,1), (0,-1), KEY_RIGHT_PADDING),
        (''LEFTPADDING'', (1,1), (1,-1), VAL_LEFT_PADDING),
        (''RIGHTPADDING'', (1,1), (1,-1), VAL_RIGHT_PADDING),
        (''TOPPADDING'', (0,1), (-1,-1), BODY_TOP_PADDING),
        (''BOTTOMPADDING'',(0,1), (-1,-1), BODY_BOTTOM_PADDING),

        # Thicker inner grid and outer box
        (''INNERGRID'', (0,1), (-1,-1), INNER_LINE, colors.black),
        (''BOX'', (0,0), (-1,-1), PANEL_BORDER, colors.black),
    ]))
    tbl.keepTogether = True
    tbl.splitByRow = 1
    return tbl

# ---------------------------------------------------------------------------------
# Row builders (left \\ gap \\ right)
# ---------------------------------------------------------------------------------
def make_row(
    left_dict, right_dict, left_title, right_title, total_width,
    left_ratio=0.62, key_col_ratio_left=0.42, key_col_ratio_right=0.50,
    col_gap=None, row_gap=None
):
    # FIX: resolve defaults at call-time to avoid import-time NameError
    col_gap = COL_GAP if col_gap is None else col_gap
    row_gap = ROW_GAP if row_gap is None else row_gap

    left_w = total_width * left_ratio
    right_w = total_width * (1 - left_ratio)
    gap_w = col_gap

    left_w_adj  = max(10, left_w  - gap_w / 2.0)
    right_w_adj = max(10, right_w - gap_w / 2.0)

    left_panel  = make_panel(left_title,  left_dict,  left_w_adj,  key_col_ratio_left)
    right_panel = make_panel(right_title, right_dict, right_w_adj, key_col_ratio_right)

    row = Table([[left_panel, "", right_panel]],
                colWidths=[left_w_adj, gap_w, right_w_adj])

    row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("BACKGROUND", (1, 0), (1, 0), colors.white),  # gap column blank
    ]))
    row.keepTogether = True
    row.splitByRow = 1
    return [row, Spacer(1, row_gap)]

def make_row_custom(
    left_panel_fn, right_panel_fn, total_width,
    left_ratio=0.62, col_gap=None, row_gap=None,
    right_top_offset=16, right_shift_px=0
):
    # FIX: resolve defaults at call-time to avoid import-time NameError
    col_gap = COL_GAP if col_gap is None else col_gap
    row_gap = ROW_GAP if row_gap is None else row_gap

    left_w = total_width * left_ratio
    right_w = total_width * (1 - left_ratio)
    gap_w = col_gap + max(0, right_shift_px)

    left_w_adj  = max(10, left_w  - gap_w / 2.0)
    right_w_adj = max(10, right_w - gap_w / 2.0)

    left_panel  = left_panel_fn(left_w_adj)
    right_panel = right_panel_fn(right_w_adj)

    row = Table([[left_panel, "", right_panel]],
                colWidths=[left_w_adj, gap_w, right_w_adj])

    row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("BACKGROUND", (1, 0), (1, 0), colors.white),
        ("TOPPADDING", (2, 0), (2, 0), right_top_offset),
    ]))
    row.keepTogether = True
    row.splitByRow = 1
    return [row, Spacer(1, row_gap)]

# ---------------------------------------------------------------------------------
# Document setup
# ---------------------------------------------------------------------------------
page_w, page_h = landscape(A3)

# Styles
styles = getSampleStyleSheet()
wrap_style = styles["BodyText"]
wrap_style.fontName = "Helvetica"
wrap_style.fontSize = 12
wrap_style.leading = 11

# Dedicated style to ensure DESCRIPTION wraps cleanly
body_wrap_style = ParagraphStyle(
    name="BodyWrap",
    parent=wrap_style,
    wordWrap="LTR",
    allowOrphans=1,
    allowWidows=1,
    splitLongWords=1,
    leading=11,
    fontSize=12,
)

section_header_style = ParagraphStyle(
    name="SectionHeader",
    parent=styles["Normal"],
    fontName="Helvetica-Bold",
    fontSize=12,
    leading=14,
    alignment=1,
    textColor=colors.white,
    spaceBefore=0,
    spaceAfter=0,
)

# ---------------------------------------------------------------------------------
# Layout knobs
# ---------------------------------------------------------------------------------
TITLE_TEXT = "OTIS ELEVATOR COMPANY ON-LINE HISTORY REPORT"
TITLE_FONT = "Helvetica-Bold"
TITLE_SIZE = 30

INSET = 14         # small left/right inset for header rule
TOP_OFFSET = 42    # distance from physical top to title baseline
GAP_TO_RULE = 22   # gap from title to rule
CONTENT_GAP = 10   # gap from rule to content frame
RULE_WIDTH = 2.8
RULE_COLOR = colors.black

COL_GAP = 10       # center gap between left/right panels
ROW_GAP = 8        # vertical gap between consecutive rows

PANEL_BORDER = 1.5 # was 0.75
INNER_LINE  = 1.2  # inner grid line thickness

# Body cell paddings (move text away from left edge)
KEY_LEFT_PADDING = 15
KEY_RIGHT_PADDING = 8
VAL_LEFT_PADDING = 12
VAL_RIGHT_PADDING = 10
BODY_TOP_PADDING = 5
BODY_BOTTOM_PADDING= 4

# Now compute DOC_TOP_MARGIN with constants defined
DOC_TOP_MARGIN = TOP_OFFSET + GAP_TO_RULE + CONTENT_GAP

# ---------------------------------------------------------------------------------
# Utilities used during page building
# ---------------------------------------------------------------------------------
def make_gv(row_dict):
    def gv(col, default=""):
        val = row_dict.get(col, default)
        try:
            if pd.isna(val):
                return default
        except Exception:
            pass
        return str(val).strip()
    return gv

def get_date_performed_from_pairs(name_desc_rows):
    """Find DATE PERFORMED from NAME/DESCRIPTION pairs for procedure pages."""
    for r in name_desc_rows or []:
        if len(r) >= 2 and str(r[0]).strip().upper() == "DATE PERFORMED":
            val = str(r[1]).strip()
            try:
                return pd.to_datetime(val).strftime("%m/%d/%Y")
            except Exception:
                return val
    return ""

def make_section_list(rows, panel_width, name_col_ratio=0.33):
    name_w = panel_width * name_col_ratio
    desc_w = panel_width * (1 - name_col_ratio)

    data = [[Paragraph("NAME", section_header_style),
             Paragraph("DESCRIPTION", section_header_style)]]

    for field, value in (rows or []):
        data.append([
            Paragraph(str(field or ""), wrap_style),
            Paragraph("" if value is None else preserve_inner_spaces(value), body_wrap_style)
        ])

    panel = Table(data, colWidths=[name_w, desc_w])
    panel.setStyle(TableStyle([
        # Header styling
        ("BACKGROUND", (0, 0), (1, 0), colors.HexColor("#081F4A")),
        ("TEXTCOLOR", (0, 0), (1, 0), colors.white),
        ("ALIGN", (0, 0), (1, 0), "CENTER"),
        ("TOPPADDING", (0, 0), (1, 0), 6),
        ("BOTTOMPADDING", (0, 0), (1, 0), 6),

        # Body: NO grids, just padding & top alignment
        ("VALIGN", (0, 1), (1, -1), "TOP"),
        ("BACKGROUND",(0, 1), (1, -1), colors.white),

        # Padding consistent with layout knobs
        ("LEFTPADDING", (0, 1), (0, -1), KEY_LEFT_PADDING),
        ("RIGHTPADDING", (0, 1), (0, -1), KEY_RIGHT_PADDING),
        ("LEFTPADDING", (1, 1), (1, -1), VAL_LEFT_PADDING),
        ("RIGHTPADDING", (1, 1), (1, -1), VAL_RIGHT_PADDING),
        ("TOPPADDING", (0, 1), (1, -1), BODY_TOP_PADDING),
        ("BOTTOMPADDING",(0, 1), (1, -1), BODY_BOTTOM_PADDING),
        # Note: NO INNERGRID / NO BOX
    ]))
    panel.keepTogether = True
    panel.splitByRow = 1
    return panel

# ---------------------------------------------------------------------------------
# Build ONE page''s flowables (ALWAYS returns a list)
# ---------------------------------------------------------------------------------
def build_page_elements(row_dict, name_desc_rows, events_rows, page_w, doc, COL_GAP, ROW_GAP):
    # NEW: vertical section rows passed via row_dict
    section_rows = row_dict.get("_SECTION_ROWS", [])
    gv = make_gv(row_dict)
    current_date_str = datetime.now().strftime("%m/%d/%Y")

    # Report Period derivation (optional)
    period_start = row_dict.get("START_DATE", None)
    period_end   = row_dict.get("END_DATE", None)
    if period_start and period_end:
        try:
            start_str = pd.to_datetime(period_start).strftime("%m/%d/%Y")
            end_str   = pd.to_datetime(period_end).strftime("%m/%d/%Y")
            report_period_val = f"{start_str} to {end_str}"
        except Exception:
            report_period_val = f"{gv(''PERIOD_START'')} to {gv(''PERIOD_END'')}"
    else:
        report_period_val = ""

    # -------- Top sections --------
    search_criteria = {
        "Requestor": gv("REQLEGALID") ,
        "Office": gv("OFFICECODE"),
        "Report Run Date": current_date_str,
        "Report Period": report_period_val or "12/31/2015 to 12/31/2015",
    }
    caller_info = {
        "Caller Name": gv("CALLERNAME"),
        "Caller Phone": gv("CALLERPHONE"),
    }
    building_location = {
        "Building Name": gv("BUILDINGNAME"),
        "Address": gv("ADDRESS"),
        "City": gv("CITY"),
        "State": gv("STATE"),
        "Zip Code": gv("ZIP_CODE"),
    }
    building_contract = {
        "Building ID": gv("BUILDINGNUMBER"),
        "Building Type": gv("BUILDINGTYPE"),
        "Contract #": gv("CONTRACTNUMBER"),
        "Contract Type": gv("CONTRACTTYPE"),
    }

    # Completion Date: prefer INSTALLATIONDATE; else DATE PERFORMED from pairs
    comp_date = ""
    if row_dict.get("INSTALLATIONDATE"):
        try:
            comp_date = pd.to_datetime(row_dict.get("INSTALLATIONDATE")).strftime("%m/%d/%Y")
        except Exception:
            comp_date = gv("INSTALLATIONDATE")
    else:
        comp_date = get_date_performed_from_pairs(name_desc_rows) or gv("name_desc_rows")

    # Safe join for Office / Route to avoid NoneNone
    office_route = "".join([x for x in [gv("OFFICECODE"), gv("ROUTENUMBER")] if x])

    mechanic_supervisor = {
        "Mechanic": gv("PERSONNAME"),
        "Supervisor": gv("SUPERVISORID"),
        "Mechanic #": gv("PERSONID"),
        "Office / Route": office_route,
    }
    machine_info = {
        "Machine #": gv("MACHINENUMBER"),
        "Owner ID": gv("OWNERID") or gv("CUSTOMERID"),
        "Product Group": gv("PRODUCTGROUP"),
        "Manufacturer": gv("MANUFACTURER"),
        "Installation Date": comp_date,
    }

    # -------- Build the page --------
    elements = []
    avail_w = page_w - doc.leftMargin - doc.rightMargin

    def safe_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return list(x)
        return [x]

    # Top rows
    elements += safe_list(make_row(
        search_criteria, caller_info, "SEARCH CRITERIA", "CALLER",
        avail_w, left_ratio=0.62, key_col_ratio_left=0.42, key_col_ratio_right=0.50,
        col_gap=COL_GAP, row_gap=ROW_GAP
    ))
    elements += safe_list(make_row(
        building_location, building_contract, "BUILDING LOCATION", "BUILDING CONTRACT",
        avail_w, left_ratio=0.62, col_gap=COL_GAP, row_gap=ROW_GAP
    ))
    elements += safe_list(make_row(
        mechanic_supervisor, machine_info, "MECHANIC/SUPERVISOR", "MACHINE",
        avail_w, left_ratio=0.62, col_gap=COL_GAP, row_gap=ROW_GAP
    ))

    # Bottom row: LEFT = list-style NAME/DESCRIPTION; RIGHT = events (shifted right)
    elements += safe_list(make_row_custom(
        left_panel_fn=lambda w: make_section_list(section_rows, w, name_col_ratio=0.33),
        right_panel_fn=lambda w: make_events_list_4col(events_rows or [], w,
                                                       col_ratios=(0.45, 0.15, 0.15, 0.14),
                                                       boxed=False),
        total_width=avail_w,
        left_ratio=0.50,
        col_gap=COL_GAP,
        row_gap=ROW_GAP,
        right_top_offset=0,
        right_shift_px=0  # nudge right panel to the right
    ))

    return elements  # ALWAYS a list

# ---------------------------------------------------------------------------------
# Section rows builders for different fact types
# ---------------------------------------------------------------------------------
def make_section_rows(rr_cb_row: pd.Series) -> list[list[str]]:
    col_map = {
        "C MECH": "MECH",
        "C CUST": "CUST",
        "C RPTD": "RPTD",
        "C PROB": "PROB",
        "C COMPONENT": "COMPONENT",
        "C PART/DESCRIPTION": "PART/DESCRIPTION",
        "C ACTION": "ACTION",
        "C EQUIPMENT RELATED": "EQUIPMENT RELATED",
        "C VANDALISM": "VANDALISM",
        "C OCCUPIED": "OCCUPIED",
        "C BILLABLE": "BILLABLE"
    }
    label_order = [
        "MECH", "CUST", "RPTD", "PROB", "COMPONENT",
        "PART/DESCRIPTION", "ACTION", "EQUIPMENT RELATED",
        "VANDALISM", "OCCUPIED", "BILLABLE"
    ]

    present_src_cols = [src for src in col_map.keys() if src in rr_cb_row.index]
    s = rr_cb_row[present_src_cols].rename(col_map)

    rows = []
    for label in label_order:
        val = s.get(label, "")
        if pd.isna(val) or str(val).strip() == "":
            val = "-"
        else:
            # ✅ keep original inner spacing
            val = preserve_inner_spaces(str(val))
        rows.append([label, val])
    return rows

def make_proc_section_rows(rr_p_row: pd.Series) -> list[list[str]]:
    col_map = {
        "P PROCEDURE NUMBER": "PROCEDURE NUMBER",
        "P EXAMINER ID": "EXAMINER ID",
        "P NAME": "NAME",
        "P DATE PERFORMED": "DATE PERFORMED",
        "P LOGGED TIME": "LOGGED TIME",
        "P TRAVEL TIME": "TRAVEL TIME",
        "P REMAINING TIME": "REMAINING TIME",
        "P HELPER ID": "HELPER ID",
        "P HELPER NAME": "HELPER NAME",
        "P HELPER TRAVEL TIME": "HELPER TRAVEL TIME",
        "P HELPER LOGGED TIME": "HELPER LOGGED TIME",
        "P LAST CHANGE BY": "LAST CHANGE BY",
        "P TOTAL LOGGED TIME": "TOTAL LOGGED TIME",
        "P TOTAL TIME": "TOTAL TIME",
    }
    label_order = [
        "PROCEDURE NUMBER",
        "EXAMINER ID",
        "NAME",
        "DATE PERFORMED",
        "LOGGED TIME",
        "TRAVEL TIME",
        "REMAINING TIME",
        "HELPER ID",
        "HELPER NAME",
        "HELPER TRAVEL TIME",
        "HELPER LOGGED TIME",
        "LAST CHANGE BY",
        "TOTAL LOGGED TIME",
        "TOTAL TIME",
    ]

    present_src_cols = [src for src in col_map if src in rr_p_row.index]
    s = rr_p_row[present_src_cols].rename(col_map)

    rows = []
    for label in label_order:
        val = s.get(label, "")
        if label == "DATE PERFORMED":
            val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        # numeric time formatting (2 decimals)
        elif label in {
            "LOGGED TIME", "TRAVEL TIME", "REMAINING TIME",
            "HELPER TRAVEL TIME", "HELPER LOGGED TIME",
            "TOTAL LOGGED TIME", "TOTAL TIME"
        }:
            try:
                val = f"{float(val):.2f}"
            except Exception:
                val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        else:
            val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        rows.append([label, val])
    return rows

def make_repair_section_rows(rr_r_row: pd.Series) -> list[list[str]]:
    col_map = {
        "R MECH": "MECH",
        "R _": "_",
        "R __": "__",
        "R ESTD": "ESTD",
        "R USED": "USED",
        "R COMPONENT": "COMPONENT",
        "R PART/DESCRIPTION": "PART/DESCRIPTION",
        "R ACTION": "ACTION",
        "R WORK TYPE": "WORK TYPE",
        "R WCD": "WCD",
    }
    label_order = [
        "MECH", "_", "__", "ESTD", "USED", "COMPONENT",
        "PART/DESCRIPTION", "ACTION", "WORK TYPE", "WCD",
    ]

    present_src_cols = [src for src in col_map if src in rr_r_row.index]
    s = rr_r_row[present_src_cols].rename(col_map)

    rows = []
    for label in label_order:
        val = s.get(label, "")
        if label == "WCD":
            # keep date as-is; show ''-'' if empty
            val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        elif label in {"ESTD", "USED"}:  # numeric
            try:
                val = f"{float(val):.2f}"
            except Exception:
                val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        else:
            # ✅ preserve inner spacing for text (e.g., WORK/TRAVEL rows)
            val = "-" if (pd.isna(val) or str(val).strip() == "") else preserve_inner_spaces(str(val))
        rows.append([label, val])
    return rows

# ---------------------------------------------------------------------------------
# Procedure handler
# ---------------------------------------------------------------------------------
def _sql_escape(value) -> str:
    """Basic SQL literal escaping for single quotes."""
    if value is None:
        return ""
    return str(value).replace("''", "''''")

def run(session) -> list:
    global LOGO_READER
    LOGO_READER = _get_logo_from_stage(session)  # ensure header has the logo

    results = []

    # -------------------------------------------------------------------------
    # 1) Load driving filters (REQUESTORS join)
    # -------------------------------------------------------------------------
    filter_query = """
    SELECT DISTINCT
      RUUOMBMEUC AS buildingnumber,
      F3K6VIU7YN AS machinenumber,
      QOOTIFJBV2 AS FACT_TYPE,
      DATE(A5Y8GN9GFO) AS WORKDATE,
      DATE(MJTGEEFAFZ) AS start_date,
      DATE(AR5PXL_LHJ) AS end_date,
      UPDATED_BY,
      r.reqlegalid
    FROM NAABO_DEV.RR_REPORTING_WRITEBACK."SIGDS_27259cad_15b0_46b3_8984_7ce243a6ef0e" s
    LEFT JOIN NAABO_DEV.RR_REPORTING.REQUESTORS r
      ON UPPER(s.UPDATED_BY) = UPPER(r.requserid)
    WHERE UPDATED_AT = (
      SELECT MAX(UPDATED_AT)
      FROM NAABO_DEV.RR_REPORTING_WRITEBACK."SIGDS_27259cad_15b0_46b3_8984_7ce243a6ef0e")
    ORDER BY WORKDATE;
    """
    filter_df = session.sql(filter_query).to_pandas()

    # scratch temp table (as in your original)
    session.sql("""
        CREATE OR REPLACE TEMP TABLE DATA_READ AS
        SELECT * FROM NAABO_DEV.RR_REPORTING_WRITEBACK.ROPES_N_REPAIR_STREAM WHERE 0=1
    """).collect()

    if filter_df.empty:
        return ["No filter rows found in TABLE_NAME_EXTRACT_TESTING for current requestors."]

    # Normalize & validate columns (UPPERCASE expected from Snowflake)
    filter_df.columns = [c.strip().upper() for c in filter_df.columns]
    required_cols = {"BUILDINGNUMBER", "MACHINENUMBER", "FACT_TYPE",
                     "START_DATE", "END_DATE", "UPDATED_BY", "REQLEGALID"}
    missing = required_cols - set(filter_df.columns)
    if missing:
        raise ValueError(f"Missing required columns in filter_df: {sorted(missing)}")

    # Overall date range (for constant display fields)
    static_start_date = filter_df["START_DATE"].min()
    static_end_date   = filter_df["END_DATE"].max()
    start_range = static_start_date.strftime("%Y-%m-%d") if pd.notnull(static_start_date) else ""
    end_range   = static_end_date.strftime("%Y-%m-%d") if pd.notnull(static_end_date) else ""

    # Use first UPDATED_BY as recipient (keep behavior)
    rec = filter_df["UPDATED_BY"].iloc[0] if not filter_df.empty else "Unknown"

    # Clean types / parse dates
    filter_df["BUILDINGNUMBER"] = filter_df["BUILDINGNUMBER"].astype(str).str.strip()
    filter_df["MACHINENUMBER"]  = filter_df["MACHINENUMBER"].astype(str).str.strip()
    filter_df["REQLEGALID"]     = filter_df["REQLEGALID"].astype(str).str.strip()
    filter_df["START_DATE"]     = pd.to_datetime(filter_df["START_DATE"], errors="coerce")
    filter_df["END_DATE"]       = pd.to_datetime(filter_df["END_DATE"], errors="coerce")

    # Drop rows with invalid essential values
    filter_df = filter_df.dropna(subset=[
        "BUILDINGNUMBER", "MACHINENUMBER", "FACT_TYPE", "START_DATE", "END_DATE", "REQLEGALID"
    ])
    if filter_df.empty:
        return ["All filter rows had missing/invalid building/machine/start/end/reqlegalid."]

    # -------------------------------------------------------------------------
    # 2) Iterate per BUILDINGNUMBER -> fetch & union data -> one PDF per building
    # -------------------------------------------------------------------------
    pagesize = landscape(A3)
    page_w, page_h = pagesize

    for bn, bn_group in filter_df.groupby("BUILDINGNUMBER"):
        combined_df_list = []
        min_start = None
        max_end   = None

        for _, frow in bn_group.iterrows():
            mn    = frow["MACHINENUMBER"]
            reqid = frow["REQLEGALID"]
            fc    = frow["FACT_TYPE"]

            # Window dates
            wcd_start = frow["START_DATE"]
            wcd_end   = frow["END_DATE"]
            dat=frow["WORKDATE"]
            wcd_start_str = wcd_start.strftime("%Y-%m-%d")
            wcd_end_str   = wcd_end.strftime("%Y-%m-%d")
            dt= dat.strftime("%Y-%m-%d")

            # Update building-level coverage
            if (min_start is None) or (wcd_start < min_start):
                min_start = wcd_start
            if (max_end is None) or (wcd_end > max_end):
                max_end = wcd_end

            # Fetch fact rows for this window
            query = f"""
            SELECT *
            FROM NAABO_DEV.RR_REPORTING.FACT_DIM_ROPES_REPAIR
            WHERE MACHINENUMBER = ''{_sql_escape(mn)}''
              AND BUILDINGNUMBER = ''{_sql_escape(bn)}''
              AND FACTTYPE = ''{_sql_escape(fc)}''
              AND WORKCOMPLETIONDATE = ''{_sql_escape(dt)}'' 
              order by workcompletiondate
            """
            main_df = session.sql(query).to_pandas()
            if main_df.empty:
                continue

            main_df.columns = [c.strip() for c in main_df.columns]

            # Carry these as static/context fields in the output (display/traceback)
            main_df["START_DATE"] = start_range
            main_df["END_DATE"]   = end_range
            main_df["REQLEGALID"] = reqid

            combined_df_list.append(main_df)

        if not combined_df_list:
            results.append(f"[SKIP] No rows found for building={bn} across all filter rows.")
            continue

        # Union all & normalize
        RR_DF = pd.concat(combined_df_list, ignore_index=True)
        RR_DF.columns = [c.strip().upper() for c in RR_DF.columns]

        # ---------------------------------------------------------------------
        # 3) Build a single PDF for the building with CHRONOLOGICAL pages
        # ---------------------------------------------------------------------
        min_start_str = min_start.strftime("%Y-%m-%d") if isinstance(min_start, pd.Timestamp) else str(min_start)
        max_end_str   = max_end.strftime("%Y-%m-%d")   if isinstance(max_end, pd.Timestamp) else str(max_end)

        safe_bn  = str(bn).replace(''/'', ''_'').replace(''\\\\'', ''_'').strip()
        out_file = f"Rope_&_Repair_{safe_bn}.pdf"

        buffer = BytesIO()
        doc = BaseDocTemplate(
            buffer,
            pagesize=pagesize,
            topMargin=DOC_TOP_MARGIN,
            leftMargin=INSET,
            rightMargin=INSET,
            bottomMargin=30
        )

        # Frames & templates
        default_frame = Frame(
            doc.leftMargin,
            doc.bottomMargin,
            page_w - doc.leftMargin - doc.rightMargin,
            page_h - doc.topMargin - doc.bottomMargin,
            id="default"
        )
        blank_frame = Frame(0, 0, page_w, page_h,
                            leftPadding=0, rightPadding=0,
                            topPadding=0, bottomPadding=0, id="BlankFull")
        frame_h = page_h - doc.topMargin - doc.bottomMargin

        start_tpl   = PageTemplate(id="Start",   frames=blank_frame,   onPage=draw_report_start)
        default_tpl = PageTemplate(id="Default", frames=default_frame, onPage=header)
        end_tpl     = PageTemplate(id="End",     frames=blank_frame,   onPage=draw_report_end)
        doc.addPageTemplates([start_tpl, default_tpl, end_tpl])

        elements = []
        # Start on a blank START page, then switch to Default
        elements.append(NextPageTemplate("Default"))
        elements.append(PageBreak())

        # -------- CHRONOLOGICAL pages (Callbacks / Procedures / Repairs mixed) --------
        def _parse_dt(row):
            """Compose a sortable timestamp from WORKCOMPLETIONDATE (+optional time)."""
            dt = row.get("WORKCOMPLETIONDATE")
            tm = row.get("WORKCOMPLETIONTIME")
            try:
                if pd.notnull(dt) and pd.notnull(tm):
                    return pd.to_datetime(str(dt).split(" ")[0] + " " + str(tm), errors="ignore", utc=False)
                return pd.to_datetime(dt, errors="coerce")
            except Exception:
                return pd.NaT

        # stable chronological order (mergesort keeps relative order for ties)
        RR_DF["_SORT_DT"] = RR_DF.apply(_parse_dt, axis=1)
        RR_DF["_SORT_DT"] = RR_DF["_SORT_DT"].fillna(pd.Timestamp.max)

        # De-dup helpers
        seen_td_keys  = set()  # callbacks
        seen_proc_keys = set()
        seen_rep_keys  = set()

        def _proc_key(r):
            # Adjust subset to your business identity (add/remove fields as needed)
            return (
                r.get("BUILDINGNUMBER"),
                r.get("MACHINENUMBER"),
                r.get("FACTTYPE"),
                r.get("WORKCOMPLETIONDATE"),
                r.get("UNIQUE_ID")
            )

        def _rep_key(r):
            # Adjust subset to your business identity (add/remove fields as needed)
            return (
                r.get("BUILDINGNUMBER"),
                r.get("MACHINENUMBER"),
                r.get("FACTTYPE"),
                r.get("WORKCOMPLETIONDATE"),
                r.get("UNIQUE_ID")
            )

        ordered = RR_DF.sort_values(["_SORT_DT", "FACTTYPE"], kind="mergesort").reset_index(drop=True)
        has_td_col = "TD_KEY" in RR_DF.columns

        for _, rr_row in ordered.iterrows():
            fact = rr_row.get("FACTTYPE")

            if fact == "Callbacks":
                # If no TD_KEY column/value, render a minimal callbacks page for this row
                if not has_td_col:
                    section_rows = make_section_rows(rr_row)
                    payload = {**rr_row.to_dict(), "_SECTION_ROWS": section_rows}
                    elements += build_page_elements(payload, [], [], page_w, doc, COL_GAP, ROW_GAP)
                    elements.append(PageBreak())
                    continue

                td_key_val = rr_row.get("TD_KEY")
                if isinstance(td_key_val, str):
                    td_key_val = td_key_val.strip()
                if not td_key_val or str(td_key_val).lower() == "nan":
                    # No usable key -> render minimal page from this single row
                    section_rows = make_section_rows(rr_row)
                    payload = {**rr_row.to_dict(), "_SECTION_ROWS": section_rows}
                    elements += build_page_elements(payload, [], [], page_w, doc, COL_GAP, ROW_GAP)
                    elements.append(PageBreak())
                    continue

                # Render this TD_KEY once at the time it first appears
                if td_key_val in seen_td_keys:
                    continue
                seen_td_keys.add(td_key_val)

                rr_cb = RR_DF[RR_DF["TD_KEY"] == td_key_val].copy()
                
                for _, cb_row in rr_cb.iterrows():
                    rr_cb_row = cb_row
                    section_rows = make_section_rows(rr_cb_row)
                    rr_cb_row_dict = rr_cb_row.to_dict()
    
                    # Pull DIM_DESCRIPTION for events under this TD_KEY
                    desc_sql = f"""
                    SELECT TD_KEY, TD_PARTDESC, VDATE, VPARTTIME, ''EST'' AS "TIME ZONE"
                    FROM NAABO_DEV.RR_REPORTING.DIM_DESCRIPTION
                    WHERE TD_KEY = ''{_sql_escape(td_key_val)}'' AND VPARTTIME IS NOT NULL
    ORDER BY
        TO_DATE(
            CASE
                WHEN LENGTH(VDATE) = 5 THEN VDATE || ''-'' || YEAR(CURRENT_DATE)
                ELSE VDATE
            END,
            ''MM-DD-YYYY''
        ),
        TO_TIME(VPARTTIME, ''HH12:MI AM'');
                    """
                    TD_DIM_DESCRIPTION = session.sql(desc_sql).to_pandas()
                    if not TD_DIM_DESCRIPTION.empty and "TD_KEY" in TD_DIM_DESCRIPTION.columns:
                        events_rows = TD_DIM_DESCRIPTION.drop(columns=["TD_KEY"]).values.tolist()
                    else:
                        events_rows = TD_DIM_DESCRIPTION.values.tolist() if not TD_DIM_DESCRIPTION.empty else []
    
                    payload = {**rr_cb_row_dict, "_SECTION_ROWS": section_rows}
                    elements += build_page_elements(payload, [], events_rows, page_w, doc, COL_GAP, ROW_GAP)
                    elements.append(PageBreak())
    
            elif fact == "Procedures":
                k = _proc_key(rr_row)
                if k in seen_proc_keys:
                    continue
                seen_proc_keys.add(k)

                section_rows = make_proc_section_rows(rr_row)
                payload = {**rr_row.to_dict(), "_SECTION_ROWS": section_rows}
                elements += build_page_elements(payload, [], [], page_w, doc, COL_GAP, ROW_GAP)
                elements.append(PageBreak())
                
            elif fact == "Repairs":
                k = _rep_key(rr_row)
                if k in seen_rep_keys:
                    continue
                seen_rep_keys.add(k)

                section_rows = make_repair_section_rows(rr_row)
                payload = {**rr_row.to_dict(), "_SECTION_ROWS": section_rows}
                elements += build_page_elements(payload, [], [], page_w, doc, COL_GAP, ROW_GAP)
                elements.append(PageBreak())

            else:
                # Unknown FACTTYPE — skip or log if needed
                continue

        # Clean helper column
        if "_SORT_DT" in RR_DF.columns:
            RR_DF.drop(columns=["_SORT_DT"], inplace=True, errors="ignore")

        # -------- End page --------
        if len(elements) and isinstance(elements[-1], PageBreak):
            elements = elements[:-1]
        elements.append(NextPageTemplate("End"))
        elements += build_center_page_elements("", frame_h)
        elements.append(PageBreak())

        # Build & upload
        doc.build(elements, canvasmaker=NumberedCanvas)
        pdf_data = buffer.getvalue()
        buffer.close()

        _upload_pdf_to_stage(session, out_file, pdf_data)
    send_email(session, rec)

    return "PDF(s) generated, uploaded to stage and sent mail to recipient."
';