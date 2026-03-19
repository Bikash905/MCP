from reportlab.platypus import (
    BaseDocTemplate, PageTemplate, Frame, NextPageTemplate, PageBreak,
    Table, TableStyle, Paragraph
)
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import pandas as pd
import snowflake.connector
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO
from reportlab.lib.utils import ImageReader
from PIL import Image
import io

def dumwaiter_report():
    storage_account_name = "qliksaaswhqprodadls"
    storage_account_key = "dKgCcGCTWBoAME6tfwaxIppyfz8KR2T9MEXjyWRWCYFfBE8+k1krik+zZTTSUXIj7AvBmCYGC4Kn+ASt9RWK4A=="
    container_name = "mcp"
    logo_directory = "otis_logo"
    logo_file_name = "otis_image.jpg"
    directory_name = "MCP_Dashboard_V3"

    # Create service client
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=storage_account_key
    )
    # Get file system and directory client
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    dir_client = file_system_client.get_directory_client(directory_name)
    if not dir_client.exists():
        dir_client.create_directory()
    # Download image into memory
    # --- Step 1: Download logo from ADLS ---
    logo_dir_client = file_system_client.get_directory_client(logo_directory)
    logo_file_client = logo_dir_client.get_file_client(logo_file_name)
    download = logo_file_client.download_file()
    logo_bytes = download.readall()
    # Convert logo to BytesIO for ReportLab
    logo_image = Image.open(io.BytesIO(logo_bytes))
    img_buffer = io.BytesIO()
    logo_image.save(img_buffer, format="JPEG")  # ✅ Use JPEG for JPG image
    img_buffer.seek(0)
    logo_path = ImageReader(img_buffer)  # ✅ Wrap BytesIO

    # Set your Snowflake account information
    conn = snowflake.connector.connect(
        user='SVC-NAABO-USER',
        password='rpwNkp5HHGfmFG5BhmYu!@!',
        account='OTISELEVATOR-OTISELEVATOR',
        role='OTIS-DL-SF-NAABO-DEVELOPER',
        warehouse='NAABO_NAA_PROD_WH',
        database='NAABO_PROD',
        schema='MCP_VIEW'
    )
    # # Create a cursor object
    cur = conn.cursor()
    # ---- Setup Local Styles ----
    styles = getSampleStyleSheet()
    wrap_style = styles["BodyText"]
    # ---- Header Function ----
    def header(canvas, doc, section_title, section_data, title):
        canvas.saveState()
        page_width, page_height = landscape(A3)
        # --- Draw top header lines ---
        canvas.line(30, 775, 750, 775)
        canvas.setFont('Helvetica-Bold', 18)
        canvas.drawString(40, 755,title )
        canvas.drawString(40, 735, "OTIS Elevator Company")
        canvas.setStrokeColor(colors.black)
        canvas.setLineWidth(1)
        canvas.line(30, 725, 750, 725)
        canvas.line(30, 721, 750, 721)

        image_width = 110
        image_height = 110
        logo_x = page_width - image_width - 120
        logo_y = page_height - image_height - 60
        try:
            canvas.drawImage(logo_path, logo_x, logo_y, width=image_width, height=image_height, mask='auto')
        except Exception as e:
            print("Logo error:", e)
        section_table = Table(section_data, colWidths=[300, 300])
        section_table.setStyle(TableStyle([  # Add grid for clarity
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTSIZE", (0, 0), (-1, -1), 12),
    ]))
        section_table.wrapOn(canvas, 40, page_height - 180)
        section_table.drawOn(canvas, 40, page_height - 210)
        canvas.setFont('Helvetica-Bold', 12)
        canvas.drawString(40, page_height - 250, section_title)
        canvas.restoreState()

    def fetch_df(sql):
        cur.execute(sql)
        return cur.fetch_pandas_all()

    def create_table_with_header(df):
            # Row 1: Custom header with merged cell for "Previous Completion Dates"
            header_row_1 = ["", "", "", "", "Previous Completion Dates"] + [""] * (len(df.columns) - 5)
            # Row 2: Actual column headers
            header_row_2 = df.columns.tolist()
            # Wrap only the "Task" column
            wrapped_data = []
            for row in df.values.tolist():
                new_row = []
                for col_name, cell in zip(df.columns, row):
                    if col_name == "Task" :
                        new_row.append(Paragraph(str(cell), wrap_style))  # Wrap Task column
                    else:
                        new_row.append(str(cell) if cell is not None else "")
                wrapped_data.append(new_row)

            # Combine headers and data
            table_data = [header_row_1, header_row_2] + wrapped_data
            col_widths = [60, 100, 300] + [60] * (len(df.columns) - 3)
            # Create table
            table = Table(table_data, colWidths=col_widths)
            # Apply styles
            table.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            # Merge cells in header row
            ("SPAN", (0, 0), (3, 0)),       # Merge first part
            ("SPAN", (4, 0), (-1, 0)),      # Merge "Previous Completion Dates"
            ("ALIGN", (4, 0), (-1, 0), "CENTER"),
            # Bold both header rows
            ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
            # Remove TOP border for merged cell (0,0 to 3,0)
            ("LINEABOVE", (0, 0), (3, 0), 0, colors.white),
            # Remove LEFT border for merged cell (0,0 to 3,0)
            ("LINEBEFORE", (0, 0), (3, 0), 0, colors.white),
            # Force transparent color for safety
            ("LINEABOVE", (0, 0), (3, 0), 0, colors.transparent),
            ("LINEBEFORE", (0, 0), (3, 0), 0, colors.transparent),

            ]))
            return table
    def wrap_table_data(df):
            wrapped_data = []
            for row in df.values.tolist():
                wrapped_row = [Paragraph(str(cell), wrap_style) for cell in row]
                wrapped_data.append(wrapped_row)
            return [df.columns.tolist()] + wrapped_data

    Abb_Df    = fetch_df(f'select * from NAABO_PROD.MCP_VIEW.OTISLINEABBREVIATIONLIST;')
    Abb_Df.rename(columns={
                    'ABBREVIATION1': 'ABBREVIATION',
                    'MEANING1': 'MEANING'
                }, inplace=True)
    Abb_row_1 = ["OTISLINE ABBREVIATION LIST"]
    Abb_row_2 = Abb_Df.columns.tolist()

        # Two empty rows
    empty_row_1 = [""]
    empty_row_2 = [""]

    Abb_data = [Abb_row_1, empty_row_1, Abb_row_2, empty_row_2] + Abb_Df.values.tolist()

    Abb_Table = Table(Abb_data, colWidths=[100, 300, 100, 200])

    Abb_Table.setStyle(TableStyle([
        # Apply grid to everything first
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        # Merge first row for title
        ("SPAN", (0, 0), (-1, 0)),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        # Bold column headers
        ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
        # Merge all columns for empty rows
        ("SPAN", (0, 1), (-1, 1)),  # Empty row after title
        ("SPAN", (0, 3), (-1, 3)),  # Empty row after headers
        # Remove vertical borders for empty rows
        ("LINEAFTER", (0, 1), (-1, 1), 0, colors.white),
        ("LINEAFTER", (0, 3), (-1, 3), 0, colors.white),
    ]))
    default_frame = Frame(30, 30, landscape(A3)[0] - 60, landscape(A3)[1] - 60, id='default')
    title = "Dumbwaiter Maintenance Control Program - Records"
    cur.execute('''
    SELECT DISTINCT DUMBWAITER_BUILDINGTOPREVCOMPLETIONKEY AS MACHINE_NUMBER
    FROM NAABO_PROD.MCP_VIEW.DUMBWAITER_MC_PREV_COMPLETION
    WHERE DATE(LASTCOMPLETEDDATE) >= CURRENT_DATE-1
    ''')
    machine_df = cur.fetch_pandas_all()
    machines = machine_df["MACHINE_NUMBER"].astype(str).tolist()
    if not machines:
        print("No Dumbwaiter Machines completed yesterday.")
        raise SystemExit(0)
    machine_numbers = "(" + ",".join(f"'{m}'" for m in machines) + ")"

    # ---- Fetch all required tables ----
    building_info_df = fetch_df(f'''
    SELECT BUILDINGNAME, BUILDING_ID, ADDRESS, CITY, CONTRACTNUMBER, CUSTOMER_DESIG, GOVERNMENT, PRODUCTGROUP, "Machine Number"
    FROM NAABO_PROD.MCP_VIEW.DUMBWAITER_BUILDING_INFO
    WHERE "Machine Number" IN {machine_numbers}
    ''')
    Unit_df    = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter Unit Visit" WHERE "Machine Number" IN {machine_numbers}')
    DGMPH_df   = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter DGMPH" WHERE "Machine Number" IN {machine_numbers}')
    DGMPM_df   = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter DGMPM" WHERE "Machine Number" IN {machine_numbers}')
    Maint_Static_df = fetch_df('SELECT * FROM NAABO_PROD.MCP_VIEW.DUMBWAITER_MAINT_STATIC order by "Type", "Code"')
    Cat1_df    = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter CAT1" WHERE "Machine Number" IN {machine_numbers}')
    Cat5_df    = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter CAT5" WHERE "Machine Number" IN {machine_numbers}')
    Cat_Static_df = fetch_df('SELECT * FROM NAABO_PROD.MCP_VIEW.DUMBWAITER_CAT_STATIC order by "Type", "Code"')
    Repair_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter Repair/Replacement Log" WHERE "Machine Number" IN {machine_numbers}')
    Call_df    = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Dumbwaiter Call Back Log" WHERE "Machine Number" IN {machine_numbers}')

    for unit_number in machines:
        # --- Section Info Table ---
        filtered_building = building_info_df[building_info_df["Machine Number"] == unit_number]
        if filtered_building.empty:
            section_data = [["No building info found for Machine Number " + str(unit_number), ""]]
        else:
            row = filtered_building.iloc[0]
            section_data = [
                [f"Building Name: {row['BUILDINGNAME']}", f"Building ID: {row['BUILDING_ID']}"],
                [f"Address: {row['ADDRESS']}", f"City: {row['CITY']}"],
                [f"Contract #: {row['CONTRACTNUMBER']}", f"Customer Desig: {row['CUSTOMER_DESIG']}"],
                [f"Machine #: {unit_number}", f"Government #: {row['GOVERNMENT']}"],
                [f"Product Group : {row['PRODUCTGROUP']}", ""]
            ]

        # --- Maintenance Table ---
        unit_data   = Unit_df[Unit_df["Machine Number"] == unit_number]
        DGMPH_data  = DGMPH_df[DGMPH_df["Machine Number"] == unit_number].sort_values(by="Code")
        DGMPM_data  = DGMPM_df[DGMPM_df["Machine Number"] == unit_number].sort_values(by="Code")
        Main_df = pd.concat([unit_data, DGMPH_data, DGMPM_data], axis=0).drop('Machine Number', axis=1)
        if Main_df.empty:
            Main_df = Maint_Static_df.copy()
        else:
            existing_types = Main_df["Type"].astype(str).unique()
            if "Unit Visit" not in existing_types:
                unit_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "Unit Visit"].copy()
                Main_df = pd.concat([Main_df, unit_static_rows], axis=0)
            if "DGMPH" not in existing_types:
                dgmph_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "DGMPH"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, dgmph_static_rows], axis=0)
            if "DGMPM" not in existing_types:
                dgmpm_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "DGMPM"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, dgmpm_static_rows], axis=0)
        Main_df.rename(columns={col: 'Date' for col in Main_df.columns if str(col).startswith('DATE')}, inplace=True)

        # --- Category Test Records Table ---
        Cat1_data = Cat1_df[Cat1_df["Machine Number"] == unit_number].sort_values(by="Code")
        Cat5_data = Cat5_df[Cat5_df["Machine Number"] == unit_number].sort_values(by="Code")
        Cat_df = pd.concat([Cat1_data, Cat5_data], axis=0).drop('Machine Number', axis=1)
        if Cat_df.empty:
            Cat_df = Cat_Static_df.copy()
        else:
            existing_types = Cat_df["Type"].astype(str).unique()
            if "CAT1 Test" not in existing_types:
                cat1_static_rows = Cat_Static_df[Cat_Static_df["Type"] == "CAT1 Test"].copy().sort_values(by="Code")
                Cat_df = pd.concat([Cat_df, cat1_static_rows], axis=0)
            if "CAT5 Test" not in existing_types:
                cat5_static_rows = Cat_Static_df[Cat_Static_df["Type"] == "CAT5 Test"].copy().sort_values(by="Code")
                Cat_df = pd.concat([Cat_df, cat5_static_rows], axis=0)
        Cat_df = Cat_df.reset_index(drop=True)
        Cat_df.rename(columns={col: 'Date' for col in Cat_df.columns if str(col).startswith('DATE')}, inplace=True)

        # --- Repair Table ---
        Repair_data = Repair_df[Repair_df["Machine Number"] == unit_number].drop('Machine Number', axis=1)
        Call_data = Call_df[Call_df["Machine Number"] == unit_number].drop('Machine Number', axis=1)
        # --- Create tables ---
        main_table = create_table_with_header(Main_df)
        cat_table = create_table_with_header(Cat_df)
        if Repair_data.empty:
            data=[Repair_data.columns.tolist()] + Repair_data.values.tolist()
            table_repair=Table(data, colWidths=[180] * len(Repair_data.columns))
        else:
            table_repair = Table(wrap_table_data(Repair_data), repeatRows=1)
        if Call_data.empty:
            data=[Call_data.columns.tolist()] + Call_data.values.tolist()
            table_call=Table(data, colWidths=[180] * len(Call_data.columns))
        else:   
            table_call = Table(wrap_table_data(Call_data), repeatRows=1)

        # Apply common style to remaining tables
        table_style = TableStyle([
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ])
        for t in [table_repair, table_call]:
            if t: t.setStyle(table_style)
        # --- PDF Setup: Frame & PageTemplates ---
        buffer = BytesIO()  # In-memory buffer instead of local file
        doc = BaseDocTemplate(buffer, pagesize=landscape(A3), topMargin=250)
        fileName = f'MCP Records DumbWaiter-{unit_number}.pdf'
        frame = Frame(30, 30, landscape(A3)[0] - 60, landscape(A3)[1] - 280, id='normal')
        doc.addPageTemplates([
            PageTemplate(id="Maintenance Records", frames=frame, onPage=lambda c, d: header(c, d, "Maintenance Records", section_data, title)),
            PageTemplate(id="Category Test Records", frames=frame, onPage=lambda c, d: header(c, d, "Category Test Records", section_data, title)),
            PageTemplate(id="Repair / Replacement Log", frames=frame, onPage=lambda c, d: header(c, d, "Repair / Replacement Log", section_data, title)),
            PageTemplate(id="Call Back Log", frames=frame, onPage=lambda c, d: header(c, d, "Call Back Log", section_data, title)),
            PageTemplate(id='Default', frames=default_frame)
        ])
        elements = []
        first_section = True
        # --- Maintenance Records Section ---
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(main_table)
        first_section = False
        # --- Category Test Records Section ---
        elements.append(NextPageTemplate('Category Test Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(cat_table)
        first_section = False
        # --- Repair / Replacement Log Section ---
        elements.append(NextPageTemplate('Repair / Replacement Log'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(table_repair)
        first_section = False
        # --- Call Back Log Section (wrapped columns) ---
        elements.append(NextPageTemplate('Call Back Log'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(table_call)
        first_section = False
        # Then before Abb_Table:
        elements.append(NextPageTemplate('Default'))
        elements.append(PageBreak())
        elements.append(Abb_Table)
        # --- Build PDF ---
        doc.build(elements)
        # Upload directly to ADLS
        pdf_data = buffer.getvalue()  # Get PDF bytes from memory
        file_client = dir_client.create_file(fileName)
        pdf_data = buffer.getvalue()  # Get PDF bytes from memory
        file_client.append_data(data=pdf_data, offset=0, length=len(pdf_data))
        file_client.flush_data(len(pdf_data))
        buffer.close()
		
		
def escalator_report():
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    storage_account_name = "qliksaaswhqprodadls"
    storage_account_key = "dKgCcGCTWBoAME6tfwaxIppyfz8KR2T9MEXjyWRWCYFfBE8+k1krik+zZTTSUXIj7AvBmCYGC4Kn+ASt9RWK4A=="
    container_name = "mcp"
    logo_directory = "otis_logo"
    logo_file_name = "otis_image.jpg"
    directory_name = "MCP_Dashboard_V3"

    # Create service client
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=storage_account_key
    )
    # Get file system and directory client
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    dir_client = file_system_client.get_directory_client(directory_name)
    if not dir_client.exists():
        dir_client.create_directory()
    # Download image into memory
    # --- Step 1: Download logo from ADLS ---
    logo_dir_client = file_system_client.get_directory_client(logo_directory)
    logo_file_client = logo_dir_client.get_file_client(logo_file_name)
    download = logo_file_client.download_file()
    logo_bytes = download.readall()
    # Convert logo to BytesIO for ReportLab
    logo_image = Image.open(io.BytesIO(logo_bytes))
    img_buffer = io.BytesIO()
    logo_image.save(img_buffer, format="JPEG")  # ✅ Use JPEG for JPG image
    img_buffer.seek(0)
    logo_path = ImageReader(img_buffer)  # ✅ Wrap BytesIO

    # Set your Snowflake account information
    conn = snowflake.connector.connect(
        user='SVC-NAABO-USER',
        password='rpwNkp5HHGfmFG5BhmYu!@!',
        account='OTISELEVATOR-OTISELEVATOR',
        role='OTIS-DL-SF-NAABO-DEVELOPER',
        warehouse='NAABO_NAA_PROD_WH',
        database='NAABO_PROD',
        schema='MCP_VIEW'
    )
    # # Create a cursor object
    cur = conn.cursor()
    # ---- Setup Local Styles ----
    styles = getSampleStyleSheet()
    wrap_style = styles["BodyText"]
    # ---- Header Function ----
    def header(canvas, doc, section_title, section_data, title):
        canvas.saveState()
        page_width, page_height = landscape(A3)
        # --- Draw top header lines ---
        canvas.line(30, 775, 750, 775)
        canvas.setFont('Helvetica-Bold', 18)
        canvas.drawString(40, 755,title )
        canvas.drawString(40, 735, "OTIS Elevator Company")
        canvas.setStrokeColor(colors.black)
        canvas.setLineWidth(1)
        canvas.line(30, 725, 750, 725)
        canvas.line(30, 721, 750, 721)

        image_width = 110
        image_height = 110
        logo_x = page_width - image_width - 120
        logo_y = page_height - image_height - 60
        try:
            canvas.drawImage(logo_path, logo_x, logo_y, width=image_width, height=image_height, mask='auto')
        except Exception as e:
            print("Logo error:", e)
        section_table = Table(section_data, colWidths=[300, 300])
        section_table.setStyle(TableStyle([  # Add grid for clarity
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTSIZE", (0, 0), (-1, -1), 12),
    ]))
        section_table.wrapOn(canvas, 40, page_height - 180)
        section_table.drawOn(canvas, 40, page_height - 210)
        canvas.setFont('Helvetica-Bold', 12)
        canvas.drawString(40, page_height - 250, section_title)
        canvas.restoreState()

    def create_table_with_header_escalator(df):
        # Row 1: Custom header with merged cell for "Previous Completion Dates"
        header_row_1 = ["", "", "", "","", "Previous Completion Dates"] + [""] * (len(df.columns) - 6)
        # Rename specific columns
        df.rename(columns={
            'Code': 'A17.1-2019 and Prior',
            'Code1': 'A17.1-2022 and Later'
        }, inplace=True)
        # Row 2: Actual column headers
        header_row_2 = df.columns.tolist()
        # Wrap only the "Task" column
        wrapped_data = []
        for row in df.values.tolist():
            new_row = []
            for col_name, cell in zip(df.columns, row):
                if col_name == "Task":
                    new_row.append(Paragraph(str(cell), wrap_style))  # Wrap Task column
                else:
                    new_row.append(str(cell) if cell is not None else "")
            wrapped_data.append(new_row)

        # Combine headers and data
        table_data = [header_row_1, header_row_2] + wrapped_data
        col_widths = [60, 110,110, 220] + [60] * (len(df.columns) - 4)
        # Create table
        table = Table(table_data, colWidths=col_widths)
        # Apply styles
        table.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("SPAN", (0, 0), (4, 0)),  # Merge from column 0 to last 4
            ("SPAN", (5, 0), (-1, 0)),  # Merge from column 5 to last column
            ("ALIGN", (5, 0), (-1, 0), "CENTER"),
            # Bold both header rows
            ("FONTNAME", (0, 0), (-1, 1), "Helvetica-Bold"),
            # Remove TOP border for merged cell (0,0 to 4,0)
            ("LINEABOVE", (0, 0), (4, 0), 0, colors.white),
            # Remove LEFT border for merged cell (0,0 to 3,0)
            ("LINEBEFORE", (0, 0), (4, 0), 0, colors.white),
            # Force transparent color for safety
            ("LINEABOVE", (0, 0), (4, 0), 0, colors.transparent),
            ("LINEBEFORE", (0, 0), (4, 0), 0, colors.transparent),
        ]))
        return table

    def fetch_df(sql):
        cur.execute(sql)
        return cur.fetch_pandas_all()

    
    def wrap_table_data(df):
            wrapped_data = []
            for row in df.values.tolist():
                wrapped_row = [Paragraph(str(cell), wrap_style) for cell in row]
                wrapped_data.append(wrapped_row)
            return [df.columns.tolist()] + wrapped_data

    Abb_Df    = fetch_df(f'select * from NAABO_PROD.MCP_VIEW.OTISLINEABBREVIATIONLIST;')
    Abb_Df.rename(columns={
                    'ABBREVIATION1': 'ABBREVIATION',
                    'MEANING1': 'MEANING'
                }, inplace=True)
    Abb_row_1 = ["OTISLINE ABBREVIATION LIST"]
    Abb_row_2 = Abb_Df.columns.tolist()

        # Two empty rows
    empty_row_1 = [""]
    empty_row_2 = [""]

    Abb_data = [Abb_row_1, empty_row_1, Abb_row_2, empty_row_2] + Abb_Df.values.tolist()

    Abb_Table = Table(Abb_data, colWidths=[100, 300, 100, 200])

    Abb_Table.setStyle(TableStyle([
        # Apply grid to everything first
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        # Merge first row for title
        ("SPAN", (0, 0), (-1, 0)),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        # Bold column headers
        ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
        # Merge all columns for empty rows
        ("SPAN", (0, 1), (-1, 1)),  # Empty row after title
        ("SPAN", (0, 3), (-1, 3)),  # Empty row after headers
        # Remove vertical borders for empty rows
        ("LINEAFTER", (0, 1), (-1, 1), 0, colors.white),
        ("LINEAFTER", (0, 3), (-1, 3), 0, colors.white),
    ]))
    default_frame = Frame(30, 30, landscape(A3)[0] - 60, landscape(A3)[1] - 60, id='default')
    title = "Escalator Maintenance Control Program - Records"
    cur.execute('''
        SELECT DISTINCT ESCLATOR_BUILDINGTOPREVCOMPLETIONKEY AS MACHINE_NUMBER
        FROM NAABO_PROD.MCP_VIEW.ESCLATOR_MC_PREV_COMPLETION
        WHERE DATE(LASTCOMPLETEDDATE) >= CURRENT_DATE-1
        ''')
    machine_df = cur.fetch_pandas_all()
    machines = machine_df["MACHINE_NUMBER"].astype(str).tolist()
    if not machines:
        print("No Escalator Machines completed yesterday.")
        raise SystemExit(0)
    machine_numbers = "(" + ",".join(f"'{m}'" for m in machines) + ")"

    # ---- Fetch all required tables ----
    building_info_df = fetch_df(f'''
    SELECT BUILDINGNAME, BUILDING_ID, ADDRESS, CITY, CONTRACTNUMBER, CUSTOMER_DESIG, GOVERNMENT, PRODUCTGROUP, "Machine Number"
    FROM NAABO_PROD.MCP_VIEW.ESCLATOR_BUILDING_INFO
    WHERE "Machine Number" IN {machine_numbers}
    ''')
    Unit_df   = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator Unit Vist" WHERE "Machine Number" IN {machine_numbers}')
    EGMP1_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator EGMP1" WHERE "Machine Number" IN {machine_numbers}')
    EGMP2_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator EGMP2" WHERE "Machine Number" IN {machine_numbers}')
    EGMP3_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator EGMP3" WHERE "Machine Number" IN {machine_numbers}')
    EGMP4_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator EGMP4" WHERE "Machine Number" IN {machine_numbers}')
    EGMP5_df  = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator EGMP5" WHERE "Machine Number" IN {machine_numbers}')
    Maint_Static_df = fetch_df('SELECT * FROM NAABO_PROD.MCP_VIEW.ESCLATOR_MAINT_STATIC order by "Type", "Code";')
    Cat1_df   = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator CAT1" WHERE "Machine Number" IN {machine_numbers}')
    Cat_Static_df   = fetch_df('SELECT * FROM NAABO_PROD.MCP_VIEW.ESCLATOR_CAT_STATIC order by "Type", "Code";')
    Repair_df = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator Repair/Replacement Log" WHERE "Machine Number" IN {machine_numbers}')
    Call_df   = fetch_df(f'SELECT * FROM NAABO_PROD.MCP_VIEW."Escalator Call Back Log" WHERE "Machine Number" IN {machine_numbers}')
    # Step 3: Format section_data for each machine
    for unit_number in machines:
        # --- Section Info Table ---
        filtered_building = building_info_df[building_info_df["Machine Number"] == unit_number]
        if filtered_building.empty:
            section_data = [["No building info found for Machine Number " + str(unit_number), ""]]
        else:
            row = filtered_building.iloc[0]
            section_data = [
                [f"Building Name: {row['BUILDINGNAME']}", f"Building ID: {row['BUILDING_ID']}"],
                [f"Address: {row['ADDRESS']}", f"City: {row['CITY']}"],
                [f"Contract #: {row['CONTRACTNUMBER']}", f"Customer Desig: {row['CUSTOMER_DESIG']}"],
                [f"Machine #: {unit_number}", f"Government #: {row['GOVERNMENT']}"],
                [f"Product Group : {row['PRODUCTGROUP']}", ""]
            ]
        # --- Maintenance Table --
        Unit_data = Unit_df[Unit_df["Machine Number"] == unit_number]
        EGMP1_data = EGMP1_df[EGMP1_df["Machine Number"] == unit_number].sort_values(by="Code")
        EGMP2_data = EGMP2_df[EGMP2_df["Machine Number"] == unit_number].sort_values(by="Code")
        EGMP3_data = EGMP3_df[EGMP3_df["Machine Number"] == unit_number].sort_values(by="Code")
        EGMP4_data = EGMP4_df[EGMP4_df["Machine Number"] == unit_number].sort_values(by="Code")
        EGMP5_data = EGMP5_df[EGMP5_df["Machine Number"] == unit_number].sort_values(by="Code")
        Main_df=pd.concat([Unit_data, EGMP1_data, EGMP2_data,EGMP3_data,EGMP4_data,EGMP5_data], axis=0).drop('Machine Number', axis=1)
        if Main_df.empty:
            Main_df = Maint_Static_df.copy()
        else:
            existing_types = Main_df["Type"].astype(str).unique()
            if "Unit Visit" not in existing_types:
                unit_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "Unit Visit"].copy()
                Main_df = pd.concat([Main_df, unit_static_rows], axis=0)
            if "EGMP1" not in existing_types:
                EGMP1_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "EGMP1"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, EGMP1_static_rows], axis=0)
            if "EGMP2" not in existing_types:
                EGMP2_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "EGMP2"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, EGMP2_static_rows], axis=0)
            if "EGMP3" not in existing_types:
                EGMP3_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "EGMP3"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, EGMP3_static_rows], axis=0)
            if "EGMP4" not in existing_types:
                EGMP4_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "EGMP4"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, EGMP4_static_rows], axis=0)
            if "EGMP5" not in existing_types:
                EGMP5_static_rows = Maint_Static_df[Maint_Static_df["Type"] == "EGMP5"].copy().sort_values(by="Code")
                Main_df = pd.concat([Main_df, EGMP5_static_rows], axis=0)
        Main_df.rename(columns={col: 'Date' for col in Main_df.columns if col.startswith('DATE')}, inplace=True) 

        # --- Category Test Records Table ---
        Cat_data = Cat1_df[Cat1_df["Machine Number"] == unit_number].drop('Machine Number', axis=1)
        if Cat_data.empty:
            Cat_data = Cat_Static_df.copy()
        # Reset index after all append operations
        Cat_data = Cat_data.reset_index(drop=True)
        Cat_data.rename(columns={col: 'Date' for col in Cat_data.columns if str(col).startswith('DATE')}, inplace=True)
        # --- Repair Table ---
        Repair_data = Repair_df[Repair_df["Machine Number"] == unit_number].drop('Machine Number', axis=1)
        Call_data = Call_df[Call_df["Machine Number"] == unit_number].drop('Machine Number', axis=1)
        # --- Create tables ---
        main_table1 = create_table_with_header_escalator(Main_df[Main_df['Type'].isin(['Unit Visit', 'EGMP1'])])
        main_table2 = create_table_with_header_escalator(Main_df[Main_df['Type']=='EGMP2'])
        main_table3 = create_table_with_header_escalator(Main_df[Main_df['Type']=='EGMP3'])
        main_table4 = create_table_with_header_escalator(Main_df[Main_df['Type']=='EGMP4'])
        main_table5 = create_table_with_header_escalator(Main_df[Main_df['Type']=='EGMP5'])
        cat_table = create_table_with_header_escalator(Cat_data)
        if Repair_data.empty:
            data=[Repair_data.columns.tolist()] + Repair_data.values.tolist()
            table_repair=Table(data, colWidths=[180] * len(Repair_data.columns))
        else:
            table_repair = Table(wrap_table_data(Repair_data), repeatRows=1)
        if Call_data.empty:
            data=[Call_data.columns.tolist()] + Call_data.values.tolist()
            table_call=Table(data, colWidths=[180] * len(Call_data.columns))
        else:   
            table_call = Table(wrap_table_data(Call_data), repeatRows=1)

        # Apply common style to remaining tables
        table_style = TableStyle([
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ])
        for t in [table_repair, table_call]:
            if t: t.setStyle(table_style)
        # --- PDF Setup: Frame & PageTemplates ---
        buffer = BytesIO()  # In-memory buffer instead of local file
        doc = BaseDocTemplate(buffer, pagesize=landscape(A3), topMargin=250)
        fileName = f'MCP Records Escalator-{unit_number}.pdf'
        frame = Frame(30, 30, landscape(A3)[0] - 60, landscape(A3)[1] - 280, id='normal')
        doc.addPageTemplates([
            PageTemplate(id="Maintenance Records", frames=frame, onPage=lambda c, d: header(c, d, "Maintenance Records", section_data, title)),
            PageTemplate(id="Category Test Records", frames=frame, onPage=lambda c, d: header(c, d, "Category Test Records", section_data, title)),
            PageTemplate(id="Repair / Replacement Log", frames=frame, onPage=lambda c, d: header(c, d, "Repair / Replacement Log", section_data, title)),
            PageTemplate(id="Call Back Log", frames=frame, onPage=lambda c, d: header(c, d, "Call Back Log", section_data, title)),
            PageTemplate(id='Default', frames=default_frame)
        ])
        elements = []
        first_section = True
        # --- Maintenance Records Section ---
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(main_table1)
        first_section = False
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(main_table2)
        first_section = False
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(main_table3)
        first_section = False
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(main_table4)
        first_section = False
        elements.append(NextPageTemplate('Maintenance Records'))
        if not first_section:   
            elements.append(PageBreak())
        elements.append(main_table5)
        first_section = False
        # --- Category Test Records Section ---
        elements.append(NextPageTemplate('Category Test Records'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(cat_table)
        first_section = False
        # --- Repair / Replacement Log Section ---
        elements.append(NextPageTemplate('Repair / Replacement Log'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(table_repair)
        first_section = False
        # --- Call Back Log Section (wrapped columns) ---
        elements.append(NextPageTemplate('Call Back Log'))
        if not first_section:
            elements.append(PageBreak())
        elements.append(table_call)
        first_section = False
        # Then before Abb_Table:
        elements.append(NextPageTemplate('Default'))
        elements.append(PageBreak())
        elements.append(Abb_Table)
        # --- Build PDF ---
        doc.build(elements)
        # Upload directly to ADLS
        pdf_data = buffer.getvalue()  # Get PDF bytes from memory
        file_client = dir_client.create_file(fileName)
        pdf_data = buffer.getvalue()  # Get PDF bytes from memory
        file_client.append_data(data=pdf_data, offset=0, length=len(pdf_data))
        file_client.flush_data(len(pdf_data))
        buffer.close()