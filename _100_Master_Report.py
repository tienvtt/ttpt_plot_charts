from _0_Dependencies import *
from _1_Sub_Func import *
from _1_Sub_Xlwings import *
from _2_Mysql import *


class master_report:
    def __init__(
        self,
        report_name="master_report",
        xlsx_template_path=None,
        xlsx_print_area=None,
        input_dict=None,
        **kwargs,
    ):
        print(f"\nCreating report [{report_name}]")

        self.report_name = report_name
        self.xlsx_template_path = xlsx_template_path
        self.xlsx_print_area = xlsx_print_area
        self.input_dict = input_dict
        self.kwargs = kwargs

        self.created_at = datetime.now()
        self.created_at_str = self.created_at.strftime("%Y%m%d_%H%M%S_%f")

        self.xlsx_path = get_path(
            "Database", "Temp", f"{self.created_at_str}_{self.report_name}.xlsx"
        )
        self.pdf_path = get_path(
            "Database", "Temp", f"{self.created_at_str}_{self.report_name}.pdf"
        )
        self.create()

        if not os.path.exists(self.xlsx_path):
            self.xlsx_path = None
        if not os.path.exists(self.pdf_path):
            self.pdf_path = None

        print(
            f"""\nReport [{self.report_name}] Done!\n- xlsx_path: {self.xlsx_path}\n- pdf_path: {self.pdf_path}"""
        )

    class Inner(object):
        pass

    def replace_text_cell(self, ws):
        used_range = ws.used_range
        active_arr = used_range.value
        if active_arr:
            for r, row in enumerate(active_arr):
                for c, cell in enumerate(row):
                    if not any([key in str(cell) for key in ["chart_", "image_"]]):
                        try:
                            active_arr[r][c] = cell.format(**self.input_dict)
                        except Exception as error:
                            pass
            used_range.value = active_arr
        return ws

    def replace_image_cell(self, ws):
        from PIL import Image

        used_range = ws.used_range
        active_arr = used_range.value

        def get_range_block(ws, data_key):
            active_range_data = ws.used_range.value
            base_row, base_col = ws.used_range.row, ws.used_range.column

            top_left_cell, bot_right_cell = None, None
            for row_idx, row in enumerate(active_range_data):
                if not isinstance(row, list):
                    continue
                for col_idx, cell_value in enumerate(row):
                    if isinstance(cell_value, str) and data_key in cell_value:
                        abs_row = base_row + row_idx
                        abs_col = base_col + col_idx
                        if top_left_cell is None:
                            top_left_cell = (abs_row, abs_col)
                        bot_right_cell = (abs_row, abs_col)

            if top_left_cell and bot_right_cell:
                img_first_address = excel_tuple_to_code(top_left_cell)
                img_last_address = excel_tuple_to_code(bot_right_cell)
                return f"{img_first_address}:{img_last_address}"

        if active_arr:
            image_keys = np.unique(
                [
                    f
                    for f in np.array(active_arr).flatten()
                    if any([key in str(f) for key in ["chart_", "image_"]])
                ]
            )

            for key in image_keys:
                target_range = get_range_block(ws, key)
                if target_range:
                    key = key.replace("{", "").replace("}", "")
                    cell = ws.range(target_range)
                    cell.clear_contents()

                    if "chart_" in key and key in self.input_dict:
                        image_path = self.input_dict[key](
                            width=cell.width, height=cell.height
                        )
                    elif "image_" in key and key in self.input_dict:
                        image_path = self.input_dict[key]
                    else:
                        continue

                    print(
                        f">> {key}: {target_range}\nParas: {[cell.left, cell.top, cell.width, cell.height]}"
                    )

                    # Điều chỉnh tỷ lệ hình ảnh
                    if image_path == None:
                        continue
                    with Image.open(image_path) as img:
                        img_width, img_height = img.size
                        cell_ratio = cell.height / cell.width
                        img_ratio = img_height / img_width

                        if img_ratio > cell_ratio:
                            # Hình ảnh cao hơn vùng ô -> điều chỉnh chiều cao
                            new_height = cell.height
                            new_width = new_height / img_ratio
                        else:
                            # Hình ảnh rộng hơn vùng ô -> điều chỉnh chiều rộng
                            new_width = cell.width
                            new_height = new_width * img_ratio

                    picture = ws.pictures.add(
                        image_path,
                        left=cell.left
                        + (cell.width - new_width) / 2,  # Căn giữa theo chiều ngang
                        top=cell.top
                        + (cell.height - new_height) / 2,  # Căn giữa theo chiều dọc
                        width=new_width,
                        height=new_height,
                    )
                    time.sleep(1)
                    ws.api.Shapes(picture.name).ZOrder(1)

        return ws

    def create_xlsx_file(self):
        print(f">> Filling xlsx template")
        wb = self.app.books.open(self.xlsx_template_path)
        for idx, ws in enumerate(wb.sheets):
            ws.activate()
            ws = self.replace_text_cell(ws)
            ws = self.replace_image_cell(ws)
        wb.save(self.xlsx_path)
        wb.close()

    def concatenate_pdfs(self, paths):
        import fitz

        print(f">> Concatenate pdf_paths!")
        output_pdf = fitz.open()
        for path in paths:
            input_pdf = fitz.open(path)
            output_pdf.insert_pdf(input_pdf)
        output_pdf.save(self.pdf_path)

    def create_pdf_file(self):
        if self.xlsx_path and os.path.exists(self.xlsx_path):
            app = xw.App(visible=False)
            wb = app.books.open(self.xlsx_path)
            pdf_paths = []
            for idx, ws in enumerate(wb.sheets):
                idx = idx + 1
                pdf_path = self.pdf_path.replace(".pdf", f"{idx}.pdf")
                print(f">> Creating sub_pdf {idx}...")
                ws.api.PageSetup.Orientation = 1
                ws.api.PageSetup.CenterHorizontally = True
                ws.api.PageSetup.CenterVertically = True
                ws.api.PageSetup.LeftMargin = 0
                ws.api.PageSetup.RightMargin = 0
                ws.api.PageSetup.TopMargin = 0
                ws.api.PageSetup.BottomMargin = 0
                ws.api.PageSetup.PrintArea = self.xlsx_print_area
                ws.api.PageSetup.FirstPageNumber = 1
                ws.api.PageSetup.Order = 1
                ws.api.PageSetup.Zoom = False
                ws.api.PageSetup.FitToPagesWide = 1
                ws.api.PageSetup.FitToPagesTall = 1
                ws.api.ExportAsFixedFormat(
                    Type=0,
                    Filename=pdf_path,
                    Quality=0,
                    IncludeDocProperties=True,
                    IgnorePrintAreas=False,
                    From=1,
                    To=1,
                    OpenAfterPublish=False,
                )
                pdf_paths.append(pdf_path)
            wb.save()
            wb.close()
            app = xw.apps.active
            if app:
                app.quit()
            self.concatenate_pdfs(pdf_paths)

    def sharepoint_url(self):
        print(f"\n>> Creating sharepoint_url...")
        return None

        if self.pdf_path and os.path.exists(self.pdf_path):
            file_name = os.path.basename(self.pdf_path)
            shutil.copy(self.pdf_path, SHAREPOINT_FOLDER)
            file_name = os.path.basename(self.pdf_path)
            file_name_without_pdf, _ = os.path.splitext(file_name)
            url = f"{SHAREPOINT_BEGIN_URL}{file_name_without_pdf}{SHAREPOINT_END_URL}"
            print(f"Result: {url}")
            return url

    def create(self):
        self.app = xw.App(visible=False)
        self.create_xlsx_file()
        self.app = xw.apps.active
        if self.app:
            self.app.quit()
        self.create_pdf_file()
