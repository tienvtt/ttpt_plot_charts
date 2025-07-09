from _0_Dependencies import *
from _1_Sub_Func import *
from _1_Sub_Xlwings import *
from _2_Mysql import *

class master_report():
	def __init__(self,
			report_name = 'master_report',
			xlsx_template_path = None,
			xlsx_print_area = None,
			input_dict = None,
			**kwargs
		):
		
		self.report_name = report_name
		self.xlsx_template_path = xlsx_template_path
		self.xlsx_print_area = xlsx_print_area
		self.input_dict = input_dict
		self.kwargs = kwargs

		self.created_at = datetime.now()
		self.created_at_str = self.created_at.strftime("%Y%m%d_%H%M%S_%f")
		self.xlsx_path = get_path('Database', 'Temp', f'{self.created_at_str}_{self.report_name}.xlsx')
		self.pdf_path = get_path('Database', 'Temp', f'{self.created_at_str}_{self.report_name}.pdf')
		self.create()

		if not os.path.exists(self.xlsx_path): self.xlsx_path = None
		if not os.path.exists(self.pdf_path): self.pdf_path = None

		print(f"""\n\nCreate Report [{self.report_name}] Done!\n- xlsx_path: {self.xlsx_path}\n- pdf_path: {self.pdf_path}""")

	class Inner(object):
		pass
		
	def replace_text_cell(self, ws):
		used_range = ws.used_range
		active_arr = used_range.value
		for r, row in enumerate(active_arr):
			for c, cell in enumerate(row):
				if not any([key in str(cell) for key in ['chart_', 'image_']]):
					try:
						active_arr[r][c] = cell.format(**self.input_dict)
					except Exception as error:
						pass
		used_range.value = active_arr
		return ws

	def replace_image_cell(self, ws):
		used_range = ws.used_range
		active_arr = used_range.value

		def get_range_block(ws, data_key):
			active_range_data = ws.used_range.value 
			base_row, base_col = ws.used_range.row, ws.used_range.column 

			top_left_cell, bot_right_cell = None, None
			for row_idx, row in enumerate(active_range_data):
				if not isinstance(row, list): continue
				for col_idx, cell_value in enumerate(row):
					if isinstance(cell_value, str) and data_key in cell_value:
						abs_row = base_row + row_idx
						abs_col = base_col + col_idx
						if top_left_cell is None: top_left_cell = (abs_row, abs_col) 
						bot_right_cell = (abs_row, abs_col)

			if top_left_cell and bot_right_cell:
				img_first_address = excel_tuple_to_code(top_left_cell)
				img_last_address = excel_tuple_to_code(bot_right_cell)
				return f"{img_first_address}:{img_last_address}"
			
		image_keys = np.unique([f for f in np.array(active_arr).flatten()
			if any([key in str(f) for key in ['chart_', 'image_']])
			])

		for key in image_keys:
			target_range = get_range_block(ws, key)
			if target_range:
				key = key.replace('{','').replace('}','')
				cell = ws.range(target_range)
				cell.clear_contents()

				if 'chart_' in key:
					image_path = self.input_dict[key](width=cell.width, height=cell.height)
				elif 'image_' in key:
					image_path = self.input_dict[key]
				else:
					image_path = None

				crop_image_path = get_path('Database', 'Temp', f'{datetime.now().strftime("%Y%m%d_%H%M%S_%f")}.jpg')
				
				image_ratio = cell.height/cell.width
				with Image.open(image_path) as image_obj:
					width, height = image_obj.size
					w = 1; h = w*image_ratio
					while w<width and h<height:
						w += 1; h = w*image_ratio
					x, y = (width - w)//2, (height - h)//2
					image_obj.crop((x, y, x + w, y + h)).convert("RGB").save(crop_image_path)

				picture = ws.pictures.add(crop_image_path, left=cell.left, top=cell.top, width=cell.width, height=cell.height)
				self.app.api.ActiveSheet.Shapes(picture.name).ZOrder(1)

				if os.path.exists(crop_image_path): os.remove(crop_image_path)
		
		return ws

	def create_xlsx_file(self):
		wb = self.app.books.open(self.xlsx_template_path)
		for ws in wb.sheets:
			ws = self.replace_text_cell(ws)
			ws = self.replace_image_cell(ws)
		wb.save(self.xlsx_path)
		wb.close()

	def create_pdf_file(self):
		if self.xlsx_path and os.path.exists(self.xlsx_path):
			app = xw.App(visible=False)
			wb = app.books.open(self.xlsx_path)
			ws = wb.sheets[0]
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
				Filename=self.pdf_path,
				Quality=0, 
				IncludeDocProperties=True,
				IgnorePrintAreas=False, 
				From=1,
				To=1,
				OpenAfterPublish=False
			)
			wb.save()
			wb.close()
			app = xw.apps.active
			if app: app.quit()

	def sharepoint_url(self):
		if self.pdf_path and os.path.exists(self.pdf_path):
			file_name = os.path.basename(self.pdf_path)
			shutil.copy(self.pdf_path, SHAREPOINT_FOLDER)
			file_name = os.path.basename(self.pdf_path)
			file_name_without_pdf, _ = os.path.splitext(file_name)
			return f'{SHAREPOINT_BEGIN_URL}{file_name_without_pdf}{SHAREPOINT_END_URL}'

	def create(self):
		self.app = xw.App(visible=False)
		self.create_xlsx_file()
		self.app = xw.apps.active
		if self.app: self.app.quit()
		self.create_pdf_file()

