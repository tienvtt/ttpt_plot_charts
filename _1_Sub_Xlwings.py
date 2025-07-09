from _0_Dependencies import *
from _0_Hardcode import *

# wb -> ws=wb.sheets['sheet_name'] -> cell = ws['A1']

def excel_create_sheet(workbook, name):
    sheet_names = [s.name for s in workbook.sheets]
    return workbook.sheets.add(name) if name not in sheet_names else None
        
def excel_notification(header, body):
    win32api.MessageBox(xw.apps.active.api.Hwnd, body, header, win32con.MB_ICONINFORMATION)

def excel_active_sheet():
    return xw.apps.active.books.active.sheets.active

def excel_sheet_gridlines(target_sheet, turn_on=True):
    if not turn_on:
        current_sheet = excel_active_sheet()
        target_sheet.activate()
        xw.apps.active.api.ActiveWindow.DisplayGridlines = turn_on
        current_sheet.activate()

def excel_sheet_freeze(target_sheet, cell_code=None):
    if cell_code!=None and isinstance(cell_code, str):
        target_sheet[cell_code].api.Activate()
        target_sheet.api.Application.ActiveWindow.FreezePanes = True

def excel_format_cell(range_obj, format_dict={}):
    # SAMPLE_FORMAT_DICT = {
    #     'data': number/string,
    #     'formula': string,
    #     'data_format': string,
    #     'row_fit': True/False,
    #     'column_fit': True/False,
    #     'font_size': number,
    #     'font_bold': True/False,
    #     'font_italic': True/False,
    #     'font_underline': True/False,
    #     'font_strikethrough': True/False,
    #     'font_color': RGB_tuple,
    #     'cell_color': RGB_tuple,
    #     'border_left': True/False,
    #     'border_right': True/False,
    #     'border_top': True/False,
    #     'border_bot': True/False,
    #     'border_all': True/False,
    #     'align': right/center/left,
    #     'vertical_align': right/center/left,
    # }
    
    border_num = {'border_left': 7,'border_top': 8,'border_bot': 9,'border_right': 10,}
    
    xlContinuous, xlDash, xlDot, xlDouble, xlLineStyleNone = 1, -4115, -4118, -4119, -4142 # BORDER VAR
   
    xlGeneral, xlLeft, xlCenter, xlRight = 1, -4131, -4108, -4152  # ALIGN VAR
    
    xlHairline, xlThin, xlMedium, xlThick = 1, 2, -4138, 4 # LINEWIDTH VAR
    
    for key, value in format_dict.items():
        if value == None: continue

        key = key.lower()

        try:
            if key == 'data': range_obj.value = value

            if key == 'formula': range_obj.formula2 = value

            if key == 'data_format': range_obj.api.NumberFormat = value

            if key == 'row_fit' and value: range_obj.api.Rows.AutoFit()

            if key == 'column_fit' and value: range_obj.api.Columns.AutoFit()

            if key == 'font_size': range_obj.api.Font.Size = value

            if key == 'font_bold': range_obj.api.Font.Bold = value
           
            if key == 'font_italic': range_obj.api.Font.Italic = value
           
            if key == 'font_underline': range_obj.api.Font.Underline = value
           
            if key == 'font_strikethrough': range_obj.api.Font.Strikethrough = value
            
            if key == 'font_color': range_obj.api.Font.Color = xw.utils.rgb_to_int(COLOR_DICT[value])

            if key == 'cell_color': range_obj.api.Interior.Color = xw.utils.rgb_to_int(COLOR_DICT[value])

            if key in ['border_left', 'border_right', 'border_top', 'border_bot', 'border_all']:
                if key == 'border_all':
                    for k, v in border_num.items():
                        range_obj.api.Borders(border_num[k]).LineStyle  = xlContinuous
                        range_obj.api.Borders(border_num[k]).Weight = xlThin
                else:
                    range_obj.api.Borders(border_num[key]).LineStyle  = xlContinuous
                    range_obj.api.Borders(border_num[key]).Weight = xlThin

            if key == 'align':
                if str(value) == 'right': range_obj.api.HorizontalAlignment = xlRight
                if str(value) == 'center': range_obj.api.HorizontalAlignment = xlCenter
                if str(value) == 'left': range_obj.api.HorizontalAlignment = xlLeft

            if key == 'vertical_align':
                if str(value) == 'right': range_obj.api.VerticalAlignment  = xlRight
                if str(value) == 'center': range_obj.api.VerticalAlignment  = xlCenter
                if str(value) == 'left': range_obj.api.VerticalAlignment  = xlLeft
            
        except Exception as error:
            print(f'Format error: {error}')


def excel_column_name(index):
    """Convert a 1-based index to an Excel column name."""
    name = ''
    while index > 0:
        index -= 1
        name = chr(index % 26 + ord('A')) + name
        index = index // 26
    return name.upper()

def excel_column_index(name):
    """Convert an Excel column name to a zero-based index."""
    index = 0
    for char in name:
        index = index * 26 + (ord(char.upper()) - ord('A') + 1)
    return int(index)

def excel_code_to_tuple(cell_code):
    cell_code = str(cell_code).replace('$', '')
    """Split an Excel cell reference into column letter and row number."""
    match = re.match(r"([A-Z]+)([0-9]+)", cell_code, re.I)
    if match:
        column = match.group(1)
        row = match.group(2)
        return int(row), excel_column_index(column)
    else:
        raise ValueError(f"Invalid cell reference: {cell_code}")
    
def excel_tuple_to_code(cell_tuple):
    cell_code = f'{excel_column_name(cell_tuple[1])}{cell_tuple[0]}'
    return cell_code

def excel_cell_offset(cell, offset_tuple):
    if isinstance(cell, str): cell = excel_code_to_tuple(cell)
    new_cell_tuple = (cell[0]+offset_tuple[0], cell[1]+offset_tuple[1])
    return excel_tuple_to_code(new_cell_tuple)

def excel_format_chart(range_obj, series_dicts=[]):
    def get_series_type_num(chart_type):
        # /// vba_chart_num_dict is in _0_Hardcode ///
        chart_type = str(chart_type).lower()
        if chart_type == 'line':
            return vba_chart_num_dict['xlLine']
        if chart_type == 'bar':
            return vba_chart_num_dict['xlColumnClustered']
        if chart_type == 'line_marker':
            return vba_chart_num_dict['xlLineMarkers']

    chart_api = range_obj.sheet.charts.add(top=range_obj.top, left=range_obj.left).api[1]

    # DEFAULT FORMAT CHART
    chart_api.HasTitle = True
    chart_api.ChartTitle.Text = str('')

    for idx, series_dict in enumerate(series_dicts):
        idx = idx + 1
        type_num = get_series_type_num(series_dict['series_type'])
       
        if type_num:
            chart_api.SeriesCollection().NewSeries()
            chart_series = chart_api.SeriesCollection(idx)

            chart_series.AxisGroup = 2 if series_dict['is_secondary'] else 1
            chart_series.XValues = series_dict['x_range'].api
            chart_series.Values = series_dict['y_range'].api
            chart_series.Name = series_dict['series_name']
            chart_series.ChartType = type_num

            # line_color = series_dict['line_color'] if 'line_color' in series_dict else None
            # fill_color = series_dict['fill_color'] if 'fill_color' in series_dict else None
            # if line_color not in COLOR_DICT: line_color = None
            # if fill_color not in COLOR_DICT: fill_color = None

            # all_colors = [color for color in COLOR_DICT if color not in ['black', 'white']]
            # if line_color==None or fill_color==None:
            #     line_color = random.choice(all_colors)
            #     line_color = fill_color

            # chart_series.Format.Line.ForeColor.RGB = xw.utils.rgb_to_int(COLOR_DICT[line_color])
            # chart_series.Format.Fill.ForeColor.RGB = xw.utils.rgb_to_int(COLOR_DICT[fill_color])

def excel_chart_dicts(series_arr):
    # series_arr = [
    #     # chart_name, series_type, series_name, x_table_loc, y_table_loc, is_secondary
    #     ['test', 'line', 'test_s1', ((1,4), (189,4)), ((1,5), (189,5)), False],
    #     ['test', 'line', 'test_s2', ((1,4), (189,4)), ((1,6), (189,6)), True],
    #     ['test_2', 'bar', 'test_s1', ((1,4), (189,4)), ((1,7), (189,7)), False],
    #     ['test_2', 'bar', 'test_s2', ((1,4), (189,4)), ((1,8), (189,8)), True],
    # ]
    if not isinstance(series_arr, np.ndarray):
        series_arr = np.array(series_arr, dtype='object')
    
    chart_dicts = []
    if len(series_arr)!=0:

        chart_names = []
        for chart_name in series_arr[:,0]:
            if chart_name not in chart_names:
                chart_names.append(chart_name)
                
        # chart_names = np.unique(series_arr[:,0])

        for chart_name in chart_names:
            chart_arr = series_arr[series_arr[:,0]==chart_name]
            series_dicts = [{
                'series_type': series[1],
                'series_name': series[2],
                'x_table_loc': series[3],
                'y_table_loc': series[4],
                'is_secondary': series[5],
            } for series in chart_arr]

            if len(series_dicts)!=0:
                chart_dicts.append({
                'chart_name': chart_name,
                'series_dicts': series_dicts,
            })
            
    return chart_dicts


def get_cell_format_dict(table_data_arr, cell_loc, datetime_format="dd/mm/yyyy", number_format="#,##0.00"):
	row, col = cell_loc
	is_header, is_field = row==0, col==0
	data = table_data_arr[row, col]
	
	# DEFAULT FORMAT
	border_all = True
	align = 'left' if is_field else 'right'
	font_bold = is_field or is_header

	# COLOR FORMAT
	is_VDSC_color = 'teal' not in COLOR_DICT
	font_color = 'white' if is_header else 'black'
	
	if is_VDSC_color:
		cell_color = 'red' if is_header else None
	else:
		cell_color = 'dark_teal' if is_header else None

	if isinstance(data, float) or isinstance(data, int): data_format = number_format
	elif isinstance(data, datetime): data_format = datetime_format
	else: data_format = None

	return {
		'data_format': data_format,
		'font_color': font_color,
		'cell_color': cell_color,
		'font_bold': font_bold,
		'border_all': border_all,
		'align': align,
	}