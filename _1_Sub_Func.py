from _0_Dependencies import *
from _0_Hardcode import *

def get_path(root, folder, file, code_folder=CODE_FOLDER):
    folder_path = glob(f'{code_folder}\{root}/**/' + folder, recursive=True)[0]
    return folder_path + (('\\' + file) if file!=None else '')

def dayname_datetime(dayname):
	return datetime.strptime(str(dayname), '%Y%m%d') \
		if len(str(dayname))==8 else datetime.strptime(str(dayname), '%Y%m%d %H:%M:%S')

def datetime_dayname(date, full):
	return date.strftime('%Y%m%d %H:%M:%S') if full else date.strftime('%Y%m%d')

def decode_time_code(time_code): # return from, to
	import calendar
	if time_code[0]=='N':
		now = datetime.now()
		return now, now
	if time_code[0]=='A':
		at = datetime.strptime(time_code, 'A%Y_%m_%d_%H_%M_%S')
		return at, at
	if time_code[0]=='D':
		date = datetime.strptime(time_code, 'D%Y_%m_%d')
		return date.replace(hour=0, minute=0, second=0), \
			date.replace(hour=23, minute=59, second=59)
	if time_code[0]=='M':
		date = datetime.strptime(time_code, 'M%Y_%m')
		year, month = date.year, date.month
		begin_date = datetime(year, month, 1)
		last_day = calendar.monthrange(year, month)[1]
		end_date = datetime(year, month, last_day)
		return begin_date, end_date
	if time_code[0]=='Q':
		year = int(time_code[1:5])
		quarter = int(time_code[6:])
		quarter_month_map = {
			1: (1, 3),  # Q1: January to March
			2: (4, 6),  # Q2: April to June
			3: (7, 9),  # Q3: July to September
			4: (10, 12) # Q4: October to December
		}
		start_month, end_month = quarter_month_map[quarter]
		begin_date = datetime(year, start_month, 1)
		last_day = calendar.monthrange(year, end_month)[1]
		end_date = datetime(year, end_month, last_day)
		return begin_date, end_date
	if time_code[0]=='Y':
		date = datetime.strptime(time_code, 'Y%Y')
		begin_date = datetime(date.year, 1, 1)
		end_date = datetime(date.year, 12, 31)
		return begin_date, end_date

def encode_time_code(datetime_obj, ecode_type):
	ecode_type = str(ecode_type).upper()
	if ecode_type=='A': return datetime_obj.strftime('A%Y_%m_%d_%H_%M_%S')
	if ecode_type=='D': return datetime_obj.strftime('D%Y_%m_%d')
	# if ecode_type=='W': return datetime_obj.strftime('W%Y_%U')
	if ecode_type=='M': return datetime_obj.strftime('M%Y_%m')
	if ecode_type=='Q': return datetime_obj.strftime('Q%Y_0') + str((datetime_obj.month-1)//3+1)
	# if ecode_type=='H': return datetime_obj.strftime('H%Y_0') + str(int(datetime_obj.month>6)+1)
	if ecode_type=='Y': return datetime_obj.strftime('Y%Y')

def Mbox(title, text, style):
	import ctypes
	return ctypes.windll.user32.MessageBoxW(0, text, title, style)

def download_file(url, save_path):
    try:
        # Send a GET request to the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Open the destination file and write the content in chunks
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                file.write(chunk)
    except Exception as e:
        print(f">> Error: {e}")

def read_pdf_report(report_path):
	content = ''
	if os.path.exists(report_path):
		reader = PdfReader(report_path)
		for page in reader.pages:
			content += page.extract_text()
	return content

def send_data_to_gg_spreadsheet(sheet_id, sheet_name, cell_code, data, credentials):
	try:
		if isinstance(data, np.ndarray): data = data.tolist()
		scopes = ['https://www.googleapis.com/auth/spreadsheets']

		credentials = Credentials.from_service_account_info(credentials, scopes=scopes)
		service = build('sheets', 'v4', credentials=credentials)

		sheet = service.spreadsheets()

		# CLEAR CONTENT
		sheet.values().clear(
			spreadsheetId=sheet_id,
			range=f"{sheet_name}"
		).execute()
		
		# ADD CONTENT
		sheet.values().update(
			spreadsheetId=sheet_id,
			range=f"{sheet_name}!{cell_code}",
			valueInputOption="RAW",
			body={'values': data}
		).execute()

		print(f'>> send_data to gg_spreadsheet successed!')
	except Exception as error:
		print(f'>> send_data to gg_spreadsheet error: {error}')

def get_final_pdf_path(pdf_paths, final_pdf_path):
	try:
		merger = PdfMerger()
		for pdf_path in pdf_paths:
			merger.append(pdf_path)
		merger.write(final_pdf_path)
		merger.close()
		return final_pdf_path
	except Exception as error:
		print(f'>> Merge PDFs error: {error}!!')
		return None
	
def import_csv(csv_path):
	with open(csv_path, 'r', encoding='utf-8') as file:
		return np.array(list(csv.reader(file, delimiter=",")), dtype="object")

def color_tupple(color_name):
	return tuple(_ /255 for _ in COLOR_DICT[color_name])