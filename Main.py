
from _100_Master_Report import *
from _100_Product_Input import *

if __name__ == '__main__':
	
	xlsx_print_area = '$A$1:$BM$63'
	xlsx_template_path = get_path('Database', 'XLSX_Template', 'xxxxxxxx_sample_template.xlsx')
	report_obj = master_report(
		xlsx_print_area = xlsx_print_area,
		xlsx_template_path = xlsx_template_path,
		input_dict = product_test().input_dict,
	)

	