from _0_Dependencies import *
from _1_Sub_Func import *
from _2_Mysql import db
from _100_Master_Report import *
from _100_Product_Input import *

# PyMuPDF, pandas, matplotlib, fitz, Pillow, openai


def call_macro_report(
    xlsx_print_area: str,
    xlsx_prompt_path: str,
    xlsx_template_path: str,
    input_dict={},
    return_input_dict=False,
):
    create_at = datetime.now()

    data_dict = {
        "public_date": create_at.strftime("%d/%m/%Y"),
        **input_dict,
    }

    input_dict = macro_report(
        xlsx_prompt_path=xlsx_prompt_path,
        data_dict=data_dict,
    ).input_dict

    if return_input_dict == True:
        xw.Book().sheets[0]["A1"].value = [["data_key", "value"]] + [
            [key, str(value)[0:1000]]
            for key, value in input_dict.items()
            if not isinstance(value, dict)
        ]
    else:
        report_obj = master_report(
            xlsx_print_area=xlsx_print_area,
            xlsx_template_path=xlsx_template_path,
            input_dict=input_dict,
        )
        if report_obj.xlsx_path and os.path.exists(report_obj.xlsx_path):
            subprocess.Popen(["explorer", report_obj.xlsx_path], shell=True)
        if report_obj.pdf_path and os.path.exists(report_obj.pdf_path):
            subprocess.Popen(["explorer", report_obj.pdf_path], shell=True)


if __name__ == "__main__":
    call_macro_report(
        xlsx_print_area="$A$1:$BM$63",
        xlsx_template_path=get_path(
            "Database", "XLSX_Template", "202507xx_macro_report_template.xlsx"
        ),
        xlsx_prompt_path=get_path(
            "Database", "XLSX_Template", "202507xx_macro_report_prompt.xlsx"
        ),
        input_dict={
            "data_name": "POW IR_NEWS - Tình hình sản xuất kinh doanh T2-2025 và kế hoạch T3-2025"
        },
    )

    pass
