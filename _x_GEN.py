from _0_Dependencies import *
from _1_Sub_Func import *
from _2_Mysql import db
import base64
from extract_text_from_url import *

load_dotenv()

""""
dynamic charts: 
 - dựa trên số lượng json objects -> vẽ chart, đối với những chart có input không hợp lệ -> bỏ

dynamic page in repport
generate valid chart->import into template, mỗi page là 1 valid chart

"""
"""
list func_name và args vào một dataframe
gpt đọc, compare with....nội dung trong text, rút ra các lệnh vẽ combinechart 

"""

"""
1. just read the url -> extract into main paras (sub titles)
2. call gpt for each title ->  funcs and args 
---> list of funcs and args 
"""

class GEN:
    def __init__(self):
        self.prompt = ""
        pass

    def get_all_macro_types(self):
        macro_types = ["stockprice", "foreign", "finance", "economy", "government"]
        result = {}

        for macro in macro_types:
            records = db.importing_objs(macro)
            if records is None or len(records) == 0:
                continue

            if macro == "stockprice":
                result[macro] = list(
                    set(r.data_name for r in records if hasattr(r, "data_name"))
                )
                continue

            result[macro] = {}
            data_names = list(
                set(r.data_name for r in records if hasattr(r, "data_name"))
            )

            for data_name in data_names:
                records_by_name = db.importing_objs(macro, data_name=data_name)
                if records_by_name is None or len(records_by_name) == 0:
                    continue

                sample = next(
                    (
                        r
                        for r in records_by_name
                        if hasattr(r, "data_dict") and r.data_dict
                    ),
                    None,
                )
                if sample is None:
                    continue

                result[macro][data_name] = list(sample.data_dict.keys())

        return result

    def get_input_url(self, data_name):
        records = db.importing_objs("analysis_report", data_name=data_name)
        if len(records) == 0:
            print(f"No data for {data_name}")
            return None
        for record in records:
            url = record.data_dict.get("URL")
            if not url:
                print("No url found for {data_name}")
                continue
            return url
        return None

    def text(self, model_name="gpt-4o", **kwargs):
        # self.url = kwargs['input_url']

        begin = datetime.now()
        self.gen_text = ""
        self.prompt_len = 0

        input_url = kwargs.get("input_url", None)
        data_name = kwargs.get("data_name", None)
        if (not input_url or len(input_url)) and data_name:
            input_url = self.get_input_url(data_name)
        if not input_url:
            raise ValueError("Thiếu input_url")

        macro_metadata = kwargs.get("macro_metadata", None)
        if not macro_metadata:
            macro_metadata = self.get_all_macro_types()

        input_text = extract_text_from_pdf_url(input_url)

        system_prompt = (
            "Bạn là một data analyst chuyên nghiệp có khả năng đọc báo cáo phân tích thành thạo."
            "Dựa trên nội dung của các input, hãy trả về output có format là một list các json object, các json object có format:"
            "- from_macro: mốc thời gian bắt đầu, định dạng phải phù hợp với timeframe:"
            "    + daily: DYYYY_MM_DD (ví dụ: D2025_07_01)"
            "    + monthly: MYYYY_MM (ví dụ: M2025_07)"
            "    + quarterly: QYYYY_QN (ví dụ: Q2025_02)"
            "    + yearly: YYYY (ví dụ: Y2025)"
            "- to_macro: mốc thời gian kết thúc, nếu to_macro giống from_macro, thì from_macro được lấy từ mốc nhỏ hơn to_macro 1 năm, định dạng to_macro vẫn giống from_macro"
            "- timeframe (daily, monthly, quarterly, yearly)"
            "- series: danh sách các loại dữ liệu cần vẽ, mỗi phần tử gồm:"
            "   - func_name: tên hàm sẽ gọi trong combinechart, ví dụ: add_stock, add_interbank_vol"
            "   - args: danh sách các tham số truyền vào, các giá trị này phải hợp lệ với func_name dựa trên metadata cung cấp và không được để trống."
            "Dữ liệu của mỗi JSON object trên được phân tích từ mỗi nội dung chính của bài báo cáo input (bài báo cáo có thể có nhiều chủ đề dựa trên khả năng brainstorm của bạn)"
        )
        user_prompt = f"""Đây là file báo cáo: "{input_text}"
        Dưới đây là metadata (danh sách các dữ liệu có sẵn):
        {json.dumps(macro_metadata, ensure_ascii=False, indent=2)}
        Hãy đảm bảo rằng args của func_name (tương ứng) có trong metadata 
        Nếu không tìm thấy bất kỳ args nào hợp lệ với func_name dựa trên metadata, không sinh ra object đó trong list kết quả
        Hãy trích xuất thông tin biểu đồ phù hợp dưới dạng JSON đúng cấu trúc như đã mô tả. Các dữ liệu trong series,  phải có trong metadata.
        """
        client = openai.OpenAI(api_key=os.getenv("openai.api_key"))
        try:
            response = client.chat.completions.create(
                model=model_name.replace("_paid", ""),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
                seed=42,
            )
            self.gen_text = response.choices[0].message.content.strip()
        except Exception as error:
            print(f"\n> GEN Text error: {error}")
            self.gen_text = f"error: {error}"

        if self.gen_text.strip() == "NO_MATCH":
            return None
        content_cleaned = re.sub(
            r"^```json\s*|\s*```$", "", self.gen_text.strip(), flags=re.MULTILINE
        )
        try:
            parsed = json.loads(content_cleaned)
            if isinstance(parsed, list):
                if not parsed:
                    return None
                filtered = []
                seen = set()
                for item in parsed:
                    # Lọc series có args rỗng
                    item["series"] = [
                        s for s in item.get("series", []) if s.get("args")
                    ]
                    # Nếu series rỗng thì bỏ qua object này
                    if not item["series"]:
                        continue
                    # Loại trùng lặp func_name + args trên toàn bộ list
                    for s in item["series"]:
                        key = (s["func_name"], tuple(s["args"]))
                        if key in seen:
                            break
                        seen.add(key)
                    else:
                        filtered.append(item)
                return filtered if filtered else None

            if isinstance(parsed, dict) and parsed.get("series"):
                # Lọc series có args rỗng
                parsed["series"] = [
                    s for s in parsed.get("series", []) if s.get("args")
                ]
                if not parsed["series"]:
                    return None
                return [parsed]
            return None

        except Exception as e:
            print("Input được trả về không đúng format:\n", self.gen_text)
            return None


# for key, value in GEN().get_all_macro_types().items():
#     print(f"{key}: {str(value)[0:100]}")
# print(
#     GEN().text(
#         model_name="gpt-4o-mini",
#         input_url="https://vdsc.com.vn/data/api/app/file-storage/bef66b74-6cc1-48a7-c018-08ddb9e21993/B%E1%BA%A3n%20tin%20s%C3%A1ng_20250704.pdf?downloadFrom=ManagementReport-9904",
#     )
# )
# print(GEN().get_input_url(data_name="VNDS STRATEGY - Thị trường năm mới, tầm cao mới"))
# print(GEN().get_input_url(data_name="SSI STRATEGY - Nhìn xa trông rộng"))
