from _0_Dependencies import *
from _1_Sub_Func import *
from _2_Mysql import db
from extract_text_from_url import *

"""9/7/2025"""
"""create a dataframe for func_name and args"""
"""commit code, ignore các folders image và .env"""
"""gen-chart + original text -> new text"""
"""bỏ parsed macro time vì đã đổi về none hết rồi"""

load_dotenv()

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

    def text(self, model_name="gpt-4.1-nano", **kwargs):
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
            "- from_macro: None"
            "- to_macro: None"
            "- timeframe (daily, monthly, quarterly hoặc yearly)"
            "- series: danh sách các loại dữ liệu cần vẽ, mỗi phần tử gồm:"
            "   - func_name: tên hàm sẽ gọi trong combinechart, ví dụ: add_stock, add_interbank_vol, add_cpi, add_gdp_real, etc"
            "   - args: danh sách các tham số truyền vào, các giá trị này phải hợp lệ với func_name dựa trên metadata cung cấp, ví dụ: vnindex, Doanh số kỳ hạn qua đêm, Chỉ số giá tiêu dùng_mom, etc"
            "Chỉ chọn tối đa 5 series quan trọng nhất cho mỗi biểu đồ, ưu tiên các mã nổi bật hoặc được nhắc nhiều."
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
                    from_macro = item.get("from_macro")
                    to_macro = item.get("to_macro")
                    if from_macro == to_macro:
                        item["from_macro"] = None
                        item["to_macro"] = None

                    item["series"] = [
                        s for s in item.get("series", []) if s.get("args")
                    ]

                    for s in item["series"]:
                        func_name = s.get("func_name","")
                        if not func_name.startswith("add_"):
                            s["func_name"] = "add_"+ func_name

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
                from_macro = parsed.get("from_macro")
                to_macro = parsed.get("to_macro")
                if from_macro == to_macro:
                    parsed["from_macro"] =None
                    parsed["to_macro"] = None
                # Lọc series có args rỗng
                parsed["series"] = [
                    s for s in parsed.get("series", []) if s.get("args")
                ]
                for s in parsed["series"]:
                    func_name = s.get
                if not parsed["series"]:
                    return None
                return [parsed]
            return None

        except Exception as e:
            print("Input được trả về không đúng format:\n", self.gen_text)
            return None


# for key, value in GEN().get_all_macro_types().items():
#     print(f"{key}: {str(value)[0:100]}")
# print(GEN().text(data_name="BSC MACRO - Cập nhật diễn biến cuộc chiến thuế quan 2025"))

# print(GEN().get_input_url(data_name="BSC MACRO - Cập nhật diễn biến cuộc chiến thuế quan 2025"))
