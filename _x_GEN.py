from _0_Dependencies import *
from _1_Sub_Func import *
from _2_Mysql import db
from extract_text_from_url import *

load_dotenv()


class GEN:
    def __init__(self):
        self.prompt = ""
        pass

    def get_func_and_args(self):
        macro = ["STOCKPRICE", "FOREIGN", "FINANCE", "ECONOMY", "GOVERNMENT"]
        rows = []

        exclude = {
            "FINANCE": ["M2"],
            "ECONOMY": ["pmi"],
            "FOREIGN": [],
            "GOVERNMENT": [],
        }

        for macro_name in macro:
            records = db.importing_objs(macro_name)
            if records is None or len(records) == 0:
                continue

            if macro_name == "STOCKPRICE":
                data_names = set(
                    getattr(r, "data_name", None)
                    for r in records
                    if hasattr(r, "data_name")
                )
                for data_name in data_names:
                    if data_name:
                        rows.append({"func_name": "add_stock", "args": data_name})
                continue

            data_names = set(
                getattr(r, "data_name", None)
                for r in records
                if hasattr(r, "data_name")
            )
            for data_name in data_names:
                if not data_name:
                    continue
                if macro_name in exclude and data_name in exclude[macro_name]:
                    continue
                records_by_name = db.importing_objs(macro_name, data_name=data_name)
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
                if not sample:
                    continue
                keys = list(sample.data_dict.keys())

                # Filter special cases in some specific tables
                if macro_name == "FOREIGN" and data_name in [
                    "export_bylocation",
                    "import_bylocation",
                ]:
                    # Get all keys except 4th, 5th & 7th keys
                    keys_to_add = [k for i, k in enumerate(keys) if i not in [3, 4, 6]]
                elif macro_name == "GOVERNMENT" and data_name in [
                    "public_investment",
                    "social_investment",
                ]:
                    # Get all keys except for 2nd, 3rd, 4th
                    keys_to_add = [k for i, k in enumerate(keys) if i not in [1, 2, 3]]
                elif macro_name == "GOVERNMENT" and data_name == "budget_in":
                    # Get all keys from 3rd but except for 4th
                    keys_to_add = [k for i, k in enumerate(keys) if i >= 2 and i != 3]
                elif macro_name == "ECONOMY" and data_name in [
                    "gdp_real_raw",
                    "gdp_real_yoy",
                    "gdp_nominal_raw",
                    "gdp_nominal_acc_yoy",
                    "gdp_nominal_yoy",
                    "gdp_nominal_acc_raw",
                    "gdp_real_acc_yoy",
                    "gdp_real_acc_raw",
                ]:
                    keys_to_add = [k for i, k in enumerate(keys) if i > 2 and i != 3]

                else:
                    # Mặc định lấy từ key thứ 4 trở đi
                    keys_to_add = keys[3:]

                for key in keys_to_add:
                    rows.append({"func_name": f"add_{data_name.lower()}", "args": key})

        rows.append({"func_name": "add_M2", "args": "M2"})
        rows.append({"func_name": "add_pmi", "args": "pmi"})
        df = pd.DataFrame(rows)
        return df

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

    def text(self, model_name="gpt-4.1-nano", **kwargs):
        begin = datetime.now()
        self.gen_text = ""
        self.prompt_len = 0

        macro_metadata = kwargs.get("macro_metadata", None)
        if not macro_metadata:
            macro_metadata = self.get_func_and_args().to_dict(orient="records")

        is_default_prompt = False
        if "system_prompt" not in kwargs and "user_prompt" not in kwargs:
            is_default_prompt = True

        input_text = kwargs.get("input_text", None)
        default_system_prompt = (
            "Bạn là một data analyst chuyên nghiệp có khả năng đọc báo cáo phân tích thành thạo. "
            "Dựa trên nội dung input, hãy trả về output là một JSON object, mỗi object gồm:\n"
            "- from_macro: None\n"
            "- to_macro: None\n"
            "- timeframe: daily, monthly, quarterly hoặc yearly (chọn timeframe phù hợp dựa trên các chi tiết phân tích trong bài bài cáo cáo, đối với stock thì nên lấy daily)\n"
            "- title: tên biểu đồ, dựa trên series được vẽ\n"
            "- series: là một list, mỗi phần tử là một dict gồm:\n"
            "   - func_name: tên hàm sẽ gọi trong combinechart (ví dụ: add_stock, add_cpi, ...)\n"
            "   - args: tham số truyền vào, phải hợp lệ với func_name dựa trên metadata\n"
            "Chỉ sinh ra các series mà func_name và args đúng như metadata. "
            "Nếu không có args hợp lệ với func_name, không sinh ra object đó. "
            "Mỗi dict trong series chỉ chứa 1 func_name và 1 args. "
            "Chỉ chọn tối đa 5 series quan trọng nhất cho mỗi object."
        )
        default_user_prompt = f"""Đây là file báo cáo: "{input_text}"
        Dưới đây là metadata (danh sách các cặp func_name và args hợp lệ):
        {json.dumps(macro_metadata, ensure_ascii=False, indent=2)}
        Chỉ được phép sinh ra các series mà func_name và args đúng như trong bảng trên.
        Mỗi phần tử trong series là một dict gồm 1 func_name và 1 args.
        Nếu không tìm thấy bất kỳ args nào hợp lệ với func_name dựa trên metadata, không sinh ra object đó trong list kết quả.
        Hãy trích xuất thông tin biểu đồ phù hợp dưới dạng JSON đúng cấu trúc như đã mô tả. Các dữ liệu trong series phải có trong metadata.
        """
        system_prompt = kwargs.get("system_prompt", default_system_prompt)
        user_prompt = kwargs.get("user_prompt", default_user_prompt)
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
            return None  
        
        if not is_default_prompt:
            return self.gen_text

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
                    item["series"] = [
                        s for s in item.get("series", []) if s.get("args")
                    ]

                    for s in item["series"]:
                        func_name = s.get("func_name", "")
                        if not func_name.startswith("add_"):
                            s["func_name"] = "add_" + func_name
                    if not item["series"]:
                        continue
                    for s in item["series"]:
                        key = (
                            s["func_name"],
                            (
                                tuple(s["args"])
                                if isinstance(s["args"], list)
                                else s["args"]
                            ),
                        )
                        if key in seen:
                            break
                        seen.add(key)
                    else:
                        filtered.append(item)
                return filtered[0] if filtered else None

            if isinstance(parsed, dict) and parsed.get("series"):
                parsed["series"] = [
                    s for s in parsed.get("series", []) if s.get("args")
                ]
                for s in parsed["series"]:
                    func_name = s.get("func_name", "")
                    if not func_name.startswith("add_"):
                        s["func_name"] = "add_" + func_name
                if not parsed["series"]:
                    return None
                return parsed
            return None

        except Exception as e:
            print("Input được trả về không đúng format:\n", self.gen_text)
            return None


# for key, value in GEN().get_all_macro_types().items():
#     print(f"{key}: {str(value)[0:100]}")
# print(GEN().text(data_name="BSC MACRO - Cập nhật diễn biến cuộc chiến thuế quan 2025"))

# # print(GEN().get_input_url(data_name="BSC MACRO - Cập nhật diễn biến cuộc chiến thuế quan 2025"))




