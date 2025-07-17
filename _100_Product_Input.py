from _0_Dependencies import *
from _1_Sub_Func import *
from _1_Sub_Xlwings import *
from _2_Mysql import *
from combine_chart import *
from _x_GEN import *


load_dotenv()

color_dict_vdsc = {
    "black": (40, 40, 40),
    "white": (255, 255, 255),
    "grey": (200, 200, 200),
    "red": (237, 52, 46),
    "green": (1, 165, 78),
    "yellow": (220, 171, 39),
    "dark_yellow": (127, 90, 61),
    "light_yellow": (237, 229, 151),
}


class macro_report:
    def __init__(self, **kwargs):
        import inspect

        self.kwargs = kwargs
        self.data_dict = self.kwargs["data_dict"] if "data_dict" in self.kwargs else {}

        self.rescale = 3
        self.color_names = [
            "red",
            "yellow",
            "green",
            "light_yellow",
            "grey",
            "dark_yellow",
        ]
        self.chart_text_font_path = get_path(
            "Database", "Font", "MYRIADPRO-REGULAR.OTF"
        )
        self.chart_text_size = int(5 * self.rescale)
        self.input_dict = copy.deepcopy(kwargs)
        self.input_dict.update(self.data_dict)
        self.data_name = self.input_dict.get("data_name", None)
        self.input_url = self.input_dict.get("input_url", None)

        self.get_data_input_dict()

        # Cập nhật input_dict với các giá trị từ self
        self.input_dict.update(
            {
                name: member
                for name, member in inspect.getmembers(self)
                if not name.startswith("__")
            }
        )

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

    def analyzed_report(self):
        if not self.input_url and self.data_name:
            self.input_url = self.get_input_url(self.data_name)

        if not self.input_url:
            raise ValueError("Thiếu input_url hoặc data_name.")
        self.input_text = extract_text_from_pdf_url(self.input_url)
        system_prompt = (
            "Bạn là một data analyst chuyên nghiệp có khả năng đọc hiểu báo cáo thành thạo. "
            "Dựa trên nội dung báo cáo bạn nhận được, hãy phân tích bài báo cáo thành 5 phân đoạn nội dung chính xúc tích hơn nhưng vẫn đảm bảo tính chính xác và đầy đủ thông tin. "
            "Mỗi đoạn phân tích là một chủ đề chính được nhắc đến trong bài phân tích. Hãy đảm bảo các đoạn phân tích này vẫn giữ được các dữ liệu, số liệu quan trọng. "
            "Dữ liệu trả về là một bài báo cáo mới bao gồm các đoạn nội dung được phân tích trên, mỗi đoạn được phân tách bằng chính xác một dòng trống."
        )
        user_prompt = f"""Đây là file báo cáo: "{self.input_text}"""
        # print("Input URL:", self.input_url)
        # print("Data Name:", self.data_name)
        # print("Extracted Text:", self.input_text)

        try:
            analyzed_report = GEN().text(
                model_name="gpt-4.1-nano",
                input_url=self.input_url,
                data_name=self.data_name,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            return analyzed_report
        except Exception as e:
            print(f"Error when calling GEN().text(): {e}")
            return ""

    def call_gpt(self, prompt, image_path=None):
        try:
            client = openai.OpenAI(api_key=os.getenv("openai.api_key_2"), timeout=30)
            messages = [{"role": "user", "content": prompt}]
            response = client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=messages,
                temperature=0,
                seed=42,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"GPT call failed: {e}")
            return "Analysis failed - API error"

    def charts(self, width=None, height=None):
        if not self.input_url and self.data_name:
            self.input_url = GEN().get_input_url(self.data_name)

        if not self.input_url:
            print("Không tìm thấy URL để vẽ biểu đồ")
            return []

        chart_inputs = GEN().text(input_url=self.input_url, data_name=self.data_name)
        input_text = extract_text_from_pdf_url(self.input_url)

        if not chart_inputs or not isinstance(chart_inputs, list):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return []

        results = []

        for idx, chart_input in enumerate(chart_inputs):
            image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            image_path = get_path("Database", "Temp", f"{image_name}_chart.jpg")
            chart = combinechart(
                from_macro=chart_input["from_macro"],
                to_macro=chart_input["to_macro"],
                timeframe=chart_input.get("timeframe", "monthly"),
                title=chart_input.get("title", ""),
            )
            for s in chart_input["series"]:
                func_name = s.get("func_name")
                args = s.get("args", [])
                try:
                    if hasattr(chart, func_name):
                        # Sửa cách xử lý args
                        if isinstance(args, list) and len(args) > 0:
                            # Kiểm tra nếu args chứa dict, chuyển thành **kwargs
                            if isinstance(args[0], dict):
                                getattr(chart, func_name)(**args[0])
                            else:
                                getattr(chart, func_name)(*args)
                        elif isinstance(args, dict):
                            getattr(chart, func_name)(**args)
                        elif args:
                            getattr(chart, func_name)(args)
                        else:
                            getattr(chart, func_name)()
                    else:
                        print(f"Hàm {func_name} không tồn tại trong combinechart")
                except Exception as e:
                    print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                    # Debug: In ra kiểu dữ liệu của args
                    print(f"Type of args: {type(args)}, Content: {args}")
            try:
                chart.plot(save_path=image_path)
                image_created = True
                print(f"Chart {idx+1} saved successfully")
            except Exception as e:
                print(f"Chart {idx+1} failed: {e}")
                image_created = False

            # Gọi GPT API để tạo analysis_text
            analysis_prompt = "Kể một câu chuyện thú vị trong 100 từ"
            analysis_text = self.call_gpt(analysis_prompt)

            results.append(
                {
                    "input_chart": chart_input,
                    "image_path": image_path if image_created else None,
                    "analysis_text": analysis_text,
                }
            )

            print(f"Chart {idx+1} processing completed")

        print(f"Total charts processed: {len(results)}")
        return results

    def get_data_input_dict(self):
        print("xxxx")
        print(self.input_url)
        print(self.data_name)
        print("xxxx")

        analyzed_text = self.analyzed_report()

        if analyzed_text:
            # Chia báo cáo thành các đoạn dựa trên một dòng trống
            sections = []
            current_section = []
            lines = analyzed_text.strip().split("\n")

            for line in lines:
                if line.strip():  # Nếu dòng không trống
                    current_section.append(line)
                elif (
                    current_section
                ):  # Nếu gặp dòng trống và đã có nội dung trong section hiện tại
                    sections.append("\n".join(current_section))
                    current_section = []

            # Thêm section cuối cùng nếu có
            if current_section:
                sections.append("\n".join(current_section))

            # Cập nhật self.input_dict với các tiêu đề và nội dung
            for i in range(min(5, len(sections))):
                lines = sections[i].split("\n")

                # Nếu đoạn có ít nhất 2 dòng, coi dòng đầu là title
                if len(lines) >= 2:
                    title = lines[0].strip()

                    # Loại bỏ số thứ tự ở đầu title (nếu có)
                    import re

                    title = re.sub(r"^\d+\.\s*", "", title)

                    # Nối các dòng còn lại thành đoạn văn
                    content = "\n".join(lines[1:]).strip()
                else:
                    # Nếu chỉ có 1 dòng, coi như không có title riêng
                    title = f"Phần {i+1}"
                    content = sections[i]

                # Cập nhật input_dict
                self.input_dict[f"text_{i+1}_title"] = title
                self.input_dict[f"text_{i+1}_body"] = content

            # Đảm bảo có đủ 5 sections
            for i in range(len(sections), 5):
                self.input_dict[f"text_{i+1}_title"] = ""
                self.input_dict[f"text_{i+1}_body"] = ""
        else:
            # Nếu không có báo cáo, đặt tất cả title và body thành chuỗi rỗng
            for i in range(5):
                self.input_dict[f"text_{i+1}_title"] = ""
                self.input_dict[f"text_{i+1}_body"] = ""

    def get_prompt_input_dict(self):
        import pandas as pd

        self.xlsx_prompt_path = (
            self.kwargs["xlsx_prompt_path"]
            if "xlsx_prompt_path" in self.kwargs
            else None
        )
        if self.xlsx_prompt_path and os.path.exists(self.xlsx_prompt_path):
            df = pd.read_excel(self.xlsx_prompt_path, sheet_name="prompt")
            for index, row_df in df.iterrows():
                print(f"GEN: {row_df['prompt_code']}")
                prompt = row_df["prompt"]
                for key, value in self.data_dict.items():
                    prompt = prompt.replace("{" + key + "}", f"{value}")
                self.input_dict[row_df["prompt_code"]] = GEN().text(prompt=prompt)

    def chart_1(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        from datetime import datetime

        # Cấu hình font và kích thước
        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )

        # Lấy đoạn văn đầu tiên từ analyzed_report
        text_1_body = self.input_dict.get("text_1_body", "")
        if not text_1_body:
            print("Không tìm thấy nội dung cho đoạn văn đầu tiên")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Lấy title đầu tiên để làm tiêu đề biểu đồ nếu có
        chart_title = self.input_dict.get("text_1_title", "")

        # Gọi GEN().text() với đoạn văn đầu tiên làm input_text
        # Sử dụng prompt mặc định để tạo JSON object
        chart_input = GEN().text(input_text=text_1_body)

        if not chart_input or not isinstance(chart_input, dict):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Tạo tên file duy nhất dựa trên timestamp
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path("Database", "Temp", f"{image_name}_chart_1.jpg")

        # Thiết lập title nếu không có trong chart_input
        if not chart_input.get("title") and chart_title:
            chart_input["title"] = chart_title

        # Tạo biểu đồ với combinechart
        chart = combinechart(
            from_macro=chart_input.get("from_macro"),
            to_macro=chart_input.get("to_macro"),
            timeframe=chart_input.get("timeframe", "monthly"),
            title=chart_input.get("title", ""),
        )

        # Thêm các series vào biểu đồ
        for s in chart_input.get("series", []):
            func_name = s.get("func_name")
            args = s.get("args", [])
            try:
                if hasattr(chart, func_name):
                    # Xử lý args khác nhau
                    if isinstance(args, list) and len(args) > 0:
                        # Kiểm tra nếu args chứa dict, chuyển thành **kwargs
                        if isinstance(args[0], dict):
                            getattr(chart, func_name)(**args[0])
                        else:
                            getattr(chart, func_name)(*args)
                    elif isinstance(args, dict):
                        getattr(chart, func_name)(**args)
                    elif args:
                        getattr(chart, func_name)(args)
                    else:
                        getattr(chart, func_name)()
                else:
                    print(f"Hàm {func_name} không tồn tại trong combinechart")
            except Exception as e:
                print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                print(f"Type of args: {type(args)}, Content: {args}")

        # Vẽ và lưu biểu đồ
        try:
            chart.plot(save_path=image_path)
            print(f"Chart 1 saved successfully to {image_path}")
            return image_path
        except Exception as e:
            print(f"Chart 1 failed: {e}")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

    def chart_2(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        from datetime import datetime

        # Cấu hình font và kích thước
        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )

        # Lấy đoạn văn thứ 2 từ analyzed_report
        text_2_body = self.input_dict.get("text_2_body", "")
        if not text_2_body:
            print("Không tìm thấy nội dung cho đoạn văn thứ hai")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Lấy title để làm tiêu đề biểu đồ nếu có
        chart_title = self.input_dict.get("text_2_title", "")

        # Gọi GEN().text() với đoạn văn làm input_text
        chart_input = GEN().text(input_text=text_2_body)

        if not chart_input or not isinstance(chart_input, dict):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Tạo tên file duy nhất dựa trên timestamp
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path("Database", "Temp", f"{image_name}_chart_2.jpg")

        # Thiết lập title nếu không có trong chart_input
        if not chart_input.get("title") and chart_title:
            chart_input["title"] = chart_title

        # Tạo biểu đồ với combinechart
        chart = combinechart(
            from_macro=chart_input.get("from_macro"),
            to_macro=chart_input.get("to_macro"),
            timeframe=chart_input.get("timeframe", "monthly"),
            title=chart_input.get("title", ""),
        )

        # Thêm các series vào biểu đồ
        for s in chart_input.get("series", []):
            func_name = s.get("func_name")
            args = s.get("args", [])
            try:
                if hasattr(chart, func_name):
                    # Xử lý args khác nhau
                    if isinstance(args, list) and len(args) > 0:
                        if isinstance(args[0], dict):
                            getattr(chart, func_name)(**args[0])
                        else:
                            getattr(chart, func_name)(*args)
                    elif isinstance(args, dict):
                        getattr(chart, func_name)(**args)
                    elif args:
                        getattr(chart, func_name)(args)
                    else:
                        getattr(chart, func_name)()
                else:
                    print(f"Hàm {func_name} không tồn tại trong combinechart")
            except Exception as e:
                print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                print(f"Type of args: {type(args)}, Content: {args}")

        # Vẽ và lưu biểu đồ
        try:
            chart.plot(save_path=image_path)
            print(f"Chart 2 saved successfully to {image_path}")
            return image_path
        except Exception as e:
            print(f"Chart 2 failed: {e}")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

    def chart_3(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        from datetime import datetime

        # Cấu hình font và kích thước
        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )

        # Lấy đoạn văn thứ 3 từ analyzed_report
        text_3_body = self.input_dict.get("text_3_body", "")
        if not text_3_body:
            print("Không tìm thấy nội dung cho đoạn văn thứ ba")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Lấy title để làm tiêu đề biểu đồ nếu có
        chart_title = self.input_dict.get("text_3_title", "")

        # Gọi GEN().text() với đoạn văn làm input_text
        chart_input = GEN().text(input_text=text_3_body)

        if not chart_input or not isinstance(chart_input, dict):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Tạo tên file duy nhất dựa trên timestamp
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path("Database", "Temp", f"{image_name}_chart_3.jpg")

        # Thiết lập title nếu không có trong chart_input
        if not chart_input.get("title") and chart_title:
            chart_input["title"] = chart_title

        # Tạo biểu đồ với combinechart
        chart = combinechart(
            from_macro=chart_input.get("from_macro"),
            to_macro=chart_input.get("to_macro"),
            timeframe=chart_input.get("timeframe", "monthly"),
            title=chart_input.get("title", ""),
        )

        # Thêm các series vào biểu đồ
        for s in chart_input.get("series", []):
            func_name = s.get("func_name")
            args = s.get("args", [])
            try:
                if hasattr(chart, func_name):
                    # Xử lý args khác nhau
                    if isinstance(args, list) and len(args) > 0:
                        if isinstance(args[0], dict):
                            getattr(chart, func_name)(**args[0])
                        else:
                            getattr(chart, func_name)(*args)
                    elif isinstance(args, dict):
                        getattr(chart, func_name)(**args)
                    elif args:
                        getattr(chart, func_name)(args)
                    else:
                        getattr(chart, func_name)()
                else:
                    print(f"Hàm {func_name} không tồn tại trong combinechart")
            except Exception as e:
                print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                print(f"Type of args: {type(args)}, Content: {args}")

        # Vẽ và lưu biểu đồ
        try:
            chart.plot(save_path=image_path)
            print(f"Chart 3 saved successfully to {image_path}")
            return image_path
        except Exception as e:
            print(f"Chart 3 failed: {e}")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

    def chart_4(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        from datetime import datetime

        # Cấu hình font và kích thước
        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )

        # Lấy đoạn văn thứ 4 từ analyzed_report
        text_4_body = self.input_dict.get("text_4_body", "")
        if not text_4_body:
            print("Không tìm thấy nội dung cho đoạn văn thứ tư")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Lấy title để làm tiêu đề biểu đồ nếu có
        chart_title = self.input_dict.get("text_4_title", "")

        # Gọi GEN().text() với đoạn văn làm input_text
        chart_input = GEN().text(input_text=text_4_body)

        if not chart_input or not isinstance(chart_input, dict):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Tạo tên file duy nhất dựa trên timestamp
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path("Database", "Temp", f"{image_name}_chart_4.jpg")

        # Thiết lập title nếu không có trong chart_input
        if not chart_input.get("title") and chart_title:
            chart_input["title"] = chart_title

        # Tạo biểu đồ với combinechart
        chart = combinechart(
            from_macro=chart_input.get("from_macro"),
            to_macro=chart_input.get("to_macro"),
            timeframe=chart_input.get("timeframe", "monthly"),
            title=chart_input.get("title", ""),
        )

        # Thêm các series vào biểu đồ
        for s in chart_input.get("series", []):
            func_name = s.get("func_name")
            args = s.get("args", [])
            try:
                if hasattr(chart, func_name):
                    # Xử lý args khác nhau
                    if isinstance(args, list) and len(args) > 0:
                        if isinstance(args[0], dict):
                            getattr(chart, func_name)(**args[0])
                        else:
                            getattr(chart, func_name)(*args)
                    elif isinstance(args, dict):
                        getattr(chart, func_name)(**args)
                    elif args:
                        getattr(chart, func_name)(args)
                    else:
                        getattr(chart, func_name)()
                else:
                    print(f"Hàm {func_name} không tồn tại trong combinechart")
            except Exception as e:
                print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                print(f"Type of args: {type(args)}, Content: {args}")

        # Vẽ và lưu biểu đồ
        try:
            chart.plot(save_path=image_path)
            print(f"Chart 4 saved successfully to {image_path}")
            return image_path
        except Exception as e:
            print(f"Chart 4 failed: {e}")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

    def chart_5(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
        from datetime import datetime

        # Cấu hình font và kích thước
        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )

        # Lấy đoạn văn thứ 5 từ analyzed_report
        text_5_body = self.input_dict.get("text_5_body", "")
        if not text_5_body:
            print("Không tìm thấy nội dung cho đoạn văn thứ năm")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Lấy title để làm tiêu đề biểu đồ nếu có
        chart_title = self.input_dict.get("text_5_title", "")

        # Gọi GEN().text() với đoạn văn làm input_text
        chart_input = GEN().text(input_text=text_5_body)

        if not chart_input or not isinstance(chart_input, dict):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"

        # Tạo tên file duy nhất dựa trên timestamp
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path("Database", "Temp", f"{image_name}_chart_5.jpg")

        # Thiết lập title nếu không có trong chart_input
        if not chart_input.get("title") and chart_title:
            chart_input["title"] = chart_title

        # Tạo biểu đồ với combinechart
        chart = combinechart(
            from_macro=chart_input.get("from_macro"),
            to_macro=chart_input.get("to_macro"),
            timeframe=chart_input.get("timeframe", "monthly"),
            title=chart_input.get("title", ""),
        )

        # Thêm các series vào biểu đồ
        for s in chart_input.get("series", []):
            func_name = s.get("func_name")
            args = s.get("args", [])
            try:
                if hasattr(chart, func_name):
                    # Xử lý args khác nhau
                    if isinstance(args, list) and len(args) > 0:
                        if isinstance(args[0], dict):
                            getattr(chart, func_name)(**args[0])
                        else:
                            getattr(chart, func_name)(*args)
                    elif isinstance(args, dict):
                        getattr(chart, func_name)(**args)
                    elif args:
                        getattr(chart, func_name)(args)
                    else:
                        getattr(chart, func_name)()
                else:
                    print(f"Hàm {func_name} không tồn tại trong combinechart")
            except Exception as e:
                print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
                print(f"Type of args: {type(args)}, Content: {args}")

        # Vẽ và lưu biểu đồ
        try:
            chart.plot(save_path=image_path)
            print(f"Chart 5 saved successfully to {image_path}")
            return image_path
        except Exception as e:
            print(f"Chart 5 failed: {e}")
            return r"E:\6_Master_Report\Database\Temp\20250715_160826_465934_chart.jpg"
