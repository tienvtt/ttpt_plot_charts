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

        self.input_dict.update(
            {
                name: member
                for name, member in inspect.getmembers(self)
                if not name.startswith("__")
            }
        )

    def call_gpt(self, prompt, image_path=None):
        try:
            client = openai.OpenAI(
                api_key=os.getenv("openai.api_key"), timeout=30  # Add timeout
            )
            messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}]
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=0,
                seed=42,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"GPT call failed: {e}")
            return "Analysis failed - API error"

    def charts(self, width=None, height=None):
        # Nếu không có input_url, thử tìm từ data_name
        if not self.input_url and self.data_name:
            self.input_url = GEN().get_input_url(self.data_name)

        # Nếu vẫn không có input_url, trả về list rỗng
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
                print(f"✅ Chart {idx+1} saved successfully")
            except Exception as e:
                print(f"❌ Chart {idx+1} failed: {e}")
                image_created = False

            # DISABLE GPT TEMPORARILY - COMMENT OUT THE FOLLOWING LINES:
            # analysis_prompt = f"""..."""
            # analysis_text = self.call_gpt(analysis_prompt)

            # USE THIS INSTEAD:
            analysis_text = f"Analysis disabled for testing - Chart {idx+1}"
            print(f"Analysis skipped for Chart {idx+1}")

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
        # Gọi charts() để lấy kết quả và chỉ lấy image_path
        print("xxxx")
        print(self.input_url)
        print(self.data_name)
        print("xxxx")
        self.results = self.charts() if self.input_url or self.data_name else []

        self.input_dict.update({})

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

        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )
        output_path = self.results[0]["image_path"]
        return output_path

    def chart_2(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm

        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )
        output_path = self.results[1]["image_path"]
        return output_path

    def chart_3(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm

        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )
        output_path = self.results[2]["image_path"]
        return output_path

    def chart_4(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm

        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )
        output_path = self.results[3]["image_path"]
        return output_path

    def chart_5(self, width=None, height=None):
        from PIL import Image
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm

        font_prop = fm.FontProperties(
            fname=self.chart_text_font_path, size=self.chart_text_size
        )
        fm.fontManager.addfont(self.chart_text_font_path)
        plt.rcParams.update(
            {"font.size": self.chart_text_size, "font.family": font_prop.get_name()}
        )
        output_path = self.results[4]["image_path"]
        return output_path


# if __name__ == "__main__":
#     print("=== Testing without GPT ===")
#     report = macro_report(data_name="SSI STRATEGY - Nhìn xa trông rộng")
#     print(f"Charts generated: {len(report.image_list)}")
#     for path in report.image_list:
#         print(f"  - {path}")
