from _0_Dependencies import *
from _1_Sub_Func import *
from _1_Sub_Xlwings import *
from _2_Mysql import *
from combine_chart import *
from _x_GEN import *

# load_dotenv()


class product_test:
    def __init__(self, **kwargs):
        import inspect

        self.kwargs = kwargs
        self.input_dict = copy.deepcopy(kwargs)
        self.input_dict.update(     
            {
                name: member
                for name, member in inspect.getmembers(self)
                if not name.startswith("__")
            }
        )

        # def text(self, model_name="gpt-4o", **kwargs):
        #     # self.url = kwargs['input_url']

        #     begin = datetime.now()
        #     self.gen_text = ""
        #     self.prompt_len = 0

        #     input_url = kwargs.get("input_url", None)
        #     data_name = kwargs.get("data_name", None)
        #     if (not input_url or len(input_url)) and data_name:
        #         input_url = self.get_input_url(data_name)
        #     if not input_url:
        #         raise ValueError("Thiếu input_url")

        #     macro_metadata = kwargs.get("macro_metadata", None)
        #     if not macro_metadata:
        #         macro_metadata = self.get_all_macro_types()

        #     input_text = extract_text_from_pdf_url(input_url)

        self.input_url = kwargs.get("input_url", None)
        self.data_name = kwargs.get("data_name", None)

    def charts(self, width = None, height = None):
        chart_inputs = GEN().text(input_url = self.input_url, data_name=self.data_name)
        print(chart_inputs)
        if not chart_inputs or not isinstance(chart_inputs, list):
            print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ")
            return []

        image_paths = []
        for idx, chart_input in enumerate(chart_inputs):
            image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            image_path = get_path("Database", "Temp", f"{image_name}_chart.jpg")

            chart = combinechart(from_macro=chart_input["from_macro"],
                to_macro=chart_input["to_macro"],
                timeframe=chart_input.get("timeframe", "monthly"),
                title=chart_input.get("title", ""),)

            for s in chart_input["series"]:
                func_name = s.get("func_name")
                args = s.get("args",[])
                try:
                    if hasattr(chart, func_name):
                        if len(args) >= 1:
                            for arg in args:
                                getattr(chart, func_name)(arg)
                    else:
                        print(f"Hàm {func_name} không tồn tại trong combinechart")
                except Exception as e:
                    print(f"Lỗi khi gọi {func_name} với args {args}: {e}")
            try:
                chart.plot(save_path=image_path)
                image_paths.append(image_path)
            except Exception as e:
                print(f"Lỗi khi vẽ chart{idx+1}: {e}")

        return image_paths
    # def chart_1(self,  width=None, height=None):

    #     chart_input = GEN().text(input_url=self.input_url)

    #     image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    #     image_path = get_path("Database", "Temp", f"{image_name}_chart.jpg")

    #     if not chart_input:
    #         print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ.")
    #         return None

    #     chart = combinechart(
    #         from_macro=chart_input["from_macro"],
    #         to_macro=chart_input["to_macro"],
    #         timeframe=chart_input.get("timeframe", "monthly"),
    #         title=chart_input.get("title", ""),
    #     )

    #     for s in chart_input["series"]:
    #         func_name = s.get("func_name")
    #         args = s.get("args", [])

    #         try:
    #             if hasattr(chart, func_name):
    #                 getattr(chart, func_name)(*args)
    #             else:
    #                 print(f"Hàm '{func_name}' không tồn tại trong combinechart.")
    #         except Exception as e:
    #             print(f"Lỗi khi gọi {func_name} với args {args}: {e}")

    #     chart.plot(save_path=image_path)
    #     return image_path

    # def chart_2(self, width=None, height=None):

    #     chart_input = GEN().text(input_url=self.input_url)

    #     image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    #     image_path = get_path("Database", "Temp", f"{image_name}_chart.jpg")

    #     if not chart_input:
    #         print("Không tìm thấy dữ liệu phù hợp để vẽ biểu đồ.")
    #         return None

    #     chart = combinechart(
    #         from_macro=chart_input["from_macro"],
    #         to_macro=chart_input["to_macro"],
    #         timeframe=chart_input.get("timeframe", "monthly"),
    #         title=chart_input.get("title", ""),
    #     )

    #     for s in chart_input["series"]:
    #         func_name = s.get("func_name")
    #         args = s.get("args", [])

    #         try:
    #             if hasattr(chart, func_name):
    #                 getattr(chart, func_name)(*args)
    #             else:
    #                 print(f"Hàm '{func_name}' không tồn tại trong combinechart.")
    #         except Exception as e:
    #             print(f"Lỗi khi gọi {func_name} với args {args}: {e}")

    #     chart.plot(save_path=image_path)
    #     return image_path


print(
    product_test(
        data_name="PHS STRATEGY - Dự báo thay đổi bộ chỉ số VN Diamond, VN30 kỳ Quý 2/2025: VN Diamond thêm mới CTD, L"
    ).charts()
)
