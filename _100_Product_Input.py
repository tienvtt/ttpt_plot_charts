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
                        if isinstance(args, list):
                            getattr(chart, func_name)(*args)
                        else:
                            getattr(chart, func_name)(args)
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


print(product_test(data_name="VNDS COMPANY - DGC").charts())
