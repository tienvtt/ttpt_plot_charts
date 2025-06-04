from _0_Dependencies import *
from _1_Sub_Func import *
from _1_Sub_Xlwings import *
from _2_Mysql import *
from combine_chart import *

class product_test():
    def __init__(self,
			**kwargs
		):

        import inspect
        self.kwargs = kwargs

        self.input_dict = copy.deepcopy(kwargs)
        self.input_dict.update({
			name: member for name, member
			in inspect.getmembers(self)
			if not name.startswith('__')
		})

    def chart_1(self, width=None, height=None):
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path('Database', 'Temp', f'{image_name}_chart.jpg')

        x = [1, 2, 3, 4, 5]
        y = [10, 20, 15, 25, 30]

        # Set the figure size in inches (width and height / DPI)
        dpi = 100  # you can change this
        fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)

        # Plot the line chart
        plt.plot(x, y, '-o', color='blue')
        plt.xlabel('X Axis')
        plt.ylabel('Y Axis')
        plt.title('Line Chart')
        plt.grid(True)

        # Save the chart to the specified image path
        plt.savefig(image_path, bbox_inches='tight')
        plt.close()

        return image_path

    def chart_2(self, width=None, height=None):
        image_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        image_path = get_path('Database', 'Temp', f'{image_name}_chart.jpg')

        combinechart(
                "Y2022",
                "Y2024",
                timeframe="yearly",
                title="Biểu đồ Giá cổ phiếu VCB vs TCB từng năm",
            ).add_stock("VCB").add_stock("TCB").plot(save_path=image_path)

        

        return image_path