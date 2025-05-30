from _3_Data_Macro import *
"""trục y thứ 2 là"""
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

def rgb_to_mpl(rgb):
    return tuple(c / 255 for c in rgb)

class combinechart:
    def __init__(
        self, from_macro=None, to_macro=None, timeframe="daily", title=""
    ):
        self.from_macro = from_macro
        self.to_macro = to_macro
        self.timeframe = timeframe.lower()
        self.title = title
        self.data_list = []
        self.color_index = 0  # Track color assignments

    def _get_color(self):
        # Use colors from color_dict_vdsc in order
        colors = list(color_dict_vdsc.keys())
        colors = [
            c for c in colors if c not in ["white", "black", "grey"]
        ]  # Skip neutral colors
        color = colors[self.color_index % len(colors)]
        self.color_index += 1
        return rgb_to_mpl(color_dict_vdsc[color])

    def _parse_macro(self, macro, end=False):
        if not macro:
            return None
        prefix = macro[0]
        date_str = macro[1:]
        try:
            if prefix == "D":
                return datetime.strptime(date_str, "%Y_%m_%d")
            elif prefix == "M":
                year, month = map(int, date_str.split("_"))
                return (
                    datetime(
                        year + int(end and month == 12),
                        month + int(end and month < 12),
                        1,
                    )
                    if end
                    else datetime(year, month, 1)
                )
            elif prefix == "Q":
                year, q = date_str.split("_0")
                year, q = int(year), int(q)
                if end:
                    month = 3 * q
                    if month == 12:
                        return datetime(year, 12, 31)
                    return datetime(year, month + 1, 1) - timedelta(days=1)
                else:
                    return datetime(year, 3 * q - 2, 1)
            elif prefix == "Y":
                return (
                    datetime(int(date_str) + int(end), 1, 1)
                    if end
                    else datetime(int(date_str), 1, 1)
                )
        except Exception as e:
            print(f"Lỗi phân tích macro: {macro} - {e}")
        return None

    def _convert_to_df(self, data_dict):
        if self.timeframe not in data_dict:
            raise ValueError(
                f"Không tìm thấy timeframe '{self.timeframe}' trong dữ liệu."
            )
        df = pd.DataFrame(data_dict[self.timeframe], columns=["Date", "Value"])
        if self.timeframe == "daily":
            df["Date"] = pd.to_datetime(
                df["Date"].str.replace("D", ""), format="%Y_%m_%d"
            )
        elif self.timeframe == "monthly":
            df["Date"] = pd.to_datetime(df["Date"].str.replace("M", ""), format="%Y_%m")
        elif self.timeframe == "quarterly":

            def convert_quarter(q_str):
                year, q = q_str.replace("Q", "").split("_0")
                return datetime(int(year), 3 * int(q) - 2, 1)

            df["Date"] = df["Date"].apply(convert_quarter)
        elif self.timeframe == "yearly":
            df["Date"] = df["Date"].apply(
                lambda x: datetime(int(x.replace("Y", "")), 1, 1)
            )

        start_date = self._parse_macro(self.from_macro) if self.from_macro else None
        end_date = self._parse_macro(self.to_macro, end=True) if self.to_macro else None
        if start_date:
            df = df[df["Date"] >= start_date]
        if end_date:
            df = df[df["Date"] <= end_date]
        return df[df["Value"] != 0].sort_values("Date")

    def _resolve_color(self, color_key):
        if isinstance(color_key, str) and color_key in color_dict_vdsc:
            return rgb_to_mpl(color_dict_vdsc[color_key])
        return self._get_color()

    def add_stock(self, stock_code, label=None, color=None):
        data = stockprice(self.from_macro).market().stock(stock_code).stretch()
        df = self._convert_to_df(data)
        self.data_list.append(
            {
                "df": df,
                "label": label or f"Giá trị {stock_code}",
                "color": color,
            }
        )
        return self

    def add_interbank_rate(self, tenor, label=None, color=None):
        data = finance(self.from_macro).interbank_rate(tenor).stretch()
        df = self._convert_to_df(data)
        self.data_list.append(
            {
                "df": df,
                "label": label or f"Lãi suất {tenor}",
                "color": color,
            }
        )
        return self

    def add_gdp_real(self, gdp, label=None, color=None):
        data = economy(self.from_macro).gdp_real(gdp).stretch()
        df = self._convert_to_df(data)
        self.data_list.append(
            {
                "df": df,
                "label": label or gdp,
                "color": color,
            }
        )
        return self

    def _setup_xaxis(self, ax):
        """Configure x-axis based on timeframe"""
        if self.timeframe == "daily":
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        elif self.timeframe == "monthly":
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        elif self.timeframe == "quarterly":
            ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 4, 7, 10]))

            def format_quarter(x, pos=None):
                date = mdates.num2date(x)
                q = (date.month - 1) // 3 + 1
                return f"Q{q} {date.year}"

            ax.xaxis.set_major_formatter(plt.FuncFormatter(format_quarter))
        elif self.timeframe == "yearly":
            ax.xaxis.set_major_locator(mdates.YearLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    def plot(self, save_path="./combine_img"):
        if not self.data_list:
            raise ValueError("Chưa có dữ liệu để vẽ.")

        fig, host = plt.subplots(figsize=(12, 5))
        fig.subplots_adjust(right=0.75)
        host.set_title(self.title)

        # Hide left spine and y-axis
        host.spines["left"].set_visible(False)
        host.yaxis.set_visible(False)

        axes = []
        for i, data in enumerate(self.data_list):
            ax = host.twinx()
            if i > 0:
                ax.spines["right"].set_position(("axes", 1 + 0.1 * i))

            color = self._resolve_color(data["color"])
            ax.plot(
                data["df"]["Date"],
                data["df"]["Value"],
                color=color,
                label=data["label"],
                linewidth=2,
            )
            ax.tick_params(axis="y", labelcolor=color)
            ax.set_ylabel(data["label"], color=color)
            ax.spines["right"].set_color(color)
            axes.append(ax)

        # Configure x-axis based on timeframe
        self._setup_xaxis(host)

        # Show legend if number of lines ≤ 5
        if len(self.data_list) <= 5:
            lines, labels = [], []
            for ax in axes:
                lns, lbls = ax.get_legend_handles_labels()
                lines.extend(lns)
                labels.extend(lbls)
            host.legend(lines, labels, loc="upper left")

        fig.tight_layout()
        # Save
        if save_path is not None:
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            filename = datetime.now().strftime("chart_%Y%m%d_%H%M%S.jpg")
            full_path = os.path.join(save_path, filename)
            fig.savefig(full_path, dpi=300, bbox_inches="tight")
            print(f"Hình ảnh đã được lưu tại {full_path}")

        plt.tight_layout()
        plt.show()


print(
    combinechart(
        "Y2022",
        "Y2024",
        timeframe="yearly",
        title="Biểu đồ Giá cổ phiếu VCB vs TCB từng năm",
    )
    .add_stock("VCB")
    .add_stock("TCB")
    .plot()
)
print(combinechart("Y2023", "Y2024", timeframe="quarterly", title="Biểu đồ Giá cổ phiếu VCB vs TCB từng quý").add_stock("VCB").add_stock("TCB").plot())
