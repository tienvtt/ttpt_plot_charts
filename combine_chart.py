from _3_Data_Macro import *
from scipy.interpolate import interp1d, make_interp_spline
import numpy as np

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
    def __init__(self, from_macro=None, to_macro=None, timeframe="daily", title=""):
        self.from_macro = from_macro
        self.to_macro = to_macro
        self.timeframe = timeframe.lower()
        self.title = title
        self.data_list = []
        self.color_index = 0
        self.smooth_data = False  # Flag để bật/tắt làm mượt
        self.smooth_factor = 300  # Số điểm interpolation
        self.smooth_method = "cubic"  # Phương thức interpolation

    def enable_smooth(self, smooth_factor=300, method="cubic"):
        """
        Bật chế độ làm mượt dữ liệu

        Parameters:
        - smooth_factor: Số điểm để interpolation (càng nhiều càng mượt)
        - method: Phương thức interpolation ('linear', 'cubic', 'spline')
        """
        self.smooth_data = True
        self.smooth_factor = smooth_factor
        self.smooth_method = method
        return self

    def disable_smooth(self):
        """Tắt chế độ làm mượt dữ liệu"""
        self.smooth_data = False
        return self

    def _smooth_data_series(self, dates, values):
        """
        Làm mượt một series dữ liệu

        Parameters:
        - dates: pandas Series chứa dates
        - values: pandas Series chứa values

        Returns:
        - smooth_dates, smooth_values: dữ liệu đã được làm mượt
        """
        if len(dates) < 3:
            return dates, values

        # Convert dates to numeric để interpolation
        numeric_dates = mdates.date2num(dates)

        try:
            if self.smooth_method == "linear":
                # Linear interpolation
                f = interp1d(numeric_dates, values, kind="linear")

            elif self.smooth_method == "cubic":
                # Cubic interpolation (yêu cầu ít nhất 4 điểm)
                if len(dates) >= 4:
                    f = interp1d(numeric_dates, values, kind="cubic")
                else:
                    f = interp1d(numeric_dates, values, kind="linear")

            elif self.smooth_method == "spline":
                # B-spline interpolation (mượt nhất)
                if len(dates) >= 4:
                    f = make_interp_spline(numeric_dates, values, k=3)
                else:
                    f = interp1d(numeric_dates, values, kind="linear")
            else:
                # Default to linear
                f = interp1d(numeric_dates, values, kind="linear")

            # Tạo các điểm mới cho interpolation
            new_numeric_dates = np.linspace(
                numeric_dates.min(), numeric_dates.max(), self.smooth_factor
            )

            # Interpolate values
            new_values = f(new_numeric_dates)

            # Convert back to dates
            new_dates = mdates.num2date(new_numeric_dates)
            new_dates = pd.to_datetime(new_dates)

            return new_dates, new_values

        except Exception as e:
            print(f"Lỗi khi làm mượt dữ liệu: {e}")
            return dates, values

    def _get_color(self):
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
                year, q = q_str.replace("Q", "").split("_")
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

    def _add_series(
        self, data_func, key, label=None, color=None, default_label_prefix=""
    ):
        data = data_func(key).stretch()
        df = self._convert_to_df(data)
        if self.smooth_data:
            df["Date"], df["Value"] = self._smooth_data_series(df["Date"], df["Value"])
        self.data_list.append(
            {
                "df": df,
                "label": label or f"{default_label_prefix}{key}",
                "color": color,
            }
        )
        return self

    def add_stock(self, stock_code, label=None, color=None):
        data = stockprice(self.from_macro).market().stock(stock_code).stretch()
        df = self._convert_to_df(data)
        if self.smooth_data:
            df["Date"], df["Value"] = self._smooth_data_series(df["Date"], df["Value"])
        self.data_list.append(
            {
                "df": df,
                "label": label or f" {stock_code.upper()}",
                "color": color,
            }
        )
        return self

    """-------------------FINANCE---------------------"""

    def add_interbank_rate(self, term, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).interbank_rate(r),
            term,
            label,
            color,
        )

    def add_reverse_repo(self, repo_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).reverse_repo(r),
            repo_type,
            label,
            color,
        )

    def add_interbank_vol(self, term, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).interbank_vol(r),
            term,
            label,
            color,
        )

    def add_sell_outright(self, outright_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).sell_outright(r),
            outright_type,
            label,
            color,
        )

    def add_borrowing(self, industry, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).borrowing(r),
            industry,
            label,
            color,
        )

    def add_lending(self, lending_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).lending(r),
            lending_type,
            label,
            color,
        )

    def add_M2(self, M2_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).M2(r),
            M2_type,
            label,
            color,
        )

    def add_moneysupply(self, moneysupply_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).moneysupply(r),
            moneysupply_type,
            label,
            color,
        )

    def add_loans(self, loans_type, label=None, color=None):
        return self._add_series(
            lambda r: finance(self.from_macro).loans(r),
            loans_type,
            label,
            color,
        )

    def add_securities_account(self, account_type, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).securities_account(r),
            account_type,
            label,
            color,
        )

    """-------------------ECONOMY---------------------"""

    def add_pmi(self, pmi_type, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).pmi(r),
            pmi_type,
            label,
            color,
        )

    def add_passenger_transport(self, transport_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).passenger_transport(r),
            transport_type,
            label,
            color,
        )

    def add_retail_revenue_acc_raw(self, revenue_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).retail_revenue_acc_raw(r),
            revenue_type,
            label,
            color,
        )

    def add_retail_revenue_acc_yoy(self, revenue_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).retail_revenue_acc_yoy(r),
            revenue_type,
            label,
            color,
        )

    def add_retail_revenue_raw(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).retail_revenue_raw(r),
            gdp,
            label,
            color,
        )

    def add_retail_revenue_yoy(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).retail_revenue_yoy(r),
            gdp,
            label,
            color,
        )

    def add_cpi(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).cpi(r),
            gdp,
            label,
            color,
        )

    def add_cpi_mom(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).cpi_mom(r),
            gdp,
            label,
            color,
        )

    def add_cpi_yoy(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).cpi_yoy(r),
            gdp,
            label,
            color,
        )

    def add_gdp_real(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_real(r),
            gdp,
            label,
            color,
        )

    def add_gdp_real_raw(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_real_raw(r),
            gdp,
            label,
            color,
        )

    def add_gdp_real_yoy(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_real_yoy(r),
            gdp,
            label,
            color,
        )

    def add_gdp_real_acc_yoy(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_real_acc_yoy(r),
            gdp,
            label,
            color,
        )

    def add_gdp_real_acc_raw(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_real_acc_raw(r),
            gdp,
            label,
            color,
        )

    def add_gdp_nominal(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_nominal(r),
            gdp,
            label,
            color,
        )

    def add_gdp_nominal_raw(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_nominal_raw(r),
            gdp,
            label,
            color,
        )

    def add_gdp_nominal_acc_yoy(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_nominal_acc_yoy(r),
            gdp,
            label,
            color,
        )

    def add_gdp_nominal_acc_raw(self, gdp, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).gdp_nominal_acc_raw(r),
            gdp,
            label,
            color,
        )

    def add_iip_mom(self, iip_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).iip_mom(r),
            iip_type,
            label,
            color,
        )

    def add_iip_qoq(self, iip_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).iip_qoq(r),
            iip_type,
            label,
            color,
        )

    def add_iip_yoy(self, iip_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).iip_yoy(r),
            iip_type,
            label,
            color,
        )

    def add_iip_acc_yoy(self, iip_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).iip_acc_yoy(r),
            iip_type,
            label,
            color,
        )

    def add_transport_index_qoq(self, transport_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).transport_index_qoq(r),
            transport_type,
            label,
            color,
        )

    def add_transport_index_yoy(self, transport_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).transport_index_yoy(r),
            transport_type,
            label,
            color,
        )

    def add_transport_index_acc_yoy(self, transport_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).transport_index_acc_yoy(r),
            transport_type,
            label,
            color,
        )

    def add_producer_index_qoq(self, producer_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).producer_index_qoq(r),
            producer_type,
            label,
            color,
        )

    def add_producer_index_yoy(self, producer_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).producer_index_yoy(r),
            producer_type,
            label,
            color,
        )

    def add_producer_index_acc_yoy(self, producer_type, label=None, color=None):
        return self._add_series(
            lambda r: economy(self.from_macro).producer_index_acc_yoy(r),
            producer_type,
            label,
            color,
        )

    """-------------------GOVERNMENT---------------------"""

    def add_public_investment(self, ministry, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).public_investment(r),
            ministry,
            label,
            color,
        )

    def add_social_investment(self, ministry, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).social_investment(r),
            ministry,
            label,
            color,
        )

    def add_budget_in(self, budget_type, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).budget_in(r),
            budget_type,
            label,
            color,
        )

    def add_budget_out(self, budget_type, label=None, color=None):
        return self._add_series(
            lambda r: government(self.from_macro).budget_out(r),
            budget_type,
            label,
            color,
        )

    """-------------------FOREIGN---------------------"""

    def add_import_byproduct(self, product, label=None, color=None):
        return self._add_series(
            lambda r: foreign(self.from_macro).import_byproduct(r),
            product,
            label,
            color,
        )

    def add_import_bylocation(self, location, label=None, color=None):
        return self._add_series(
            lambda r: foreign(self.from_macro).import_bylocation(r),
            location,
            label,
            color,
        )

    def add_export_byproduct(self, product, label=None, color=None):
        return self._add_series(
            lambda r: foreign(self.from_macro).export_byproduct(r),
            product,
            label,
            color,
        )

    def add_export_bylocation(self, location, label=None, color=None):
        return self._add_series(
            lambda r: foreign(self.from_macro).export_bylocation(r),
            location,
            label,
            color,
        )

    def add_export_import_total(self, total_type, label=None, color=None):
        return self._add_series(
            lambda r: foreign(self.from_macro).export_import_total(r),
            total_type,
            label,
            color,
        )

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
        ax.xaxis.set_major_locator(ticker.MaxNLocator(nbins=10))

    def plot(self, save_path="None"):
        if not self.data_list:
            raise ValueError("Chưa có dữ liệu để vẽ.")

        fig, host = plt.subplots(figsize=(12, 6))
        fig.subplots_adjust(right=0.75)
        host.set_title(self.title)

        host.spines["left"].set_visible(False)
        host.yaxis.set_visible(False)

        # định dạng giá trị
        def auto_format(value, pos):
            if value == 0:
                return "0"
            abs_value = abs(value)
            if abs_value >= 1e9:
                return f"{value/1e9:.1f}B"
            elif abs_value >= 1e6:
                return f"{value/1e6:.1f}M"
            elif abs_value >= 1e3:
                return f"{value/1e3:.1f}K"
            return f"{value:.3f}"

        axes = []
        for i, data in enumerate(self.data_list):
            ax = host.twinx()
            if i > 0:
                ax.spines["right"].set_position(("axes", 1 + 0.1 * i))

            color = self._resolve_color(data["color"])
            # Shorten legend
            label = data["label"]
            if isinstance(label, str) and len(label) > 30:
                label = label[:30] + "..."

            # Áp dụng làm mượt nếu được bật
            if self.smooth_data:
                smooth_dates, smooth_values = self._smooth_data_series(
                    data["df"]["Date"], data["df"]["Value"]
                )
                ax.plot(
                    smooth_dates,
                    smooth_values,
                    color=color,
                    label=data["label"],
                    linewidth=1.5,  # Làm dày hơn cho đường mượt
                    alpha=0.8,
                )
                # Có thể thêm điểm gốc nếu muốn
                # ax.scatter(data["df"]["Date"], data["df"]["Value"],
                #           color=color, s=10, alpha=0.6, zorder=5)
            else:
                ax.plot(
                    data["df"]["Date"],
                    data["df"]["Value"],
                    color=color,
                    label=data["label"],
                    linewidth=1,
                )

            ax.yaxis.set_major_formatter(plt.FuncFormatter(auto_format))
            ax.tick_params(axis="y", labelcolor=color)
            # ax.set_ylabel(data["label"], color=color)
            ax.spines["right"].set_color(color)
            axes.append(ax)

        self._setup_xaxis(host)

        if len(self.data_list) <= 5:
            lines, labels = [], []
            for ax in axes:
                lns, lbls = ax.get_legend_handles_labels()
                lines.extend(lns)
                labels.extend(lbls)

            host.legend(lines, labels, loc="upper left", framealpha=0.5)

        fig.tight_layout()

        saved_path = None
        if save_path is not None:

            if os.path.isdir(save_path) or not os.path.splitext(save_path)[1]:
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                filename = datetime.now().strftime("LineChart_%Y%m%d_%H%M%S.jpg")
                saved_path = os.path.join(save_path, filename)
            else:

                saved_path = save_path

            fig.savefig(saved_path, dpi=300, bbox_inches="tight")
            print(f"Hình ảnh đã được lưu tại {saved_path}")

        plt.close(fig)

        return saved_path

    def plot_bar(self, save_path="None"):
        if not self.data_list:
            raise ValueError("Chưa có dữ liệu để vẽ.")

        fig, host = plt.subplots(figsize=(12, 6))
        fig.subplots_adjust(right=0.75)
        host.set_title(self.title)

        host.spines["left"].set_visible(False)
        host.yaxis.set_visible(False)

        plotted_axes = []
        num_series_to_plot = len(self.data_list)

        all_dates_from_valid_dfs = [
            data_item["df"]["Date"] for data_item in self.data_list
        ]
        concatenated_dates = pd.concat(all_dates_from_valid_dfs)
        all_unique_sorted_dates = sorted(concatenated_dates.unique())

        min_interval_days = 1.0  #
        if len(all_unique_sorted_dates) > 1:
            numeric_dates = mdates.date2num(all_unique_sorted_dates)
            diffs = np.diff(numeric_dates)
            positive_diffs = diffs[diffs > 0]  #
            if len(positive_diffs) > 0:
                min_interval_days = np.min(positive_diffs)
            else:
                if self.timeframe == "daily":
                    min_interval_days = 1.0
                elif self.timeframe == "monthly":
                    min_interval_days = 25.0
                elif self.timeframe == "quarterly":
                    min_interval_days = 75.0
                elif self.timeframe == "yearly":
                    min_interval_days = 300.0
        elif len(all_unique_sorted_dates) == 1:
            if self.timeframe == "daily":
                min_interval_days = 1.0
            elif self.timeframe == "monthly":
                min_interval_days = 25.0
            elif self.timeframe == "quarterly":
                min_interval_days = 75.0
            elif self.timeframe == "yearly":
                min_interval_days = 300.0

        bar_group_total_width_data_units = min_interval_days * 0.8  #
        individual_bar_width_data_units = (
            bar_group_total_width_data_units / num_series_to_plot
        )

        def auto_format(value, pos):
            if value == 0:
                return "0"
            abs_value = abs(value)
            if abs_value >= 1e9:
                return f"{value/1e9:.1f}B"
            elif abs_value >= 1e6:
                return f"{value/1e6:.1f}M"
            elif abs_value >= 1e3:
                return f"{value/1e3:.1f}K"
            return f"{value:.1f}"

        for series_idx, data_item in enumerate(self.data_list):
            df = data_item["df"]
            ax = host.twinx()

            if len(plotted_axes) > 0:
                ax.spines["right"].set_position(("axes", 1 + 0.1 * len(plotted_axes)))

            color = self._resolve_color(data_item["color"])
            # shorten legends
            label = data_item["label"]
            if isinstance(label, str) and len(label) > 30:
                label = label[:30] + "..."
            x_numeric = mdates.date2num(df["Date"])

            offset = (
                series_idx - (num_series_to_plot - 1) / 2.0
            ) * individual_bar_width_data_units

            ax.bar(
                x_numeric + offset,
                df["Value"],
                width=individual_bar_width_data_units
                * 0.95,  # Bar width (slightly less for small gap)
                color=color,
                label=data_item["label"],
            )

            ax.yaxis.set_major_formatter(FuncFormatter(auto_format))
            ax.tick_params(axis="y", labelsize=8, labelcolor=color)
            # ax.set_ylabel(data_item["label"], color=color)  # Set y-axis label
            ax.spines["right"].set_color(color)
            ax.set_ylim(bottom=0)

            plotted_axes.append(ax)

        if not plotted_axes:
            print("No data was plotted for the bar chart.")
            plt.close(fig)
            return None

        self._setup_xaxis(host)

        if all_unique_sorted_dates:
            padding_x = max(min_interval_days * 0.5, 0.5)
            host.set_xlim(
                mdates.date2num(all_unique_sorted_dates[0]) - padding_x,
                mdates.date2num(all_unique_sorted_dates[-1]) + padding_x,
            )

        if len(plotted_axes) <= 5 and len(plotted_axes) > 0:
            lines, labels = [], []
            for ax_k in plotted_axes:  # Get legend items from plotted axes
                lns, lbls = ax_k.get_legend_handles_labels()
                lines.extend(lns)
                labels.extend(lbls)
            if lines and labels:  # Check if legend items were successfully gathered
                host.legend(lines, labels, loc="upper left", framealpha=0.5)

        fig.tight_layout()

        saved_path = None
        if save_path is not None:

            if os.path.isdir(save_path) or not os.path.splitext(save_path)[1]:
                if not os.path.exists(save_path):
                    os.makedirs(save_path)
                filename = datetime.now().strftime("BarChart_%Y%m%d_%H%M%S.jpg")
                saved_path = os.path.join(save_path, filename)
            else:

                saved_path = save_path

            fig.savefig(saved_path, dpi=300, bbox_inches="tight")
            print(f"Hình ảnh đã được lưu tại {saved_path}")

        plt.close(fig)

        return saved_path


# combinechart(timeframe="daily").add_interbank_vol("Doanh số kỳ hạn qua đêm").plot()
