from _0_Dependencies import *
from _2_Mysql import db


def stretch(data_arr, method="last"):
    def detect_level(code):
        if code.startswith("D"):
            return "daily"
        if code.startswith("M"):
            return "monthly"
        if code.startswith("Q"):
            return "quarterly"
        if code.startswith("Y"):
            return "yearly"
        return None

    def parse_date(code):
        prefix, body = code[0], code[1:]
        if prefix == "D":
            return datetime.strptime(body, "%Y_%m_%d")
        elif prefix == "M":
            return datetime.strptime(body, "%Y_%m")
        elif prefix == "Q":
            y, q = body.split("_0")
            return datetime(int(y), 3 * int(q) - 2, 1)
        elif prefix == "Y":
            return datetime(int(body), 1, 1)
        return None

    def to_code(dt, level):
        if level == "daily":
            return "D" + dt.strftime("%Y_%m_%d")
        elif level == "monthly":
            return "M" + dt.strftime("%Y_%m")
        elif level == "quarterly":
            q = (dt.month - 1) // 3 + 1
            return f"Q{dt.year}_0{q}"
        elif level == "yearly":
            return "Y" + str(dt.year)

    def aggregate(grouped, level):
        result = []
        for code in sorted(grouped):
            values = grouped[code]
            if method == "average":
                agg_value = mean(values)
            elif method == "sum":
                agg_value = sum(values)
            elif method == "last":
                agg_value = values[-1]
            elif method == "min":
                agg_value = min(values)
            elif method == "max":
                agg_value = max(values)
            else:
                raise ValueError(f"Phương pháp không hợp lệ: {method}")
            result.append([code, agg_value])
        return result

    if not data_arr:
        return {"daily": [], "monthly": [], "quarterly": [], "yearly": []}

    # Detect original level
    from_level = detect_level(data_arr[0][0])
    level_order = ["daily", "monthly", "quarterly", "yearly"]
    level_index = level_order.index(from_level)

    # Parse raw data
    parsed = [[parse_date(code), float(value)] for code, value in data_arr]

    result = {
        "daily": [],
        "monthly": [],
        "quarterly": [],
        "yearly": [],
    }

    # Store original level data
    result[from_level] = data_arr

    # Stretch to higher levels
    for target_level in level_order[level_index + 1 :]:
        grouped = defaultdict(list)
        for dt, value in parsed:
            code = to_code(dt, target_level)
            grouped[code].append(value)
        result[target_level] = aggregate(grouped, target_level)

    return result


class stockprice:
    def __init__(self, from_macro=None, to_macro=None):
        self.from_macro = from_macro
        self.to_macro = to_macro

    # determine prefix of input time
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
                start_month = 3 * int(q) - 2
                return (
                    datetime(int(year), start_month + 3, 1)
                    if end
                    else datetime(int(year), start_month, 1)
                )
            elif prefix == "Y":
                return (
                    datetime(int(date_str) + int(end), 1, 1)
                    if end
                    else datetime(int(date_str), 1, 1)
                )
        except:
            print(f"Lỗi phân tích macro: {macro}")
        return None

    def market(self):
        fin = self

        class market:
            def vnindex(self):
                class vnindex:
                    def __init__(self):
                        self.from_date = fin._parse_macro(fin.from_macro)
                        self.to_date = fin._parse_macro(fin.to_macro, end=True)
                        self.stretch_method = "last"
                        self.candles = self._load_candles()

                    # get candle
                    def _load_candles(self):
                        records = db.importing_objs(
                            "STOCKPRICE", data_name="VNINDEX", time_code="NOW"
                        )
                        return (
                            records[0].data_dict.get("candle_arr", [])
                            if records
                            else []
                        )

                    def _filter_candles(self):
                        result = []
                        for candle in self.candles:
                            try:
                                dt = datetime.strptime(str(candle[0]), "%Y%m%d")
                                if (not self.from_date or dt >= self.from_date) and (
                                    not self.to_date or dt < self.to_date
                                ):
                                    result.append(candle)
                            except:
                                continue
                        return result

                    def _format_to_daily(self, candles):
                        result = []
                        for candle in candles:
                            try:
                                dt = datetime.strptime(str(candle[0]), "%Y%m%d")
                                date_str = dt.strftime("D%Y_%m_%d")
                                result.append([date_str, *candle[1:]])
                            except:
                                continue
                        return result

                    def get_data_arr(self):
                        filtered = self._filter_candles()
                        return self._format_to_daily(filtered)

                    def stretch(self, method=None):
                        return stretch(
                            self.get_data_arr(),
                            method or self.stretch_method,
                        )

                    # def visualize(self, chart_type="candlestick"):
                    #     df = pd.DataFrame(
                    #         self._format_to_daily(self._filter_candles()),
                    #         columns=["Date", "Open", "High", "Low", "Close", "Volume"],
                    #     )
                    #     df["Date"] = pd.to_datetime(df["Date"], format="D%Y_%m_%d")
                    #     df.set_index("Date", inplace=True)
                    #     if chart_type == "line":
                    #         df["Close"].plot(
                    #             figsize=(15, 6), title="VNINDEX Close Price", grid=True
                    #         )
                    #     else:
                    #         mpf.plot(
                    #             df,
                    #             type="candle",
                    #             style="charles",
                    #             title="VNINDEX Candlestick Chart",
                    #             volume=True,
                    #             mav=(5, 10),
                    #             figratio=(12, 6),
                    #         )
                    #     plt.show()

                return vnindex()

            def stock(self, symbol):
                class stock:
                    def __init__(self):
                        self.symbol = symbol.upper()
                        self.from_date = fin._parse_macro(fin.from_macro)
                        self.to_date = fin._parse_macro(fin.to_macro, end=True)
                        self.stretch_method = "last"
                        self.candles = self._load_candles()

                    def _load_candles(self):
                        records = db.importing_objs(
                            "STOCKPRICE", data_name=self.symbol, time_code="NOW"
                        )
                        if not records:
                            print(f"Không tìm thấy dữ liệu cho mã {self.symbol}")
                            return []
                        return records[0].data_dict.get("candle_arr", [])

                    def _filter_candles(self):
                        result = []
                        for candle in self.candles:
                            try:
                                dt = datetime.strptime(str(candle[0]), "%Y%m%d")
                                if (not self.from_date or dt >= self.from_date) and (
                                    not self.to_date or dt < self.to_date
                                ):
                                    result.append(candle)
                            except:
                                continue
                        return result

                    def _format_to_daily(self, candles):
                        result = []
                        for candle in candles:
                            try:
                                dt = datetime.strptime(str(candle[0]), "%Y%m%d")
                                date_str = dt.strftime("D%Y_%m_%d")
                                result.append([date_str, *candle[1:]])
                            except:
                                continue
                        return result

                    def get_data_arr(self):
                        if not self.candles:
                            return []
                        filtered = self._filter_candles()
                        return self._format_to_daily(filtered)

                    def stretch(self, method=None):
                        return stretch(
                            [
                                [row[0], row[4]]
                                for row in self.get_data_arr()
                                if len(row) >= 5
                            ],
                            self.stretch_method,
                        )

                    def visualize(self, chart_type="candlestick"):
                        if not self.candles:
                            print(f"Không có dữ liệu cho mã {self.symbol}")
                            return
                        df = pd.DataFrame(
                            self._format_to_daily(self._filter_candles()),
                            columns=["Date", "Open", "High", "Low", "Close", "Volume"],
                        )
                        df["Date"] = pd.to_datetime(df["Date"], format="D%Y_%m_%d")
                        df.set_index("Date", inplace=True)
                        if chart_type == "line":
                            df["Close"].plot(
                                figsize=(15, 6),
                                title=f"{self.symbol} Close Price",
                                grid=True,
                            )
                        else:
                            mpf.plot(
                                df,
                                type="candle",
                                style="charles",
                                title=f"{self.symbol} Candlestick Chart",
                                volume=True,
                                mav=(5, 10),
                                figratio=(12, 6),
                            )
                        plt.show()

                return stock()

        return market()

# print(
#     stockprice("M2024_04").market()
#     .stock(
#         "FPT"
#     )
#     .get_data_arr()
# )
# print(stockprice("M2024_04").market().stock("FPT").stretch())


class finance:
    def __init__(self, from_macro=None, to_macro=None):
        self.from_macro = from_macro
        self.to_macro = to_macro

    class Inner(object):
        pass

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
                start_month = 3 * int(q) - 2
                return (
                    datetime(int(year), start_month + 3, 1)
                    if end
                    else datetime(int(year), start_month, 1)
                )
            elif prefix == "Y":
                return (
                    datetime(int(date_str) + int(end), 1, 1)
                    if end
                    else datetime(int(date_str), 1, 1)
                )
        except:
            print(f"Lỗi phân tích macro: {macro}")
        return None

    def interbank_rate(self, rate_type):
        fin = self

        class interbank_rate(fin.Inner):
            def __init__(self, rate_type):
                self.rate_type = rate_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.term_mapping = {
                    "ON": "Lãi suất BQ liên NH kỳ hạn qua đêm",
                    "1W": "Lãi suất BQ liên NH kỳ hạn 1 tuần",
                    "2W": "Lãi suất BQ liên NH kỳ hạn 2 tuần",
                    "1M": "Lãi suất BQ liên NH kỳ hạn 1 tháng",
                    "3M": "Lãi suất BQ liên NH kỳ hạn 3 tháng",
                    "6M": "Lãi suất BQ liên NH kỳ hạn 6 tháng",
                    "9M": "Lãi suất BQ liên NH kỳ hạn 9 tháng",
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "average"
                self.data_arr = self._load_data(self.rate_type)

            def _load_data(self, rate_type):
                term_field = self.term_mapping.get(rate_type)
                if not term_field:
                    raise ValueError(f"Kỳ hạn '{rate_type}' không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="interbank_rate")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    rate_info = data_dict.get(term_field)

                    if rate_info and "data" in rate_info:
                        result.append([time_code, rate_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    try:
                        dt = datetime.strptime(row[0], "D%Y_%m_%d")
                        if (not self.from_date or dt >= self.from_date) and (
                            not self.to_date or dt <= self.to_date
                        ):
                            result.append([dt, row[1]])
                    except:
                        continue
                return result

            def _format_to_daily(self, data):
                result = []
                for item in data:
                    try:
                        date_str = item[0].strftime("D%Y_%m_%d")
                        result.append([date_str, item[1]])
                    except:
                        continue
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return self._format_to_daily(filtered)

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )
        return interbank_rate(rate_type)

# print(finance("Y2025").interbank_rate("1M").stretch())


class economy:
    def __init__(self, from_macro=None, to_macro=None):
        self.from_macro = from_macro
        self.to_macro = to_macro

    class Inner(object):
        pass

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
                start_month = 3 * int(q) - 2
                return (
                    datetime(int(year), start_month + 3, 1)
                    if end
                    else datetime(int(year), start_month, 1)
                )
            elif prefix == "Y":
                return (
                    datetime(int(date_str) + int(end), 1, 1)
                    if end
                    else datetime(int(date_str), 1, 1)
                )
        except:
            print(f"Lỗi phân tích macro: {macro}")
        return None

    def gdp_real(self, gdp):
        fin = self

        class gdp_real(fin.Inner):
            def __init__(self, gdp):
                self.gdp = gdp
                self.stretch_method = "average"
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.gdp_mapping = {
                    key: key
                    for key in [
                        "Tổng GDP_value",
                        "Tổng GDP_growth",
                        "Tổng GDP_Dịch vụ_value",
                        "Tổng GDP_Dịch vụ_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_value",
                        "Tổng GDP_Công nghiệp và xây dựng_growth",
                        "Tổng GDP_Dịch vụ_Vận tải, kho bãi_value",
                        "Tổng GDP_Dịch vụ_Vận tải, kho bãi_growth",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_value",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_growth",
                        "Tổng GDP_Dịch vụ_Giáo dục và đào tạo_value",
                        "Tổng GDP_Dịch vụ_Giáo dục và đào tạo_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Xây dựng_value",
                        "Tổng GDP_Dịch vụ_Thông tin và truyền thông_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Xây dựng_growth",
                        "Tổng GDP_Dịch vụ_Thông tin và truyền thông_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động dịch vụ khác_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_value",
                        "Tổng GDP_Dịch vụ_Hoạt động dịch vụ khác_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_growth",
                        "Tổng GDP_Dịch vụ_Dịch vụ lưu trú và ăn uống_value",
                        "Tổng GDP_Dịch vụ_Dịch vụ lưu trú và ăn uống_growth",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Thủy sản_value",
                        "Tổng GDP_Thuế sản phẩm trừ trợ cấp sản phẩm_value",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Lâm nghiệp_value",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Thủy sản_growth",
                        "Tổng GDP_Thuế sản phẩm trừ trợ cấp sản phẩm_growth",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Lâm nghiệp_growth",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Nông nghiệp_value",
                        "Tổng GDP_Dịch vụ_Nghệ thuật, vui chơi và giải trí_value",
                        "Tổng GDP_Nông, lâm nghiệp và thủy sản_Nông nghiệp_growth",
                        "Tổng GDP_Dịch vụ_Nghệ thuật, vui chơi và giải trí_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động kinh doanh bất động sản_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Khai khoáng_value",
                        "Tổng GDP_Dịch vụ_Hoạt động kinh doanh bất động sản_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Khai khoáng_growth",
                        "Tổng GDP_Dịch vụ_Y tế và hoạt động trợ giúp xã hội_value",
                        "Tổng GDP_Dịch vụ_Y tế và hoạt động trợ giúp xã hội_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động hành chính và dịch vụ hỗ trợ_value",
                        "Tổng GDP_Dịch vụ_Hoạt động hành chính và dịch vụ hỗ trợ_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động chuyên môn, khoa học và công nghệ_value",
                        "Tổng GDP_Dịch vụ_Hoạt động tài chính, ngân hàng và bảo hiểm_value",
                        "Tổng GDP_Dịch vụ_Hoạt động chuyên môn, khoa học và công nghệ_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động tài chính, ngân hàng và bảo hiểm_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Công nghiệp chế biến, chế tạo_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Công nghiệp chế biến, chế tạo_growth",
                        "Tổng GDP_Dịch vụ_Bán buôn và bán lẻ; sửa chữa ô tô, mô tô, xe máy và xe có động cơ khác_value",
                        "Tổng GDP_Dịch vụ_Bán buôn và bán lẻ; sửa chữa ô tô, mô tô, xe máy và xe có động cơ khác_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Cung cấp nước; hoạt động quản lý và xử lý rác thải, nước thải_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Cung cấp nước; hoạt động quản lý và xử lý rác thải, nước thải_growth",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Sản xuất và phân phối điện, khí đốt, nước nóng, hơi nước và điều hòa không khí_value",
                        "Tổng GDP_Công nghiệp và xây dựng_Công nghiệp_Sản xuất và phân phối điện, khí đốt, nước nóng, hơi nước và điều hòa không khí_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động của Đảng Cộng sản, tổ chức chính trị-xã hội; quản lý Nhà nước, an ninh quốc phòng; đảm bảo xã hội bắt buộc_value",
                        "Tổng GDP_Dịch vụ_Hoạt động của Đảng Cộng sản, tổ chức chính trị-xã hội; quản lý Nhà nước, an ninh quốc phòng; đảm bảo xã hội bắt buộc_growth",
                        "Tổng GDP_Dịch vụ_Hoạt động làm thuê các công việc trong các hộ gia đình, sản xuất sản phẩm vật chất và dịch vụ tự tiêu dùng của hộ gia đình_value",
                        "Tổng GDP_Dịch vụ_Hoạt động làm thuê các công việc trong các hộ gia đình, sản xuất sản phẩm vật chất và dịch vụ tự tiêu dùng của hộ gia đình_growth",
                    ]
                }
                # self.industry_keys =
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "average"
                self.data_arr = self._load_data(self.gdp)

            def _load_data(self, gdp):
                gdp_field = self.gdp_mapping.get(gdp)
                if not gdp_field:
                    raise ValueError(f"{gdp_field} không hợp lệ.")

                records = db.importing_objs("Economy", data_name="gdp_real")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    gdp_info = data_dict.get(gdp_field)

                    if gdp_info and "data" in gdp_info:
                        result.append([time_code, gdp_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if dt and (not self.from_date or dt >= self.from_date) and (not self.to_date or dt <= self.to_date):
                        result.append(row)
                return result
                    

            # def _format_to_daily(self, data):
            #     result = []
            #     for item in data:
            #         try:
            #             date_str = item[0].strftime("D%Y_%m_%d")
            #             result.append([date_str, item[1]])
            #         except:
            #             continue
            #     return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

            # def visualize(self):
            #     df = pd.DataFrame(self.get_data_arr(), columns=["Date", "Value"])
            #     df["Date"] = pd.to_datetime(df["Date"], format="D%Y_%m_%d")
            #     df.set_index("Date", inplace=True)
            #     df["Value"].plot(
            #         figsize=(15, 6),
            #         title=f"Interbank Rate - {self.rate_type}",
            #         grid=True,
            #     )
            #     plt.show()

        return gdp_real(gdp)


class foreign():
	def __init__(self, macro):
		self.macro = macro

	class Inner(object):
		pass

	def exchange_rate(self):
		foreig = self
		class exchange_rate(foreig.Inner):
			def __init__(self):
				pass

		return exchange_rate()

class government():
    def __init__(self, macro):
        self.macro = macro

    class Inner(object):
        pass

    # print(stockprice('M2025_04').market().stock('vnindex').get_data_arr())

    # print(economy("M2020_04", "M2022_05").gdp_real("Tổng GDP_value").get_data_arr())


# print(
#     economy("Q2020_04")
#     .gdp_real(
#         "Tổng GDP_Dịch vụ_Hoạt động làm thuê các công việc trong các hộ gia đình, sản xuất sản phẩm vật chất và dịch vụ tự tiêu dùng của hộ gia đình_value"
#     )
#     .stretch()
# )


# print(
#     economy("Y2020")
#     .gdp_real(
#         "Tổng GDP_Dịch vụ_Hoạt động làm thuê các công việc trong các hộ gia đình, sản xuất sản phẩm vật chất và dịch vụ tự tiêu dùng của hộ gia đình_value"
#     )
#     .stretch()
# )

# print(
#     economy("Y2020")
#     .gdp_real(
#         "Tổng GDP_Dịch vụ_Hoạt động làm thuê các công việc trong các hộ gia đình, sản xuất sản phẩm vật chất và dịch vụ tự tiêu dùng của hộ gia đình_value"
#     )
#     .get_data_arr()
# )
