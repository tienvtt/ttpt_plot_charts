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
                agg_value = statistics.mean(values)
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

                return stock()

        return market()

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

    def reverse_repo(self, repo_type):
        fin = self
        class reverse_repo(fin.Inner):
            def __init__(self, repo_type):
                self.repo_type = repo_type
                self.from_macro= fin.from_macro
                self.to_macro = fin.to_macro
                self.type_mapping = {
                    "LH": "KL lưu hành Reverse Repo",
                    "PH": "KL phát hành Reverse Repo",
                    "ĐH": "KL đáo hạn Reverse Repo"
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end = True)
                self.stretch_method = "average"
                self.data_arr = self._load_data(self.repo_type)

            def _load_data(self, repo_type):
                repo_field = self.type_mapping.get(repo_type)
                if not repo_field:
                    raise ValueError(f"{repo_type} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="reverse_repo")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    repo_info = data_dict.get(repo_field)

                    if repo_info and "data" in repo_info:
                        result.append([time_code, repo_info["data"]])

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

            def stretch(self, method = None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )
        return reverse_repo(repo_type)

    def interbank_vol(self, term):
        fin = self

        class interbank_vol(fin.Inner):
            def __init__(self, term):
                self.term = term
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.term_mapping = {
                    "1M": "Doanh số kỳ hạn 1 tháng",
                    "1W": "Doanh số kỳ hạn 1 tuần",
                    "2W": "Doanh số kỳ hạn 2 tuần",
                    "3M": "Doanh số kỳ hạn 3 tháng",
                    "6M": "Doanh số kỳ hạn 6 tháng",
                    "9M": "Doanh số kỳ hạn 9 tháng",
                    "ON": "Doanh số kỳ hạn qua đêm"
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end = True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.term)

            def _load_data(self, term):
                term_field = self.term_mapping.get(term)
                if not term_field:
                    raise ValueError(f"Kỳ hạn không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name = "interbank_vol")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    term_info = data_dict.get(term_field)

                    if term_info and "data" in term_info:
                        result.append([time_code, term_info["data"]])

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
                    method or self.stretch_method
                )

        return interbank_vol(term)

    def sell_outright(self, outright_type):
        fin = self

        class sell_outright(fin.Inner):
            def __init__(self, outright_type):
                self.outright_type = outright_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.outright_type_mapping = {
                    "LH": "KL lưu hành Tín phiếu",
                    "PH": "KL phát hành Tín phiếu",
                    "ĐH": "KL đáo hạn Tín phiếu",
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end = True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.outright_type)

            def _load_data(self, outright_type):
                outright_type_field = self.outright_type_mapping.get(outright_type)
                if not outright_type_field:
                    raise ValueError(f"KL {outright_type} Tín phiếu không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name = "sell_outright")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    outright_type_info = data_dict.get("outright_type_field")

                    if outright_type_info and "data" in outright_type_info:
                        result.append([time_code, outright_type_info["data"]])

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
                filtered = self._parse_macro(self.from_macro)
                return self.__format__to_daily(filtered)

            def stretch(self, method = None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method
                )
        return sell_outright(outright_type)

    def borrowing (self, industry):
        fin = self

        class borrowing(fin.Inner):
            def __init__(self, industry):
                self.industry = industry
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.industry_mapping = {
                    key: key 
                    for key in [
                    "Xây dựng",
                    "Thương mại",
                    "Vận tải và viễn thông",
                    "Công nghiệp và xây dựng", 
                    "Các hoạt động dịch vụ khác",
                    "TỔNG TÍN DỤNG TRONG NỀN KINH TẾ",
                    "Nông nghiệp, lâm nghiệp và thuỷ sản",
                    "Hoạt động thương mại, vận tải và viễn thông"
                ]
            }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.industry)

            def _load_data(self, industry):
                industry_field = self.industry_mapping.get(industry)
                if not industry_field:
                    raise ValueError(f"{industry_field} không hợp lệ.")

                records = db.importing_objs("Economy", data_name="gdp_real")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    industry_info = data_dict.get(industry_field)

                    if industry_info and "data" in industry_info:
                        result.append([time_code, industry_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if dt and (not self.from_date or dt >= self.from_date) and (not self.to_date or dt <= self.to_date):
                        result.append(row)
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return borrowing(industry)  

    def lending(self, lending_type):
        fin = self

        class lending(fin.Inner):
            def __init__(self, lending_type):
                self.lending_type = lending_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.lending_type_mapping = {
                    key: key
                    for key in [
                        "Tổng tiền gửi",
                        "Tiền gửi của TCKT",
                        "Tiền gửi của dân cư"
                    ]
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.lending_type)

            def _load_data(self, lending_type):
                lending_type_field = self.lending_type_mapping.get(lending_type)
                if not lending_type_field:
                    raise ValueError(f"{lending_type_field} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="lending")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    lending_type_info = data_dict.get(lending_type_field)

                    if lending_type_info and "data" in lending_type_info:
                        result.append([time_code, lending_type_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if (
                        dt
                        and (not self.from_date or dt >= self.from_date)
                        and (not self.to_date or dt <= self.to_date)
                    ):
                        result.append(row)
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return lending(lending_type)

    def M2(self, M2_type):
        fin = self

        class M2(fin.Inner):
            def __init__(self, M2_type):
                self.M2_type = M2_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.M2_type_mapping = {
                    "M2": "M2"
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.M2_type)

            def _load_data(self, M2_type):
                M2_field = self.M2_type_mapping.get(M2_type)
                if not M2_field:
                    raise ValueError(f"{M2_type} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="M2")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    M2_type_info = data_dict.get(M2_field)

                    if M2_type_info and "data" in M2_type_info:
                        result.append([time_code, M2_type_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if (
                        dt
                        and (not self.from_date or dt >= self.from_date)
                        and (not self.to_date or dt <= self.to_date)
                    ):
                        result.append(row)
                return result
            
            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return M2(M2_type)

    def moneysupply(self, moneysupply_type):
        fin = self

        class moneysupply(fin.Inner):
            def __init__(self, moneysupply_type):
                self.moneysupply_type = moneysupply_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.moneysupply_type_mapping = {
                    key: key
                    for key in [
                        "Tổng phương tiện thanh toán",
                        "Tổng phương tiện thanh toán_Tiền gửi của cư dân",
                        "Tổng phương tiện thanh toán_Tiền gửi của các TCKT",
                    ]
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.moneysupply_type)

            def _load_data(self, moneysupply_type):
                moneysupply_type_field = self.moneysupply_type_mapping.get(
                    moneysupply_type
                )
                if not moneysupply_type_field:
                    raise ValueError(f"{moneysupply_type} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="moneysupply")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    moneysupply_type_info = data_dict.get(moneysupply_type_field)

                    if moneysupply_type_info and "data" in moneysupply_type_info:
                        result.append([time_code, moneysupply_type_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if (
                        dt
                        and (not self.from_date or dt >= self.from_date)
                        and (not self.to_date or dt <= self.to_date)
                    ):
                        result.append(row)
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return moneysupply(moneysupply_type)

    def loans(self, loans_type):
        fin = self

        class loans(fin.Inner):

            def __init__(self, loans_type):
                self.loans_type = loans_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.loans_type_mapping = {
                    key: key
                    for key in [
                        "Tổng dư nợ tín dụng",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng xây dựng",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng các HĐ khác",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng công nghiệp",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng thương mại",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng nông, lâm, thủy sản",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng vận tải và viễn thông",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng công nghiệp và xây dựng",
                        "Tổng dư nợ tín dụng_Dư nợ tín dụng thương mại, vận tải, viễn thông"
                    ]
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.loans_type)

            def _load_data(self, loans_type):
                loans_type_field = self.loans_type_mapping.get(
                    loans_type
                )
                if not loans_type_field:
                    raise ValueError(f"{loans_type} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="loans")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    loans_type_info = data_dict.get(loans_type_field)

                    if loans_type_info and "data" in loans_type_info:
                        result.append([time_code, loans_type_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if (
                        dt
                        and (not self.from_date or dt >= self.from_date)
                        and (not self.to_date or dt <= self.to_date)
                    ):
                        result.append(row)
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return loans(loans_type)

    def securities_account(self, account_type):
        fin = self

        class securities_account(fin.Inner):

            def __init__(self, account_type):
                self.account_type = account_type
                self.from_macro = fin.from_macro
                self.to_macro = fin.to_macro
                self.account_type_mapping = {
                    key: key
                    for key in [
                        "Tổng số tài khoản trong nước", 
                        "Tổng số tài khoản chứng khoán",
                        "Tổng số tài khoản nước ngoài",
                        "Số tài khoản cá nhân trong nước",
                        "Số tài khoản cá nhân nước ngoài",
                        "Số tài khoản tổ chức trong nước",
                        "Số tài khoản tổ chức nước ngoài"
                    ]
                }
                self.from_date = fin._parse_macro(self.from_macro)
                self.to_date = fin._parse_macro(self.to_macro, end=True)
                self.stretch_method = "last"
                self.data_arr = self._load_data(self.account_type)

            def _load_data(self, account_type):
                account_type_field = self.account_type_mapping.get(account_type)
                if not account_type_field:
                    raise ValueError(f"{account_type} không hợp lệ.")

                records = db.importing_objs("FINANCE", data_name="securities_account")
                if len(records) == 0:
                    return []

                result = []
                for record in records:
                    time_code = record.time_code
                    data_dict = record.data_dict
                    account_type_info = data_dict.get(account_type_field)

                    if account_type_info and "data" in account_type_info:
                        result.append([time_code, account_type_info["data"]])

                return result

            def _filter_data(self):
                result = []
                for row in self.data_arr:
                    time_code = row[0]
                    dt = fin._parse_macro(time_code)
                    if (
                        dt
                        and (not self.from_date or dt >= self.from_date)
                        and (not self.to_date or dt <= self.to_date)
                    ):
                        result.append(row)
                return result

            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

        return securities_account(account_type)


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
                    
            def get_data_arr(self):
                filtered = self._filter_data()
                return filtered

            def stretch(self, method=None):
                return stretch(
                    self.get_data_arr(),
                    method or self.stretch_method,
                )

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
