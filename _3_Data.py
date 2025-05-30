from _0_Dependencies import *
from _3_Data_Macro import *

# from _3_Data_Company import *
# from _3_Data_Stock import *


class world:
    def __init__(self):
        self.world_code = "world"


class country(world):
    def __init__(self, country_code):
        self._1_args = (country_code,)
        world.__init__(self)
        self.country_code = country_code

    def world(self):
        return world()

    def industry_dicts(self, exchanges=["hose", "hnx", "upcom"]):
        objs = db.importing_objs("COMPANY")
        exchanges = [str(_).lower() for _ in exchanges]
        industry_names = np.unique(
            [obj.data_dict["icbName"] for obj in objs if "icbName" in obj.data_dict]
        )

        i_dicts = {}
        for industry_name in industry_names:
            same_objs = list(
                filter(
                    lambda o: (
                        "icbName" in o.data_dict
                        and "exchange" in o.data_dict
                        and o.data_dict["icbName"] == industry_name
                        and (
                            o.data_dict["exchange"].lower() in exchanges
                            or (
                                "index_codes" in o.data_dict
                                and any(
                                    [
                                        e.lower() in o.data_dict["index_codes"]
                                        for e in exchanges
                                    ]
                                )
                            )
                        )
                    ),
                    objs,
                )
            )

            company_codes = [o.data_name for o in same_objs]
            company_count = len(company_codes)
            if company_count != 0:
                industry_code = industry_name.replace(" ", ".").lower()
                sector_name = same_objs[0].data_dict["sector"]
                i_dicts[industry_code] = {
                    "industry_code": industry_code,
                    "industry_name": industry_name,
                    "company_codes": company_codes,
                    "company_count": company_count,
                    "sector_name": sector_name,
                }
        return i_dicts


class macro(country):
    def __init__(self, country_code):
        self._1_args = (country_code,)
        country.__init__(self, *self._1_args[-1:])
        self.macro_code = f"macro{country_code}"

    def world(self):
        return world()

    def country(self):
        return country(*self._1_args[-1:])

    def finance(self):
        return finance(self)

    def economy(self):
        return economy(self)

    def foreign(self):
        return foreign(self)

    def government(self):
        return government(self)


# class industry(country):
# 	def __init__(self, industry_code, country_code):
# 		self._2_args = (industry_code, country_code, )
# 		country.__init__(self, *self._2_args[-1:])
# 		self.industry_code = industry_code

# 	def world(self): return world()
# 	def macro(self): return macro(*self._2_args[-1:])
# 	def country(self): return country(*self._2_args[-1:])
# 	def peer_data_dicts(self):
# 		objs = list(filter(lambda o: ('icbName' in o.data_dict and
# 			o.data_dict['icbName'].replace(' ','.').lower() == self.industry_code
# 		), db.importing_objs('COMPANY')))
# 		return [obj.data_dict for obj in objs]

# class company(industry):
# 	def __init__(self, company_code, industry_code=None, country_code=None):
# 		self.company_obj = db.importing_obj('COMPANY', company_code, 'NOW')
# 		self.company_data_dict = self.company_obj.data_dict if self.company_obj else {}
# 		self._3_args = self.get_args(company_code) if None in [industry_code, country_code] else (company_code, industry_code, country_code)
# 		industry.__init__(self, *self._3_args[-2:])
# 		self.company_code = company_code

# 	def get_args(self, company_code):
# 		industry_code, country_code = None, 'vn'
# 		if 'icbName' in self.company_data_dict:
# 			industry_code = self.company_data_dict['icbName']
# 			industry_code = industry_code.replace(' ','.').lower()
# 		return company_code, industry_code, country_code

# 	def world(self): return world()
# 	def macro(self): return macro(*self._3_args[-1:])
# 	def country(self): return country(*self._3_args[-1:])
# 	def industry(self): return industry(*self._3_args[-2:])
# 	def FS(self, target_fields=[], target_time_codes=[]): return FS(self, target_fields, target_time_codes)
# 	def stock(self): return stock(self)

if __name__ == "__main__":
    
    pass