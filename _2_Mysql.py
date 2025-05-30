# SQL DEPENDENCIES
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import declarative_base, Session #pip install mysqlclient
from sqlalchemy import Column, Integer, String, JSON, tuple_, inspect

from _0_Dependencies import *
from _0_Hardcode import *
from _1_Sub_Func import *

Base = declarative_base()

class database():
	def __init__(self, name, host):
		self.host = host
		self.port = mysql_database_dict['port']
		self.user = mysql_database_dict['user']
		self.password = mysql_database_dict['password']
		self.database_name = name
		self.connecting()
		
	def connecting(self):
		try:
			connect_string = f"mysql://{qp(self.user)}:{qp(self.password)}@{qp(self.host)}:{qp(self.port)}/{qp(self.database_name)}"
			self.engine = create_engine(connect_string, pool_pre_ping=True, pool_size=10, max_overflow=15)
			Base.metadata.create_all(self.engine)
		except DatabaseError as error:
			print(f'DB Error Connecting: {error}')
	
	def error_handler(self, error):
		if "Can't connect to server" in str(error.args[0]):
			self.connecting() 
		else:
			print(f'DB Error: {error}')
				
	# --------------------------------------------------------------------------------------------

	class FINANCE(Base):
		__tablename__ = 'FINANCE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_REALTIME(Base):
		__tablename__ = 'STOCKTRADING_REALTIME'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_INDEX(Base):
		__tablename__ = 'FINANCE_INDEX'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_INTERESTRATE(Base):
		__tablename__ = 'FINANCE_INTERESTRATE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_GBOND(Base):
		__tablename__ = 'FINANCE_GBOND'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_GBOND_RATE(Base):
		__tablename__ = 'FINANCE_GBOND_RATE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_GBOND_BIDDING(Base):
		__tablename__ = 'FINANCE_GBOND_BIDDING'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_GBOND_TRADING(Base):
		__tablename__ = 'FINANCE_GBOND_TRADING'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_GBOND_BUYBACK(Base):
		__tablename__ = 'FINANCE_GBOND_BUYBACK'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_CBOND(Base):
		__tablename__ = 'FINANCE_CBOND'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_CBOND_COMPANY(Base):
		__tablename__ = 'FINANCE_CBOND_COMPANY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_CBOND_TRADING(Base):
		__tablename__ = 'FINANCE_CBOND_TRADING'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FINANCE_CBOND_BUYBACK(Base):
		__tablename__ = 'FINANCE_CBOND_BUYBACK'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class ECONOMY(Base):
		__tablename__ = 'ECONOMY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class ANALYSIS_REPORT(Base):
		__tablename__ = 'ANALYSIS_REPORT'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class ECONOMY_REAL_ESTATE(Base):
		__tablename__ = 'ECONOMY_REAL_ESTATE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FOREIGN(Base):
		__tablename__ = 'FOREIGN'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FOREIGN_FX(Base):
		__tablename__ = 'FOREIGN_FX'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FOREIGN_FDI(Base):
		__tablename__ = 'FOREIGN_FDI'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class GOVERNMENT(Base):
		__tablename__ = 'GOVERNMENT'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING(Base):
		__tablename__ = 'STOCKTRADING'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_BYINVESTOR(Base):
		__tablename__ = 'STOCKTRADING_BYINVESTOR'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_FOREIGN(Base):
		__tablename__ = 'STOCKTRADING_FOREIGN'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class FSORDER(Base):
		__tablename__ = 'FSORDER'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_PROPRIETARY(Base):
		__tablename__ = 'STOCKTRADING_PROPRIETARY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_ORDER(Base):
		__tablename__ = 'STOCKTRADING_ORDER'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKPRICE(Base):
		__tablename__ = 'STOCKPRICE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANY(Base):
		__tablename__ = 'COMPANY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANYIS(Base):
		__tablename__ = 'COMPANYIS'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANYBS(Base):
		__tablename__ = 'COMPANYBS'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANYCF(Base):
		__tablename__ = 'COMPANYCF'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANYNOTE(Base):
		__tablename__ = 'COMPANYNOTE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class COMPANYRATIO(Base):
		__tablename__ = 'COMPANYRATIO'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_ECONOMY_PMI(Base):
		__tablename__ = 'WORLD_ECONOMY_PMI'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_ECONOMY_CPI(Base):
		__tablename__ = 'WORLD_ECONOMY_CPI'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_FOREIGN_FX(Base):
		__tablename__ = 'WORLD_FOREIGN_FX'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_INDUSTRY_COMMODITY(Base):
		__tablename__ = 'WORLD_INDUSTRY_COMMODITY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class VN_INDUSTRY_COMMODITY(Base):
		__tablename__ = 'VN_INDUSTRY_COMMODITY'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_FINANCE_GBOND(Base):
		__tablename__ = 'WORLD_FINANCE_GBOND'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class WORLD_FINANCE_CBRATE(Base):
		__tablename__ = 'WORLD_FINANCE_CBRATE'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class _TEMP(Base):
		__tablename__ = '_TEMP'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class STOCKTRADING_CONTRIBUTION(Base):
		__tablename__ = 'STOCKTRADING_CONTRIBUTION'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	class BBAPI(Base):
		__tablename__ = 'BBAPI'
		id = Column(Integer, primary_key=True)
		data_name = Column(String(100), nullable=False)
		time_code = Column(String(100), nullable=False)
		data_dict = Column(JSON, nullable=False)
		other = Column(JSON, nullable=False)

		def __init__(self, data_name, time_code, data_dict, other):
			self.data_name = data_name
			self.time_code = time_code
			self.data_dict = data_dict
			self.other = other

	# --------------------------------------------------------------------------------------------
	def import_time_codes(self, table, data_name=None, decode=False):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			if data_name==None: objs = ss.query(table_obj.time_code).all()
			else: objs = ss.query(table_obj.time_code).filter(table_obj.data_name == data_name).all()
			return np.unique([decode_time_code(obj[0])[1] if decode else obj[0] for obj in objs])
		
	def import_data_names(self, table):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			objs = ss.query(table_obj.data_name).all()
			return np.unique([obj.data_name for obj in objs])

	def convert_to_json(self, data):
		if isinstance(data, dict):
			for key, value in data.items():
				if isinstance(value, datetime): data[key] = datetime_dayname(value, True)
				if isinstance(value, np.ndarray): data[key] = value.tolist()

		return json.loads(json.dumps(data))
	
	def inserting(self, table, data):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			ss.bulk_insert_mappings(table_obj, data)
			ss.commit()

	def deleting(self, table, data):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			conditions = [(d['data_name'], d['time_code']) for d in data]
			ss.query(table_obj).filter(
				tuple_(table_obj.data_name, table_obj.time_code).in_(conditions)
			).delete(synchronize_session=False)
			ss.commit()

	def deleting_id(self, table, id):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			ss.query(table_obj).filter(table_obj.id==id).delete(synchronize_session=False)
			ss.commit()

	def importing_obj(self, table, data_name, time_code):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			objs = ss.query(table_obj).filter(
					table_obj.data_name==data_name,
					table_obj.time_code==time_code,
				).all()
			return objs[0] if len(objs)==1 else None
		
	def importing_objs(self, table, data_name=None, time_code=None):
		with Session(self.engine) as ss:
			table_obj = getattr(self, str(table).upper())
			if data_name==None and time_code==None:
				objs = ss.query(table_obj).all()
			elif data_name==None and time_code!=None:
				objs = ss.query(table_obj).filter(
						table_obj.time_code==time_code,
					).all()
			elif data_name!=None and time_code==None:
				objs = ss.query(table_obj).filter(
						table_obj.data_name==data_name,
					).all()
			elif data_name!=None and time_code!=None:
				objs = ss.query(table_obj).filter(
						table_obj.data_name==data_name,
						table_obj.time_code==time_code,
					).all()
			return np.array(objs, dtype='object')
		
	def updating(self, data_dicts):
		data_tables = np.unique([d_dict['data_table'] for d_dict in data_dicts])
		
		for data_table in data_tables:
			d_dicts = list(filter(lambda d_dict: d_dict['data_table']==data_table, data_dicts))

			update_data = []
			for d_dict in d_dicts:

				data_name, time_code, data_dict, other = d_dict['data_name'], d_dict['time_code'], d_dict['data_dict'], d_dict['other']
				
				data_obj = self.importing_obj(data_table, data_name, time_code)

				if data_obj==None:

					data_dict.update({'data_table': data_table,'data_name': data_name,'time_code': time_code,})

					other.update({'update_at': datetime.now()})
					
					update_data.append({
						'data_name': data_name,
						'time_code': time_code,
						'data_dict': self.convert_to_json(data_dict),
						'other': self.convert_to_json(other),
						})
					
			if len(update_data)>0: self.inserting(data_table, update_data)
		
		print(f'>> Updated___ {len(data_tables)} table(s): {data_tables}')

	def overwriting(self, data_dicts):
		data_tables = np.unique([d_dict['data_table'] for d_dict in data_dicts])

		for data_table in data_tables:

			d_dicts = list(filter(lambda d_dict: d_dict['data_table']==data_table, data_dicts))

			overwrite_data = []
			for d_dict in d_dicts:

				data_name, time_code, data_dict, other = d_dict['data_name'], d_dict['time_code'], d_dict['data_dict'], d_dict['other']

				data_dict.update({'data_table': data_table,'data_name': data_name,'time_code': time_code,})
				
				other.update({'overwrite_at': datetime.now()})

				overwrite_data.append({
					'data_name': data_name,
					'time_code': time_code,
					'data_dict': self.convert_to_json(data_dict),
					'other': self.convert_to_json(other),
					})
				
					
			if len(overwrite_data)>0:
				self.deleting(data_table, overwrite_data)
				self.inserting(data_table, overwrite_data)

		print(f'>> Overwrited {len(data_tables)} table(s): {data_tables}')

	def modifying(self, data_dicts):
		data_tables = np.unique([d_dict['data_table'] for d_dict in data_dicts])
		
		for data_table in data_tables:

			d_dicts = list(filter(lambda d_dict: d_dict['data_table']==data_table, data_dicts))

			modify_data = []
			for d_dict in d_dicts:

				data_name, time_code, data_dict, other = d_dict['data_name'], d_dict['time_code'], d_dict['data_dict'], d_dict['other']
				
				data_obj = self.importing_obj(data_table, data_name, time_code)
				
				if data_obj!=None:
					data_obj.data_dict.update(data_dict)
					data_obj.other.update(other)
					data_dict = copy.deepcopy(data_obj.data_dict)
					other = copy.deepcopy(data_obj.other)

				data_dict.update({'data_table': data_table,'data_name': data_name,'time_code': time_code,})

				other.update({'modify_at': datetime.now()})
					
				modify_data.append({
					'data_name': data_name,
					'time_code': time_code,
					'data_dict': self.convert_to_json(data_dict),
					'other': self.convert_to_json(other),
					})
					
			if len(modify_data)>0:
				self.deleting(data_table, modify_data)
				self.inserting(data_table, modify_data)
		
		print(f'>> Modified__ {len(data_tables)} table(s): {data_tables}')

	def database_info_dict(self, target_tables=[]):
		tables = inspect(self.engine).get_table_names()
		if len(target_tables)==0: target_tables = tables
		target_tables = [_.upper() for _ in target_tables]

		database_info_dict = {}
		with Session(self.engine) as ss:
			for data_table in tables:
				if data_table.upper() not in target_tables: continue

				try:

					print(f'\nChecking DB: {self.database_name} - Table: {data_table}')
					table_obj = getattr(self, str(data_table).upper())
					objs = ss.query(table_obj.data_name, table_obj.time_code).all()

					unique_data_names, count_data_names = np.unique([obj.data_name for obj in objs], return_counts=True)

					data_name_dicts = []
					for data_name in unique_data_names:
						data_name_objs = list(filter(lambda obj: obj.data_name==data_name, objs))
						unique_time_codes, count_time_codes = np.unique([obj.time_code for obj in data_name_objs], return_counts=True)
						decoded_time_codes = [decode_time_code(time_code)[1] for time_code in unique_time_codes]
						data_from, data_to = None, None
						if len(decoded_time_codes)!=0:
							data_from, data_to = min(decoded_time_codes), max(decoded_time_codes)
						time_code_types = np.unique([time_code[0] for time_code in unique_time_codes])
						data_name_dict = {
							'data_table': data_table,
							'data_table_rows': len(objs),
							'data_name': data_name,
							'count_data_name': len(data_name_objs),
							'unique_time_codes': unique_time_codes,
							'count_time_codes': count_time_codes,
							'time_code_types': time_code_types,
							'data_from': data_from,
							'data_to': data_to,
						}
						data_name_dicts.append(data_name_dict)

					table_dict = {
						'data_table_rows': len(objs),
						'unique_data_names': unique_data_names,
						'count_data_names': count_data_names,
						'data_name_dicts': data_name_dicts,
					}
					database_info_dict[data_table] = table_dict
					
					print(f"> Rows: {table_dict['data_table_rows']}\n>> D-Names: {table_dict['unique_data_names']}\n>> N-Count: {table_dict['count_data_names']}")
				
				except Exception as error:
					print(f'> Error DB: {self.database_name} - Table: {data_table} - Error: {error}')
					pass

		return database_info_dict
	
	def database_info_arr(self, is_full=False, target_tables=[]):
		db_info_arr = []
		if is_full:
			headers = np.array([ 'data_table', 'data_name', 'count_data_name', 'unique_time_codes', 'time_code_types', 'data_from', 'data_to'])
			for data_table, table_dict in db.database_info_dict(target_tables=target_tables).items():
				table_arr = []
				for data_name_dict in table_dict['data_name_dicts']:
					line = [
						str(data_name_dict[header]).upper()
						if header not in ['unique_time_codes'] 
						else len(data_name_dict[header])
						for header in headers
					]
					table_arr.append(line)
				db_info_arr += table_arr
		
		else:
			headers = np.array([ 'data_table', 'data_name', ])
			tables = inspect(self.engine).get_table_names()
			if len(target_tables)==0: target_tables = tables
			target_tables = [_.upper() for _ in tables]
			
			with Session(self.engine) as ss:
				for data_table in tables:
					if data_table.upper() not in target_tables: continue

					table_obj = getattr(self, str(data_table).upper())
					objs = ss.query(table_obj.data_name, table_obj.time_code).all()
					unique_data_names, count_data_names = np.unique([obj.data_name for obj in objs], return_counts=True)
					for data_name in unique_data_names:
						line = [ data_table, data_name, ]
						db_info_arr.append(line)
			
		return np.append(headers.reshape(1,-1), db_info_arr, axis=0)
	
	def importing_table_arr(self, table, data_name=None, time_code=None, with_headers=True):
		objs = self.importing_objs(table, data_name, time_code)

		fixed_headers = ['data_table', 'data_name', 'time_code', 'time_code_begin', 'time_code_end']
		
		all_keys = [
			[key for key, value in obj.data_dict.items()
			# if isinstance(value, dict) and 'data' in value
			if key not in fixed_headers
				] for obj in objs]
		unique_keys = np.unique(sum(all_keys, []))

		# GET HEADERS
		headers = np.array([fixed_headers + list(unique_keys)], dtype='object')

		# GET BODY
		body_arr = []
		for obj in objs:
			data_dict = obj.data_dict
			time_code_begin, time_code_end = decode_time_code(data_dict['time_code'])
			line = [data_dict['data_table'], data_dict['data_name'], data_dict['time_code'], time_code_begin, time_code_end,]
			for key in unique_keys:
				if key in data_dict:
					data = data_dict[key]['data'] if isinstance(data_dict[key], dict) else data_dict[key]
				else:
					data = None
				line += [data]
			body_arr.append(line)
		
		body_arr = np.array(body_arr, dtype='object') if len(body_arr)!=0 else np.empty((0, 5), int)
		body_arr = body_arr[body_arr[:,4].argsort()]

		# APPEND HEADER AND BODY
		return np.append(headers, body_arr, axis=0) if with_headers else body_arr
				
db = database(mysql_database_dict['db'], mysql_database_dict['host'])
