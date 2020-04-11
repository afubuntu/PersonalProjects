import psycopg2
import configparser


class ctennisDbClass:
	def __init__(self,_connection_file='cTennisDbConnection.ini',_environ='DEV_DB'):
		self.__dbObject=None
		self.__dbCursor=None
		self.__environ=_environ
		self.__configFile=_connection_file

	def connectWithConfig(self):
		try:
			self.__dbObject=psycopg2.connect(**self.__getDbConfig())
			self.__dbCursor=self.__dbObject.cursor()

			return 1
		except:
			return 0

	def connect(self,dsn=None, connection_factory=None, cursor_factory=None, async=False, **kwargs):
		try:
			self.__dbObject=psycopg2.connect(dsn=dsn,connection_factory=connection_factory,cursor_factory=cursor_factory,async=async,**kwargs)
			self.__dbCursor=self.__dbObject.cursor()

			return 1
		except:
			return 0

	def __getDbConfig(self):
		_parser=configparser.ConfigParser()
		_parser.read(self.__configFile)
		_db={}

		if _parser.has_section(self.__environ):
			_params=_parser.items(self.__environ)

			for _p in _params:
				_db[_p[0]]=_p[1]
		else:
			raise Exception(f'The section {self.__environ} does not exist in the file : {self.__configFile}')
		return _db

	def newDbCursor(self):
		try:
			return self.__dbObject.cursor()
		except:
			return None

	def execute(self,sqlstatement,vars=None):
		self.__dbCursor.execute(sqlstatement,vars)

	def callproc(self,procname, parameters=()):
		self.__dbCursor.callproc(procname,parameters)

	def fetchone(self):
		return self.__dbCursor.fetchone()

	def fetchall(self):
		return self.__dbCursor.fetchall()

	def commit(self):
		self.__dbObject.commit()

	@property
	def autocommit(self):
		return self.__dbObject.autocommit

	@autocommit.setter
	def autocommit(self,value):
		self.__dbObject.autocommit=value

	def close(self):
		if self.__dbCursor is not None:
			self.__dbCursor.close()

		if self.__dbObject is not None:
			self.__dbObject.close()




def getDbConfig(filename='cTennisDbConnection.ini', environ='DEV_DB'):
	_parser=configparser.ConfigParser()
	_parser.read(filename)
	
	_db={}
	if _parser.has_section(environ):
		params=_parser.items(environ)
		print(params)
		for _p in params:
			_db[_p[0]]=_p[1]
	else:
		raise Exception(f'The section {envrion} does not exist in the file : {filename}')
	return _db


if __name__=='__main__':
	_params=getDbConfig()
	db=ctennisDbClass()
	
	if db.connect(**_params):
		print('Connection with connect() method was successful')
		db.autocommit=True
		db.execute('select version()')
		print(db.fetchone())
		db.close()

	print()

	db=ctennisDbClass()
	if db.connectWithConfig():
		print('autocommit : ',db.autocommit)
		print('Connection with connectWithConfig() method was successful')
		#db.callproc('sp_insert_ctcategory',('finalsbis2','Masters Bis 2',1250))
		db.execute('select * from ctennistournaments')

		with open('data_ctennismappingtournaments.txt','w') as f:
			while True:
				_r=db.fetchone()
				if _r is None:
					break
				_r1=_r[5].replace('/',', ')
				print(f'union all select \'{_r[1]} - {_r1}\' as ctname, \'{_r[1]}\' as ctnameofficial')
				f.write(f'union all select \'{_r[1]} - {_r1}\' as ctname, \'{_r[1]}\' as ctnameofficial\n')
				f.write(f'union all select \'{_r1}\' as ctname, \'{_r[1]}\' as ctnameofficial\n')

		db.commit()
		db.close()
