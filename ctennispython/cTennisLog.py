####################################################################################################
#                                                                                                  #
#     This module allows to track the program activities in a more customized way                  #
#                                                                                                  #
####################################################################################################

import logging
import enum   # Just to create the enum logLevel but finally not used
import threading
import functools

class logLevel(enum.Enum):
	DEBUG=enum.auto()
	INFO=enum.auto()
	WARNING=enum.auto()
	ERROR=enum.auto()
	CRITICAL=enum.auto()

logLevelDico={'DEBUG':logging.DEBUG,
			  'INFO':logging.INFO,
			  'WARNING':logging.WARNING,
			  'ERROR':logging.ERROR,
			  'CRITICAL':logging.CRITICAL
			 }

lock=threading.Lock()

def synchronized(lock):
	""" Synchronization decorator """
	def wrapper(f):
		@functools.wraps(f)
		def inner_wrapper(*args,**kw):
			with lock:
				return f(*args,**kw)
		return inner_wrapper
	return wrapper

class logSingletonDecorator:
	__instance=None
	def __init__(self,klass):
		self.klass=klass

	@synchronized(lock)
	def __lock_call__(self,*args,**kwargs):
		if logSingletonDecorator.__instance is None:
			logSingletonDecorator.__instance=self.klass(*args,**kwargs)

	def __call__(self,*args,**kwargs):
		if logSingletonDecorator.__instance is None:
			self.__lock_call__(*args,**kwargs)
		return logSingletonDecorator.__instance

class logTennisException(Exception):
	def __init__(self,msg=''):
		self.__msg=msg

	def __str__(self):
		return 'User defined exception [logTennisException] : {}'.format(self.__msg)

# In order to attain the singleton behavior, the logTennis class must be instantiated using the decorator class logSingletonDecorator :
# logSingleton=logSingletonDecorator(logTennis)
# lg=logSingleton() or lg=logSingletonDecorator(logTennis)()

class logTennis:
	__DEFAULT_H_FORMAT=('[%(asctime)s][%(levelname)s] :: %(message)s',
		                '[%(asctime)s][%(levelname)s] :: %(message)s'
		               )
	def __init__(self,loggername='main',filename='file.log',filefmt='',consolefmt='',fhlevel='DEBUG',chlevel='INFO'):
		self.__filename=filename
		self.__fileFormat=filefmt
		if self.__fileFormat=='':
			self.__fileFormat=logTennis.__DEFAULT_H_FORMAT[0]

		self.__consoleFormat=consolefmt
		if self.__consoleFormat=='':
			self.__consoleFormat=logTennis.__DEFAULT_H_FORMAT[1]

		self.__logger=None
		self.__fileHandler=None
		self.__consoleHandler=None
		self.__fileHandlerLevel=fhlevel
		self.__consoleHandlerLevel=chlevel
		self.__loggername=loggername
		self.__exceptionMsg=''

		if self.__createlogger()==-1:
			raise logTennisException(self.__exceptionMsg)

	def __createlogger(self):
		# Create handlers
		try:
			self.__logger=logging.getLogger(self.__loggername)
			self.__logger.setLevel(logging.DEBUG) # Filter will be made of handlers
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the logger object]'
			return -1

		try:
			self.__consoleHandler=logging.StreamHandler()
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creationg the StreamHandler object]'
			return -1
			
		try:
			self.__fileHandler=logging.FileHandler(self.__filename)
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the FileHandler object]'
			return -1

		self.__consoleHandler.setLevel(logLevelDico[self.__consoleHandlerLevel])
		self.__fileHandler.setLevel(logLevelDico[self.__fileHandlerLevel])

		# Create formatters and add them to the handlers
		try:
			cofmt=logging.Formatter(self.__consoleFormat)
			self.__consoleHandler.setFormatter(cofmt)
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the Formatter object for the console handler]'
			return -1

		try:
			fhfmt=logging.Formatter(self.__fileFormat)
			self.__fileHandler.setFormatter(fhfmt)
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the Formatter object for the file handler]'
			return -1

		# Add handlers to the logger
		self.__logger.addHandler(self.__consoleHandler)
		self.__logger.addHandler(self.__fileHandler)

		return 0

	def __createloggerWithFileHandler(self):
		# Create handlers
		try:
			self.__logger=logging.getLogger(self.__loggername)
			self.__logger.setLevel(logging.DEBUG) # Filter will be made of handlers
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the logger object]'
			return -1

		try:
			self.__fileHandler=logging.FileHandler(self.__filename)
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the FileHandler object]'
			return -1

		self.__fileHandler.setLevel(logLevelDico[self.__fileHandlerLevel])

		# Create formatters and add them to the handlers
		try:
			fhfmt=logging.Formatter(self.__fileFormat)
			self.__fileHandler.setFormatter(fhfmt)
		except:
			self.__exceptionMsg=self.__exceptionMsg+'[Exception while creating the Formatter object for the file handler]'
			return -1

		# Add handlers to the logger
		self.__logger.addHandler(self.__fileHandler)

		return 0

	@property
	def logfilename(self):
		return self.__filename

	@logfilename.setter
	def logfilename(self,_f):
		self.__filename=_f
		if self.__createloggerWithFileHandler()==-1:
			raise logTennisException(self.__exceptionMsg)

	def debug(self,msg):
		self.__logger.debug(msg)

	def info(self,msg):
		self.__logger.info(msg)

	def warning(self,msg):
		self.__logger.warning(msg)

	def error(self,msg):
		self.__logger.error(msg)

	def critical(self,msg):
		self.__logger.critical(msg)


def test():
	for l in logLevel:
		print('logLevel name is {0} and the corresponding value is {1}'.format(l.name,l.value))
	e=logTennisException('The logger was not created properly')
	print(e)
	print('')
	print('Test the logTennis object :')
	try:
		lg=logSingletonDecorator(logTennis)()
	except logTennisException as e:
		print(e)
	lg.debug('[Function test()] : We are now ready to use this object')
	print(lg)
	lg1=logSingletonDecorator(logTennis)()
	print(lg1)
	#print(lg1.__DEFAULT_H_FORMAT)


if __name__=='__main__':
	test()