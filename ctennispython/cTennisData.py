###########################################################################################################################
#                                                                                                                         #
#        This module allows to extract tennis data from different sources and save them into a file or a database         #
#                                                                                                                         #
###########################################################################################################################

import os
import requests
import lxml
import bs4
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import abc
import datetime
import time
import threading
import multiprocessing
import ctypes
import queue
import ast
import re
import os
import shutil
import ctypes
import cTennisLog
import cTennisDb


class cTennisPoolOfThreadsIsRunning(Exception):
	def __init__(self,msg=''):
		self.__msg=msg

	def __str__(self):
		return 'User defined exception [cTennisPoolOfThreadsIsRunning Exception] : {}'.format(self.__msg)


##########################################################################  Class cTennisThread  #################################################################################
class cTennisThread(threading.Thread):
	"""
	This class inherits of the Thread class in order to customize the thread behavior suitable to the customized class of the threads pool
	"""

	_available_thread_lock=threading.Lock()
	_main_thread_lock=threading.Lock()
	_thread_is_available=threading.Condition(_available_thread_lock)
	_main_thread_condition=threading.Condition(_main_thread_lock)
	_available_thread=None

	def __init__(self,group=None,target=None,name=None,args=(),kwargs=None,daemon=None):
		super().__init__(group=group,target=target,name=name,args=args,kwargs=kwargs,daemon=daemon)
		self.__all_tasks_done=False
		self._thread_result=[]
		self._restart_task=threading.Event()
		self._is_done=False

	@property
	def args(self):
		return self._args

	@args.setter
	def args(self,args=()):
		self._args=args

	@property
	def kwargs(self):
		return self._kwargs

	@kwargs.setter
	def kwargs(self,kwargs={}):
		self._kwargs=kwargs

	@property
	def target(self):
		return self._target

	@target.setter
	def target(self,target=None):
		self._target=target

	def restart(self):
		self._restart_task.set()

	def run(self):
		"""
		The thread can be restarted as soon as it has finished to do its work.
		The main thread is waiting to be notified as soon as the current thread is done.
		"""
		try:
			self._is_done=False
			self.__all_tasks_done=False
			self._restart_task.set()

			while (True):
				if self._target is not None:
					self._restart_task.wait()
					self._restart_task.clear()	
									
					if self.__all_tasks_done and self._is_done:
						break

					self._is_done=False					
					rs=self._target(*self._args,**self._kwargs)
					if rs is not None:
						self._thread_result.append(rs)

					self._is_done=True

					with cTennisThread._main_thread_condition:
						while (True):
							if self.__all_tasks_done:
								break
							
							if len(cTennisThread._thread_is_available._waiters)>0:
								with cTennisThread._thread_is_available:
									cTennisThread._available_thread=self
									cTennisThread._thread_is_available.notify_all()
								break

							cTennisThread._main_thread_condition.wait()

							with cTennisThread._thread_is_available:
								cTennisThread._available_thread=self
								cTennisThread._thread_is_available.notify_all()
							break

				else:
					with cTennisThread._thread_is_available:
						cTennisThread._available_thread=self
						self._is_done=True
						cTennisThread._thread_is_available.notify_all()
					break
		except:
			pass


	def join(self,timeout=None):
		"""
		The call of join() won't have any effect unless all of the tasks have been processed by the thread
		Call this join() after the last call of the run() method of the thread
		"""
		self.__all_tasks_done=True
		with cTennisThread._main_thread_condition:
			cTennisThread._main_thread_condition.notify_all()

		self._restart_task.set()
		super().join(timeout)


############################################################################  Class cTennisThreadForQueue  #######################################################################
class cTennisThreadForQueue(threading.Thread):
	"""
	This class will implement a customized thread which handles properly the Queue object.
	It will be used in the cTennisPoolOfThreads class.
	"""

	_sharedQueue=queue.Queue()
	_sharedQueueLock=threading.Lock()
	_main_thread_lock=threading.Lock()
	_queue_thread_is_available=threading.Condition(_sharedQueueLock)
	_main_thread_condition=threading.Condition(_main_thread_lock)
	_available_thread=None

	def __init__(self,group=None,target=None,name=None,args=(),kwargs=None,daemon=None):
		super().__init__(group=group,target=target,name=name,args=args,kwargs=kwargs,daemon=daemon)

		self._isConsumer=False
		self._thread_result=[]
		self._queue_thread_restart=threading.Event()
		self.__all_tasks_procuded=False
		self._is_done=False
		self._timeout=None
		self.__all_tasks_consumed=False

	@property
	def args(self):
		return self._args

	@args.setter
	def args(self,args=()):
		self._args=args

	@property
	def kwargs(self):
		return self._kwargs

	@kwargs.setter
	def kwargs(self,kwargs={}):
		self._kwargs=kwargs

	@property
	def target(self):
		return self._target

	@target.setter
	def target(self,target=None):
		self._target=target

	@property
	def is_consumer(self):
		return self._isConsumer

	@is_consumer.setter
	def is_consumer(self,boolvalue):
		self._isConsumer=boolvalue

	def restart(self):
		self._queue_thread_restart.set()

	def __produceQueue(self):
		if self._target:
			rs=self._target(*self._args,**self._kwargs)
			if type(rs) is type([]) or type(rs) is type(()):
				for q in rs:
					cTennisThreadForQueue._sharedQueue.put(q)
			else:
				if rs is not None:
					cTennisThreadForQueue._sharedQueue.put(rs)

	def __consumeQueue(self,queue_data):
		if self._target:
			if type(queue_data) is type(()):
				self._args=queue_data
			elif type(queue_data) is type({}):
				self._kwargs=queue_data
			else:
				self._args=(queue_data,)

			return self._target(*self._args,**self._kwargs)

	def run(self):
		if self._isConsumer:
			while(True):
				try:
					queue_data=cTennisThreadForQueue._sharedQueue.get(timeout=1)
				except queue.Empty:
					if self.__all_tasks_consumed:
						break
					else:
						continue

				rs=self.__consumeQueue(queue_data)
				if rs is not None:
					self._thread_result.append(rs)

				cTennisThreadForQueue._sharedQueue.task_done()
		else:

			self.__all_tasks_procuded=False	
			self._is_done=False
			self._queue_thread_restart.set()

			while(True):
				self._queue_thread_restart.wait(self._timeout)
				self._queue_thread_restart.clear()

				if self.__all_tasks_procuded and self._is_done:
					break

				self._is_done=False
				self.__produceQueue()
				self._is_done=True

				with cTennisThreadForQueue._main_thread_condition:
					while(True):
						if self.__all_tasks_procuded:
							break

						if len(cTennisThreadForQueue._queue_thread_is_available._waiters)>0:
							with cTennisThreadForQueue._queue_thread_is_available:
								cTennisThreadForQueue._available_thread=self
								cTennisThreadForQueue._queue_thread_is_available.notify_all()
							break

						cTennisThreadForQueue._main_thread_condition.wait(self._timeout)

						with cTennisThreadForQueue._queue_thread_is_available:
							cTennisThreadForQueue._available_thread=self
							cTennisThreadForQueue._queue_thread_is_available.notify_all()
						break

	def interrupt(self):
		_target_id=0
		for _tid,_toj in threading._active.items():
			if _toj is self:
				_target_id=_tid
				break
		else:
			raise ValueError('Invalid thread object')

		_ta = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(_target_id), ctypes.py_object(SystemExit))
		if _ta==0:
			raise ValueError('Invalid thread ID')
		elif _ta>1:
			ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(_target_id), 0)
			raise SystemError('PyThreadState_SetAsyncExc failed')


	def join(self,timeout=None):
		if not self._isConsumer:
			self.__all_tasks_procuded=True
			with cTennisThreadForQueue._main_thread_condition:
				cTennisThreadForQueue._main_thread_condition.notify_all()

			self._queue_thread_restart.set()
			super().join(timeout)
		else:
			self.__all_tasks_consumed=True
			super().join(timeout)


class cTennisThreadAdditional(threading.Thread):
	def __init__(self,group=None,target=None,name=None,args=(),kwargs=None,daemon=None):
		super().__init__(group=group,target=target,name=name,args=args,kwargs=kwargs,daemon=daemon)

	def interrupt(self):
		_target_id=0
		for _tid,_toj in threading._active.items():
			if _toj is self:
				_target_id=_tid
				break
		else:
			raise ValueError('Invalid thread object')

		_ta = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(_target_id), ctypes.py_object(SystemExit))
		if _ta==0:
			raise ValueError('Invalid thread ID')
		elif _ta>1:
			ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(_target_id), 0)
			raise SystemError('PyThreadState_SetAsyncExc failed')

	def run(self):
		try:
			self._target(*self._args,**self._kwargs)
		finally:
			pass

		
#####################################################################  Class cTennisPoolOfThreads  ###############################################################################
class cTennisPoolOfThreads:
	"""
	This class will implement a customized pool of threads that will be used to extract tennis data concurrently
	- The default max size of the pool is an arbitrary choice
	- The pool data must be either a tuple or a list
	- The pool queue is a regular Queue object
	- An index or counter will be used to go through the pool data concurrently
	- A threading.Lock object will be used to lock the index or counter
	- An threading.Condition object will be used to notify the pool whether a thread is done with its job
	"""
	def __init__(self,pool_size=None,thread_names=[],thread_target=None,pool_data=None,is_pool_queue=False,additional_target=None,additional_args=()):
		self._pool_threads_dico={}
		self._pool_result=[]

		try:
			self._pool_max_size=8*os.cpu_count() if os.cpu_count() is not None else 8*8
		except:
			self._pool_max_size=30

		self._internal_pool_size=pool_size if pool_size is not None and pool_size<=self._pool_max_size else self._pool_max_size
		self._thread_names=thread_names
		self._thread_target=thread_target
		self._pool_data=pool_data
		self._is_pool_queue=is_pool_queue
		self._internal_data_index=0
		self._pool_data_size=len(self._pool_data)
		self.poolStats={}
		self.additional_target=additional_target
		self.additional_thread=None
		self.additional_args=additional_args

		self.__create_pool_threads()

	def __create_pool_threads(self):
		"""
		The private method creates the threads with the names provided in threads_names.
		In the normal case, all data must be provided through _pool_data before being consumed by the threads.
		In this case, all of threads should be running the same code.
		When the data are provided through a queue object, there must be two versions of code (consumer and producer) in the thread_target argument
		If the thread is a consumer, it must be prefixed by 'consumer_' otherwise it is considered as a producer thread
		"""
		if len(self._thread_names)==self._internal_pool_size and self._is_pool_queue:
			for tn in self._thread_names:
				self._pool_threads_dico[tn]=cTennisThreadForQueue(name=tn)
				self._pool_threads_dico[tn].setDaemon(True)
				self.poolStats[tn]=0

				if tn[:9].lower()=='consumer_':
					self._pool_threads_dico[tn].is_consumer=True
			
			if (type(self._thread_target) is type([])) and (len(self._thread_target)==self._internal_pool_size):
				for n,t in zip(self._thread_names,self._thread_target):
					self._pool_threads_dico[n].target=t
		else:
			"""
			Otherwise all of the threads of the pool will run the same code
			"""
			for i in range(self._internal_pool_size):
				t=cTennisThread(target=self._thread_target)
				t.setDaemon(True)

				try:
					if self._thread_names[i]!='':
						t.name=self._thread_names[i]
				except:
					t.name=f'cTennisPoolOfThreads_{i+1}'				
				finally:
					self._pool_threads_dico[t.name]=t
					self.poolStats[t.name]=0
		
		if self.additional_target:
			self.additional_thread=cTennisThreadAdditional(name='cTennisPoolOfThreads_additional_thread',target=self.additional_target,daemon=True,args=self.additional_args)


	def _process_pool_data(self,timeout=None):
		"""
		All of the threads consume the data from pool_data and should run the same code
		"""
		self._internal_data_index=0
		self._pool_data_size=len(self._pool_data)

		for th in self._pool_threads_dico:
			if self._internal_data_index<self._pool_data_size:
				d=self._pool_data[self._internal_data_index]
				if type(d) is type(()):
					self._pool_threads_dico[th].args=d
				elif type(d) is type({}):
					self._pool_threads_dico[th].kwargs=d
				else:
					self._pool_threads_dico[th].args=(d,)

				self._internal_data_index+=1
				self._pool_threads_dico[th].start()
				self.poolStats[th]+=1
			else:
				break

		if self.additional_target:
			self.additional_thread.start()

		# Waiting for the available thread here. Then process the next data with the available thread
		if self._pool_data_size>0:
			with cTennisThread._thread_is_available:
				while(True):
					with cTennisThread._main_thread_condition:
						cTennisThread._main_thread_condition.notify()

					cTennisThread._thread_is_available.wait(timeout)

					if self._internal_data_index<self._pool_data_size:
						d=self._pool_data[self._internal_data_index]
						if type(d) is type(()):
							cTennisThread._available_thread.args=d

						elif type(d) is type({}):
							cTennisThread._available_thread.kwargs=d

						else:
							cTennisThread._available_thread.args=(d,)

						self._internal_data_index+=1
						cTennisThread._available_thread.restart()
						self.poolStats[cTennisThread._available_thread.name]+=1			
					else:
						with cTennisThread._main_thread_condition:
							cTennisThread._main_thread_condition.notify_all()
						break

			# Exit comes when there is no longer data to process. We can then join the threads of the pool
			for th in self._pool_threads_dico:
				if self._pool_threads_dico[th].is_alive():
					self._pool_threads_dico[th].join(timeout)

			# Copy the result of each thread in self._pool_result
			for th in self._pool_threads_dico:
				while self._pool_threads_dico[th]._thread_result:
					self._pool_result.append(self._pool_threads_dico[th]._thread_result.pop())


	def _process_pool_queue(self,timeout=None):
		"""
		This method handles properly the pool of threads which produce and consume the data in the queue
		The input data to the producer are provided through the _pool_data variable.
		"""
		self._internal_data_index=0
		self._pool_data_size=len(self._pool_data)		

		for th in self._pool_threads_dico:
			if not self._pool_threads_dico[th].is_consumer:
				if self._internal_data_index<self._pool_data_size:
					d=self._pool_data[self._internal_data_index]
					if type(d) is type(()):
						self._pool_threads_dico[th].args=d
					elif type(d) is type({}):
						self._pool_threads_dico[th].kwargs=d
					else:
						self._pool_threads_dico[th].args=(d,)

					self._internal_data_index+=1
					self._pool_threads_dico[th].start()
					self.poolStats[th]+=1
			else:
				self._pool_threads_dico[th].start()
				self.poolStats[th]+=1

		if self.additional_target:
			self.additional_thread.start()

		# Waiting for the available thread here. Then process the next data with the available thread
		if self._pool_data_size>0:				
			with cTennisThreadForQueue._queue_thread_is_available:
				while(True):
					with cTennisThreadForQueue._main_thread_condition:
						cTennisThreadForQueue._main_thread_condition.notify()

					cTennisThreadForQueue._queue_thread_is_available.wait(timeout)	

					if self._internal_data_index>=self._pool_data_size:
						break

					if not cTennisThreadForQueue._available_thread.is_consumer:
						if self._internal_data_index<self._pool_data_size:
							d=self._pool_data[self._internal_data_index]
							if type(d) is type(()):
								cTennisThreadForQueue._available_thread.args=d
							elif type(d) is type({}):
								cTennisThreadForQueue._available_thread.kwargs=d
							else:
								cTennisThreadForQueue._available_thread.args=(d,)

							self._internal_data_index+=1
							cTennisThreadForQueue._available_thread.restart()
							self.poolStats[cTennisThreadForQueue._available_thread.name]+=1

			# Exit comes when there is no longer data to process by the producer. We can then join the threads of the pool
			for th in self._pool_threads_dico:
				if self._pool_threads_dico[th].is_alive() and not self._pool_threads_dico[th].is_consumer:
					self._pool_threads_dico[th].join(timeout)

			cTennisThreadForQueue._sharedQueue.join()

			for th in self._pool_threads_dico:
				if self._pool_threads_dico[th].is_alive() and self._pool_threads_dico[th].is_consumer:
					self._pool_threads_dico[th].join(timeout)

			# Copy the result of each thread in self._pool_result
			for th in self._pool_threads_dico:
				while self._pool_threads_dico[th]._thread_result:
					self._pool_result.append(self._pool_threads_dico[th]._thread_result.pop())


	def process_pool(self,timeout=None):
		if self._pool_data is not None and self._is_pool_queue:
			self._process_pool_queue(timeout)
		elif self._pool_data is not None and not self._is_pool_queue:
			self._process_pool_data(timeout)
		else:
			raise Exception('[Pool Process Exception] The parameters for running the pool are not correctly provided')

	def close_pool(self):
		ks=[]
		for k in self._pool_threads_dico.keys():
			ks.append(k)

		for k in ks:
			del self._pool_threads_dico[k]


############################################################################  Class cTennisProcessForQueue  ######################################################################
class cTennisProcessForQueue(multiprocessing.Process):
	def __init__(self,group=None,target=None,name=None,args=(),kwargs={},daemon=None):
		super().__init__(group=group,target=target,name=name,args=args,kwargs=kwargs,daemon=daemon)

		self.is_consumer=False
		self.__all_tasks_consumed=multiprocessing.Value('i',0)
		self._process_result=multiprocessing.Queue()
		self.process_runtime=multiprocessing.Value('d',0.0)

	def run(self):
		if self.is_consumer:
			try:
				self.process_runtime.value=time.time()
				_tmp_queue=self._args[1]
				_tmp_args=list(self._args[0])
				while True:
					try:
						queue_data=_tmp_queue.get(timeout=1)
					except queue.Empty:
						if self.__all_tasks_consumed.value:
							self.process_runtime.value=time.time()-self.process_runtime.value
							break
						else:
							continue		
					_tmp_args.append(queue_data)
					self._args=tuple(_tmp_args)

					_rs=self._target(*self._args,**self._kwargs)

					if _rs is not None:
						self._process_result.put(_rs)

					_tmp_args=_tmp_args[:-1]
					_tmp_queue.task_done()
			except:
				self.process_runtime.value=time.time()-self.process_runtime.value
		else:
			try:
				self.process_runtime.value=time.time()
				_tmp_queue=self._args[1]
				self._args=self._args[0]

				_rs=self._target(*self._args,**self._kwargs)

				if _rs is not None:
					for _d in _rs:
						_tmp_queue.put(_d)
				self.process_runtime.value=time.time()-self.process_runtime.value
			except:
				self.process_runtime.value=time.time()-self.process_runtime.value

	def join(self):
		if not self.is_consumer:
			super().join()
		else:
			self.__all_tasks_consumed.value=1
			super().join()


#####################################################################  Class cTennisPoolOfProcesses  #############################################################################
class cTennisPoolOfProcesses:
	"""
		Only one producer process is expected.
		Therefore, the total number of processes is : 1+ the number of consumer processes
	"""
	def __init__(self,pool_size=4,pool_target_prod=None,pool_target_cons=None,pool_data_prod=(),pool_args_cons=()):
		self.poolStats={}
		self.processesDico={}
		self.pool_result=[]
		self.poolQueue=multiprocessing.JoinableQueue()

		try:
			self.__internalPoolSize=min(pool_size,os.cpu_count())
		except:
			self.__internalPoolSize=min(pool_size,8)

		self.poolTargetProd,self.poolTargetCons,self.poolDataProd,self.poolArgsCons=pool_target_prod,pool_target_cons,pool_data_prod,pool_args_cons
		self.__create_pool_processes()

	def __create_pool_processes(self):
		if self.__internalPoolSize>1:
			_name='cTennisProcessForQueue_producer'
			self.processesDico[_name]=cTennisProcessForQueue(name=_name,target=self.poolTargetProd,args=(self.poolDataProd,self.poolQueue),daemon=True)

			for _i in range(self.__internalPoolSize-1):
				_name=f'cTennisProcessForQueue_consumer_{_i+1}'
				self.processesDico[_name]=cTennisProcessForQueue(name=_name,target=self.poolTargetCons,args=(self.poolArgsCons,self.poolQueue),daemon=True)
				self.processesDico[_name].is_consumer=True

	def process_pool(self):
		for _p in self.processesDico:
			self.processesDico[_p].start()

		for _p in self.processesDico:
			if not self.processesDico[_p].is_consumer and self.processesDico[_p].is_alive():
				self.processesDico[_p].join()

		self.poolQueue.join()

		for _p in self.processesDico:
			if self.processesDico[_p].is_consumer and self.processesDico[_p].is_alive():
				self.processesDico[_p].join()

		for _p in self.processesDico:
			self.poolStats[_p]=self.processesDico[_p].process_runtime.value
			if self.processesDico[_p].is_consumer:
				while not self.processesDico[_p]._process_result.empty():
					self.pool_result.append(self.processesDico[_p]._process_result.get())

		return self.poolStats

	def close_pool(self):
		ks=[]
		for k in self.processesDico.keys():
			ks.append(k)

		for k in ks:
			del self.processesDico[k]


######################################################################  Class AbstractTennisDataClass  ###########################################################################
class AbstractTennisDataClass(metaclass=abc.ABCMeta):
	"""
	This abstract class allows to define an interface that several other classes will implement to extract data
	"""
	__dicoHeaders={'player_profile':['player_name', 'player_birth_date', 'player_country_code', 'player_country_label', 'player_height', 
	                                 'player_weight', 'player_begin_pro','player_style_hand', 'player_rank, player_points', 'player_prize_money',
                                     'player_base_link','player_code'
	                                ],
	               'player_match_stats':['match_date', 'tournament_name', 'tournament_surface','tournament_category','tournament_country', 'tournament_round',
	                                     'player1_name', 'player2_name', 'match_score', 'match_result', 'stat_player1_first_serv', 'stat_player2_first_serv', 
	                                     'stat_player1_first_serv_pts', 'stat_player2_first_serv_pts', 'stat_player1_2nd_serv_pts', 'stat_player2_2nd_serv_pts', 
	                                     'stat_player1_brk_pts_won', 'stat_player2_brk_pts_won', 'stat_player1_return_pts_won', 'stat_player2_return_pts_won', 
	                                     'stat_player1_dble_faults','stat_player2_dble_faults', 'stat_player1_aces', 'stat_player2_aces','player1_code','player2_code'
	                                    ]
	              }

	__dtFormat={'fr-FR':'%d-%m-%Y','en-US':'%Y-%m-%d'}

	__tourn_cat_dict={'atpcup':'atpcup','250':'atp250','500':'atp500','1000':'atpmasters1000','grandslam':'grandslam','finals':'finals',
					  'lvr':'lvr','itf':'itf','olympicgames':'olympicgames','international':'international','premier':'premier',
					  'premier5':'premier5','premiermandatory':'premiermandatory'
	                 }

	def __init__(self,site_base_link,output_file='data_out.txt',date_range=[],players_list=[],_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',_log_file='main.log'):
		self.siteBaseLink=site_base_link
		self.outputFile=output_file
		self.dateRange=date_range
		self.countriesList={}
		self.playersList=players_list
		self.playersInfoList=[]
		self.allMatchesToExtract=[]
		self.queueMatchsStats=queue.Queue()
		self.allMatchesInserted=False
		self.ctennisLogger=cTennisLog.logSingletonDecorator(cTennisLog.logTennis)(filename=_log_file)
		self.lock=threading.Lock()
		self.startDate=self.convertStringToDate('2007-01-01')
		self.dbConfigFile=_db_config_file
		self.dbEnviron=_db_environ
	"""
		self.__resetLogFile()

	def __resetLogFile(self):
		_abs_path=os.path.abspath(self.ctennisLogger.logfilename)
		if os.path.exists(_abs_path):
			if os.stat(_abs_path).st_size>0:
				_path_root,_path_file=os.path.split(_abs_path)
				_file_name,_file_ext=os.path.splitext(_path_file)
				_renamed_file=_path_root+'/'+_file_name+'_'+datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')+_file_ext
				shutil.copy(_abs_path,_renamed_file)
				os.remove(_abs_path)
				self.ctennisLogger.logfilename=_abs_path
	"""
	@abc.abstractmethod
	def extractSingleMatchStats(self,linkndata):
		"""
		Extracts the stats of a single match based on the link to the stats of this match
		It is the consumer of the queue

		The parameter: linkndata must be compatible to the conversion to a dictionary object. 
		The key 'link' in this dictionary has the link to the stats of the match as value
		"""
		pass

	@abc.abstractmethod
	def extractSinglePlayerInfo(self,link):
		"""
		Extracts the details of a single player based on the link to the page of this player
		It will be used in a multi-threading approach
		"""
		pass

	@abc.abstractmethod
	def extractMultiplePlayersInfo(self,links_list,timeout=None):
		"""
		Extracts the details of several players based on the list of links to these players
		The default implementation uses the method extractSinglePlayerInfo() in a pool of threads
		"""
		pass

	@abc.abstractmethod
	def getAllPlayersLinkInList(self):
		"""
		Gets the links pointing to the players in the variable .playersList
		The result of this method will be an input to the method extractMultiplePlayersInfo()
		"""
		pass

	@abc.abstractmethod
	def getAllMatchesToExtract(self):
		"""
		The result of this method will be used to feed the producer threads using the method getMatchesToExtractInYearForAPlayer()
		It will be based on .playersInfoList
		"""

	@abc.abstractmethod
	def getMatchesToExtractInYearForAPlayer(self,player_link_year):
		"""
		It produces the queue by taking as arguments the player's link and the year in which we want to extract the matches
		The output of this method is type of the input of the method extractSingleMatchStats()
		"""
		pass

	@abc.abstractmethod
	def getAllTournaments(self):
		pass

	@abc.abstractmethod
	def saveTournamentsData(self,file_name='data_ctennistournaments.txt',dico_path='dico_tournaments.txt'):
		pass

	@abc.abstractmethod
	def extractStatsIntoFile(self):
		pass

	@abc.abstractmethod
	def extractStatsIntoDb(self):
		pass

	@abc.abstractmethod
	def insertStatsFromQueueIntoDb(self):
		pass

	@abc.abstractmethod
	def insertPlayersIntoDb(self):
		pass

	def insertCountriesIntoDb(self):
		pass

	@abc.abstractmethod
	def insertCategoriesIntoDb(self,_ctcode,_ctlabel,_ctpoints):
		pass

	@abc.abstractmethod
	def insertRoundsIntoDb(self,_ctcodeofficial,_ctlabel):
		pass

	@abc.abstractmethod
	def insertTournamentsIntoDb(self):
		pass

	@classmethod
	def getDicoHeaders(cls):
		return cls.__dicoHeaders

	@classmethod
	def getdtFormat(cls):
		return cls.__dtFormat

	@classmethod
	def getDicoTournaments(cls):
		return cls.__tourn_cat_dict

	@classmethod
	def serializeDicoTournaments(cls,file_name='dico_tournaments.txt'):
		try:
			with open(file_name,'w') as f:
				f.write(str(cls.__tourn_cat_dict)+'\n')
			return 1
		except:
			return 0

	@classmethod
	def unserializeDicoTournaments(cls,file_name='dico_tournaments.txt'):
		try:
			with open(file_name,'r') as f:
				_r=f.read()
			return cls.convertStringToDico(_r)
		except:
			return None

	@staticmethod
	def convertStringToDico(st):
		try:
			return ast.literal_eval(st)
		except:
			return None

	@staticmethod
	def convertDateToString(d,fmt='%Y-%m-%d'):
		try:
			return d.strftime(fmt)
		except:
			return None

	@staticmethod
	def convertStringToDate(s,fmt='%Y-%m-%d'):
		try:
			return datetime.datetime.strptime(s,fmt).date()
		except:
			return None

	@staticmethod
	def rscore(s):
		_s=s.split(' ')
		_r=''
		for _i in _s:
			_tb=_i[_i.find('['):] if _i[-1]==']' else ''
			_i.replace(_tb,'')
			_i0=_i.split('-')
			_r=_r+' '+_i0[1]+'-'+_i0[0]+_tb
		return _r.strip()


#################################################################  Class SingleMenOfficialTennisDataClass  #######################################################################
class SingleMenOfficialTennisDataClass(AbstractTennisDataClass):

	def __init__(self,output_file='data_out.txt',date_range=[],players_list=[],_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',_log_file='main.log'):
		super().__init__(site_base_link='https://www.atptour.com',output_file=output_file,date_range=date_range,players_list=players_list,_db_config_file=_db_config_file,_db_environ=_db_environ,_log_file=_log_file)


	def getAllPlayersLinkInList(self):
		_ct_url=self.siteBaseLink+'/en/rankings/singles'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'dropdown-layout-wrapper'})
			_bs_result=_bs_result[0].find_all('div',{'class':'dropdown-wrapper'})

			_rank_date=_bs_result[0].find_all('ul',{'class':'dropdown','data-value':'rankDate'})[0].find_all('li',{'class':'current'})[0]['data-value']
			_rank_range=_bs_result[0].find_all('ul',{'class':'dropdown','data-value':'rankRange'})[0].find_all('li',{'data-value':'1-5000'})[0]['data-value']

			_bs_country_code=_bs_result[0].find_all('ul',{'class':'dropdown','data-value':'countryCode'})[0].find_all('li')
			for c in _bs_country_code:
				self.countriesList[c['data-value']]=c.text.strip()

			if 'all' in self.countriesList:
				del self.countriesList['all']
				_country_code='all'
			else:
				_country_code=_bs_result[0].find_all('ul',{'class':'dropdown','data-value':'countryCode'})[0].find_all('li',{'class':'dropdown-default-label'})[0]['data-value']

			del _bs_country_code

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The parameters :rankDate==>{_rank_date}, rankRange==>{_rank_range}, countryCode==>{_country_code} have been successfully extracted.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] At least one of the parameters :rankDate==>{_rank_date}, rankRange==>{_rank_range}, countryCode==>{_country_code} failed to be extracted.')
			return None

		_ct_url=_ct_url+f'?rankDate={_rank_date}&rankRange={_rank_range}&countryCode={_country_code}'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'table-rankings-wrapper','id':'rankingDetailAjaxContainer'})[0].find_all('table',{'class':'mega-table'})
			_bs_result=_bs_result[0].tbody.find_all('td',{'class':'player-cell'})

			_list_result=[]

			if self.playersList:
				for i in _bs_result:
					if i.a['data-ga-label'].lower() in self.playersList:  # Make sure that all names in the self.playersList are in lower case
						_list_result.append(self.siteBaseLink+i.a['href'])
			else:
				for i in _bs_result:
					_list_result.append(self.siteBaseLink+i.a['href'])

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] All of the players links have been successfully extracted.')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Exception while extracting the players links')
			return None


	def extractSingleMatchStats(self,linkndata):
		_data_dico=self.convertStringToDico(linkndata)
		_ct_url=_data_dico['link']
		del _data_dico['link']

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')

			_bs_result=_bs.find_all('div',{'class':'match-stats-scores'})

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-left-name'})
			_data_dico['player1_name']=_tmp_var[0].find_all('span',{'class':'first-name'})[0].text.strip()+', '+_tmp_var[0].find_all('span',{'class':'last-name'})[0].text.strip()
			_data_dico['player1_code']=_tmp_var[0].find('a')['href'][len('/en/players/'):-len('/overview')].replace('/','-')

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-right-name'})
			_data_dico['player2_name']=_tmp_var[0].find_all('span',{'class':'first-name'})[0].text.strip()+', '+_tmp_var[0].find_all('span',{'class':'last-name'})[0].text.strip()
			_data_dico['player2_code']=_tmp_var[0].find('a')['href'][len('/en/players/'):-len('/overview')].replace('/','-')

			len1,len2=len(self.siteBaseLink)+1,len(_ct_url)-len('match-stats')-1
			_tmp_arr=_ct_url[len1:len2].split('/')

			_id_span_base1,_id_span_base2,_tiebreak=_tmp_arr[-2]+'_'+_tmp_arr[-1]+'_TeamOne_',_tmp_arr[-2]+'_'+_tmp_arr[-1]+'_TeamTwo_','Tiebreak'

			_match_score=''
			_tmp_var=_bs_result[0].find_all('div',{'class':'scoring-section'})

			for i in range(5):
				_s1=_tmp_var[0].find_all('span',{'id':_id_span_base1+f'{i+1}'})[0].text.strip() if len(_tmp_var[0].find_all('span',{'id':_id_span_base1+f'{i+1}'}))>0 else '0'
				_s2=_tmp_var[0].find_all('span',{'id':_id_span_base2+f'{i+1}'})[0].text.strip() if len(_tmp_var[0].find_all('span',{'id':_id_span_base2+f'{i+1}'}))>0 else '0'
				_ti=''

				if int(_s1)-int(_s2)>0 and int(_s1)-int(_s2)<2:
					_ti='['+_tmp_var[0].find('sup',{'id':_id_span_base2+f'{i+1}'+_tiebreak}).text.strip()+']'
					_match_score=_match_score+' '+_s1+'-'+_s2+_ti
				if int(_s2)-int(_s1)>0 and int(_s2)-int(_s1)<2:
					_ti='['+_tmp_var[0].find('sup',{'id':_id_span_base1+f'{i+1}'+_tiebreak}).text.strip()+']'
					_match_score=_match_score+' '+_s1+'-'+_s2+_ti
				if _ti=='':
					_match_score=_match_score+' '+_s1+'-'+_s2

			_data_dico['match_score']=_match_score.replace(' 0-0','').strip()

			_bs_result=_bs.find_all('div',{'id':'completedMatchStats'})
			_tmp_var=_bs_result[0].find_all('table',{'class':'match-stats-table'})[0].find_all('td',{'class':'match-stats-label'})
			for i in _tmp_var:
				if i.text.strip().lower()=='aces':
					_data_dico['stat_player1_aces']=i.parent.find('td',{'class':'match-stats-number-left'}).span.text.strip()
					_data_dico['stat_player2_aces']=i.parent.find('td',{'class':'match-stats-number-right'}).span.text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='double faults':
					_data_dico['stat_player1_dble_faults']=i.parent.find('td',{'class':'match-stats-number-left'}).span.text.strip()
					_data_dico['stat_player2_dble_faults']=i.parent.find('td',{'class':'match-stats-number-right'}).span.text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='1st serve':
					_data_dico['stat_player1_first_serv']=i.parent.find('td',{'class':'match-stats-number-left'}).find('span',{'class':'stat-breakdown'}).text.strip()
					_data_dico['stat_player2_first_serv']=i.parent.find('td',{'class':'match-stats-number-right'}).find('span',{'class':'stat-breakdown'}).text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='1st serve points won':
					_data_dico['stat_player1_first_serv_pts']=i.parent.find('td',{'class':'match-stats-number-left'}).find('span',{'class':'stat-breakdown'}).text.strip()
					_data_dico['stat_player2_first_serv_pts']=i.parent.find('td',{'class':'match-stats-number-right'}).find('span',{'class':'stat-breakdown'}).text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='2nd serve points won':
					_data_dico['stat_player1_2nd_serv_pts']=i.parent.find('td',{'class':'match-stats-number-left'}).find('span',{'class':'stat-breakdown'}).text.strip()
					_data_dico['stat_player2_2nd_serv_pts']=i.parent.find('td',{'class':'match-stats-number-right'}).find('span',{'class':'stat-breakdown'}).text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='break points converted':			
					_data_dico['stat_player1_brk_pts_won']=i.parent.find('td',{'class':'match-stats-number-left'}).find('span',{'class':'stat-breakdown'}).text.strip()
					_data_dico['stat_player2_brk_pts_won']=i.parent.find('td',{'class':'match-stats-number-right'}).find('span',{'class':'stat-breakdown'}).text.strip()
					break

			for i in _tmp_var:
				if i.text.strip().lower()=='return points won':			
					_data_dico['stat_player1_return_pts_won']=i.parent.find('td',{'class':'match-stats-number-left'}).find('span',{'class':'stat-breakdown'}).text.strip()
					_data_dico['stat_player2_return_pts_won']=i.parent.find('td',{'class':'match-stats-number-right'}).find('span',{'class':'stat-breakdown'}).text.strip()
					break

			self.queueMatchsStats.put(str(_data_dico))
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from Official] The stats associated to the link :{_ct_url} have been successfully extracted.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The stats associated to the link :{_ct_url} failed to be extracted with the following exception : {e}')
			return None


	def extractSinglePlayerInfo(self,link):
		_ct_url=link

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player-profile-hero-overflow'})

			_player_info_dico={}

			_player_info_dico['player_name']=_bs_result[0].find_all('div',{'class':'player-profile-hero-name'})[0].find_all('div',{'class':'first-name'})[0].text.strip()
			_player_info_dico['player_name']=_player_info_dico['player_name']+', '+_bs_result[0].find_all('div',{'class':'player-profile-hero-name'})[0].find_all('div',{'class':'last-name'})[0].text.strip()

			if _bs_result[0].find_all('div',{'class':'player-profile-hero-ranking'})[0].find_all('span',{'class':'hero-rank-label'})[0].text.strip().lower()=='singles':
				_player_info_dico['player_rank']=_bs_result[0].find_all('div',{'class':'player-profile-hero-ranking'})[0].find_all('div',{'class':'data-number'})[0].text.strip()

			_player_info_dico['player_country_code']=_bs_result[0].find_all('div',{'class':'player-profile-hero-ranking'})[0].find_all('div',{'class':'player-flag-code'})[0].text.strip()
			_player_info_dico['player_country_label']=self.countriesList.get(_player_info_dico['player_country_code'],_player_info_dico['player_country_code'])

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('span',{'class':'table-birthday'})
			if len(_tmp_var)>0:
				_birthdate=_tmp_var[0].text.strip()
				_player_info_dico['player_birth_date']=_birthdate[1:-1].replace('.','-')

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('span',{'class':'table-weight-lbs'})
			if len(_tmp_var)>0:
				_player_info_dico['player_weight']='('+_tmp_var[0].text.strip()+'lbs)'

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('span',{'class':'table-weight-kg-wrapper'})
			if len(_tmp_var)>0:
				if 'player_weight' in _player_info_dico:
					_player_info_dico['player_weight']=_player_info_dico['player_weight']+_tmp_var[0].text.strip()
				else:
					_player_info_dico['player_weight']=_tmp_var[0].text.strip()

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('span',{'class':'table-height-ft'})
			if len(_tmp_var)>0:
				_player_info_dico['player_height']='('+_tmp_var[0].text.strip()+')'

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('span',{'class':'table-height-cm-wrapper'})
			if len(_tmp_var)>0:
				if 'player_height' in _player_info_dico:
					_player_info_dico['player_height']=_player_info_dico['player_height']+_tmp_var[0].text.strip()
				else:
					_player_info_dico['player_height']=_tmp_var[0].text.strip()

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('div',{'class':'table-big-label'})
			for i in _tmp_var:
				if i.text.strip().lower()=='turned pro':
					_player_info_dico['player_begin_pro']=i.parent.find('div',{'class':'table-big-value'}).text.strip()
					break

			_tmp_var=_bs_result[0].find_all('div',{'class':'player-profile-hero-table'})[0].find_all('div',{'class':'table-label'})
			for i in _tmp_var:
				if i.text.strip().lower()=='plays':
					if i.parent.find('div',{'class':'table-value'}).text.strip()!='':
						_player_info_dico['player_style_hand']=i.parent.find('div',{'class':'table-value'}).text.strip()
					break

			# len('overview')=8
			_player_info_dico['player_base_link']=_ct_url[:len(_ct_url)-8]
			_player_info_dico['player_code']=_player_info_dico['player_base_link'][len(self.siteBaseLink+'/en/players/'):-1].replace('/','-')

			with self.lock:
				self.playersInfoList.append(str(_player_info_dico))
			
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from Official] The following player info :{str(_player_info_dico)} has been successfully extracted.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from Official] Failed to extract the player info for the following link :{_ct_url}, with the exception : {e}')
			return None


	def extractMultiplePlayersInfo(self,links_list,timeout=None):
		"""
		Extracts the details of several players based on the list of links to these players
		The default implementation uses the method extractSinglePlayerInfo() in a pool of threads
		"""
		try:
			ctennis_pool=cTennisPoolOfThreads(pool_size=5,thread_target=self.extractSinglePlayerInfo,pool_data=links_list)
			ctennis_pool.process_pool(timeout)
			ctennis_pool.close_pool()
			return ctennis_pool.poolStats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The extraction of players with the pool of threads failed with the exception : {e}')
			return None


	def getAllMatchesToExtract(self):
		if len(self.dateRange)==2:
			start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
		else:
			start_date,end_date=self.startDate, datetime.date.today()

		for _p in self.playersInfoList:
			_pp=self.convertStringToDico(_p)

			_plink=_pp['player_base_link']
			_pbpro=self.convertStringToDate(_pp['player_begin_pro']+'-01-01') if _pp.get('player_begin_pro','')!='' else self.startDate
			
			if _pbpro is None:
				_pbpro=self.startDate			
			start_date=max(start_date,_pbpro)

			_pdate_year,_pend_year=start_date.year,end_date.year
			while  _pdate_year<=_pend_year:
				_plink_for_producer=_plink+f'player-activity?year={_pdate_year}&matchType=Singles'
				self.allMatchesToExtract.append(_plink_for_producer)
				_pdate_year+=1
					

	def getMatchesToExtractInYearForAPlayer(self,player_link_year):
		_ct_url=player_link_year

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_list_result=[]	

			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'data-filtered-module':'playerActivityTables'})
			_bs_result=_bs_result[0].find_all('div',{'class':'activity-tournament-table'})

			if len(self.dateRange)==2:
				start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
			else:
				start_date,end_date=self.startDate, datetime.date.today()

			for t in _bs_result:
				_ti=t.find_all('a',{'class':''})
				if len(_ti)>0:
					_dates=t.find_all('span',{'class':'tourney-dates'})[0].text.strip().replace(' ','').split('-')
					_mbdate,_medate=self.convertStringToDate(_dates[0],'%Y.%m.%d'),self.convertStringToDate(_dates[1],'%Y.%m.%d')

					if _mbdate>=start_date and _medate<=end_date:
						_data_dico={}

						_data_dico['match_date']=_dates[0]+'-'+_dates[1]
						_data_dico['tournament_name']=t.find_all('a',{'class':'tourney-title'})[0].text.strip()
						_data_dico['tournament_surface']=t.find('div',{'class':'icon-court image-icon'}).parent.parent.find('div',{'class':'item-details'}).text.strip().replace(' ','').replace('\r\n','-')
						_data_dico['tournament_country']=t.find_all('span',{'class':'tourney-location'})[0].text.strip().split(', ')[-1]

						_mpattern=re.compile(r'categorystamps[_](challenger|250|500|1000|grandslam)')
						_mr=_mpattern.findall(t.find_all('td',{'class':'tourney-badge-wrapper'})[0].img['src'])

						_data_dico['tournament_category']=_mr[0] if _mr else None

						for i in _ti:
							_data_dico['tournament_round']=i.parent.parent.td.text.strip().replace(' ','').replace('-','')
							_data_dico['link']=self.siteBaseLink+i['href']
							
							_list_result.append(str(_data_dico))

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from Official] {len(_list_result)} link(s) has(ve) been extracted for the link :{_ct_url}')
			return _list_result
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] An exception occured while extracting the links for the link :{_ct_url} with the following exception : {e}')
			return _list_result if _list_result else None


	def getAllTournaments(self):
		_ct_url=self.siteBaseLink+'/en/tournaments'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find('div',{'id':'mainContent'}).find('div',{'id':'contentAccordionWrapper'}).find_all('tr',{'class':'tourney-result'})

			_list_result=[]

			_cat_pattern=re.compile(r'categorystamps[_]([a-zA-Z0-9]+)[.](png|svg)')

			for _r in _bs_result:
				_t=_r.find_all('td')
				_m=_cat_pattern.findall(_t[0].img['src'] if _t[0].img else '')

				_tourn_cat=_m[0][0] if _m else 'olympicgames'
				_tourn_name=_t[1].a['data-ga-label'].replace(' (Suspended)','')
				_tourn_loc=_t[1].find('span',{'class':'tourney-location'}).text.strip()
				_tourn_surface=_t[2].table.tbody.tr.find_all('td')[1].div.div.text.strip().replace('\r\n','').replace('  ','')

				_list_result.append((_tourn_cat,_tourn_name,_tourn_loc,_tourn_surface))

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] {len(_list_result)} tournament(s) has(ve) been extracted from the link :{_ct_url}')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] An exception occured while extracting the tournaments from the link :{_ct_url}')
			return _list_result


	def saveTournamentsData(self,file_name='data_ctennistournaments.txt',dico_path='dico_tournaments.txt'):
		_list_result=self.getAllTournaments()
		_tourn_dico=self.unserializeDicoTournaments(dico_path)

		if not _tourn_dico:
			_tourn_dico=self.getDicoTournaments()

		_data_dico={}
		try:
			with open(file_name,'a') as f:
				for _t in _list_result:
					_data_dico['tournament_category']=_tourn_dico.get(_t[0],'notdefined')
					_data_dico['tournament_name']=_t[1]
					_data_dico['tournament_country']=_t[2].split(', ')[-1]
					_data_dico['tournament_label']=_t[2].split(', ')[0]
					_data_dico['tournament_surface']=_t[3]

					f.write(str(_data_dico)+'\n')
			return 1
		except:
			return 0


	def extractStatsIntoFile(self):
		pass

	def extractStatsIntoDb(self):
		_consumer_nb=5
		_producer_nb=3
		_thread_names=[]
		_thread_target=[]

		for i in range(_producer_nb):
			_thread_names.append(f'producer_thread_{i+1}')
			_thread_target.append(self.getMatchesToExtractInYearForAPlayer)

		for i in range(_consumer_nb):
			_thread_names.append(f'consumer_thread_{i+1}')
			_thread_target.append(self.extractSingleMatchStats)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')
				db.autocommit=True
			else:
				return None

			self.getAllMatchesToExtract()
			ctennis_pool=cTennisPoolOfThreads(pool_size=len(_thread_names),thread_names=_thread_names,thread_target=_thread_target,
			                                  pool_data=self.allMatchesToExtract,is_pool_queue=True,additional_target=self.insertStatsFromQueueIntoDb,additional_args=(db,))

			ctennis_pool.process_pool()
			pool_stats=ctennis_pool.poolStats
			self.queueMatchsStats.join()
			self.allMatchesInserted=True
			ctennis_pool.additional_thread.join()
			
			db.close()
			ctennis_pool.close_pool()

			return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertStatsFromQueueIntoDb(self,db):
		while True:
			try:
				_tmp_data=self.queueMatchsStats.get(timeout=1)
			except queue.Empty:
				if self.allMatchesInserted:
					break
				else:
					continue

			_one_match_stats=self.convertStringToDico(_tmp_data)

			_tmp_var=_one_match_stats['match_date'].split('-')

			_ctstartdate = self.convertStringToDate(_tmp_var[0],'%Y.%m.%d')
			_ctenddate = self.convertStringToDate(_tmp_var[1],'%Y.%m.%d')
			_ctplayer1 = _one_match_stats['player1_code']
			_ctname1=_one_match_stats['player1_name']
			_ctplayer2 = _one_match_stats['player2_code']
			_ctname2=_one_match_stats['player2_name']
			_cttournament = _one_match_stats['tournament_name']
			_cttourcategory = _one_match_stats['tournament_category']
			_ctourncountry = _one_match_stats['tournament_country']
			_ctsurface = _one_match_stats['tournament_surface']
			_ctround = _one_match_stats['tournament_round']
			_ctscore = _one_match_stats['match_score']
			_ctstat1stserv1 = _one_match_stats['stat_player1_first_serv']
			_ctstat1stserv2 = _one_match_stats['stat_player2_first_serv']
			_ctstat1stservwon1 = _one_match_stats['stat_player1_first_serv_pts']
			_ctstat1stservwon2 = _one_match_stats['stat_player2_first_serv_pts']
			_ctstat2ndservwon1 = _one_match_stats['stat_player1_2nd_serv_pts']
			_ctstat2ndservwon2 = _one_match_stats['stat_player2_2nd_serv_pts']
			_ctstatbrkwon1 = _one_match_stats['stat_player1_brk_pts_won']
			_ctstatbrkwon2 = _one_match_stats['stat_player2_brk_pts_won']
			_ctstatretwon1 = _one_match_stats['stat_player1_return_pts_won']
			_ctstatretwon2 = _one_match_stats['stat_player2_return_pts_won']
			_ctstatdble1 = _one_match_stats['stat_player1_dble_faults']
			_ctstatdble2 = _one_match_stats['stat_player2_dble_faults']
			_ctstataces1 = _one_match_stats['stat_player1_aces']
			_ctstataces2 = _one_match_stats['stat_player2_aces']
			_ctstattype='M'
			_ctstatsource = self.siteBaseLink

			try:
				db.callproc('sp_insert_ctmstats',(_ctstartdate,_ctenddate,_ctplayer1,_ctname1,_ctplayer2,_ctname2,_cttournament,_cttourcategory,_ctourncountry,_ctsurface,_ctround,_ctscore,
			        	                          _ctstat1stserv1,_ctstat1stserv2,_ctstat1stservwon1,_ctstat1stservwon2,_ctstat2ndservwon1,
			        	                          _ctstat2ndservwon2,_ctstatbrkwon1,_ctstatbrkwon2,_ctstatretwon1,_ctstatretwon2,_ctstatdble1,
			        	                          _ctstatdble2,_ctstataces1,_ctstataces2,_ctstattype,_ctstatsource))

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from Official] The following match stats : {str(_one_match_stats)} have been successfully inserted')
				self.queueMatchsStats.task_done()
			except Exception as e:
				self.queueMatchsStats.task_done()
				self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertPlayersIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		self.insertCountriesIntoDb()
		self.insertTournamentsIntoDb()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')

				db.autocommit=True
				for _p in self.playersInfoList:
					_d=self.convertStringToDico(_p)

					_ctcode =_d['player_code']
					_ctname =_d['player_name']
					_ctcountry =_d['player_country_code']
					_ctgender ='M'
					_ctbirthdate =self.convertStringToDate(_d['player_birth_date']) if _d.get('player_birth_date','') !='' else None
					_ctweight =_d.get('player_weight',None)
					_ctheight =_d.get('player_height',None)
					_ctbeginpro =self.convertStringToDate(_d['player_begin_pro']+'-01-01') if _d.get('player_begin_pro','') !='' else None
					_cthandstyle=_d.get('player_style_hand',None)

					db.callproc('sp_insert_ctplayer', (_ctcode,_ctname,_ctcountry,_ctgender,_ctbirthdate,_ctweight,_ctheight,_ctbeginpro,_cthandstyle))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] players have been successfully inserted or updated.')
				return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')	
			return None	


	def insertCountriesIntoDb(self):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')

				db.autocommit=True
				for _c in self.countriesList.items():
					db.callproc('sp_insert_ctcountry',_c)

				db.close()
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Countries have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertCategoriesIntoDb(self,_ctcode,_ctlabel,_ctpoints):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')

				db.autocommit=True
				db.callproc('sp_insert_ctcategory',(_ctcode,_ctlabel,_ctpoints))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The tournament category [{_ctcode} : {_ctlabel} : {_ctpoints}] has been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertRoundsIntoDb(self,_ctcodeofficial,_ctlabel):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')

				db.autocommit=True
				db.callproc('sp_insert_ctround',(_ctcodeofficial,_ctlabel))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] The round [{_ctcodeofficial} ==> {_ctlabel}] has been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertTournamentsIntoDb(self):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Connection to the database is successful.')
				
				_tourn_list=self.getAllTournaments()

				db.autocommit=True
				for _t in _tourn_list:
					db.callproc('sp_insert_cttournament',(_t[1],_t[2].split(',')[-1].strip(),_t[0],_t[3],_t[2].split(',')[0].strip()))

				db.close()
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Tournaments have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from Official] Insertion into the database failed with the following exception : {e}')


#####################################################################  Class SingleMenLiveTennisDataClass  #######################################################################
class SingleMenLiveTennisDataClass(AbstractTennisDataClass):

	def __init__(self,output_file='data_out.txt',date_range=[],players_list=[],_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',_log_file='main.log'):
		super().__init__(site_base_link='http://www.tennislive.net',output_file=output_file,date_range=date_range,players_list=players_list,_db_config_file=_db_config_file,_db_environ=_db_environ,_log_file=_log_file)


	def getAllPlayersLinkInList(self):
		_ct_url=self.siteBaseLink+'/atp/ranking/'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find('select',{'name':'rankC'}).find_all('option')

			for c in _bs_result:
				if c.text.strip()!='World':
					self.countriesList[c['value']]=c.text.strip()

			_bs_result=_bs.find_all('div',{'class':'rank_block'})
			for _rb in _bs_result:
				if _rb.h2.span.text.strip().lower()=='atp ranking':
					break

			_bs_result=_rb.find('table',{'class':'table_pranks'}).find_all('a')

			_list_result=[]

			if self.playersList:
				for a in _bs_result:
					if a['title'].lower() in self.playersList:  # Make sure that all names in the self.playersList are in lower case
						_list_result.append(a['href'])
			else:
				for a in _bs_result:
					_list_result.append(a['href'])

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from tennislive] All of the players links have been successfully extracted.')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from tennislive] Exception while extracting the players links')
			return None

	def getAllPlayersNames(self):
		_ct_url=self.siteBaseLink+'/atp/ranking/'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'rank_block'})
			for _rb in _bs_result:
				if _rb.h2.span.text.strip().lower()=='atp ranking':
					break

			_bs_result=_rb.find('table',{'class':'table_pranks'}).find_all('a')
			_list_result=[]
			for a in _bs_result:
				_list_result.append(a['title'].strip().lower())

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from tennislive] All of the players names have been successfully extracted.')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from tennislive] Exception while extracting the players names')
			return None


	def extractSingleMatchStats(self,linkndata):
		_data_dico=self.convertStringToDico(linkndata)
		_ct_url=_data_dico['link']
		del _data_dico['link']

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_matches'})

			for _pm in _bs_result:
				if _pm.h2.span.text.strip().lower()=='match':
					break

			_bs_result=_pm.find('table',{'class':'table_pmatches'}).find('tr').find_all('td')

			_data_dico['player1_name']=_bs_result[2].find('a')['title']
			_data_dico['player1_code']=_bs_result[2].find('a')['href'][len(self.siteBaseLink+'/atp/'):-1]			
			_data_dico['player2_name']=_bs_result[3].find('a')['title']
			_data_dico['player2_code']=_bs_result[3].find('a')['href'][len(self.siteBaseLink+'/atp/'):-1]
			_data_dico['match_score']=str(_bs_result[4].find('span',{'id':'score'}))[len('<span id="score">'):-len('</span>')].replace('<sup>','[').replace('</sup>',']').replace(',','').strip()

			_bs_result=_pm.find('table',{'class':'table_stats_match'}).find_all('tr')
			del _bs_result[0]

			for _s in _bs_result:
				_srw=_s.find_all('td')

				if _srw[0].text.strip().lower()=='1st serve %':
					_data_dico['stat_player1_first_serv']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_first_serv']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='1st serve points won':
					_data_dico['stat_player1_first_serv_pts']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_first_serv_pts']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='2nd serve points won':
					_data_dico['stat_player1_2nd_serv_pts']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_2nd_serv_pts']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='break points won':
					_data_dico['stat_player1_brk_pts_won']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_brk_pts_won']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='total return points won':
					_data_dico['stat_player1_return_pts_won']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_return_pts_won']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='double faults':
					_data_dico['stat_player1_dble_faults']=_srw[1].text.strip()
					_data_dico['stat_player2_dble_faults']=_srw[2].text.strip()

				elif _srw[0].text.strip().lower()=='aces':
					_data_dico['stat_player1_aces']=_srw[1].text.strip()
					_data_dico['stat_player2_aces']=_srw[2].text.strip()

			self.queueMatchsStats.put(str(_data_dico))
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The stats associated to the link :{_ct_url} have been successfully extracted.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The stats associated to the link :{_ct_url} failed to be extracted with the exception : {e}')
			return None


	def extractSinglePlayerInfo(self,link):
		_ct_url=link

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_stats'})
			
			_player_stats_text=_bs_result[0].text.strip()
			_info_arr=_player_stats_text.split(' '*12)

			_player_info_dico={}

			for i in _info_arr:
				if i.find('Name:')>=0:
					_player_info_dico['player_name']=i[len('Name: '):]

				elif i.find('Country:')>=0:
					_player_info_dico['player_country_label']=i[len('Country: '):]

					for j in self.countriesList.items():
						if j[1]==_player_info_dico['player_country_label']:
							_player_info_dico['player_country_code']=j[0]
							break

				elif i.find('Birthdate:')>=0:
					_current_date=datetime.date.today()
					_pbirthdate=[]

					_tmp_var=i[len('Birthdate: '):i.find(' years')].split(', ')
					_page=int(_tmp_var[1])
					_tmp_var=_tmp_var[0].split('.')

					for ii in _tmp_var:
						_pbirthdate.append(int(ii))				

					_tmp_pbirthdate=datetime.date(_current_date.year,_pbirthdate[1],_pbirthdate[0])

					if _tmp_pbirthdate>_current_date:
						_page+=1

					_player_info_dico['player_birth_date']=f'{_current_date.year-_page}-{_tmp_var[1]}-{_tmp_var[0]}'

				elif i.find('Height:')>=0:
					_player_info_dico['player_height']=i[len('Height: '):]

				elif i.find('Weight:')>=0:
					_player_info_dico['player_weight']=i[len('Weight: '):]

				elif i.find('ATP ranking:')>=0:
					ll1=i.find('ATP ranking:')
					_player_info_dico['player_rank']=i[ll1+len('ATP ranking: '):]
					_player_info_dico['player_style_hand']=i[:ll1-1]

			if not 'player_begin_pro' in _player_info_dico:
				_tmp_var=_bs.find('div',{'class':'player_match_info'}).find('table',{'class':'table_stats'}).find_all('tr')[1:-2]
				_tmp_bp=''

				for _r in _tmp_var:
					_tmp_bp=_r.td.a['href'].replace('?y=','')+';'+_tmp_bp
				_player_info_dico['player_begin_pro']=_tmp_bp[:-1]

			_player_info_dico['player_base_link']=_ct_url
			_player_info_dico['player_code']=_ct_url[len(self.siteBaseLink+'/atp/'):-1]

			with self.lock:
				self.playersInfoList.append(str(_player_info_dico))
			
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The following player info :{str(_player_info_dico)} has been successfully extracted.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Failed to extract the player info for the following link :{_ct_url}')
			return None		


	def extractMultiplePlayersInfo(self,links_list,timeout=None):
		"""
		Extracts the details of several players based on the list of links to these players
		The default implementation uses the method extractSinglePlayerInfo() in a pool of threads
		"""
		try:
			ctennis_pool=cTennisPoolOfThreads(pool_size=5,thread_target=self.extractSinglePlayerInfo,pool_data=links_list)
			ctennis_pool.process_pool(timeout)
			ctennis_pool.close_pool()
			return ctennis_pool.poolStats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The extraction of players with the pool of threads failed with the exception : {e}')
			return None


	def getAllMatchesToExtract(self):
		if len(self.dateRange)==2:
			start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
		else:
			start_date,end_date=self.startDate, datetime.date.today()

		for _p in self.playersInfoList:
			_pp=self.convertStringToDico(_p)
			_plink=_pp['player_base_link']
			_pbpro=_pp['player_begin_pro'].split(';')

			for _d in _pbpro:
				_dy=self.convertStringToDate(_d+'-01-01')
				if _dy>=start_date and _dy<=end_date:
					self.allMatchesToExtract.append(_plink+f'?y={_d}')


	def getMatchesToExtractInYearForAPlayer(self,player_link_year):
		_ct_url=player_link_year

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_list_result=[]

			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_matches'})

			_pm_year=_ct_url.split('?y=')[1]
			
			for _pm in _bs_result:
				if _pm.h2.span.text.split('/')[1].strip().lower()==f'year {_pm_year}':
					break

			_bs_result=_pm.find('table',{'class':'table_pmatches'}).find_all('tr')

			_tour_name,_tour_surface,_tour_head='','','tour_head'

			if len(self.dateRange)==2:
				start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
			else:
				start_date,end_date=self.startDate, datetime.date.today()
	
			_davis_cup_mapping={'Finals':'Rubber 1','QF':'1/4','SF':'1/2','F':'fin'}

			for t in _bs_result:
				if t['class'][0]==_tour_head:
					_tour_name=t.find('td',{'class':'w200'}).find('a')['title']
					_tour_name=_tour_name.split('/')[0].strip()

					_tour_surface=t.find_all('td')[-1].text.strip().replace('. ','-')

				_pms=t.find_all('td')
				_mdate=self.convertStringToDate(_pms[0].text.strip()[:-2]+f'{_pm_year}','%d.%m.%Y') if int(_pms[0].text.strip()[-2:])-int(_pm_year[-2:])==0 else self.convertStringToDate(_pms[0].text.strip()[:-2]+f'{int(_pm_year)-1}','%d.%m.%Y')
				
				if _mdate>=start_date and _mdate<=end_date:
					_data_dico={}

					_data_dico['match_date']=_mdate.strftime('%Y.%m.%d')+'-'+_mdate.strftime('%Y.%m.%d')
					_data_dico['tournament_name']='Davis Cup' if _tour_name.lower().find('davis cup')>=0 else _tour_name
					_data_dico['tournament_surface']=_tour_surface
					_data_dico['tournament_round']=_davis_cup_mapping.get(_tour_name[:_tour_name.rfind(', ')].split(', ')[-1],'Rubber 1') if _data_dico['tournament_name']=='Davis Cup' else (_pms[1].text.strip() if _pms[1].text.strip()!='' else 'Rubber 1')
					_data_dico['link']=_pms[6].a['href']

					_list_result.append(str(_data_dico))

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] {len(_list_result)} link(s) has(ve) been extracted for the link :{_ct_url}')
			return _list_result
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] An exception occured while extracting the links for the link :{_ct_url} with the exception : {e}')
			return _list_result if _list_result else None


	def getAllTournaments(self):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""
		pass

	def saveTournamentsData(self,file_name='data_ctennistournaments.txt',dico_path='dico_tournaments.txt'):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""		
		pass

	def extractStatsIntoFile(self):
		pass

	def extractStatsIntoDb(self):
		_consumer_nb=5
		_producer_nb=3
		_thread_names=[]
		_thread_target=[]

		for i in range(_producer_nb):
			_thread_names.append(f'producer_thread_{i+1}')
			_thread_target.append(self.getMatchesToExtractInYearForAPlayer)

		for i in range(_consumer_nb):
			_thread_names.append(f'consumer_thread_{i+1}')
			_thread_target.append(self.extractSingleMatchStats)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Connection to the database is successful.')
				db.autocommit=True
			else:
				return None

			self.getAllMatchesToExtract()
			ctennis_pool=cTennisPoolOfThreads(pool_size=len(_thread_names),thread_names=_thread_names,thread_target=_thread_target,
			                                  pool_data=self.allMatchesToExtract,is_pool_queue=True,additional_target=self.insertStatsFromQueueIntoDb,additional_args=(db,))

			ctennis_pool.process_pool()
			pool_stats=ctennis_pool.poolStats
			self.queueMatchsStats.join()
			self.allMatchesInserted=True
			ctennis_pool.additional_thread.join()
			
			db.close()
			ctennis_pool.close_pool()

			return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertStatsFromQueueIntoDb(self,db):
		while True:
			try:
				_tmp_data=self.queueMatchsStats.get(timeout=1)
			except queue.Empty:
				if self.allMatchesInserted:
					break
				else:
					continue

			_one_match_stats=self.convertStringToDico(_tmp_data)

			_tmp_var=_one_match_stats['match_date'].split('-')

			_ctstartdate = self.convertStringToDate(_tmp_var[0],'%Y.%m.%d')
			_ctenddate = self.convertStringToDate(_tmp_var[1],'%Y.%m.%d')
			_ctplayer1 = _one_match_stats['player1_code']
			_ctname1=_one_match_stats['player1_name']
			_ctplayer2 = _one_match_stats['player2_code']
			_ctname2=_one_match_stats['player2_name']
			_cttournament = _one_match_stats['tournament_name']
			_cttourcategory = _one_match_stats.get('tournament_category',None)
			_ctourncountry = _one_match_stats.get('tournament_country',None)
			_ctsurface = _one_match_stats['tournament_surface']
			_ctround = _one_match_stats['tournament_round']
			_ctscore = _one_match_stats['match_score']
			_ctstat1stserv1 = _one_match_stats.get('stat_player1_first_serv',None)
			_ctstat1stserv2 = _one_match_stats.get('stat_player2_first_serv',None)
			_ctstat1stservwon1 = _one_match_stats.get('stat_player1_first_serv_pts',None)
			_ctstat1stservwon2 = _one_match_stats.get('stat_player2_first_serv_pts',None)
			_ctstat2ndservwon1 = _one_match_stats.get('stat_player1_2nd_serv_pts',None)
			_ctstat2ndservwon2 = _one_match_stats.get('stat_player2_2nd_serv_pts',None)
			_ctstatbrkwon1 = _one_match_stats.get('stat_player1_brk_pts_won',None)
			_ctstatbrkwon2 = _one_match_stats.get('stat_player2_brk_pts_won',None)
			_ctstatretwon1 = _one_match_stats.get('stat_player1_return_pts_won',None)
			_ctstatretwon2 = _one_match_stats.get('stat_player2_return_pts_won',None)
			_ctstatdble1 = _one_match_stats.get('stat_player1_dble_faults',None) if _one_match_stats.get('stat_player1_dble_faults',None)!='' else None
			_ctstatdble2 = _one_match_stats.get('stat_player2_dble_faults',None) if _one_match_stats.get('stat_player2_dble_faults',None)!='' else None
			_ctstataces1 = _one_match_stats.get('stat_player1_aces',None) if _one_match_stats.get('stat_player1_aces',None)!='' else None
			_ctstataces2 = _one_match_stats.get('stat_player2_aces',None) if _one_match_stats.get('stat_player2_aces',None)!='' else None
			_ctstattype='M'
			_ctstatsource = self.siteBaseLink

			try:
				db.callproc('sp_insert_ctmstats',(_ctstartdate,_ctenddate,_ctplayer1,_ctname1,_ctplayer2,_ctname2,_cttournament,_cttourcategory,_ctourncountry,_ctsurface,_ctround,_ctscore,
			        	                          _ctstat1stserv1,_ctstat1stserv2,_ctstat1stservwon1,_ctstat1stservwon2,_ctstat2ndservwon1,
			        	                          _ctstat2ndservwon2,_ctstatbrkwon1,_ctstatbrkwon2,_ctstatretwon1,_ctstatretwon2,_ctstatdble1,
			        	                          _ctstatdble2,_ctstataces1,_ctstataces2,_ctstattype,_ctstatsource))

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Men Data from tennislive] The following match stats : {str(_one_match_stats)} have been successfully inserted')
				self.queueMatchsStats.task_done()
			except Exception as e:
				self.queueMatchsStats.task_done()
				self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertPlayersIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		self.insertCountriesIntoDb()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Connection to the database is successful.')

				db.autocommit=True
				for _p in self.playersInfoList:
					_d=self.convertStringToDico(_p)

					_ctcode =_d['player_code']
					_ctname =_d['player_name']
					_ctcountry =_d['player_country_code']
					_ctgender ='M'
					_ctbirthdate =self.convertStringToDate(_d['player_birth_date']) if _d.get('player_birth_date','') !='' else None
					_ctweight =_d.get('player_weight',None)
					_ctheight =_d.get('player_height',None)
					_ctbeginpro =self.convertStringToDate(_d['player_begin_pro'].split(';')[0]+'-01-01')
					_cthandstyle=_d.get('player_style_hand',None)

					db.callproc('sp_insert_ctplayer', (_ctcode,_ctname,_ctcountry,_ctgender,_ctbirthdate,_ctweight,_ctheight,_ctbeginpro,_cthandstyle))
				db.close()
			
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] players have been successfully inserted or updated.')
				return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Insertion into the database failed with the following exception : {e}')	
			return None	

	def insertPlayersMappingIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		_ctcode_list=[]
		for _i in self.playersInfoList:
			_d=self.convertStringToDico(_i)
			_ctcode_list.append((_d['player_code'],(_d['player_name'].lower())))

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Connection to the database is successful.')
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] {len(_ctcode_list)} players to be mapped.')

				db.execute("select ctcode,lower(replace(ctname,',','')) from ctennisplayers where ctgender='M' order by ctdid")
				newCursor=db.newDbCursor()

				_found=False
				_ctcode=db.fetchone()
				_cc=0
				_ilen=0
				while _ctcode:
					_tmp_ctcode,_tmp_ctname=_ctcode[0],_ctcode[1]

					for j,_i in enumerate(_ctcode_list):
						if _tmp_ctcode.find(_i[0])>=0:
							_found=True
							break
						else:
							_ii=_i[0].split('-')
							_tmp_ctcode_j=_tmp_ctcode
							for _i0 in _ii:
								_ilen=_tmp_ctcode_j.find(_i0)
								if _ilen<0:
									break
								else:
									_tmp_ctcode_j=_tmp_ctcode_j[:_ilen]+_tmp_ctcode_j[_ilen+len(_i0):]
							else:
								_found=True

							if _found:
								break

							_ii=_ctcode[0][:_ctcode[0].rfind('-')].split('-')
							_i1=_i[0]
							for _i0 in _ii:
								_ilen=_i1.find(_i0)
								if _ilen<0:
									break
								else:
									_i1=_i1[:_ilen]+_i1[_ilen+len(_i0):]
							else:
								if len(_ii)>1:
									_found=True

							if _found:
								break

							if _tmp_ctname==_i[1]:
								_found=True
								break								
								
					if _found:
						newCursor.callproc('sp_insert_ctplayermapping',(_i[0],_ctcode[0]))
						self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] The mapping : [{_i[0]} <=> {_ctcode[0]}] has been successfully inserted.')
						_cc+=1
						_ctcode_list.pop(j)

					_found=False
					_ctcode=db.fetchone()

				db.commit()
				newCursor.close()
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] {_cc} mappings has(ve) been successfully inserted.')
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] {_ctcode_list} has(ve) not been mapped.')
		except Exception as e:
			newCursor.close()
			db.close()
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertCountriesIntoDb(self):		
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Connection to the database is successful.')

				db.autocommit=True
				for _c in self.countriesList.items():
					db.callproc('sp_insert_ctcountry',_c)

				db.close()
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Countries have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Men Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertCategoriesIntoDb(self,_ctcode,_ctlabel,_ctpoints):
		"""
		The tournament categories are inserted only from the reference sources : atptour.com and wtatennis.com
		"""		
		pass

	def insertRoundsIntoDb(self,_ctcodeofficial,_ctlabel):
		"""
		The rounds are inserted only from the reference sources : atptour.com and wtatennis.com
		"""
		pass

	def insertTournamentsIntoDb(self):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""			
		pass


################################################################  Class SingleWomenOfficialTennisDataClass  ######################################################################
class SingleWomenOfficialTennisDataClass(AbstractTennisDataClass):

	def __init__(self,output_file='data_out.txt',date_range=[],players_list=[],_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',_log_file='main.log'):
		super().__init__(site_base_link='https://www.wtatennis.com',output_file=output_file,date_range=date_range,players_list=players_list,_db_config_file=_db_config_file,_db_environ=_db_environ,_log_file=_log_file)
		self.startDate=self.convertStringToDate('2016-01-01')
		self.allTournamentsList=[]

	def getAllPlayersLinkInList(self):
		try:
			chrome_options=webdriver.ChromeOptions()
			chrome_options.headless=True

			chrome_driver=webdriver.Chrome(chrome_options=chrome_options)
			chrome_driver.get(self.siteBaseLink)

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{self.siteBaseLink} has been successfully received.')
		except:
			chrome_driver.quit()
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{self.siteBaseLink} failed to be received.')
			return None

		try:
			chrome_driver.find_element_by_xpath('//button[@data-text="Accept Cookies"]').click()
			chrome_driver.find_element_by_link_text('Rankings').click()
			time.sleep(2)

			btn_country=chrome_driver.find_element_by_xpath('//div[@class="rankings-filter__content js-filter-trigger js-historical-disable"]')
			chrome_driver.execute_script('arguments[0].click();',btn_country)

			chrome_driver.find_element_by_xpath('//button[@data-text="Show More"]').click()

			_target=chrome_driver.find_element_by_xpath('//main[@id="main-content"]/section/div[@class="wrapper rankings__wrapper"]/div[@class="loader js-loader"]')

			while True:
				chrome_driver.execute_script('arguments[0].scrollIntoView(false);', _target)
				time.sleep(0.1)
				chrome_driver.execute_script('window.scroll(0, window.scrollY+10);')

				try:
					p1=chrome_driver.find_element_by_xpath('//p[@class="rankings__error js-rankings-error"]')
				except NoSuchElementException:
					p1=None

				if p1 is not None:
					break

			time.sleep(1)
			_bs=bs4.BeautifulSoup(chrome_driver.page_source,'lxml')
			chrome_driver.quit()

			_bs_result=_bs.find('div',{'class':'country-filter__dropdown js-country-filter'}).ul.find_all('li',{'role':'option'})
			for o in _bs_result:
				self.countriesList[o['data-code']]=o.find('span').text.strip()

			_bs_result=_bs.find('main',{'id':'main-content'}).find('section',{'data-type':'rankSingles'}).find('div',{'class':'wrapper rankings__wrapper'}).table.find_all('tr',{'class':'rankings__row'})

			_list_result=[]

			if self.playersList:
				for _rw in _bs_result:
					_a=_rw.find('td',{'class':'rankings__cell--player'}).find('a')

					if _a['title'].lower() in self.playersList:
						_list_result.append('https:'+_a['href'])
			else:
				for _rw in _bs_result:
					_list_result.append('https:'+_rw.find('td',{'class':'rankings__cell--player'}).find('a')['href'])			

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] All of the players links have been successfully extracted.')
			return _list_result
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Exception while extracting the players links with the exception : {e}')
			return None


	def extractSingleMatchStats(self,linkndata):
		_data_dico=self.convertStringToDico(linkndata)
		_ct_url=_data_dico['link']
		del _data_dico['link']

		_count_failure=0
		while True:
			try:
				_raw_page=requests.get(_ct_url)
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} has been successfully received.')
			except:
				_count_failure+=1
				if _count_failure>=500:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} failed to be received.')
					return None
				time.sleep(0.1)

			try:
				_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
				_bs_result=_bs.find('table',{'class':'match-table match-table--dark'}).find_all('tr',{'class':'match-table__row'})

				_tmp_data_dico={}

				if len(_bs_result)==2:
					_w=_bs_result[0].find('div',{'class':'match-table__team match-table__team--winner'})
					_l=_bs_result[1].find('div',{'class':'match-table__team'})
					if _w is not None and _l is not None:
						_tmp_data_dico['match_winner']=1
						_tmp_data_dico['match_loser']=2
					else:
						_w=_bs_result[1].find('div',{'class':'match-table__team match-table__team--winner'})
						_l=_bs_result[0].find('div',{'class':'match-table__team'})
						if _w is not None and _l is not None:
							_tmp_data_dico['match_winner']=2
							_tmp_data_dico['match_loser']=1

					_tmp_data_dico['player1_name']=_bs_result[0].th.div.div.a['title']
					_tmp_data_dico['player1_code']=_bs_result[0].th.div.div.a['href'][len('/players/'):].replace('/','-')
					_tmp_data_dico['player2_name']=_bs_result[1].th.div.div.a['title']
					_tmp_data_dico['player2_code']=_bs_result[1].th.div.div.a['href'][len('/players/'):].replace('/','-')

					if self.playersList:
						if not _tmp_data_dico['player1_name'].lower() in self.playersList and not _tmp_data_dico['player2_name'].lower() in self.playersList:
							return None
				else:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The stats-score associated to the link :{_ct_url} failed to be extracted.')
					return None

				_w,_l,_s=_w.parent.parent.find_all('td'),_l.parent.parent.find_all('td'),''
				if len(_w)==len(_l):
					_len_tie,str_tie=len('<sup class="match-table__tie-break">'),'<sup class="match-table__tie-break">'
					for _i,_j in zip(_w,_l):
						_iw=str(_i)
						_jl=str(_j)
						_wtie,_ltie='',''

						_iw=_iw[:-5].strip()
						_jl=_jl[:-5].strip()

						if _iw[-1]=='>':
							_wpos=_iw.find(str_tie)
							_wtie='['+_iw[_wpos+_len_tie:-6]+']'
							_iw=_iw[:_wpos].strip()
							_iw=_iw[_iw.find('>')+1:].strip()
						else:
							_iw=_i.text.strip()

						if _jl[-1]=='>':
							_lpos=_jl.find(str_tie)
							_ltie='['+_jl[_lpos+_len_tie:-6]+']'
							_jl=_jl[:_lpos].strip()
							_jl=_jl[_jl.find('>')+1:].strip()
						else:
							_jl=_j.text.strip()

						_s=_s+' '+_iw+'-'+_jl+_wtie+_ltie

					_tmp_data_dico['match_score']=_s.replace('--','').replace(' -','').strip()

					del _w
					del _l
					del _s
				else:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The stats-score associated to the link :{_ct_url} failed to be extracted.')
					return None

				_bs_result=_bs.find('div',{'data-tab-key':'match'}).find_all('p',{'class':'mc-stats__stat-label'})
				for _p in _bs_result:
					if _p.text.strip().lower()=='aces':
						_tmp_data_dico['stat_player1_aces']=_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).text.strip()
						_tmp_data_dico['stat_player2_aces']=_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).text.strip()

					elif _p.text.strip().lower()=='double faults':
						_tmp_data_dico['stat_player1_dble_faults']=_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).text.strip()
						_tmp_data_dico['stat_player2_dble_faults']=_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).text.strip()

					elif _p.text.strip().lower()=='1st serve':
						_tmp_data_dico['stat_player1_first_serv']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'
						_tmp_data_dico['stat_player2_first_serv']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'

					elif _p.text.strip().lower()=='1st serve points won':
						_tmp_data_dico['stat_player1_first_serv_pts']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'
						_tmp_data_dico['stat_player2_first_serv_pts']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'

					elif _p.text.strip().lower()=='2nd serve points won':
						_tmp_data_dico['stat_player1_2nd_serv_pts']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'
						_tmp_data_dico['stat_player2_2nd_serv_pts']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'

					elif _p.text.strip().lower()=='break points converted':
						_tmp_data_dico['stat_player1_brk_pts_won']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'
						_tmp_data_dico['stat_player2_brk_pts_won']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'

					elif _p.text.strip().lower()=='total return points won':
						_tmp_data_dico['stat_player1_return_pts_won']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-a'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'
						_tmp_data_dico['stat_player2_return_pts_won']='('+_p.parent.find('div',{'class':'mc-stats__stat-player-container mc-stats__stat-player-container--player-b'}).find('div',{'class':'mc-stats__stat-player-number'}).span.text.strip()+')'

				del _bs_result

				if _tmp_data_dico['match_winner']==1:
					for _d in _tmp_data_dico:
						_data_dico[_d]=_tmp_data_dico[_d]

					del _data_dico['match_winner']
					del _data_dico['match_loser']

				elif _tmp_data_dico['match_winner']==2:
					_data_dico['player1_name']=_tmp_data_dico['player2_name']
					_data_dico['player1_code']=_tmp_data_dico['player2_code']
					_data_dico['player2_name']=_tmp_data_dico['player1_name']
					_data_dico['player2_code']=_tmp_data_dico['player1_code']
					_data_dico['match_score']=_tmp_data_dico['match_score']
					_data_dico['stat_player1_aces']=_tmp_data_dico['stat_player2_aces']
					_data_dico['stat_player2_aces']=_tmp_data_dico['stat_player1_aces']
					_data_dico['stat_player1_dble_faults']=_tmp_data_dico['stat_player2_dble_faults']
					_data_dico['stat_player2_dble_faults']=_tmp_data_dico['stat_player1_dble_faults']
					_data_dico['stat_player1_first_serv']=_tmp_data_dico['stat_player2_first_serv']
					_data_dico['stat_player2_first_serv']=_tmp_data_dico['stat_player1_first_serv']
					_data_dico['stat_player1_first_serv_pts']=_tmp_data_dico['stat_player2_first_serv_pts']
					_data_dico['stat_player2_first_serv_pts']=_tmp_data_dico['stat_player1_first_serv_pts']
					_data_dico['stat_player1_2nd_serv_pts']=_tmp_data_dico['stat_player2_2nd_serv_pts']
					_data_dico['stat_player2_2nd_serv_pts']=_tmp_data_dico['stat_player1_2nd_serv_pts']
					_data_dico['stat_player1_brk_pts_won']=_tmp_data_dico['stat_player2_brk_pts_won']
					_data_dico['stat_player2_brk_pts_won']=_tmp_data_dico['stat_player1_brk_pts_won']
					_data_dico['stat_player1_return_pts_won']=_tmp_data_dico['stat_player2_return_pts_won']
					_data_dico['stat_player2_return_pts_won']=_tmp_data_dico['stat_player1_return_pts_won']

				self.queueMatchsStats.put(str(_data_dico))
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The stats associated to the link :{_ct_url} have been successfully extracted.')
				break
			except Exception as e:
				_count_failure+=1
				if _count_failure>=500:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The stats associated to the link :{_ct_url} failed to be extracted with the exception : {e}')
					return None
				time.sleep(0.1)

	def extractSinglePlayerInfo(self,link):
		_ct_url=link

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find('div',{'class':'profile-header-info__inner'})

			_player_info_dico={}

			try:
				_player_info_dico['player_name']=_bs_result.find('span',{'class':'profile-header-info__firstname'}).text.strip()+', '+_bs_result.find('span',{'class':'profile-header-info__surname'}).text.strip()
			except:
				_player_info_dico['player_name']=_ct_url[len(self.siteBaseLink+'/players/'):].replace('/','-')

			try:
				_div=_bs_result.find('div',{'class':'profile-header-info__nationality'})

				_player_info_dico['player_country_code']=_div.img['alt']
				_player_info_dico['player_country_label']=_div.div.text.strip()
			except:
				_player_info_dico['player_country_code']=None
				_player_info_dico['player_country_label']=None

			try:
				_player_info_dico['player_birth_date']=_bs_result.find('div',{'class':'profile-header-info__detail-stat js-profile-header-info__age'})['data-dob']
			except:
				_player_info_dico['player_birth_date']=None
			
			try:
				_div=_bs_result.find('div',{'class':'profile-header-info__detail profile-header-info__height'})
				_player_info_dico['player_height']='('+_div.find('div',{'class':'profile-header-info__detail-stat'}).span.text.strip()+')('+_div.find('div',{'class':'profile-header-info__detail-stat--small'}).text.strip()+')'
			except:
				_player_info_dico['player_height']=None

			try:
				_div=_bs.find('div',{'class':'profile-header-image-col__rank'})
				_player_info_dico['player_rank']=_div.find('span',{'class':'profile-header-image-col__rank-pos js-profile-header-update-label'})['data-single']
			except:
				_player_info_dico['player_rank']=None

			try:
				_div=_bs_result.find('div',{'class':'profile-header-info__detail profile-header-info__handed'})
				_player_info_dico['player_style_hand']=_div.find('div',{'class':'profile-header-info__detail-stat--small'}).text.strip()
			except:
				_player_info_dico['player_style_hand']=None

			_player_info_dico['player_base_link']=_ct_url
			_player_info_dico['player_code']=_ct_url[len(self.siteBaseLink+'/players/'):].replace('/','-')
			_player_info_dico['player_begin_pro']=''

			with self.lock:
				self.playersInfoList.append(str(_player_info_dico))
			
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The following player info :{str(_player_info_dico)} has been successfully extracted.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Failed to extract the player info for the following link :{_ct_url} with the exception : {e}')
			return None	


	def extractMultiplePlayersInfo(self,links_list,timeout=None):
		"""
		Extracts the details of several players based on the list of links to these players
		The default implementation uses the method extractSinglePlayerInfo() in a pool of threads
		"""
		try:
			ctennis_pool=cTennisPoolOfThreads(pool_size=5,thread_target=self.extractSinglePlayerInfo,pool_data=links_list)
			ctennis_pool.process_pool(timeout)
			ctennis_pool.close_pool()
			return ctennis_pool.poolStats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The extraction of players with the pool of threads failed with the exception : {e}')
			return None


	def getAllMatchesToExtract(self):
		_ct_url='https://www.wtatennis.com/tournaments/'

		if len(self.dateRange)==2:
			start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
		else:
			start_date,end_date=self.startDate, self.convertStringToDate(datetime.datetime.today().strftime('%Y-%m-%d'))

		_cyear,_eyear=start_date.year,end_date.year
		_processes=[]

		__manager=multiprocessing.Manager()
		__allTournamentsList=__manager.Queue()
		__lock=multiprocessing.Lock()
		__internalFlag=multiprocessing.Value('i',0)
		__allMatchesToExtract=__manager.Queue()

		while _cyear<=_eyear:
			_processes.append(multiprocessing.Process(target=self.__getAllMatchesToExtract,args=(f'{_ct_url}{_cyear}',__internalFlag,__allTournamentsList,__allMatchesToExtract,__lock),daemon=True))
			_processes[-1].start()
			_cyear+=1

		for p in _processes:
			p.join()

		while not __allTournamentsList.empty():
			self.allTournamentsList.append(__allTournamentsList.get())		

		while not __allMatchesToExtract.empty():
			self.allMatchesToExtract.append(__allMatchesToExtract.get())


	def __getAllMatchesToExtract(self,_ct_url,__internalFlag,__allTournamentsList,__allMatchesToExtract,__lock):
		try:
			chrome_options=webdriver.ChromeOptions()
			chrome_options.headless=False
			chrome_driver=webdriver.Chrome(chrome_options=chrome_options)
			chrome_driver.get(_ct_url)

			_tourn_months=chrome_driver.find_element_by_xpath('//div[@class="tournament-list__filters-months js-horizontal-scroll"]').find_elements_by_tag_name('a')
			for _i in _tourn_months:
				chrome_driver.execute_script('arguments[0].click();',_i)
				time.sleep(1)

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} has been successfully received.')
		except :
			chrome_driver.quit()
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} failed to be received.')
			return None

		if len(self.dateRange)==2:
			start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
		else:
			start_date,end_date=self.startDate, self.convertStringToDate(datetime.datetime.today().strftime('%Y-%m-%d'))

		try:
			_bs=bs4.BeautifulSoup(chrome_driver.page_source,'lxml')
			chrome_driver.quit()

			_bs_result=_bs.find_all('li',{'class':'tournament-list__item'})
			_pdata_for_producer={} # tournament_name, tournament_surface, tournament_link

			with __lock:
				if not __internalFlag.value:
					for _t in _bs_result:
						_tc=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent.parent.find_all('span')[1]['class'][1][len('tournament-tag--'):]
						_tn=_t.find('h3',{'class':'tournament-thumbnail__title'})['data-text']
						_tl=_t.find('span',{'class':'tournament-thumbnail__location'}).text.strip()
						_ts=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent['class'][1][len('tournament-tag--'):]

						__allTournamentsList.put((_tc,_tn,_tl,_ts))
					__internalFlag.value=1

			for _t in _bs_result:
				_tourn_dates=_t.find_all('time')
				if self.convertStringToDate(_tourn_dates[0]['date-time'])>=start_date and self.convertStringToDate(_tourn_dates[1]['date-time'])<=end_date:
					_pdata_for_producer['tournament_name']=_t.find('h3',{'class':'tournament-thumbnail__title'})['data-text']
					_pdata_for_producer['tournament_category']=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent.parent.find_all('span')[1]['class'][1][len('tournament-tag--'):]
					_pdata_for_producer['tournament_country']=_t.find('span',{'class':'tournament-thumbnail__location'}).text.strip().split(', ')[1]
					_pdata_for_producer['tournament_surface']=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent['class'][1][len('tournament-tag--'):]
					_pdata_for_producer['tournament_link']='https:'+_t.find('a',{'class':'tournament-thumbnail__link'})['href']

					__allMatchesToExtract.put(str(_pdata_for_producer))
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] An exception occured while extracting the tournament data for the link :{_ct_url} with the following exception : {e}')			
			return None


	def getMatchesToExtractInYearForAPlayer(self,player_link_year):
		_pdata_for_producer=self.convertStringToDico(player_link_year)
		_ct_url=_pdata_for_producer['tournament_link']

		_count_failure=0
		while True:
			try:
				_raw_page=requests.get(_ct_url)
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} has been successfully received.')
			except:
				_count_failure+=1
				if _count_failure>=500:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The page :{_ct_url} failed to be received.')
					return None
				time.sleep(0.1)

			try:
				_list_result=[]

				_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
				_bs_result_active=_bs.find('div',{'data-ui-tab':'Singles'}).find_all('div',{'class':'tournament-scores__day js-scores-day is-active'})
				_bs_result_inactive=_bs.find('div',{'data-ui-tab':'Singles'}).find_all('div',{'class':'tournament-scores__day js-scores-day'})

				if len(self.dateRange)==2:
					start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
				else:
					start_date,end_date=self.startDate, self.convertStringToDate(datetime.datetime.today().strftime('%Y-%m-%d'))

				_data_dico={}
				_data_dico['tournament_name']=_pdata_for_producer['tournament_name']
				_data_dico['tournament_surface']=_pdata_for_producer['tournament_surface']
				_data_dico['tournament_category']=_pdata_for_producer['tournament_category']
				_data_dico['tournament_country']=_pdata_for_producer['tournament_country']

				_mpattern=re.compile(r'<a class="tennis-match__match-link" href=(.*) title=["]([a-zA-Z.’-]+ vs[.] [a-zA-Z.’-]+) [|] ([a-zA-Z0-9 .’;@&-]*) [|] (Match Center)')

				_round_dico={'Round of 128':'Roundof128',
				             'Round of 64':'Roundof64',
				             'Round of 32':'Roundof32',
				             'Round of 16':'Roundof16',
				             'Quarterfinal':'Quarterfinal',
				             'Semifinal':'Semifinal',
				             'Final':'Final',
				             'Group Stage':'GroupStage'
				            }
				_tmp_str=''
				
				for _dm in _bs_result_active:
					_mdate=self.convertStringToDate(_dm['data-date'])
					if _mdate>=start_date and _mdate<=end_date:
						_data_dico['match_date']=_dm['data-date'].replace('-','.')+'-'+_dm['data-date'].replace('-','.')
						_s=str(_dm)
						_matches=_mpattern.findall(_s)

						for _m in _matches:
							_tmp_str=_m[2].replace(_data_dico['tournament_name'],'')
							for _d in _round_dico:
								if _tmp_str.find(_d)>=0:
									_data_dico['tournament_round']=_round_dico[_d]
									break
							else:
								_data_dico['tournament_round']=_round_dico['Group Stage']

							_data_dico['link']='https://www.wtatennis.com'+_m[0][1:-1]
							_list_result.append(str(_data_dico))

				for _dm in _bs_result_inactive:
					_mdate=self.convertStringToDate( _dm['data-date'])
					if _mdate>=start_date and _mdate<=end_date:
						_data_dico['match_date']=_dm['data-date'].replace('-','.')+'-'+_dm['data-date'].replace('-','.')
						_s=str(_dm)
						_matches=_mpattern.findall(_s)

						for _m in _matches:
							_tmp_str=_m[2].replace(_data_dico['tournament_name'],'')
							for _d in _round_dico:
								if _tmp_str.find(_d)>=0:
									_data_dico['tournament_round']=_round_dico[_d]
									break
							else:
								_data_dico['tournament_round']=_round_dico['Group Stage']
								
							_data_dico['link']='https://www.wtatennis.com'+_m[0][1:-1]
							_list_result.append(str(_data_dico))

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] {len(_list_result)} link(s) has(ve) been extracted for the link :{_ct_url}')
				return _list_result
			except Exception as e:
				_count_failure+=1
				if _count_failure>=500:
					self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] An exception occured while extracting the links for the link :{_ct_url} with the exception : {e}')
					return _list_result if _list_result else None
				time.sleep(0.1)


	def getAllTournaments(self):
		return self.allTournamentsList

	def saveTournamentsData(self,file_name='data_ctennistournaments.txt',dico_path='dico_tournaments.txt'):
		_list_result=self.getAllTournaments()
		_tourn_dico=self.unserializeDicoTournaments(dico_path)

		if not _tourn_dico:
			_tourn_dico=self.getDicoTournaments()

		_data_dico={}
		try:
			with open(file_name,'a') as f:
				for _t in _list_result:
					_data_dico['tournament_category']=_tourn_dico.get(_t[0],'notdefined')
					_data_dico['tournament_name']=_t[1]
					_data_dico['tournament_country']=_t[2].split(', ')[-1]
					_data_dico['tournament_label']=_t[2]
					_data_dico['tournament_surface']=_t[3]

					f.write(str(_data_dico)+'\n')
			return 1
		except:
			return 0

	def extractStatsIntoFile(self):
		pass

	def extractStatsIntoDb(self):
		_consumer_nb=5
		_producer_nb=3
		_thread_names=[]
		_thread_target=[]

		for i in range(_producer_nb):
			_thread_names.append(f'producer_thread_{i+1}')
			_thread_target.append(self.getMatchesToExtractInYearForAPlayer)

		for i in range(_consumer_nb):
			_thread_names.append(f'consumer_thread_{i+1}')
			_thread_target.append(self.extractSingleMatchStats)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')
				db.autocommit=True
			else:
				return None

			self.getAllMatchesToExtract()
			print('OUT')
			self.insertTournamentsIntoDb()
			ctennis_pool=cTennisPoolOfThreads(pool_size=len(_thread_names),thread_names=_thread_names,thread_target=_thread_target,
			                                  pool_data=self.allMatchesToExtract,is_pool_queue=True,additional_target=self.insertStatsFromQueueIntoDb,additional_args=(db,))

			ctennis_pool.process_pool()
			pool_stats=ctennis_pool.poolStats
			self.queueMatchsStats.join()
			self.allMatchesInserted=True
			ctennis_pool.additional_thread.join()
			
			db.close()
			ctennis_pool.close_pool()

			return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertStatsFromQueueIntoDb(self,db):
		while True:
			try:
				_tmp_data=self.queueMatchsStats.get(timeout=1)
			except queue.Empty:
				if self.allMatchesInserted:
					break
				else:
					continue

			_one_match_stats=self.convertStringToDico(_tmp_data)

			_tmp_var=_one_match_stats['match_date'].split('-')

			_ctstartdate = self.convertStringToDate(_tmp_var[0],'%Y.%m.%d')
			_ctenddate = self.convertStringToDate(_tmp_var[1],'%Y.%m.%d')
			_ctplayer1 = _one_match_stats['player1_code']
			_ctname1=_one_match_stats['player1_name']
			_ctplayer2 = _one_match_stats['player2_code']
			_ctname2=_one_match_stats['player2_name']
			_cttournament = _one_match_stats['tournament_name']
			_cttourcategory = _one_match_stats.get('tournament_category',None)
			_ctourncountry = _one_match_stats.get('tournament_country',None)
			_ctsurface = _one_match_stats['tournament_surface']
			_ctround = _one_match_stats['tournament_round']
			_ctscore = _one_match_stats['match_score']
			_ctstat1stserv1 = _one_match_stats.get('stat_player1_first_serv',None)
			_ctstat1stserv2 = _one_match_stats.get('stat_player2_first_serv',None)
			_ctstat1stservwon1 = _one_match_stats.get('stat_player1_first_serv_pts',None)
			_ctstat1stservwon2 = _one_match_stats.get('stat_player2_first_serv_pts',None)
			_ctstat2ndservwon1 = _one_match_stats.get('stat_player1_2nd_serv_pts',None)
			_ctstat2ndservwon2 = _one_match_stats.get('stat_player2_2nd_serv_pts',None)
			_ctstatbrkwon1 = _one_match_stats.get('stat_player1_brk_pts_won',None)
			_ctstatbrkwon2 = _one_match_stats.get('stat_player2_brk_pts_won',None)
			_ctstatretwon1 = _one_match_stats.get('stat_player1_return_pts_won',None)
			_ctstatretwon2 = _one_match_stats.get('stat_player2_return_pts_won',None)
			_ctstatdble1 = _one_match_stats.get('stat_player1_dble_faults',None) if _one_match_stats.get('stat_player1_dble_faults',None)!='' else None
			_ctstatdble2 = _one_match_stats.get('stat_player2_dble_faults',None) if _one_match_stats.get('stat_player2_dble_faults',None)!='' else None
			_ctstataces1 = _one_match_stats.get('stat_player1_aces',None) if _one_match_stats.get('stat_player1_aces',None)!='' else None
			_ctstataces2 = _one_match_stats.get('stat_player2_aces',None) if _one_match_stats.get('stat_player2_aces',None)!='' else None
			_ctstattype='W'
			_ctstatsource = self.siteBaseLink

			try:
				db.callproc('sp_insert_ctmstats',(_ctstartdate,_ctenddate,_ctplayer1,_ctname1,_ctplayer2,_ctname2,_cttournament,_cttourcategory,_ctourncountry,_ctsurface,_ctround,_ctscore,
			        	                          _ctstat1stserv1,_ctstat1stserv2,_ctstat1stservwon1,_ctstat1stservwon2,_ctstat2ndservwon1,
			        	                          _ctstat2ndservwon2,_ctstatbrkwon1,_ctstatbrkwon2,_ctstatretwon1,_ctstatretwon2,_ctstatdble1,
			        	                          _ctstatdble2,_ctstataces1,_ctstataces2,_ctstattype,_ctstatsource))

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Women Data from Official] The following match stats : {str(_one_match_stats)} have been successfully inserted')
				self.queueMatchsStats.task_done()
			except Exception as e:
				self.queueMatchsStats.task_done()
				self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertPlayersIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		self.insertCountriesIntoDb()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')

				db.autocommit=True
				for _p in self.playersInfoList:
					_d=self.convertStringToDico(_p)

					_ctcode =_d['player_code']
					_ctname =_d['player_name']
					_ctcountry =_d['player_country_code']
					_ctgender ='W'
					_ctbirthdate =self.convertStringToDate(_d['player_birth_date']) if _d.get('player_birth_date','') !='' else None
					_ctweight =_d.get('player_weight',None)
					_ctheight =_d.get('player_height',None)
					_ctbeginpro =self.convertStringToDate(_d['player_begin_pro']+'-01-01') if _d.get('player_begin_pro','') !='' else None
					_cthandstyle=_d.get('player_style_hand',None)

					db.callproc('sp_insert_ctplayer', (_ctcode,_ctname,_ctcountry,_ctgender,_ctbirthdate,_ctweight,_ctheight,_ctbeginpro,_cthandstyle))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] players have been successfully inserted or updated.')
				return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertCountriesIntoDb(self):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')

				db.autocommit=True
				for _c in self.countriesList.items():
					db.callproc('sp_insert_ctcountry',_c)

				db.close()
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Countries have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertCategoriesIntoDb(self,_ctcode,_ctlabel,_ctpoints):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')

				db.autocommit=True
				db.callproc('sp_insert_ctcategory',(_ctcode,_ctlabel,_ctpoints))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The tournament category [{_ctcode} : {_ctlabel} : {_ctpoints}] has been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertRoundsIntoDb(self,_ctcodeofficial,_ctlabel):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')

				db.autocommit=True
				db.callproc('sp_insert_ctround',(_ctcodeofficial,_ctlabel))
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] The round [{_ctcodeofficial} ==> {_ctlabel}] has been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')


	def insertTournamentsIntoDb(self):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Connection to the database is successful.')
				
				_tourn_list=self.getAllTournaments()

				db.autocommit=True
				for _t in _tourn_list:
					db.callproc('sp_insert_cttournament',(_t[1],_t[2].split(',')[1].strip(),_t[0],_t[3],_t[2].split(',')[0].strip()))

				db.close()
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Tournaments have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from Official] Insertion into the database failed with the following exception : {e}')



###################################################################  Class SingleWomenLiveTennisDataClass  #######################################################################
class SingleWomenLiveTennisDataClass(AbstractTennisDataClass):

	def __init__(self,output_file='data_out.txt',date_range=[],players_list=[],_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',_log_file='main.log'):
		super().__init__(site_base_link='http://www.tennislive.net',output_file=output_file,date_range=date_range,players_list=players_list,_db_config_file=_db_config_file,_db_environ=_db_environ,_log_file=_log_file)


	def getAllPlayersLinkInList(self):
		_ct_url=self.siteBaseLink+'/wta/ranking/'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find('select',{'name':'rankC'}).find_all('option')

			for c in _bs_result:
				if c.text.strip()!='World':
					self.countriesList[c['value']]=c.text.strip()

			_bs_result=_bs.find_all('div',{'class':'rank_block'})
			for _rb in _bs_result:
				if _rb.h2.span.text.strip().lower()=='wta ranking':
					break

			_bs_result=_rb.find('table',{'class':'table_pranks'}).find_all('a')

			_list_result=[]

			if self.playersList:
				for a in _bs_result:
					if a['title'].lower() in self.playersList:  # Make sure that all names in the self.playersList are in lower case
						_list_result.append(a['href'])
			else:
				for a in _bs_result:
					_list_result.append(a['href'])

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] All of the players links have been successfully extracted.')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Exception while extracting the players links')
			return None


	def getAllPlayersNames(self):
		_ct_url=self.siteBaseLink+'/wta/ranking/'

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'rank_block'})
			for _rb in _bs_result:
				if _rb.h2.span.text.strip().lower()=='wta ranking':
					break

			_bs_result=_rb.find('table',{'class':'table_pranks'}).find_all('a')
			_list_result=[]
			for a in _bs_result:
				_list_result.append(a['title'].strip().lower())

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Women Data from tennislive] All of the players names have been successfully extracted.')
			return _list_result
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Women Data from tennislive] Exception while extracting the players names')
			return None


	def extractSingleMatchStats(self,linkndata):
		_data_dico=self.convertStringToDico(linkndata)
		_ct_url=_data_dico['link']
		del _data_dico['link']

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_matches'})

			for _pm in _bs_result:
				if _pm.h2.span.text.strip().lower()=='match':
					break

			_bs_result=_pm.find('table',{'class':'table_pmatches'}).find('tr').find_all('td')

			_data_dico['player1_name']=_bs_result[2].find('a')['title']
			_data_dico['player1_code']=_bs_result[2].find('a')['href'][len(self.siteBaseLink+'/wta/'):-1]			
			_data_dico['player2_name']=_bs_result[3].find('a')['title']
			_data_dico['player2_code']=_bs_result[3].find('a')['href'][len(self.siteBaseLink+'/wta/'):-1]
			_data_dico['match_score']=str(_bs_result[4].find('span',{'id':'score'}))[len('<span id="score">'):-len('</span>')].replace('<sup>','[').replace('</sup>',']').replace(',','').strip()

			_bs_result=_pm.find('table',{'class':'table_stats_match'}).find_all('tr')
			del _bs_result[0]

			for _s in _bs_result:
				_srw=_s.find_all('td')

				if _srw[0].text.strip().lower()=='1st serve %':
					_data_dico['stat_player1_first_serv']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_first_serv']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='1st serve points won':
					_data_dico['stat_player1_first_serv_pts']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_first_serv_pts']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='2nd serve points won':
					_data_dico['stat_player1_2nd_serv_pts']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_2nd_serv_pts']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='break points won':
					_data_dico['stat_player1_brk_pts_won']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_brk_pts_won']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='total return points won':
					_data_dico['stat_player1_return_pts_won']='('+_srw[1].text[:_srw[1].text.find('(')].strip()+')'
					_data_dico['stat_player2_return_pts_won']='('+_srw[2].text[:_srw[2].text.find('(')].strip()+')'

				elif _srw[0].text.strip().lower()=='double faults':
					_data_dico['stat_player1_dble_faults']=_srw[1].text.strip()
					_data_dico['stat_player2_dble_faults']=_srw[2].text.strip()

				elif _srw[0].text.strip().lower()=='aces':
					_data_dico['stat_player1_aces']=_srw[1].text.strip()
					_data_dico['stat_player2_aces']=_srw[2].text.strip()

			self.queueMatchsStats.put(str(_data_dico))
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The stats associated to the link :{_ct_url} have been successfully extracted.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The stats associated to the link :{_ct_url} failed to be extracted.')
			return None


	def extractSinglePlayerInfo(self,link):
		_ct_url=link

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_stats'})
			
			_player_stats_text=_bs_result[0].text.strip()
			_info_arr=_player_stats_text.split(' '*12)

			_player_info_dico={}

			for i in _info_arr:
				if i.find('Name:')>=0:
					_player_info_dico['player_name']=i[len('Name: '):]

				elif i.find('Country:')>=0:
					_player_info_dico['player_country_label']=i[len('Country: '):]

					for j in self.countriesList.items():
						if j[1]==_player_info_dico['player_country_label']:
							_player_info_dico['player_country_code']=j[0]
							break

				elif i.find('Birthdate:')>=0:
					_current_date=datetime.date.today()
					_pbirthdate=[]

					_tmp_var=i[len('Birthdate: '):i.find(' years')].split(', ')
					_page=int(_tmp_var[1])
					_tmp_var=_tmp_var[0].split('.')

					for ii in _tmp_var:
						_pbirthdate.append(int(ii))				

					_tmp_pbirthdate=datetime.date(_current_date.year,_pbirthdate[1],_pbirthdate[0])

					if _tmp_pbirthdate>_current_date:
						_page+=1

					_player_info_dico['player_birth_date']=f'{_current_date.year-_page}-{_tmp_var[1]}-{_tmp_var[0]}'

				elif i.find('Height:')>=0:
					_player_info_dico['player_height']=i[len('Height: '):]

				elif i.find('Weight:')>=0:
					_player_info_dico['player_weight']=i[len('Weight: '):]

				elif i.find('ATP ranking:')>=0:
					ll1=i.find('ATP ranking:')
					_player_info_dico['player_rank']=i[ll1+len('WTA ranking: '):]
					_player_info_dico['player_style_hand']=i[:ll1-1]

			if not 'player_begin_pro' in _player_info_dico:
				_tmp_var=_bs.find('div',{'class':'player_match_info'}).find('table',{'class':'table_stats'}).find_all('tr')[1:-2]
				_tmp_bp=''

				for _r in _tmp_var:
					_tmp_bp=_r.td.a['href'].replace('?y=','')+';'+_tmp_bp
				_player_info_dico['player_begin_pro']=_tmp_bp[:-1]

			_player_info_dico['player_base_link']=_ct_url
			_player_info_dico['player_code']=_ct_url[len(self.siteBaseLink+'/wta/'):-1]

			with self.lock:
				self.playersInfoList.append(str(_player_info_dico))
			
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The following player info :{str(_player_info_dico)} has been successfully extracted.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Failed to extract the player info for the following link :{_ct_url}')
			return None		


	def extractMultiplePlayersInfo(self,links_list,timeout=None):
		"""
		Extracts the details of several players based on the list of links to these players
		The default implementation uses the method extractSinglePlayerInfo() in a pool of threads
		"""
		try:
			ctennis_pool=cTennisPoolOfThreads(pool_size=5,thread_target=self.extractSinglePlayerInfo,pool_data=links_list)
			ctennis_pool.process_pool(timeout)
			ctennis_pool.close_pool()
			return ctennis_pool.poolStats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The extraction of players with the pool of threads failed with the exception : {e}')
			return None


	def getAllMatchesToExtract(self):
		if len(self.dateRange)==2:
			start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
		else:
			start_date,end_date=self.startDate, datetime.date.today()

		for _p in self.playersInfoList:
			_pp=self.convertStringToDico(_p)
			_plink=_pp['player_base_link']
			_pbpro=_pp['player_begin_pro'].split(';')

			for _d in _pbpro:
				_dy=self.convertStringToDate(_d+'-01-01')
				if _dy>=start_date and _dy<=end_date:
					self.allMatchesToExtract.append(_plink+f'?y={_d}')


	def getMatchesToExtractInYearForAPlayer(self,player_link_year):
		_ct_url=player_link_year

		try:
			_raw_page=requests.get(_ct_url)
			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} has been successfully received.')
		except:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The page :{_ct_url} failed to be received.')
			return None

		try:
			_list_result=[]

			_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
			_bs_result=_bs.find_all('div',{'class':'player_matches'})

			_pm_year=_ct_url.split('?y=')[1]
			
			for _pm in _bs_result:
				if _pm.h2.span.text.split('/')[1].strip().lower()==f'year {_pm_year}':
					break

			_bs_result=_pm.find('table',{'class':'table_pmatches'}).find_all('tr')

			_tour_name,_tour_surface,_tour_head='','','tour_head'

			if len(self.dateRange)==2:
				start_date,end_date=self.convertStringToDate(self.dateRange[0]),self.convertStringToDate(self.dateRange[1])
			else:
				start_date,end_date=self.startDate, datetime.date.today()
	
			_fed_cup_mapping={'Finals':'Rubber 1','QF':'1/4','SF':'1/2','F':'fin'}

			for t in _bs_result:
				if t['class'][0]==_tour_head:
					_tour_name=t.find('td',{'class':'w200'}).find('a')['title']
					_tour_name=_tour_name.split('/')[0].strip()

					_tour_surface=t.find_all('td')[-1].text.strip().replace('. ','-')

				_pms=t.find_all('td')
				_mdate=self.convertStringToDate(_pms[0].text.strip()[:-2]+f'{_pm_year}','%d.%m.%Y') if int(_pms[0].text.strip()[-2:])-int(_pm_year[-2:])==0 else self.convertStringToDate(_pms[0].text.strip()[:-2]+f'{int(_pm_year)-1}','%d.%m.%Y')

				if _mdate>=start_date and _mdate<=end_date:
					_data_dico={}

					_data_dico['match_date']=_mdate.strftime('%Y.%m.%d')+'-'+_mdate.strftime('%Y.%m.%d')
					_data_dico['tournament_name']='Fed Cup' if _tour_name.lower().find('fed cup')>=0 else _tour_name
					_data_dico['tournament_surface']=_tour_surface
					_data_dico['tournament_round']=_fed_cup_mapping.get(_tour_name[:_tour_name.rfind(', ')].split(', ')[-1],'Rubber 1') if _data_dico['tournament_name']=='Fed Cup' else (_pms[1].text.strip() if _pms[1].text.strip()!='' else 'Rubber 1')
					_data_dico['link']=_pms[6].a['href']

					_list_result.append(str(_data_dico))

			self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] {len(_list_result)} link(s) has(ve) been extracted for the link :{_ct_url}')
			return _list_result
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] An exception occured while extracting the links for the link :{_ct_url} with the exception : {e}')
			return _list_result if _list_result else None


	def getAllTournaments(self):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""			
		pass

	def saveTournamentsData(self,file_name='data_ctennistournaments.txt',dico_path='dico_tournaments.txt'):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""	
		pass

	def extractStatsIntoFile(self):
		pass

	def extractStatsIntoDb(self):
		_consumer_nb=5
		_producer_nb=3
		_thread_names=[]
		_thread_target=[]

		for i in range(_producer_nb):
			_thread_names.append(f'producer_thread_{i+1}')
			_thread_target.append(self.getMatchesToExtractInYearForAPlayer)

		for i in range(_consumer_nb):
			_thread_names.append(f'consumer_thread_{i+1}')
			_thread_target.append(self.extractSingleMatchStats)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Connection to the database is successful.')
				db.autocommit=True
			else:
				return None

			self.getAllMatchesToExtract()
			ctennis_pool=cTennisPoolOfThreads(pool_size=len(_thread_names),thread_names=_thread_names,thread_target=_thread_target,
			                                  pool_data=self.allMatchesToExtract,is_pool_queue=True,additional_target=self.insertStatsFromQueueIntoDb,additional_args=(db,))

			ctennis_pool.process_pool()
			pool_stats=ctennis_pool.poolStats
			self.queueMatchsStats.join()
			self.allMatchesInserted=True
			ctennis_pool.additional_thread.join()
			
			db.close()
			ctennis_pool.close_pool()

			return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertStatsFromQueueIntoDb(self,db):
		while True:
			try:
				_tmp_data=self.queueMatchsStats.get(timeout=1)
			except queue.Empty:
				if self.allMatchesInserted:
					break
				else:
					continue

			_one_match_stats=self.convertStringToDico(_tmp_data)

			_tmp_var=_one_match_stats['match_date'].split('-')

			_ctstartdate = self.convertStringToDate(_tmp_var[0],'%Y.%m.%d')
			_ctenddate = self.convertStringToDate(_tmp_var[1],'%Y.%m.%d')
			_ctplayer1 = _one_match_stats['player1_code']
			_ctname1=_one_match_stats['player1_name']
			_ctplayer2 = _one_match_stats['player2_code']
			_ctname2=_one_match_stats['player2_name']
			_cttournament = _one_match_stats['tournament_name']
			_cttourcategory = _one_match_stats.get('tournament_category',None)
			_ctourncountry = _one_match_stats.get('tournament_country',None)
			_ctsurface = _one_match_stats['tournament_surface']
			_ctround = _one_match_stats['tournament_round']
			_ctscore = _one_match_stats['match_score']
			_ctstat1stserv1 = _one_match_stats.get('stat_player1_first_serv',None)
			_ctstat1stserv2 = _one_match_stats.get('stat_player2_first_serv',None)
			_ctstat1stservwon1 = _one_match_stats.get('stat_player1_first_serv_pts',None)
			_ctstat1stservwon2 = _one_match_stats.get('stat_player2_first_serv_pts',None)
			_ctstat2ndservwon1 = _one_match_stats.get('stat_player1_2nd_serv_pts',None)
			_ctstat2ndservwon2 = _one_match_stats.get('stat_player2_2nd_serv_pts',None)
			_ctstatbrkwon1 = _one_match_stats.get('stat_player1_brk_pts_won',None)
			_ctstatbrkwon2 = _one_match_stats.get('stat_player2_brk_pts_won',None)
			_ctstatretwon1 = _one_match_stats.get('stat_player1_return_pts_won',None)
			_ctstatretwon2 = _one_match_stats.get('stat_player2_return_pts_won',None)
			_ctstatdble1 = _one_match_stats.get('stat_player1_dble_faults',None) if _one_match_stats.get('stat_player1_dble_faults',None)!='' else None
			_ctstatdble2 = _one_match_stats.get('stat_player2_dble_faults',None) if _one_match_stats.get('stat_player2_dble_faults',None)!='' else None
			_ctstataces1 = _one_match_stats.get('stat_player1_aces',None) if _one_match_stats.get('stat_player1_aces',None)!='' else None
			_ctstataces2 = _one_match_stats.get('stat_player2_aces',None) if _one_match_stats.get('stat_player2_aces',None)!='' else None
			_ctstattype='W'
			_ctstatsource = self.siteBaseLink

			try:
				db.callproc('sp_insert_ctmstats',(_ctstartdate,_ctenddate,_ctplayer1,_ctname1,_ctplayer2,_ctname2,_cttournament,_cttourcategory,_ctourncountry,_ctsurface,_ctround,_ctscore,
			        	                          _ctstat1stserv1,_ctstat1stserv2,_ctstat1stservwon1,_ctstat1stservwon2,_ctstat2ndservwon1,
			        	                          _ctstat2ndservwon2,_ctstatbrkwon1,_ctstatbrkwon2,_ctstatretwon1,_ctstatretwon2,_ctstatdble1,
			        	                          _ctstatdble2,_ctstataces1,_ctstataces2,_ctstattype,_ctstatsource))

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][{threading.currentThread().name}][Single Women Data from tennislive] The following match stats : {str(_one_match_stats)} have been successfully inserted')
				self.queueMatchsStats.task_done()
			except Exception as e:
				self.queueMatchsStats.task_done()
				self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertPlayersIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		self.insertCountriesIntoDb()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Connection to the database is successful.')

				db.autocommit=True
				for _p in self.playersInfoList:
					_d=self.convertStringToDico(_p)

					_ctcode =_d['player_code']
					_ctname =_d['player_name']
					_ctcountry =_d['player_country_code']
					_ctgender ='W'
					_ctbirthdate =self.convertStringToDate(_d['player_birth_date']) if _d.get('player_birth_date','') !='' else None
					_ctweight =_d.get('player_weight',None)
					_ctheight =_d.get('player_height',None)
					_ctbeginpro =self.convertStringToDate(_d['player_begin_pro'].split(';')[0]+'-01-01')
					_cthandstyle=_d.get('player_style_hand',None)

					db.callproc('sp_insert_ctplayer', (_ctcode,_ctname,_ctcountry,_ctgender,_ctbirthdate,_ctweight,_ctheight,_ctbeginpro,_cthandstyle))
				db.close()
			
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] players have been successfully inserted or updated.')
				return pool_stats
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Insertion into the database failed with the following exception : {e}')	
			return None


	def insertPlayersMappingIntoDb(self):
		players_list_links=self.getAllPlayersLinkInList()
		pool_stats=self.extractMultiplePlayersInfo(players_list_links)

		_ctcode_list=[]
		for _i in self.playersInfoList:
			_d=self.convertStringToDico(_i)
			_ctcode_list.append((_d['player_code'],(_d['player_name'].lower())))

		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Connection to the database is successful.')
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] {len(_ctcode_list)} players to be mapped.')

				db.execute("select ctcode,lower(replace(ctname,',','')) from ctennisplayers where ctgender='W' order by ctdid")
				newCursor=db.newDbCursor()

				_found=False
				_ctcode=db.fetchone()
				_cc=0
				_ilen=0
				while _ctcode:
					_tmp_ctcode,_tmp_ctname=_ctcode[0],_ctcode[1]

					for j,_i in enumerate(_ctcode_list):
						if _tmp_ctcode.find(_i[0])>=0:
							_found=True
							break
						else:
							_ii=_i[0].split('-')
							_tmp_ctcode_j=_tmp_ctcode
							for _i0 in _ii:
								_ilen=_tmp_ctcode_j.find(_i0)
								if _ilen<0:
									break
								else:
									_tmp_ctcode_j=_tmp_ctcode_j[:_ilen]+_tmp_ctcode_j[_ilen+len(_i0):]
							else:
								_found=True

							if _found:
								break

							_ii=_ctcode[0][_ctcode[0].find('-')+1 if _ctcode[0].find('-')>=0 else 0:].split('-')
							_i1=_i[0]
							for _i0 in _ii:
								_ilen=_i1.find(_i0)
								if _ilen<0:
									break
								else:
									_i1=_i1[:_ilen]+_i1[_ilen+len(_i0):]
							else:
								if len(_ii)>1:
									_found=True

							if _found:
								break

							if _tmp_ctname==_i[1]:
								_found=True
								break
								
					if _found:
						newCursor.callproc('sp_insert_ctplayermapping',(_i[0],_ctcode[0]))
						self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] The mapping : [{_i[0]} <=> {_ctcode[0]}] has been successfully inserted.')
						_cc+=1
						_ctcode_list.pop(j)

					_found=False
					_ctcode=db.fetchone()

				db.commit()
				newCursor.close()
				db.close()

				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] {_cc} mappings has(ve) been successfully inserted.')
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] {_ctcode_list} has(ve) not been mapped.')
		except Exception as e:
			newCursor.close()
			db.close()
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertCountriesIntoDb(self):
		try:
			db=cTennisDb.ctennisDbClass(_connection_file=self.dbConfigFile,_environ=self.dbEnviron)
			if db.connectWithConfig():
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Connection to the database is successful.')

				db.autocommit=True
				for _c in self.countriesList.items():
					db.callproc('sp_insert_ctcountry',_c)

				db.close()
				self.ctennisLogger.info(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Countries have been successfully inserted or updated.')
		except Exception as e:
			self.ctennisLogger.error(f'[{multiprocessing.current_process().name}][Single Women Data from tennislive] Insertion into the database failed with the following exception : {e}')


	def insertCategoriesIntoDb(self,_ctcode,_ctlabel,_ctpoints):
		"""
		The tournament categories are inserted only from the reference sources : atptour.com and wtatennis.com
		"""		
		pass

	def insertRoundsIntoDb(self,_ctcodeofficial,_ctlabel):
		"""
		The rounds are inserted only from the reference sources : atptour.com and wtatennis.com
		"""
		pass

	def insertTournamentsIntoDb(self):
		"""
		Not implemented because tournaments are not available on the source tennislive.net
		"""			
		pass


######################################################################### Class cTennisRunProcess ################################################################################
class cTennisRunProcess:
	dataSourceDico={'smo':SingleMenOfficialTennisDataClass,
					'sml':SingleMenLiveTennisDataClass,
					'swo':SingleWomenOfficialTennisDataClass,
					'swl':SingleWomenLiveTennisDataClass}

	def __init__(self,pool_size=4,subset_count=2,_start=0,_end=-1,data_source='smo',log_file='main.log',_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',date_range=[],players_list=[]):
		self.__internalPoolOfProcesses=None
		self.__playersList=players_list
		self.__dateRange=date_range
		self.__poolSize=pool_size
		self.__subsetCount=subset_count
		self.__start=_start
		self.__end=_end
		self.__dataSource=data_source
		self.__logFile=log_file
		self.__dbConfigFile=_db_config_file
		self.__dbEnviron=_db_environ

	def __removeLogFile(self):
		_abs_path=os.path.abspath(self.__logFile)
		if os.path.exists(_abs_path):
			if os.stat(_abs_path).st_size>0:
				_path_root,_path_file=os.path.split(_abs_path)
				_file_name,_file_ext=os.path.splitext(_path_file)
				_renamed_file=_path_root+'/'+_file_name+'_'+datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')+_file_ext
				shutil.copy(_abs_path,_renamed_file)
				os.remove(_abs_path)

	@staticmethod
	def __sProcessProducer(data=[],_nb=2,_start=0,_end=-1):
		_rs=[]
		_len=len(data)
		if _end <0:
			_end=_len

		while _start<_end and _start<_len:
			_rs.append(data[_start:_start+_nb])
			_start+=_nb
		return _rs

	@staticmethod
	def __sProcessConsumer(data_source='smo',log_file='main.log',_db_config_file='cTennisDbConnection.ini',_db_environ='DEV_DB',date_range=[],players_list=[]):
		_s=cTennisRunProcess.dataSourceDico[data_source](date_range=date_range,players_list=players_list,_db_config_file=_db_config_file,_db_environ=_db_environ,_log_file=log_file)
		
		if data_source in ('smo','sml','swl'):
			print(f'[{multiprocessing.current_process().name}] Retrieval of players :')
			t1=time.time()
			pool_stats=_s.insertPlayersIntoDb()
			t1=time.time()-t1

			print()
			print(f'[{multiprocessing.current_process().name}] Pool of threads stats : ')
			for t in pool_stats:
				print(f'Thread : {t} :: Usage : {pool_stats[t]}')
			print(f'[{multiprocessing.current_process().name}] The retrieval and insertion/update of players took : {t1}')
			print()

		print(f'[{multiprocessing.current_process().name}] Insertion of matches stats :')
		t1=time.time()
		pool_stats=_s.extractStatsIntoDb()
		t1=time.time()-t1

		print()
		print(f'[{multiprocessing.current_process().name}] Pool of threads stats : ')
		for t in pool_stats:
			print(f'Thread : {t} :: Usage : {pool_stats[t]}')
		print(f'[{multiprocessing.current_process().name}] The extraction and insertion of matches stats took : {t1}')
		
	def start(self):
		self.__removeLogFile()

		if self.__dataSource=='smo':
			if not self.__playersList:
				db=cTennisDb.ctennisDbClass(_connection_file=self.__dbConfigFile,_environ=self.__dbEnviron)
				if db.connectWithConfig():
					db.execute("select lower(replace(ctname,',','')) as ctname from ctennisplayers where ctgender='M' order by ctdid")
					_r=db.fetchone()
					while _r:
						self.__playersList.append(_r[0])
						_r=db.fetchone()
				db.close()

			self.__internalPoolOfProcesses=cTennisPoolOfProcesses(pool_size=self.__poolSize,pool_target_prod=self.__sProcessProducer,pool_target_cons=self.__sProcessConsumer,
				                                                  pool_data_prod=(self.__playersList,self.__subsetCount,self.__start,self.__end),
				                                                  pool_args_cons=(self.__dataSource,self.__logFile,self.__dbConfigFile,self.__dbEnviron,self.__dateRange))
			pool_stats=self.__internalPoolOfProcesses.process_pool()
			self.__internalPoolOfProcesses.close_pool()
			print(pool_stats)

		elif self.__dataSource=='sml':
			if not self.__playersList:
				_s=cTennisRunProcess.dataSourceDico['sml']()
				self.__playersList=_s.getAllPlayersNames()
				del _s

			self.__internalPoolOfProcesses=cTennisPoolOfProcesses(pool_size=self.__poolSize,pool_target_prod=self.__sProcessProducer,pool_target_cons=self.__sProcessConsumer,
				                                                  pool_data_prod=(self.__playersList,self.__subsetCount,self.__start,self.__end),
				                                                  pool_args_cons=(self.__dataSource,self.__logFile,self.__dbConfigFile,self.__dbEnviron,self.__dateRange))
			pool_stats=self.__internalPoolOfProcesses.process_pool()
			self.__internalPoolOfProcesses.close_pool()
			print(pool_stats)

		elif self.__dataSource=='swo':
			self.__sProcessConsumer(data_source=self.__dataSource,log_file=self.__logFile,_db_config_file=self.__dbConfigFile,_db_environ=self.__dbEnviron,date_range=self.__dateRange,players_list=self.__playersList)

		elif self.__dataSource=='swl':
			if not self.__playersList:
				_s=cTennisRunProcess.dataSourceDico['swl']()
				self.__playersList=_s.getAllPlayersNames()
				del _s

			self.__internalPoolOfProcesses=cTennisPoolOfProcesses(pool_size=self.__poolSize,pool_target_prod=self.__sProcessProducer,pool_target_cons=self.__sProcessConsumer,
				                                                  pool_data_prod=(self.__playersList,self.__subsetCount,self.__start,self.__end),
				                                                  pool_args_cons=(self.__dataSource,self.__logFile,self.__dbConfigFile,self.__dbEnviron,self.__dateRange))
			pool_stats=self.__internalPoolOfProcesses.process_pool()
			self.__internalPoolOfProcesses.close_pool()
			print(pool_stats)


################################################################################# TESTS ##########################################################################################
def getControlDates(_source='https://www.atptour.com',_min_dates='2010-01-01'):
	_control_dates=[]
	db=cTennisDb.ctennisDbClass(_connection_file='cTennisDbConnection.ini',_environ='DEV_DB')
	if db.connectWithConfig():
		db.execute(f"select ctstartdate,max(ctenddate) ctenddate from ctennismatchesstats where ctstatsource='{_source}' and ctstartdate>='{_min_dates}' group by ctstartdate order by ctenddate")
		_r=db.fetchone()
		while _r:
			_control_dates.append(_r[1])
			_r=db.fetchone()
	db.close()
	return _control_dates

def prodCleanStats(date_range=[],_split=0.5,_control_dates=[]):
	if not date_range:
		date_range=['2010-01-01','2020-03-31']
	d0,d1=datetime.datetime.strptime(date_range[0],'%Y-%m-%d').date(),datetime.datetime.strptime(date_range[1],'%Y-%m-%d').date()
	res=[]
	d0i=None
	_split=int(365*_split)
	_control_dates=getControlDates()
	_clen=len(_control_dates)
	_cur_index=0
	while d0<d1:
		d0i=d0+datetime.timedelta(days=_split)
		while _cur_index<_clen:
			if d0i<=_control_dates[_cur_index]:
				d0i=_control_dates[_cur_index]
				break
			else:
				_cur_index+=1
		res.append([d0.strftime('%Y-%m-%d'),d0i.strftime('%Y-%m-%d')])
		d0=d0i
	return res

def consCleanStats(_ctcode=None,_ctgender='M',date_range=[]):
	try:
		db=cTennisDb.ctennisDbClass(_connection_file='cTennisDbConnection.ini',_environ='DEV_DB')
		if db.connectWithConfig():
			print(f'[{multiprocessing.current_process().name}] Connection to the database is successful. Insertion of : {date_range}')

			db.autocommit=True
			_ctstartdate=datetime.datetime.strptime(date_range[0],'%Y-%m-%d').date()
			_ctenddate=datetime.datetime.strptime(date_range[1],'%Y-%m-%d').date()
			db.callproc('sp_feed_ctcleanmstats', (_ctcode,_ctstartdate,_ctenddate,_ctgender))
			db.close()
		
			print(f'[{multiprocessing.current_process().name}] Stats from alternative source have been added.')
	except Exception as e:
		print(f'[{multiprocessing.current_process().name}] Stats from alternative source failed to be added with the following exception : {e}')	

def testCleanStats():
	print(prodCleanStats())

	pool_proc=cTennisPoolOfProcesses(pool_size=6,pool_target_prod=prodCleanStats,pool_target_cons=consCleanStats,pool_data_prod=([],(1./12)),pool_args_cons=(None,'M'))
	pool_stats=pool_proc.process_pool()
	pool_proc.close_pool()
	print(pool_stats)


def testsmocTennisRunProcess():
	p=cTennisRunProcess(data_source='smo',_start=0,_end=50,date_range=['2010-01-01','2020-03-31'])
	p.start()

def functionForPool(p):
	print(f'The thread [{threading.currentThread().getName()}] receives the data [{p}]')

def testcTennisPoolOfThreads1():
	pool_data=[d for d in range(100)]

	pool_thread=cTennisPoolOfThreads(pool_size=2,thread_target=functionForPool,pool_data=pool_data)
	pool_thread.process_pool()
	pool_thread.close_pool()

	print(f'The main [{threading.currentThread().getName()}] is done')
	print('Threads pool stats : ')

	for t in pool_thread.poolStats:
		print(f'Thread : {t} :: Usage : {pool_thread.poolStats[t]}')

def producerForPool(p):
	r=[i for i in range(p)]
	for i in r:
		print(f'[++] The thread-producer [{threading.currentThread().getName()}] has produced the data [{i}]')
	return r

def consumerForPool(p):
	print(f'[--] The thread-consumer [{threading.currentThread().getName()}] has consumed the data [{p}]')

def testcTennisPoolOfThreads2():
	pool_data=[10,5,8,10,15]
	thread_names=[]
	thread_target=[]

	for i in range(3):
		thread_names.append(f'producer_thread-{i+1}')
		thread_target.append(producerForPool)

	for i in range(10):
		thread_names.append(f'consumer_thread-{i+1}')
		thread_target.append(consumerForPool)		

	pool_thread=cTennisPoolOfThreads(pool_size=len(thread_names),thread_names=thread_names,thread_target=thread_target,pool_data=pool_data,is_pool_queue=True)
	pool_thread.process_pool()
	pool_thread.close_pool()

def testextractSingleMatchStats():
	sm=SingleMenOfficialTennisDataClass()
	sm.extractSingleMatchStats(str({'link':'https://www.atptour.com/en/scores/2020/451/MS026/match-stats'}))
	print(sm.queueMatchsStats.get())

def testSingleMenLiveTennisDataClass():
	players_list=[]
	db=cTennisDb.ctennisDbClass(_connection_file='cTennisDbConnection.ini',_environ='DEV_DB')
	if db.connectWithConfig():
		db.execute("select lower(replace(ctname,',','')) as ctname from ctennisplayers where ctgender='M' order by ctdid limit 10")
		_r=db.fetchone()
		while _r:
			players_list.append(_r[0])
			_r=db.fetchone()
	db.close()
	players_list=[] #['dominic thiem']

	sm=SingleMenLiveTennisDataClass(date_range=['2010-01-01','2020-03-28'],players_list=players_list)
	sm.insertPlayersMappingIntoDb()
	#sm.playersInfoList=[]

	#print('Players insertion :')
	#t1=time.time()
	#pool_stats=sm.insertPlayersIntoDb()
	#t1=time.time()-t1

	#print()
	#print('Pool of threads stats : ')
	#for t in pool_stats:
	#	print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	#print(f'The extraction and insertion of players took : {t1}')

	#print()
	#print('Matches stats insertion :')
	#t1=time.time()
	#pool_stats=sm.extractStatsIntoDb()
	#t1=time.time()-t1

	#print()
	#print('Pool of threads stats : ')
	#for t in pool_stats:
	#	print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	#print(f'The extraction and insertion of matches stats took : {t1}')

def testInternalSingleMenOfficialTennisDataClass(date_range=[],players_list=[]):
	sm=SingleMenOfficialTennisDataClass(date_range=date_range,players_list=players_list)
	print('Players insertion :')
	t1=time.time()
	pool_stats=sm.insertPlayersIntoDb()
	t1=time.time()-t1

	print()
	print('Pool of threads stats : ')
	for t in pool_stats:
		print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	print(f'The extraction and insertion of players took : {t1}')

	print()
	print('Matches stats insertion :')
	t1=time.time()
	pool_stats=sm.extractStatsIntoDb()
	t1=time.time()-t1

	print()
	print('Pool of threads stats : ')
	for t in pool_stats:
		print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	print(f'The extraction and insertion of matches stats took : {t1}')


def testpool_target_prod(data=[],_nb=1):
	_rs=[]
	_len=len(data)
	_start=1500
	_end=2000

	while _start<_end and _start<_len:
		_rs.append(data[_start:_start+_nb])
		_start+=_nb
	return _rs


def testSingleMenOfficialTennisDataClass():
	players_list=[]
	db=cTennisDb.ctennisDbClass(_connection_file='cTennisDbConnection.ini',_environ='DEV_DB')
	if db.connectWithConfig():
		db.execute("select lower(replace(ctname,',','')) as ctname from ctennisplayers where ctgender='M' order by ctdid")
		_r=db.fetchone()
		while _r:
			players_list.append(_r[0])
			_r=db.fetchone()
	db.close()

	#players_list=[]#['steven diez'] #['roberto quiroz','arthur reymond','arvid nordquist'] #['stefanos tsitsipas']

	date_range=['2010-01-01','2020-03-28']

	#print(testpool_target_prod(data=players_list))

	pool_proc=cTennisPoolOfProcesses(pool_size=4,pool_target_prod=testpool_target_prod,pool_target_cons=testInternalSingleMenOfficialTennisDataClass,pool_data_prod=(players_list,2),pool_args_cons=(date_range,))
	pool_stats=pool_proc.process_pool()

	pool_proc.close_pool()

	print(pool_stats)
	#print(pool_proc.pool_result)
	
	#_processes=[]
	#for _i in (256,259,262):
	#	_processes.append(multiprocessing.Process(target=testInternalSingleMenOfficialTennisDataClass,args=(['2016-01-01','2020-03-28'],players_list[_i:_i+3]),daemon=True))
	#	_processes[-1].start()

	#for _i in _processes:
	#	_i.join()
	#	print(_i.name,' is terminated. : ',_i.is_alive())

	#_i=24
	#_ilen=26 #len(players_list)
	#while _i<_ilen:
	#	testInternalSingleMenOfficialTennisDataClass(date_range=['2016-01-01','2020-03-28'],players_list=players_list[_i:_i+1])

	#	for _tid,_toj in threading._active.items():
	#		print(f'{_toj.name}  is active')
	#	_i+=1


def testSingleWomenLiveTennisDataClass():
	players_list=[]
	db=cTennisDb.ctennisDbClass(_connection_file='cTennisDbConnection.ini',_environ='DEV_DB')
	if db.connectWithConfig():
		db.execute("select lower(replace(ctname,',','')) as ctname from ctennisplayers where ctgender='W' order by ctdid limit 10")
		_r=db.fetchone()
		while _r:
			players_list.append(_r[0])
			_r=db.fetchone()
	db.close()
	players_list=[]

	sm=SingleWomenLiveTennisDataClass(date_range=['2016-01-01','2020-03-28'],players_list=players_list)
	#sm.insertPlayersMappingIntoDb()
	#sm.playersInfoList=[]

	sm.insertPlayersMappingIntoDb()

	#print('Players insertion :')
	#t1=time.time()
	#pool_stats=sm.insertPlayersIntoDb()
	#t1=time.time()-t1

	#print()
	#print('Pool of threads stats : ')
	#for t in pool_stats:
	#	print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	#print(f'The extraction and insertion of players took : {t1}')

	#print()
	#print('Matches stats insertion :')
	#t1=time.time()
	#pool_stats=sm.extractStatsIntoDb()
	#t1=time.time()-t1

	#print()
	#print('Pool of threads stats : ')
	#for t in pool_stats:
	#	print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	#print(f'The extraction and insertion of matches stats took : {t1}')	

def testSingleWomenOfficialTennisDataClass():
	sm=SingleWomenOfficialTennisDataClass(date_range=['2016-01-01','2020-03-28'])
	#sm.extractSingleMatchStats(str({'link':'https://www.wtatennis.com/tournament/1059/taipei/2019/scores/LS022'}))
	#print(sm.queueMatchsStats.get())
	#if sm.serializeDicoTournaments():
	#	print('The dico of tournaments has been successfully serialized')
	#_player_list=sm.getAllPlayersLinkInList()
	
	#print('Display the players list :')

	#for i,j in enumerate(_player_list):
	#	print(i,'] ',j)

	#sm.getAllMatchesToExtract()
	#_all_tourn=sm.getAllTournaments()

	#print('\nDisplay all tournaments : ',len(_all_tourn))
	#for i in _all_tourn:
	#	print(f'Tournament category : {i[0]} ; Tournament name : {i[1]} ; Tournament location : {i[2]} ; Tournament surface : {i[3]}')

	#print('\nDisplay all matches to extract for all tournaments: ',len(sm.allMatchesToExtract))
	#for i in sm.allMatchesToExtract:
	#	print(i)

	#print()
	#_one_link_matches=sm.getMatchesToExtractInYearForAPlayer(sm.allMatchesToExtract[0])
	#print('\nDisplay all matches to extract : ',len(_one_link_matches))
	#for i in _one_link_matches:
	#	print(i)

	#print()
	#_one_match_stats=sm.extractSingleMatchStats(_one_link_matches[0])
	#print('\nDisplay one match stats :')
	#print(_one_match_stats)
	#print('Players insertion :')
	#t1=time.time()
	#pool_stats=sm.insertPlayersIntoDb()
	#t1=time.time()-t1

	#print()
	#print('Pool of threads stats : ')
	#for t in pool_stats:
	#	print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	##print(f'The extraction and insertion of players took : {t1}')

	print()
	print('Matches stats insertion :')
	t1=time.time()
	pool_stats=sm.extractStatsIntoDb()
	t1=time.time()-t1

	print()
	print('Pool of threads stats : ')
	for t in pool_stats:
		print(f'Thread : {t} :: Usage : {pool_stats[t]}')
	print(f'The extraction and insertion of matches stats took : {t1}')

def testselenium():
	_url='https://www.wtatennis.com/tournaments/2020'

	chrome_driver=webdriver.Chrome()
	chrome_driver.get(_url)

	_tourn_months=chrome_driver.find_element_by_xpath('//div[@class="tournament-list__filters-months js-horizontal-scroll"]').find_elements_by_tag_name('a')
	for i,j in enumerate(_tourn_months):
		print(i+1,') ',j.get_attribute('outerHTML'))

		chrome_driver.execute_script('arguments[0].click();',j)
		time.sleep(1)

	_bs=bs4.BeautifulSoup(chrome_driver.page_source,'lxml')
	chrome_driver.quit()

	_bs_result=_bs.find_all('li',{'class':'tournament-list__item'})
	__allTournamentsList=[]

	for _i,_t in enumerate(_bs_result):
		_tc=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent.parent.find_all('span')[1]['class'][1][len('tournament-tag--'):]
		_tn=_t.find('h3',{'class':'tournament-thumbnail__title'})['data-text']
		_tl=_t.find('span',{'class':'tournament-thumbnail__location'}).text.strip()
		_ts=_t.find('svg',{'class':'icon tournament-tag__icon'}).parent['class'][1][len('tournament-tag--'):]

		print(f'Tournament number [{_i+1}] ; Tournament category: {_tc} ; Tournament name : {_tn} ; Tournament location : {_tl} ; Tournament surface : {_ts}')
		__allTournamentsList.append((_tc,_tn,_tl,_ts))

def testregex():
	#str_txt='<a class="tennis-match__match-link" href="/tournament/1081/zhuhai/2019/scores/LS009" title="Bertens vs. Yastremska | Group Stage Hengqin Life WTA Elite Trophy Zhuhai 2019 2019 | Match Center" aria-label="Bertens vs. Yastremska | Group Stage Hengqin Life WTA Elite Trophy Zhuhai 2019 2019 | Match Center"></a>'
	#str_txt='<a class="tennis-match__match-link" href="/tournament/1081/zhuhai/2019/scores/LS002" title="Bertens vs. Zheng |  Hengqin Life WTA Elite Trophy Zhuhai 2019 2019 | Match Center" aria-label="Bertens vs. Zheng |  Hengqin Life WTA Elite Trophy Zhuhai 2019 2019 | Match Center"></a>'
	#str_txt='<a class="tennis-match__match-link" href="/tournament/1099/zhengzhou/2018/scores/LS001" title="Wang vs. Zheng | Final 2018 Zhengzhou Women’s Tennis Open 2018 | Match Center" aria-label="Wang vs. Zheng | Final 2018 Zhengzhou Women’s Tennis Open 2018 | Match Center"></a>'
	str_txt='<a class="tennis-match__match-link" href="/tournament/1100/mumbai/2018/scores/LS001" title="Kumkhum vs. Khromacheva | Final L&amp;T Mumbai Open 2018 | Match Center" aria-label="Kumkhum vs. Khromacheva | Final L&amp;T Mumbai Open 2018 | Match Center"></a>'

	#_mpattern=re.compile(r'<a class="tennis-match__match-link" href=(.*) title=["][a-zA-Z]+ vs[.] [a-zA-Z]+ [|] (Final|Semifinal|Quarterfinal|Round of 16|Round of 32|Round of 64|Round of 128|Group Stage|Hengqin Life WTA|Shiseido WTA Finals| Hengqin Life WTA| Shiseido WTA Finals)')
	_mpattern=re.compile(r'<a class="tennis-match__match-link" href=(.*) title=["]([a-zA-Z.’-]+ vs[.] [a-zA-Z.’-]+) [|] ([a-zA-Z0-9 .’;@&-]*) [|] (Match Center)')
	_ms=_mpattern.findall(str_txt)

	print(_ms)

	_rs='Semifinal' if _ms[0][1].strip()=='Hengqin Life WTA' or _ms[0][1].strip()=='Shiseido WTA Finals' else _ms[0][1].replace(' ','')
	print(_rs)

	for i in _ms:
		print(i,'::',i[0],'::',i[1])

if __name__=='__main__':
#	testcTennisPoolOfThreads1()
#	testcTennisPoolOfThreads2()
#	testSingleMenOfficialTennisDataClass()
#	testSingleWomenOfficialTennisDataClass()
#	testselenium()
#	testSingleMenLiveTennisDataClass()
#	testregex()
#	testSingleWomenLiveTennisDataClass()
#	testextractSingleMatchStats()
	testsmocTennisRunProcess()
#	testCleanStats()
#	print(prodCleanStats())
