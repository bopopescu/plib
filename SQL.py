# coding=utf8
""" SQL Module

Handles creating, storing to, and fetching from SQL tables
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-07-08"

import datetime
import re
import sys
import time

# Import pip modules
from enum import IntEnum
import MySQLdb

## ESelect
class ESelect(IntEnum):
	ALL			= 1
	CELL		= 2
	COLUMN		= 3
	HASH		= 4
	HASH_ROWS	= 5
	ROW			= 6

# Connection exception
class SqlConnectException(Exception):
	"""SqlConnectException class

	Used for raising connection specific exceptions

	Extends:
		Exception
	"""
	pass

# Exception
class SqlException(Exception):
	"""SqlException class

	Used for raising SQL specific exceptions

	Extends:
		Exception
	"""
	pass

# Duplicate key exception
class SqlDuplicateException(Exception):
	"""SqlDuplicateException class

	Used for raising issues with duplicate records

	Extends:
		Exception
	"""
	pass

# MySQL class
class MySQL(object):
	"""MySQL class

	Extends:
		object
	"""

	# List of connections to MariaDB servers
	_dConnections	= dict()	# dictionary of objects

	# List of host details by name for ease of use
	_dHosts	= {}

	# Optional DB Prefix used for changing DB names across the board. e.g. for
	#	testing purposes
	_DB_PREFIX		= ''

	@classmethod
	def _clearConnection(cls, host, rel, ):
		"""Clear Connection

		Handles removing a connection from the global list

		Args:
			host (str): The name of the connection
			rel (str): The relationship of the server, master or slave

		Returns:
			None
		"""

		# Save the full name of the connection
		sName	= host + ':' + rel

		# If we have the connection
		if sName in cls._dConnections:

			# Try to close the connection just in case
			try:
				cls._dConnections[sName]['con'].close()

			except MySQLdb.ProgrammingError as e:

				print('\n------------------------------------------------------------')
				print('ProgrammingError in SQL_MySQL._clearConnection')
				print('name = ' + str(sName))
				print('args = ' + ', '.join([str(s) for s in e.args]))

			except Exception as e:

				print('\n------------------------------------------------------------')
				print('Unknown exception in SQL_MySQL._clearConnection')
				print('exception = ' + str(e.__class__.__name__))
				print('name = ' + str(sName))
				print('args = ' + ', '.join([str(s) for s in e.args]))

			del cls._dConnections[sName]

	@staticmethod
	def _converterTimestamp(ts):
		"""Converter Timestamp

		Converts timestamps received from MySQL into proper integers

		Args:
			ts (str): The timestamp to convert

		Returns:
			uint
		"""

		# If there is no time
		if ts == '0000-00-00 00:00:00':
			return 0

		# Get a datetime tuple
		tDT	= datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

		# Convert it to a timestamp and return it
		return int(tDT.strftime('%s'))

	@classmethod
	def _fetchConnection(cls, host, rel, errcnt=0, dictCursor=False):
		"""Fetch Connection

		Returns a connection to the given server, if there isn't one, it creates
		the instance and connects before return

		Args:
			host (str): The name of the instance to fetch
			rel (str): The relationship of the server, master or slave

		Returns:
			cursor
		"""

		# Save the full name of the connection
		sName	= host + ':' + rel

		# Get the current timestamp
		#iTS		= time.time()

		# If we already have the connection
		if sName in cls._dConnections:

			# If the connection is still valid
			#if cls._dConnections[sName]['ts'] > iTS:
			if dictCursor:	return cls._dConnections[sName]['con'].cursor(MySQLdb.cursors.DictCursor)
			else:			return cls._dConnections[sName]['con'].cursor()

			# Else, clear it
			#else:
			#	cls._clearConnection(host, rel)

		# If the host doesn't exist
		if host not in cls._dHosts:
			raise ValueError(cls.__name__ + '.' + sys._getframe().f_code.co_name + ' no such host "' + host + '"')

		# Get the config
		dConf	= cls._dHosts[host][rel]

		# If the config is a string
		if isinstance(dConf, basestring):

			# Then it represents the sibling, so get their data instead
			dConf	= cls._dHosts[host][dConf]

		# Create a new connection
		try:
			oDB	= MySQLdb.connect(**dConf)

			# Turn autocommit on
			oDB.autocommit(True)

			# Change conversions
			conv	= oDB.converter.copy()
			for k in conv:
				if k in [7]:			conv[k]	= cls._converterTimestamp
				elif k in [10,11,12]:	conv[k]	= str
			oDB.converter	= conv

		# If there was an error
		except MySQLdb.Error, e:

			# Increment the error count
			errcnt	+= 1

			# If we've hit our max errors, raise an exception
			if errcnt == 3:
				raise SqlConnectException('SQL connection error (' + str(e.args[0]) + '): ' + str(e.args[1]))

			# Else just sleep for a second and try again
			else:
				time.sleep(1)
				return cls._fetchConnection(host, rel, errcnt)

		# Store the connection
		cls._dConnections[sName]	= {
			'ts': 	time.time(),# + SQL.dSqlConfig['seconds_alive'],
			'con':	oDB
		}

		# Get the cursor
		if dictCursor:	oCur	= oDB.cursor(MySQLdb.cursors.DictCursor)
		else:			oCur	= oDB.cursor()

		# Make absolutely sure we're on UTF
		oCur.execute('SET NAMES utf8')

		# Return the connection
		return oCur

	@classmethod
	def addHost(cls, name, details):
		"""Add Host

		Adds a host entry to the list so that it can be used apps

		Args:
			name (str): The name of the host
			details (dict): The details needed to connect to the host

		Returns:
			None
		"""

		# Make sure name is a valid string
		if not isinstance(name, basestring):
			raise ValueError(cls.__name__ + '.' + sys._getframe().f_code.co_name + ' first argument (name) must be a string')

		# Store the details under the name
		cls._dHosts[name]	= details

	@classmethod
	def escape(cls, host, value, rel='master', errcnt=0):
		"""Escape

		Used to escape string values for the DB

		Args:
			host (str): The name of the instance to escape for
			value (str): The value to escape
			rel (str): The relationship of the server, master or slave

		Returns:
			str
		"""

		# Get the connection
		oCur	= cls._fetchConnection(host, rel)

		# Get the value
		try:
			sRet	= oCur.connection.escape_string(value)

		# Else there's an operational problem so close the connection and
		#	restart
		except MySQLdb.OperationalError as e:

			# Close the cursor
			oCur.close()

			# Clear the connection, sleep for a second, and try again
			cls._clearConnection(host, rel)
			time.sleep(1)
			return cls.escape(host, value, rel, errcnt=errcnt)

		except Exception as e:

			# Close the cursor
			oCur.close()

			print('\n------------------------------------------------------------')
			print('Unknown Error in SQL_MySQL.escape')
			print('exception = ' + str(e.__class__.__name__))
			print('value = ' + str(value))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

		# Close the cursor
		oCur.close()

		# Return the escaped string
		return sRet

	@classmethod
	def execute(cls, host, sql, errcnt=0):
		"""Execute

		Used to run SQL that doesn't return any rows

		Args:
			host (str): The name of the host
			sql (str|tuple): The SQL (or SQL plus a list) statement to run

		Returns:
			uint
		"""

		# Get the connection
		oCur		= cls._fetchConnection(host, 'master')

		try:

			# If the sql arg is a tuple we've been passed a string with a list for the purposes
			#	of replacing parameters
			if isinstance(sql, tuple):
				iRet	= oCur.execute(sql[0], sql[1])
			else:
				iRet	= oCur.execute(sql)

			# Close the cursor
			oCur.close()

			# Return the changed rows
			return iRet

		# If the SQL is bad
		except MySQLdb.ProgrammingError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Exception
			raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

		# Else, a duplicate key error
		except MySQLdb.IntegrityError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Duplicate Exception
			raise SqlDuplicateException(e.args[0], e.args[1])

		# Else there's an operational problem so close the connection and
		#	restart
		except MySQLdb.OperationalError as e:

			# Close the cursor
			oCur.close()

			# If the error code is one that won't change
			if e.args[0] in [1054]:
				raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# If the max error count hasn't been hit yet
			if errcnt < 5:

				# Clear the connection, sleep for a second, and try again
				cls._clearConnection(host, 'master')
				time.sleep(1)
				return cls.execute(host, sql, errcnt=errcnt+1)

			else:
				raise e

		# Else, catch any Exception
		except Exception as e:

			# Close the cursor
			oCur.close()

			print('\n------------------------------------------------------------')
			print('Unknown Error in SQL_MySQL.execute')
			print('exception = ' + str(e.__class__.__name__))
			print('errcnt = ' + str(errcnt))
			print('sql = ' + str(sql))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

	@classmethod
	def getGlobalPrefix(cls):
		"""Get Global Prefix

		Gets the name of the currently set DB prefix

		Returns:
			str
		"""
		return cls._DB_PREFIX;

	@classmethod
	def hasHost(cls, name):
		"""Has Host

		Returns True if we already have the host stored

		Args:
			name (str): The name of the host to check for

		Returns:
			bool
		"""
		return name in cls._dHosts

	@classmethod
	def insert(cls, host, sql, errcnt=0):
		"""Insert

		Handles INSERT statements and returns the new ID. To insert records
		without auto_increment it's best to just stick to CSQL.execute()

		Args:
			host (str): The name of the host
			sql (str): The SQL statement to run

		Returns:
			mixed
		"""

		# Get the connection
		oCur	= cls._fetchConnection(host, 'master')

		try:

			# If the sql arg is a tuple we've been passed a string with a list for the purposes
			#	of replacing parameters
			if isinstance(sql, tuple):
				oCur.execute(sql[0], sql[1])
			else:
				oCur.execute(sql)

			# Get the ID
			mInsertID	= oCur.lastrowid

			# Close the cursor
			oCur.close()

			# Return the last inserted ID
			return mInsertID

		# If the SQL is bad
		except MySQLdb.ProgrammingError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Exception
			raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

		# Else, a duplicate key error
		except MySQLdb.IntegrityError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Duplicate Exception
			raise SqlDuplicateException(e.args[0], e.args[1])

		# Else there's an operational problem so close the connection and
		#	restart
		except MySQLdb.OperationalError as e:

			# Close the cursor
			oCur.close()

			# If the error code is one that won't change
			if e.args[0] in [1054]:
				raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# If the max error count hasn't been hit yet
			if errcnt < 5:

				# Clear the connection, sleep for a second, and try again
				cls._clearConnection(host, 'master')
				time.sleep(1)
				return cls.insert(host, sql, errcnt=errcnt+1)

			else:
				raise e


		# Else, catch any Exception
		except Exception as e:

			# Close the cursor
			oCur.close()

			print('\n------------------------------------------------------------')
			print('Unknown Error in SQL_MySQL.insert')
			print('exception = ' + str(e.__class__.__name__))
			print('errcnt = ' + str(errcnt))
			print('sql = ' + str(sql))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

	@classmethod
	def select(cls, host, sql, seltype=ESelect.ALL, field=None, master=False, errcnt=0):
		"""Select

		Handles SELECT queries and returns the data

		Args:
			host (str): The name of the host
			sql (str): The SQL statement to run
			seltype (ESelect): The format to return the data in
			field (str): Only used by HASH_ROWS since MySQLdb has no ordereddict
				for associative rows
			master (bool): Set to true to run the select statement off the
				master and not the slave, necessary for functions that change
				data

		Returns:
			mixed
		"""

		# Get a cursor
		bDictCursor	= seltype in (ESelect.ALL, ESelect.HASH_ROWS, ESelect.ROW)

		# Get the connection
		sRel	= (master and 'master' or 'slave')
		oCur	= cls._fetchConnection(host, sRel, dictCursor=bDictCursor)

		try:
			# If the sql arg is a tuple we've been passed a string with a list for the purposes
			#	of replacing parameters
			if isinstance(sql, tuple):
				oCur.execute(sql[0], sql[1])
			else:
				oCur.execute(sql)

			# If we want all rows
			if seltype == ESelect.ALL:
				mData	= list(oCur.fetchall())

			# If we want the first cell 0,0
			elif seltype == ESelect.CELL:
				mData	= oCur.fetchone()
				if mData != None:
					mData	= mData[0]

			# If we want a list of one field
			elif seltype == ESelect.COLUMN:
				mData	= []
				mTemp	= oCur.fetchall()
				for i in mTemp:
					mData.append(i[0])

			# If we want a hash of the first field and the second
			elif seltype == ESelect.HASH:
				mData	= {}
				mTemp	= oCur.fetchall()
				for n,v in mTemp:
					mData[n]	= v

			# If we want a hash of the first field and the entire row
			elif seltype == ESelect.HASH_ROWS:
				# If the field arg wasn't set
				if field == None:
					raise SqlException('Must specificy a field for the dictionary key when using HASH_ROWS')

				mData	= {}
				mTemp	= oCur.fetchall()

				for o in mTemp:
					# Store the entire row under the key
					mData[o[field]]	= o

			# If we want just the first row
			elif seltype == ESelect.ROW:
				mData	= oCur.fetchone()

			# Close the cursor
			oCur.close()

			# Return the results
			return mData

		# If the SQL is bad
		except MySQLdb.ProgrammingError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Exception
			raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

		# Else, a duplicate key error
		except MySQLdb.IntegrityError as e:

			# Close the cursor
			oCur.close()

			# Raise an SQL Duplicate Exception
			raise SqlDuplicateException(e.args[0], e.args[1])

		# Else there's an operational problem so close the connection and
		#	restart
		except MySQLdb.OperationalError as e:

			# Close the cursor
			oCur.close()

			# If the error code is one that won't change
			if e.args[0] in [1054]:
				raise SqlException(e.args[0], 'SQL error (' + str(e.args[0]) + '): ' + str(e.args[1]) + '\n' + str(sql))

			# If the max error count hasn't been hit yet
			if errcnt < 5:

				# Clear the connection, sleep for a second, and try again
				cls._clearConnection(host, sRel)
				time.sleep(1)
				return cls.select(host, sql, seltype, errcnt=errcnt+1)

			else:
				raise e

		# Else, catch any Exception
		except Exception as e:

			# Close the cursor
			oCur.close()

			print('\n------------------------------------------------------------')
			print('Unknown Error in SQL_MySQL.select')
			print('exception = ' + str(e.__class__.__name__))
			print('errcnt = ' + str(errcnt))
			print('sql = ' + str(sql))
			print('args = ' + ', '.join([str(s) for s in e.args]))

			# Rethrow
			raise e

	@classmethod
	def setGlobalPrefix(cls, prefix):
		"""Set Global Prefix

		Use this to rename every DB so we can easily switch for testing and
		debugging. Will never be used in production.

		Args:
			prefix (str): The prefix for every DB name

		Returns:
			None
		"""
		cls._DB_PREFIX	= prefix;
