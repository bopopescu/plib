# coding=utf8
""" Storage Module

Handles creating, storing to, and fetching from RethinkDB NoSQL tables
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-18"

# Import python modules
import json
from hashlib import md5
import sys
from time import sleep

# Include pip modules
import rethinkdb as r

# Include local modules
from . import Dictionaries as Dict
from .OS import print_error

# Init module variables
__mdServers = {}
__msPrefix = ''

# DB create function
def db_create(name, server = 'default'):
	"""DB Create

	Creates a new DB on the given server

	Args:
		name (str): The name of the DB to create
		server (str): The name of the server to create the DB on

	Returns:
		bool
	"""

	try:

		# Fetch the connection
		with connect_with(server) as oCon:

			# Create the DB
			dRes = r.db_create(name).run(oCon)

			# If the DB wasn't created
			if 'dbs_created' not in dRes or not dRes['dbs_created']:
				return False

	# If there's already a DB with that name
	except r.errors.ReqlOpFailedError:
		return True

	# If there's any other error
	except r.errors.RqlRuntimeError:
		return False

	# Return ok
	return True

def db_drop(name, server = 'default'):
	"""DB Drop

	Deletes an existing DB from the given server

	Args:
		name (str): The name of the DB to create
		server (str): The name of the server to create the DB on

	Returns:
		bool
	"""

	try:

		# Fetch the connection
		with connect_with(server) as oCon:

			# Delete the DB
			dRes = r.db_drop(name).run(oCon)

			# If the DB wasn't deleted
			if 'dbs_dropped' not in dRes or not dRes['dbs_dropped']:
				return False

	# If there's no such DB
	except r.errors.RqlRuntimeError:
		return False

	# Return ok
	return True

# get/set global prefix
def globalPrefix(v = None):
	"""Global Prefix

	Call with no arguments to get current prefix, call with an argument to set
	the prefix to the argument

	Args:
		v (str): The new prefix to store

	Returns:
		None|str
	"""

	# PUll in the global var
	global __msPrefix

	# If nothing was passed
	if not v:
		return __msPrefix

	# Else, store the new prefix
	else:
		__msPrefix = v

# server function
def server(name, details, update=False):
	"""Server

	Adds a server to the list so that we can use it with Tables

	Args:
		name (str): Will be used to store the details
		details (dict): The credentials necessary to connect to the server

	Returns:
		bool
	"""

	# If we don't have the name, or we do but we can update
	if name not in __mdServers or update:

		# Store the details under the name
		__mdServers[name] = details

		# Return that the details were stored
		return True

	# We did nothing, return False
	return False

# connection function
def _connection(server, errcnt=0):
	"""Connection

	Fetches a connection to the given server

	Args:
		server (str): A name representing details stored using server()

	Returns:
		rethinkdb.net.DefaultConnection

	Raises:
		StorageException
	"""

	# If we can't find the server in the list
	if server not in __mdServers:
		raise ValueError('%s: no such server "%s"' % (sys._getframe().f_code.co_name, str(server)))

	# Try to make a new connection
	try:
		oCon = r.connect(**__mdServers[server])

	# Catch possible error
	except r.errors.RqlDriverError as e:

		# If there was an error, increment the error count
		errcnt	+= 1

		# If we've hit our max errors, raise an exception
		if errcnt == 3:
			raise StorageException(*e.args)
		else:
			# Else just sleep for a second and try again
			sleep(1)
			return _connection(server, errcnt)

	# Return the connection
	return oCon

# connect_with class
class connect_with(object):
	"""Connect With

	Used in conjunction with the python keyword "with" in order to make sure
	open connections are closed when the client is done with it

	Extends: object
	"""

	# constructor
	def __init__(self, server):
		self.con = _connection(server)

	# __enter__ magic method
	def __enter__(self):
		return self.con

	# __exit__ magic method
	def __exit__(self, exc_type, exc_value, traceback):
		self.con.close()
		if exc_type is not None:
			return False

# StorageException class
class StorageException(Exception):
	"""Storage Exception

	Used when critical errors happen in the Storage module

	Extends: Exception
	"""
	pass

# Document class
class Document(object):
	"""Table

	Handles all interaction with RethinkDB tables

	Extends: object
	"""

	# constructor
	def __init__(self, data={}, db={}):
		"""Constructor

		Initialises the instance and returns it

		Args:
			data (dict): The current state of the record
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			Table

		Raises:
			StorageException
		"""

		# Get the info
		self.__dInfo = self.info(db)

		# If the data isn't empty
		if data:

			# Validate it
			if not self.__dInfo['tree'].valid(data):
				raise ValueError("%s: %s" % (
					self.__dInfo['tree'].validation_fail_name,
					self.__dInfo['tree'].validation_fail_value
				))

		# Store the data
		self._dData = self.__dInfo['tree'].clean(data)
		self._dChanged = {}

	def __contains__(self, field):
		"""Contains (__contains__)

		Returns true if the string exists as a key in the Document

		Args:
			field (str): The field to check for

		Returns:
			bool
		"""
		return field in self._dData

	def __delitem__(self, field):
		"""Delete Item (__delitem__)

		Removes a specific field from a record and flags the entire document
		as needing to be updated

		Args:
			field (str): The field to delete

		Returns:
			None
		"""
		self.d(field);

	def __getitem__(self, field):
		"""Get Item (__getitem__)

		Returns a specific field from the document

		Args:
			field (str): The field to get

		Returns:
			mixed
		"""
		return self.g(field)

	def __setitem__(self, field, value):
		"""Set Item (__setitem__)

		Sets a specific key in the dict

		Args:
			field (str): The field to set
			value (mixed): The value to set to the field

		Returns:
			None
		"""
		self.s(field, value)

	def __str__(self):
		"""Str (__str__)

		Returns a string representation of the document

		Returns:
			str
		"""
		return str(self._dData)

	# revision protected method
	def _revision(self, init=False):
		"""Revision

		Creates or updates the revision number of the instance

		Args:
			init (bool): Initialises the revision value
				Defaults to false, set to True for a new revision value

		Returns:
			bool

		Raises:
			StorageException
		"""

		# If we need a new value
		if init:

			# Make sure the revision itself is not in the value
			if '_rev' in self._dData:
				del self._dData['_rev']

			# Generate and set the revision
			self._dData['_rev'] = '1-%s' % md5(json.dumps(self._dData)).hexdigest()

			# Return OK
			return True

		# Else we are updating the old value
		else:

			# Pull out the old revision
			sOldRev = self._dData['_rev']
			del self._dData['_rev']

			# Split it into version and hash
			sVer, sHash = sOldRev.split('-')

			# Generate the hash part of the revision
			sMD5 = md5(json.dumps(self._dData)).hexdigest()

			# If the MD5s have changed
			if sHash != sMD5:

				# Generate the new revision
				self._dData['_rev'] = '%d-%s' % (int(sVer)+1, sMD5)

				# Add the _rev field as changed if it isn't already
				if self._dChanged != True:
					self._dChanged['_rev'] = True

				# Return OK
				return True

			# Nothing changed
			return False

	def d(self, field):
		"""D (delete)

		Unlike the static delete which is used delete a record by ID, this
		delete removes a field within the document

		Args:
			Args:
			field (str): The name of the field to return, or None for the entire
				document

		Returns:
			self for chaining

		Raise:
			KeyError: field doesn't exist
		"""

		# If the field doesn't exists in the document
		if field not in self._dData:
			raise KeyError(field)

		# Remove the field from the document
		del self._dData[field]

		# Flag the entire document as needing to be updated
		self._dChanged = True

		# Return ok
		return self

	# del method
	def delete(self):
		"""Delete

		Deletes the record represented by the instance

		Returns:
			bool

		Raises:
			StorageException
		"""

		# If the instance lacks a primary key
		if self.__dInfo['conf']['primary'] not in self._dData:
			raise StorageException('Can not delete document with no primary key')

		# Fetch the DB connection
		with connect_with(self.__dInfo['server']) as oCon:

			# Try to delete the record by its primary key
			dRes = r \
				.db(self.__dInfo['db']) \
				.table(self.__dInfo['tree']._name) \
				.get(self._dData[self.__dInfo['conf']['primary']]) \
				.delete() \
				.run(oCon)

			# If there was an error
			if dRes['deleted'] != 1:
				return False

			# Remove the ID
			del self._dData[self.__dInfo['conf']['primary']]

		# Return ok
		return True

	# delete get method
	@classmethod
	def deleteGet(cls, _id, index=None, db={}):
		"""Delete Get

		Deletes one or many documents by ID or by index

		Args:
			_id (mixed|mixed[]): The ID or IDs to delete, None for all documents
			index (str): If set, used as the index to search instead of the
				primary key
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Return:
			uint: the number of records deleted

		Raises:
			StorageException
		"""

		# Get the config values associated with the Tree
		dInfo = cls.info(db)

		# If there is an index passed
		if index:

			# If the index doesn't exist
			if index not in dInfo['conf']['indexes']:
				raise StorageException('no index', index, 'tree')

		# Get a connection to the server
		with connect_with(dInfo['server']) as oCon:

			# Create a cursor for all records
			oCur = r \
				.db(dInfo['db']) \
				.table(dInfo['tree']._name) \

			# If all records must be returned, we don't need to modify the
			#	cursor any further
			if _id == None:
				pass

			# Else, if there's an index
			elif index:

				# Continue to filter using get all
				oCur = oCur.get_all(_id, index=index)

			# Else, we are dealing with the primary key
			else:

				# If we got multiple IDs
				if isinstance(_id, (tuple,list)):

					# Continue to filter using get all
					oCur = oCur.get_all(_id)

				# Else we want one record
				else:

					# Filter to a single ID
					oCur = oCur.get(_id)

			# Run the delete
			dRes = oCur.delete().run(oCon)

			# Return the number of documents deleted
			return dRes['deleted']

	# exists static method
	@classmethod
	def exists(cls, _id, index=None, db={}):
		"""Exists

		Checkes if the specifed document exists. Set an index to check for
		something other than the primary key. To check if a record exists with
		any non-indexed fields, use filter() instead

		Args:
			_id (mixed): The value or values to check
				In the case of a primary key this is always a single value, but
				for complex indexes a tuple may be passed
			index (str): If set, used as the index to search instead of the
				primary key
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			bool

		Raises:
			StorageException
		"""

		# Get the primary key, it's the only thing we need
		dInfo = cls.info(db)

		# Use get to save repeating ourselves
		if not cls.get(_id, index=index, raw=[dInfo['conf']['primary']], db=db):

			# If absolutely nothing was found, return failure
			return False

		# If one or more primary keys were returned, return success
		return True

	# filter static method
	@classmethod
	def filter(cls, obj, raw=None, orderby=None, db={}):
		"""Filter

		Finds records based on the specific fields and values passed in the obj

		Args:
			obj (dict): A dictionary of field names to the values they should
				match
			raw (bool|list): If set to true, raw dicts will be returned instead
				of Document instances. If set to a list or tuple, only those
				fields listed will be returned
			orderby (str|str[]): The field(s) to order the result by
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			Table[]|dict[]

		Raises:
			StorageException
		"""

		# Get the info
		dInfo = cls.info(db)

		# Clean the object
		obj = dInfo['tree'].clean(obj)

		# Fetch the DB connection
		with connect_with(sServer) as oCon:

			# Generate the request
			oCur = r \
				.db(dInfo['db']) \
				.table(dInfo['tree']._name) \
				.filter(obj)

			# If a raw request was done with specific fields
			if isinstance(raw, (tuple,list)):
				oCur = oCur.pluck(*raw).default(None)

			# If an order by list was sent
			if isinstance(orderby, (tuple,list)):
				oCur = oCur.order_by(*orderby)
			# Else if an order field was sent
			elif isinstance(orderby, basestring):
				oCur = oCur.order_by(orderby)

			# Run the request
			itRes = oCur.run(oCon)

			# If there's no data
			if not itRes:
				return []

			# If Raw requested, return as is
			if raw:
				return [d for d in itRes]

			# Else create instances for each
			else:
				return [cls(d, db) for d in itRes]

	# g method
	def g(self, field=None, default=None):
		"""G (get)

		Unlike the static get which is used to fetch by ID, this get returns
		fields within the document

		Args:
			field (str): The name of the field to return, or None for the entire
				document
			default (mixed): The value to return if the field doesn't exist
				defaults to None

		Returns:
			mixed
		"""

		# If got nothing for the field
		if not field:
			return self._dData

		# If the field doesn't exist
		if field not in self._dData:
			return default

		# Else, return the field
		return self._dData[field]

	# generate config static method
	@staticmethod
	def generateConfig(tree):
		"""Generate Config

		Uses a Format-OC tree to generate the base DB config for the Document

		Args:
			tree (FormatOC.Tree): the tree associated with the document

		Returns:
			dict
		"""

		# Merge them with the default values
		dConf = Dict.merge({
			"auto_id": True,
			"server": "default",
			"db": "Test",
			"indexes": {},
			"primary": "_id",
			"revisions": False,
		}, tree.special('rethinkdb', default={}))

		# If there's no name throw an exception
		if not tree._name:
			raise StorageException('Tree must contain a __name__ field to be used by Storage.Document')

		# Return the config
		return dConf

	# get static method
	@classmethod
	def get(cls, _id=None, index=None, filter=None, contains=None, raw=None, orderby=None, limit=0, db={}):
		"""Get

		Returns one or more records from the table. Send no ID to fetch all
		records in the table. Set an index to look for something other than the
		primary key

		Args:
			_id (str|str[]): The ID(s) to fetch from the table
			index (str): If set, used as the index to search instead of the
				primary key
			filter (dict): If set, used as an additional filter to the ID or
				index lookup
			raw (bool|list): If set to true, raw dicts will be returned instead
				of Document instances. If set to a list or tuple, only those
				fields listed will be returned
			orderby (str|str[]): The field(s) to order the result by
			limit (uint): The number of records to return
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			Table|Table[]|dict|dict[]

		Raises:
			StorageException
		"""

		# Assume multiple records
		bMultiple = True

		# Get the info
		dInfo = cls.info(db)

		# If there is an index passed
		if index:

			# If the index doesn't exist
			if index not in dInfo['conf']['indexes']:
				raise StorageException('no index', index, 'tree')

		# Get a connection to the server
		with connect_with(dInfo['server']) as oCon:

			# Create a cursor for all records
			oCur = r \
				.db(dInfo['db']) \
				.table(dInfo['tree']._name) \

			# If all records must be returned, we don't need to modify the
			#	cursor any further
			if _id == None:
				pass

			# Else, if there's an index
			elif index:

				# If it's a tuple
				if isinstance(_id, tuple):

					# Check if one of the values is None
					iNone = -1
					for i in range(len(_id)):

						# If a value is None
						if _id[i] is None:

							# If we already have an index
							if iNone != -1:
								raise StorageException('can\'t list more than one None in an index tuple')

							# Store the index
							iNone = i

					# If we have one
					if iNone > -1:

						# Copy the tuples
						idMax = list(_id)
						idMin = list(_id)

						# Change the None accordingly
						idMax[iNone] = r.maxval
						idMin[iNone] = r.minval

						# Call between instead of get_all
						oCur = oCur.between(idMin, idMax, index=index)

					# Else we have no Nones, pass it through
					else:
						oCur = oCur.get_all(_id, index=index)

				# Else if it's a list
				elif isinstance(_id, list):
					oCur = oCur.get_all(r.args(_id), index=index)

				# Else just pass it through
				else:
					oCur = oCur.get_all(_id, index=index)

			# Else, we are dealing with the primary key
			else:

				# If we got multiple IDs
				if isinstance(_id, (tuple,list)):

					# Continue to filter using get all
					oCur = oCur.get_all(*_id)

				# Else we want one record
				else:

					# Turn off the multiple flag
					bMultiple = False

					# Filter to a single ID
					oCur = oCur.get(_id)

			# If an additional filter was passed
			if filter:
				oCur = oCur.filter(filter)

			# If there's a contains
			if contains:

				# If we don't have a list
				if not isinstance(contains[1], (tuple,list)):
					contains = [contains[0], [contains[1]]]

				# Add the contains filter
				oCur = oCur.filter(
					lambda obj: obj[contains[0]].contains(*contains[1])
				)

			# If there's a limit
			if limit > 0:
				oCur = oCur.limit(limit)

			# If a raw request was done with specific fields
			if isinstance(raw, (tuple,list)):
				oCur = oCur.pluck(*raw).default(None)

			# If an order by list was sent
			if isinstance(orderby, (tuple,list)):
				oCur = oCur.order_by(*orderby)
			# Else if an order field was sent
			elif isinstance(orderby, basestring):
				oCur = oCur.order_by(orderby)

			try:
				# Run the request
				itRes = oCur.run(oCon)

			except r.errors.ReqlOpFailedError as e:

				# The index doesn't exist
				if e.args[0][:5] == 'Index':
					raise StorageException('no index', index, 'table')

				# Else, re-raise
				raise e

			# If we are expecting a single record
			if limit == 1:

				# Try to get one row
				try:
					dRow = itRes.next()
				except r.net.DefaultCursorEmpty as e:
					return None

				# If it's raw, don't instantiate it
				return (raw and dRow or cls(dRow, db))

			# If there's no data
			if not itRes:
				if bMultiple:
					if limit == 1: return None
					else: return []
				else: return None

			# If multiple records are expected
			if bMultiple:

				# If Raw requested, return as is
				if raw:
					return [d for d in itRes]

				# Else create instances for each
				else:
					return [cls(d, db) for d in itRes]

			# Else, one record requested
			else:
				return raw and itRes or cls(itRes, db)

	# info method
	@classmethod
	def info(cls, db={}):
		"""Info

		Returns table and db info for the given Document

		Args:
			db (dict): Optional 'postfix' and 'server' values for the Document

		Returns:
			dict
		"""

		# Get the config values associated with the Tree
		dStruct = cls.struct()

		# Init the return value
		dRet = {
			"tree": dStruct['tree'],
			"conf": dStruct['conf'],
			"server": dStruct['conf']['server'],
			"db": dStruct['conf']['db']
		}

		# If there's a server name passed
		if 'server' in db:
			dRet['server'] = db['server']

		# If there's a postfix passed
		if 'postfix' in db:
			dRet['db'] = "%s_%s" % (dRet['db'], db['postfix'])

		# Get the prefix and prefix it if there is one
		sPrefix = globalPrefix()
		if sPrefix:
			dRet['db'] = sPrefix + dRet['db']

		# Return the structure
		return dRet

	# insert method
	def insert(self, conflict='error'):
		"""Insert

		Inserts the current instance's data as a new record in the table and
		returns the ID regardless if it was given or generated

		Args:
			conflict (str): Must be one of 'error', 'replace', or 'update'

		Returns:
			mixed: None if no record was created, replaced, or updated
		"""

		# Clean conflict
		if conflict not in ('error', 'replace', 'update'):
			conflict = 'error'

		# If revisions are turned on, generate a new value
		if self.__dInfo['conf']['revisions']:
			self._revision(True)

		# Get a connection to the server
		with connect_with(self.__dInfo['server']) as oCon:

			# Create a new document
			dRes = r \
				.db(self.__dInfo['db']) \
				.table(self.__dInfo['tree']._name) \
				.insert(self._dData, conflict=conflict) \
				.run(oCon)

		# If there was an error
		if dRes['inserted'] != 1 and dRes['replaced'] != 1:
			return None

		# Store the ID if necessary
		if self.__dInfo['conf']['auto_id']:
			self._dData[self.__dInfo['conf']['primary']] = dRes['generated_keys'][0]

		# Return the ID
		return self._dData[self.__dInfo['conf']['primary']]

	# s method
	def s(self, field, value):
		"""S (set)

		Sets a field in the document

		Args:
			field (str): The name of the field to set
			value (mixed): The value to set the field to

		Returns:
			self for chaining

		Raise:
			KeyError: field doesn't exist
			ValueError: value is not valid for the field
		"""

		# Get the config values associated with the Tree
		dStruct = self.struct()

		# If the field doesn't exist in the tree
		if field not in dStruct['tree']:
			raise KeyError(field)

		# If the value isn't valid
		if not dStruct['tree'][field].valid(value):
			raise ValueError(field)

		# Store the value and update the changes
		mClean = dStruct['tree'][field].clean(value)
		self._dData[field] = mClean
		if isinstance(self._dChanged, dict):
			self._dChanged[field] = mClean

		# Return ok
		return self

	# tableCreate static method
	@classmethod
	def tableCreate(cls, db={}):
		"""Table Create

		Creates the table using the data from the Tree on the given DB and
		server

		Args:
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			bool

		Raises
			StorageException
		"""

		# Get the info
		dInfo = cls.info(db)

		# Get a connection to the server
		with connect_with(dInfo['server']) as oCon:

			try:

				# Try to create the table
				dRes = r \
					.db(dInfo['db']) \
					.table_create(dInfo['tree']._name, primary_key=dInfo['conf']['primary']) \
					.run(oCon)

				# If the table wasn't created
				if 'tables_created' not in dRes or not dRes['tables_created']:
					return False

				# If there are indexes
				if dInfo['conf']['indexes']:

					# Go through each one and split it into name and fields
					for sIndex,mFields in dInfo['conf']['indexes'].iteritems():

						# Create the cursor up to the table
						oCur = r \
							.db(dInfo['db']) \
							.table(dInfo['tree']._name)

						# If there's no field, the name is the field
						if not mFields:

							# Create the index
							oCur.index_create(sIndex).run(oCon)

						# Else if it's a string
						elif isinstance(mFields, basestring):

							# Create the index
							oCur.index_create(sIndex, r.row[mFields]).run(oCon)

						# Else if it's a list
						elif isinstance(mFields, (tuple,list)):

							# Generate the list of fields
							lFields = []
							for sField in mFields:
								lFields.append(r.row[sField])

							# Create the index
							dRes = oCur.index_create(sIndex, lFields).run(oCon)

						# Else, wtf?
						else:
							raise StorageException("Unknown index format: %s" % str(mFields))

			# If there's already a table with that name
			except r.errors.RqlRuntimeError as e:
				print_error(str(e))
				return False

		# Return OK
		return True

	# tableDelete static method
	@classmethod
	def tableDelete(cls, db={}):
		"""Table Delete

		Deletes the table from the given DB and server

		Args:
			db (dict): Optional DB info
				'server' for the name of the host info passed to server()
				'postfix' for the postfix added to the DB name

		Returns:
			bool

		Raises
			StorageException
		"""

		# Get the info
		dInfo = cls.info(db)

		# Get a connection to the server
		with connect_with(dInfo['server']) as oCon:

			try:

				# Try to drop the table
				dRes = r \
					.db(dInfo['db']) \
					.table_drop(dInfo['tree']._name) \
					.run(oCon)

				# If the table wasn't dropped
				if 'tables_dropped' not in dRes or not dRes['tables_dropped']:
					return False

			# If the table didn't exist
			except r.errors.RqlRuntimeError:
				return False

		# Return ok
		return True

	# tree abstract static method
	@classmethod
	def struct(cls):
		"""Structure

		Returns the FormatOC Tree associated with the table

		Returns:
			FormatOC.Tree

		Raises:
			StorageException
		"""
		raise StorageException('child did not implement tree()')

	# update method
	def update(self, replace=False):
		"""Update

		Updates the record using the ID stored and only the fields that have
		been changed since it was last inserted/updated/replaced

		Args:
			replace (bool):

		Returns:
			bool|str: False if there was nothing to update, True on success, or
				the new revision value on success of a revisionable document

		Raises:
			StorageException
		"""

		# If nothing has changed
		if not self._dChanged:
			return False

		# If the instance lacks a primary key
		if self.__dInfo['conf']['primary'] not in self._dData:
			raise StorageException('Can not update document with no primary key')

		# Create a cursor to update the existing document
		oCur = r \
			.db(self.__dInfo['db']) \
			.table(self.__dInfo['tree']._name) \
			.get(self._dData[self.__dInfo['conf']['primary']])

		# If we are replacing
		if replace or (isinstance(self._dChanged, bool) and self._dChanged):
			oCur = oCur.replace(self._dData)

		# Else we are updating
		else:
			oCur = oCur.update(self._dChanged)

		# If revisions are turned on, generate an updated value
		if self.__dInfo['conf']['revisions']:

			# Store the old revision
			sRev = self._dData['_rev']

			# If updating the revision results in no changes
			if not self.revision():
				return False

			# Get a connection to the server
			with connect_with(self.__dInfo['server']) as oCon:

				# Find the document via the ID
				dDoc = r \
					.db(self.__dInfo['db']) \
					.table(self.__dInfo['tree']._name) \
					.get(self._dData[self.__dInfo['conf']['primary']]) \
					.pluck(['_rev']) \
					.default(None) \
					.run(oCon)

				# If the document is not found
				if not dDoc:
					return False

				# If it is found, but the revisions don't match up
				if dDoc['_rev'] != sRev:
					raise StorageException("Document can not be updated because it is out of sync with the DB")

				# Update the document
				dRes = oCur.run(oCon)

		else:

			# Get a connection to the server
			with connect_with(self.__dInfo['server']) as oCon:

				# Update the document
				dRes = oCur.run(oCon)

		# If there was an error
		if dRes['replaced'] != 1:
			return False

		# Clear the changed fields
		self._dChanged = {}

		# Return OK
		return True
