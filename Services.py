# coding=utf8
""" Services Module

Holds all the necessary classes needed to either make a new service, or
communicate with existing ones
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-14"

# Import python modules
from hashlib import sha1
import json
from time import time
from urllib import quote_plus as urlencode

# Import pip modules
import requests

# Import local modules
from .Session import Session

# Module variables
_mbVerbose		= False
_mdRegistered	= {}

# Maps
__funcToRequest	= {
	'create':	requests.post,
	'delete':	requests.delete,
	'read':		requests.get,
	'update':	requests.put
}

# Request function
def __request(name, action, uri, data, session=None):
	"""Request

	Can only be used internally, sends a properly formatted request to another
	service whether local or remote

	Args:
		name (str): The name of the service we are creating on
		action (str): The name of the action to take on the uri
		uri (str): The uri representing the object to create
		data (mixed): The data to send with the request
		session (Session): Optional Session associated with the request

	Return:
		Result

	Raises:
		ServiceException
	"""

	# If verbose mode is on
	if _mbVerbose:	print('Calling API.%s("%s", "%s", %s)' % (str(action), str(name), str(uri), str(data)))

	# If we have a registered service
	if name in _mdRegistered:

		# If the session is not set
		if not session:
			session	= Session.create()

		# If we have an instance of the service
		if 'instance' in _mdRegistered[name]:

			# Call the child action method, create, delete, read, or update, and
			#	store the result
			oResult	= getattr(_mdRegistered[name]['instance'], action)(session, uri, data)

		# Else it's remote
		else:

			# Generate the full URL from the service and uri
			sURL	= _mdRegistered[name]['url'] + uri

			# Convert the data to JSON
			sData	= json.dumps(data)

			# Set the body to the data
			sBody	= sData

			# Create the headers
			dHeaders	= {
				'Content-Type':		'application/json; charset=utf-8',
				'Content-Length':	str(len(sBody))
			}

			# If we have a session token add it to the headers
			if session:
				dHeaders['Authorization']	= session['token']

			# Try to make the request and store the response
			try:
				if action in __funcToRequest:
					oResult	= __funcToRequest[action](sURL, data=sBody, headers=dHeaders)
				# Else the method is invalid
				else:
					return ResultError((6, action))

				# If the response is anything other than 200
				if oResult.status_code != 200:

					# Fail
					return ResultError((5, 'Received status code %d: %s' % (oResult.status_code, oResult.content)))

				# If the content type is wrong
				if oResult.headers['Content-Type'].lower() != 'application/json; charset=utf-8':
					return ResultError((5, 'Invalid Content-Type: %s' % oResult.headers['content-type']))

			# If there's a connection error
			except requests.ConnectionError,e:
				return ResultError((5, str(e)))

			# Else decode the content as JSON and return it
			oResult = Result.fromString(json.loads(oResult.text))

		# If verbose mode is on
		if _mbVerbose:	print('Returning %s\n' % str(oResult))

		# Return the result
		return oResult

	# Else, no such service
	else:

		# If we don't have the service
		raise ServiceException(5, name)

# Create function
def create(name, uri, data, session=None):
	"""Create

	Make a POST request to an API service

	Args:
		name (str): The name of the service we are creating on
		uri (str): The uri representing the object to create
		data (mixed): The data to send with the request
		session (Session): Optional Session associated with the request

	Return:
		Result
	"""
	return __request(name, 'create', uri, data, session)

# Delete function
def delete(name, uri, data, session=None):
	"""Delete

	Make a DELETE request to an API service

	Args:
		name (str): The name of the service we are deleting from
		uri (str): The uri representing the object to delete
		data (mixed): The data to send with the request
		session (Session): Optional Session associated with the request

	Return:
		Result
	"""
	return __request(name, 'delete', uri, data, session)

# Read function
def read(name, uri, data, session=None):
	"""Read

	Make a GET request to an API service

	Args:
		name (str): The name of the service we are reading from
		uri (str): The uri representing the object to read
		data (mixed): The data to send with the request
		session (Session): Optional Session associated with the request

	Return:
		Result
	"""
	return __request(name, 'read', uri, data, session)

# Update function
def update(name, uri, data, session=None):
	"""Update

	Make a PUT request to an API service

	Args:
		name (str): The name of the service we are update
		uri (str): The uri representing the object to update
		data (mixed): The data to send with the request
		session (Session): Optional Session associated with the request

	Return:
		Result
	"""
	return __request(name, 'update', uri, data, session)

# Register function
def register(services, pathinfo=None):
	"""Register

	Takes a list of services (keys) and the date (values) needed to access them,
	be it by local instance, or remote URL

	Args:
		services (dict): A list of services and how to contact them
		pathinfo (REST.PathInfo): A object used to find services with no
			instance

	Raises:
		ValueError: If services is not a dictionary, or any values in it are
			invalid

	Returns:
		None
	"""

	# If we didn't get a dictionary
	if not isinstance(services, dict):
		raise ValueError(services)

	# Go through each service
	for k,v in services.iteritems():

		# If verbose mode is on
		if _mbVerbose:	print('Registering service: %s: ' % str(k), end='')

		# If we got a Service instance
		if isinstance(v, Service):

			# Store it
			_mdRegistered[k]	= {"instance":v}

			# If verbose mode is on
			if _mbVerbose:	print('local')

			# Initialize the service
			v.initialize()

		# Else it must be a url we need to generate
		elif v is None:

			# Make sure we have the service
			if k not in pathinfo:
				raise ValueError('services.%s' % k)

			# Store it
			_mdRegistered[k]	= {"url":pathinfo[k]['url']}

			# If verbose mode is on
			if _mbVerbose:	print('%s' % _mdRegistered[k]['url'])

		# Else, the value is invalid
		else:
			raise ValueError('services.%s' % str(k))

# Verbose function
def verbose(flag=True):
	"""Verbose

	Puts the API in verbose mode which will cause every request and its result
	to be echoed to the screen

	Args:
		flag (bool) Optional, defaults to True, must be set to False to turn off
			verbose mode

	Returns:
		None
	"""

	global _mbVerbose

	if _mbVerbose and not flag:
		print('API verbose mode will be turned off')

	_mbVerbose	= flag

	if _mbVerbose:
		print ('API verbose mode has been turned on')

# Service Exception class
class ServiceException(Exception):
	"""Service Exception

	Used to exceptions that are specific to Services

	Extends: Exception
	"""

	# fromResultError classmethod
	@classmethod
	def fromResult(cls, err):
		"""From Result

		Generates an ServiceException from a Result

		Args:
			err (Result): The error to convert

		Returns:
			ServiceException
		"""

		# Generate the argument list
		lErrs	= [err.error['code']]

		# If there's a message
		if 'msg' in err.error:
			lErrs.append(err.error['msg'])

		# Create and return the exception
		return cls(*lErrs)

# Result class
class Result(object):
	"""Result

	Represents a result from any API request

	Extends: object
	"""

	# Constructor
	def __init__(self, data, error = None):
		"""Constructor

		Called to instantiate a new Result instance

		Args:
			self (Result): A pointer to the current instance
			data (mixed): Data returned on a successful request
			error (mixed): A code and message representing an error that has
				occurred during the request

		Returns:
			Result

		Raises
			ValueError: If the error is not in a valid format
		"""

		# Store the data as is
		self.data	= data

		# Check the error for type
		#	If it's false, there's no error
		if error in (False,None):
			self.error = error

		# Else if it's an int
		elif isinstance(error, int):
			self.error	= {
				"code":	error,
				"msg":	''
			}

		# Else if it's a string
		elif isinstance(error, basestring):
			self.error	= {
				"code":	0,
				"msg":	error
			}

		# Else if it's a tuple
		elif isinstance(error, tuple):
			self.error	= {
				"code": error[0],
				"msg":	error[1]
			}

		# Else if it's a dictionary
		elif isinstance(error, dict):
			self.error	= error

		# Else if it's an Exception
		elif isinstance(error, Exception):

			# If the exception's first argument is itself an Result instance
			if isinstance(error.args[0], Result):
				dErr	= error.args[0].error

			# Else pull out the code and possibly the message
			else:
				dErr	= {"code": error.args[0]}
				if len(error.args) > 1:
					dErr['msg']	= error.args[1]

		# Else the error is invalid
		else:
			raise ValueError('error')

	# __str__ method
	def __str__(self):
		"""str

		Python magic method that's called when the instance is passed to a str
		class

		Args:
			self (Result): A pointer to the current instance

		Returns:
			str: The instance represented as JSON
		"""

		# Store the error and data in a dict and convert it to JSON
		return json.dumps({
			'error':	self.error,
			'data':		self.data
		})

	# From String method
	@classmethod
	def fromString(cls, json_val):
		"""From String

		Converts the results of __str__ back into a Result instance and returns
		it

		Args:
			cls (object): The class associated with this method call
			json_val (str): A string which must hold valid JSON

		Returns:
			Result

		Raises:
			ServiceException: If some part of the JSON is invalid or missing
				parameters
		"""

		# If we don't already have an array, assume the string is JSON and
		#	convert it to a dict
		if not isinstance(json_val, dict):
			json_val	= json.loads(json_val)

		# If we are missing either of the parts
		if 'error' not in json_val or 'data' not in json_val:
			raise ServiceException({"code": 6, "msg": str(json_val)})

		# Create a new ApiResult instance and return it
		return cls(json_val['data'], json_val['error'])

	# Has Data method
	def hasData(self):
		"""Has Data

		Returns true if the Result has data

		Args:
			self (Result): A pointer to the current instance

		Returns:
			bool
		"""
		return self.data != None

	# Has Error method
	def hasError(self):
		"""Has Error

		Returns true if the Result has an error

		Args:
			self (Result): A pointer to the current instance

		Returns:
			bool
		"""
		return self.error not in (False,None)

# Result Error class
class ResultError(Result):
	"""Result Error

	Shorthand for returning a Result instance with only an error

	Extends: Result
	"""

	# Constructor
	def __init__(self, error):
		"""Constructor

		Sets just the error part of the Result

		Args:
			self (ResultError): A pointer to the current instance
			error (mixed): Error returned on unsuccessful request

		Returns:
			ResultError
		"""
		super(ResultError,self).__init__(None, error)

# Service class
class Service(object):
	"""Service

	The base class used to create new API services

	Extends: object
	"""

	# Salt
	_sSalt	= 'hTQv6Ho7*us8#mEq'

	# checkInternalKey classmethod
	@classmethod
	def checkInternalKey(cls, key):
		"""Check Internal Key

		Check that the internal communication key is valid and no one is trying
		to hack the system

		Args:
			key (str): The key to validate

		Returns:
			bool
		"""

		try:

			# Split the key by timestamp and sha1
			sTimestamp, sSHA1	= key.split(',')

			# Split the timestamp in half and add the halfs to the salt
			sKey	= sTimestamp[5:] + cls._sSalt + sTimestamp[:5]

			# If the sha1 of the key matches the sha1 we received
			return sha1(sKey).hexdigest() == sSHA1

		except Exception:
			return False

	# create method
	def create(self, session, uri, data):
		"""Create

		Represents an http POST request which should create one or many new
		objects

		Args:
			self (Service): A pointer to the current instance
			session (Session): The requests associated session
			uri (str): The URI including the noun and and any other actions associated with the POST request
			data (mixed): The data (JSON) sent with the request

		Return:
			Result

		Raises:
			NotImplementedError: If the child did not override the method and it
				is called
		"""
		raise NotImplementedError('Child did not implement the "create" method.')

	# delete method
	def delete(self, session, uri, data):
		"""Delete

		Represents an http DELETE request which should delete one or many
		existing objects

		Args:
			self (Service): A pointer to the current instance
			session (Session): The requests associated session
			uri (str): The URI including the noun and and any other actions associated with the DELETE request
			data (mixed): The data (JSON) sent with the request

		Return:
			Result

		Raises:
			NotImplementedError: If the child did not override the method and it
				is called
		"""
		raise NotImplementedError('Child did not implement the "delete" method.')

	# generateInternalKey classmethod
	@classmethod
	def generateInternalKey(cls):
		"""Generate Internal Key

		Generates a key that can be used to verify internal communications

		Returns:
			str
		"""

		# Get the time
		sTime	= str(int(time()))

		# Using the salt and the time, generate a sha1
		sSHA1	= sha1(sTime[5:] + cls._sSalt + sTime[:5]).hexdigest()

		# Put the timestamp and the sha1 together to return a key
		return sTime + ',' + sSHA1

	# initialize static method
	@classmethod
	def initialize(cls):
		"""Initialize

		Called after everything has been loaded so that the Service can set
		itself up if needed

		Returns:
			None
		"""
		pass

	# install static method
	@classmethod
	def install(cls):
		"""Install

		Used to install any configuration or storage when the service is first
		installed on any system

		Returns:
			bool

		Raises:
			NotImplementedError: If the child did not override the method and it
				is called
		"""
		raise NotImplementedError('Child did not implement the "install" method.')

	# ping static method
	@staticmethod
	def ping(data):
		"""Ping

		Simple method to validate a service can be accessed and is performing as
		expected

		Args:
			data (str): A simple string of any length which will be returned in
				reverse

		Returns:
			str
		"""
		return data[::-1]

	# read method
	def read(self, session, uri, data = {}):
		"""Read

		Represents an http GET request which should return one or many existing
		objects

		Args:
			self (Service): A pointer to the current instance
			session (Session): The requests associated session
			uri (str): The URI including the noun and and any other actions associated with the GET request
			data (mixed): The data (JSON) sent with the request

		Return:
			Result

		Raises:
			NotImplementedError: If the child did not override the method and it
				is called
		"""
		raise NotImplementedError('Child did not implement the "read" method.')

	# update method
	def update(self, session, uri, data):
		"""Update

		Represents an http PUT request which should update an existing object

		Args:
			self (Service): A pointer to the current instance
			session (Session): The requests associated session
			uri (str): The URI including the noun and and any other actions associated with the PUT request
			data (mixed): The data (JSON) sent with the request

		Return:
			Result

		Raises:
			NotImplementedError: If the child did not override the method and it
				is called
		"""
		raise NotImplementedError('Child did not implement the "update" method.')
