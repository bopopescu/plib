# coding=utf8
""" WebServer Module

Extends Bottle to simplify connecting web requests to service requests
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__version__		= "1.0.0"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-14"

# Import python modules
import json

# Import pip modules
from bottle import BaseRequest, Bottle, request as breq, response as bres, run as brun
from enum import IntEnum

# Import local modules
from .Services import create, delete, read, update, ResultError
from .Session import Session

# Method bitwise enum
class M(IntEnum):
	C		= 0x1
	CREATE	= 0x1
	D		= 0x2
	DELETE	= 0x2
	U		= 0x8
	UPDATE	= 0x8
	R		= 0x4
	READ	= 0x4
	ALL		= 0xF
	CD		= 0x3

# WebRoute class
class WebRoute(object):
	"""WebRoute

	Holds data relevent to a specific route. Is callable so it can be used as
	a callback to bottle.route()

	Extends:
		object
	"""

	# constructor
	def __init__(self, service, noun, session):
		"""Constructor (__init__)

		Constructor, generates a route object

		Args:
			service (string): The name of the service to route to
			noun (string): The noun part of the service request
			session (bool): If True, a session Authorization value must be found

		Returns:
			None
		"""

		self.service	= service
		self.noun		= noun
		self.session	= session

	# call
	def __call__(self):
		"""Call (__call__)

		Used to allow the instance to be called like a callback

		Args:
			None

		Returns:
			str
		"""

		# Init the data in case nothing is passed
		dData	= {}

		# Set response headers
		bres.headers['Content-Type']	= 'application/json; charset=UTF-8'

		# If the method is GET
		if breq.method == 'GET' and 'json' in breq.query:

			# Store the JSON from the arg
			try:
				dData	= json.loads(breq.query['json'])

			except Exception as e:
				return str(ResultError((1, '%s\n%s' % (breq.query['json'], str(e)))))

		# Else check the body and the headers for JSON
		else:

			# Check request headers
			try:
				if breq.headers['Content-Type'].lower() not in ('application/json; charset=utf-8', 'application/json; charset=utf8'):
					return str(ResultError(27))
			except KeyError:
				return str(ResultError(27))

			# Get the body
			try:
				sBody = breq.body.getvalue()
			except AttributeError as e:
				sBody = req.body.read()

			# Check JSON is valid while storing it
			try:
				dData = json.loads(sBody)
			except Exception as e:
				return str(ResultError((1,'%s\n%s' % (sBody, str(e)))))

			# Check JSON object isn't empty
			if dData == None:
				return str(ResultError((1, 'empty')))

		# If a session is required
		if self.session:

			# Check for an existing authorization token
			if 'Authorization' not in breq.headers:
				bres.status	= 401
				return str(ResultError(8))

			# Get the session from the Authorization token
			oSession	= Session.start(breq.headers['Authorization'])

			# If the session is not found
			if not oSession:
				bres.status	= 401
				return str(ResultError(101))

		else:
			oSession	= None

		# Call the appropriate API method based on the HTTP/request method
		if breq.method == 'DELETE':
			oResult	= delete(self.service, self.noun, dData, oSession)
		elif breq.method == 'GET':
			oResult	= read(self.service, self.noun, dData, oSession)
		elif breq.method == 'POST':
			oResult	= create(self.service, self.noun, dData, oSession)
		elif breq.method == 'PUT':
			oResult	= update(self.service, self.noun, dData, oSession)

		# Return the result as a string
		return str(oResult)

# WebServer class
class WebServer(Bottle):
	"""WebServer

	Extends Bottle to add the ability to connect HTTP web requests to API
	service requests

	Extends:
		Bottle
	"""

	# init method
	def __init__(self, requests):
		"""Constructor (__init__)

		Constructor, generates the list of routes to bottle based on the
		requests dict

		Args:
			requests (dict): A list of routes mapped to methods and flags

		Returns:
			None
		"""

		# Call the parent constructor first so the object is setup
		super(WebServer, self).__init__()

		# Go through each request
		for k,v in requests.iteritems():

			# Generate the list of methods from the bitwise method value
			lMethods	= []
			if v['methods'] & M.C:	lMethods.append('POST')
			if v['methods'] & M.D:	lMethods.append('DELETE')
			if v['methods'] & M.R:	lMethods.append('GET')
			if v['methods'] & M.U:	lMethods.append('PUT')

			# If the session value is not passed
			if 'session' not in v:
				v['session']	= True

			# Create a new WebRoute
			route	= WebRoute(v['service'], k[1:], v['session'])

			# Add a new route
			self.route(k, lMethods, route)

	# api route method
	def api_route(self, uri, methods, service, session=True):
		"""API Route

		Adds a new route to a request in an API service

		Args:
			uri (str): The URI to capture
			methods (uint): The methods to associate with the URI
			service (str): The name of the service to route the requests to
			session (bool): False to bypass authorization check

		Returns:
			None
		"""

		# Generate the list of methods from the bitwise method value
		lMethods	= []
		if methods & M.C:	lMethods.append('POST')
		if methods & M.D:	lMethods.append('DELETE')
		if methods & M.R:	lMethods.append('GET')
		if methods & M.U:	lMethods.append('PUT')

		# Create a new WebRoute
		route	= WebRoute(service, uri[1:], session)

		# Add a new route
		self.route(uri, lMethods, route)

	# run method
	def run(self,	server="gunicorn", host="127.0.0.1", port=8080,
					reloader=False, interval=1, quiet=False, plugins=None,
					debug=None, maxfile=20971520, **kargs):
		"""Run

		Calls the Bottle run method to start the server using this instance

		Args:
			server (str) Server adapter to use
			host (str) Server address to bind to
			port (uint) Server port to bind to
			reloader (bool) Start auto-reloading server?
			interval (uint) Auto-reloader interval in seconds
			quiet (bool) Suppress output to stdout and stderr?
			options - Options passed to the server adapter.

		Returns:
			None
		"""

		# Set the max file size
		BaseRequest.MEMFILE_MAX = maxfile

		# Call run
		brun(
			app=self, server=server, host=host, port=port, reloader=reloader,
			interval=interval, quiet=quiet, plugins=plugins, debug=debug,
			**kargs
		)
