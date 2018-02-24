# coding=utf8
""" REST Module

Holds methods and classes for simplifying REST operations
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-21"

## PathInfo class
class PathInfo(object):
	"""Path INFO

	Holds information on how to access a list of services given the shared
	config

	Extends:
		object
	"""

	# __contains__ method
	def __contains__(self, service):
		"""__contains__

		Returns true if the specific key exists in the session

		Args:
			service (str): The name of the service to check for

		Returns:
			bool
		"""
		return service in self.__services

	# __getitem__ method
	def __getitem__(self, service):
		"""__getitem__

		Returns a specific key from the dict

		Args:
			service (str): The name of the service return

		Returns:
			mixed
		"""
		return self.__services[service].copy()

	# Constructor method
	def __init__(self, services, defaults = {}):
		"""Constructor

		Instantiates the instance

		Args:
			services (dict): The list of services as keys to data
				Each key can point to either a single string representing an
				absolute URL to the service, or a dict with optional elements of
				'protocol', 'domain', 'port', and 'path'
			defaults (dict): The default configuration
				Optional elements include: 'protocol', 'domain', and 'port'. If
				'port' is set it's used as a modifier to the service port
				instead of being overwritten

		Returns:
			PathInfo
		"""

		# If we didn't get a dictionary for the services
		if not isinstance(services, dict):
			raise ValueError('services')

		# If we didn't get a dictionary for the defaults
		if not isinstance(defaults, dict):
			raise ValueError('defaults')

		# Init the port modifier var
		port_modifier	= 0

		# If there's a port in the defaults
		if 'port' in defaults:

			# Check it's an int
			if not isinstance(defaults['port'], (int,long)):
				raise ValueError('default.port must be an int')

			# Pop it
			port_modifier	= defaults.pop('port')

		# Init the list of Services
		self.__services	= {}

		# Go through each service
		for service in services:

			# If it's not a dictionary
			if not isinstance(services[service], dict):
				raise ValueError('services.%s' % service)

			# Copy the defaults to the parts var
			parts	= defaults.copy()

			# Update parts with the service info
			parts.update(services[service])

			# If we have no port
			if 'port' not in parts:

				# But we have a modifier
				if port_modifier:
					parts['port']	= 80 + port_modifier

			else:
				parts['port']	+= port_modifier

			# Get defaults for the missing parts
			if not parts['protocol']:	parts['protocol']	= 'http'
			if not parts['domain']:		parts['domain']		= 'localhost'
			if 'path' not in parts:		parts['path']		= ''
			else:						parts['path']		= '%s/' % str(parts['path'])

			# Store the parts
			self.__services[service]	= parts.copy()

			# Generate and store the URL
			self.__services[service]['url']	= '%s://%s%s/%s' % (
				parts['protocol'],
				parts['domain'],
				'port' in parts and ":%d" % parts['port'] or '',
				parts['path']
			)

			# If there's no port at all, store 80 so that calls to it won't fail
			if 'port' not in self.__services[service]:
				self.__services[service]['port']	= 80

	# __iter__ method
	def __iter__(self):
		"""__iter__

		Return an iterator for the services

		Returns:
			iterator
		"""
		return iter(self.__services)

	# __str__ method
	def __str__(self):
		"""__str__

		Returns a string representation of the services

		Returns:
			str
		"""
		return str(self.__services)

	# services method
	def services(self):
		"""services

		Returns the list of services in the instance

		Returns:
			str[]
		"""
		return self.__services.keys()
