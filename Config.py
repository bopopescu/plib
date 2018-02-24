# coding=utf8
""" Config Module

Holds all the necessary methods for loading JSON config files to make them
globally accessible to all other modules and services
"""
# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-14"

# Import python modules
import json

# Import local modules
import shared.Dictionaries as Dictionaries

# Global vars
_dmValues	= dict()

# get function
def get(key = None, default = None):
	"""Get

	Gets a configuration setting and returns it. If no value is set, default is
	returned. If no key is passed, all current values stored will be returned

	Args:
		key (string|tuple): The key, either separated by colons, or represented
			as a tuple, to get
		default (mixed): The default value to return if the key is not found

	Returns:
		mixed
	"""

	# Import the module variable
	global _dmValues

	# Init local vars
	iCount	= 0;

	# If the key is a string
	if isinstance(key, basestring):

		# Find colons and split the key up if necessary
		if key.find(':'):
			key		= key.split(':')
			iCount	= len(key)
		else:
			key		= (key,)
			iCount	= 1

	# Get the length of the tuple/list
	elif key != None:

		# Get the length of the key
		iCount	= len(key)

	# Return all values
	else:
		return _dmValues


	# Set the first level
	mData	= _dmValues

	# Loop through and find the value
	for i in range(0, iCount):

		# If mData is not a dict
		if not isinstance(mData, dict):
			return default

		# If the key doesn't exist
		if key[i] not in mData:
			return default

		# Set the current level
		mData	= mData[key[i]]

	# Return the value
	return mData

# load function
def load(values, key=None):
	"""Load

	Either resets the current config values with the file passed, or sets them
	to the given key in the current config

	Args:
		values (str|dict): The path to the file, or an existing dictionary, to
			either overwrite or append to the current state
		key (str): Optional key used to append instead of overwriting

	Returns:
		None
	"""

	# Import the module variable
	global _dmValues

	# If we got a string
	if isinstance(values, basestring):

		# Open the file and store the contents
		with open(values) as oF:
			values	= json.load(oF)

	# Load the JSON into a python dictionary under the root or a specific key
	if key == None:	_dmValues		= values
	else:			_dmValues[key]	= values

# merge function
def merge(values):
	"""Merge

	Merges the existing values with the passed ones. Duplicates in the original
	will be overwritten

	Args:
		values (str|dict): The path to the file, or an existing dictionary, to
			merge with the current state

	Returns:
		None
	"""

	# Import the module variable
	global _dmValues

	# If we got a string
	if isinstance(values, basestring):

		# Open the file and store the contents
		with open(values) as oF:
			values	= json.load(oF)

	# Call the worker function
	Dictionaries.merge(_dmValues, values)

# set function
def set(key, value):  # @ReservedAssignment
	"""Set

	Adds or overwrites a specific key in the config

	Args:
		key (string|tuple): The key, either separated by colons, or represented
			as a tuple, to set
		value (mixed): The value to set to the key

	Returns:
		None
	"""

	# Import the module variable
	global _dmValues

	# Init local vars
	iCount	= 0;

	# If the key is a string
	if isinstance(key, basestring):

		# Find colons and split the key up if necessary
		if key.find(':'):
			key		= key.split(':')
			iCount	= len(key)
		else:
			key		= (key,)
			iCount	= 1

	# Get the length of the tuple/list
	else:

		# Get the length of the key
		iCount	= len(key)

	# Get the first level
	mData	= _dmValues

	# Go through each level of the passed key
	for i in range(0, iCount):

		# If we're on the last part of the key
		if i == (iCount - 1):
			mData[key[i]]	= value
		else:
			# If the key doesn't exist
			if key[i] not in mData.keys():
				mData[key[i]]	= dict()

			# Set the data to the current level
			mData	= mData[key[i]]
