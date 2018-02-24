# coding=utf8
""" Dictionaries Module

Holds several methods for validating and manipulating dictionaries
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-14"

# Import python modules
import sys

# Copy function
def copy(orig):
	"""Copy

	Recursively go through a dictionary and copy it, returning the new, non
	referenced, dictionary

	Args:
		orig (dict): The dictionary to copy

	Returns:
		dict
	"""

	# Init the return dictionary
	d = {}

	# Go through each key
	for k in orig:

		# If it's a dictionary
		if isinstance(orig[k], dict):

			# Recurse
			d[k] = copy(orig[k])

		# Else if it's a list
		elif isinstance(orig[k], list):

			# Copy it
			d[k] = orig[k][:]

		# Else just reference it
		else:
			d[k] = orig[k]

# Evaluate function
def evaluate(source, expected):
	"""Evaluate

	Tests a flat dictionary to see if the values in it correspond to the proper
	value types expected

	Args:
		source (dict): The dictionary to test
		expected (dict): The value types to test against the dictionary

	Return: bool
	"""

	# If the source is not a valid dictionary
	if not isinstance(source, dict):
		return False

	# If the expected is not a valid dictionary
	if not isinstance(expected, dict):
		raise ValueError(expected)

	# Go through each expected element
	for i,t in expected.iteritems():

		# If the element doesn't exist in the source, return False
		if i not in source:
			return False

		# If the element exists but the type is wrong, return False
		elif not isinstance(source[i], t):
			return False

	# No failures, return True
	return True

# merge function
def merge(existing, new):
	"""Merge

	Takes two dictionaries and merges them together as a tree, not as a flat
	dictionary. Be careful as even though this returns the modified dictionary,
	it also modifies the existing dictionary

	Args:
		existing (dict): The existing dictionary, it's values will be
			overwritten
		new (dict): The new dictionary, it will overwrite the existing

	Returns:
		dict
	"""

	# If either argument isn't a dictionary
	if not isinstance(existing, dict):	raise ValueError('%s first argument must be a valid dictionary. Received: %s' % (sys._getframe().f_code.co_name, str(existing)))
	if not isinstance(new, dict):		raise ValueError('%s second argument must be a valid dictionary. Received: %s' % (sys._getframe().f_code.co_name, str(new)))

	# Go through each new value
	for i in new.keys():

		# If the key points to another dictionary, and it exists in both
		if isinstance(new[i], dict) and i in existing and isinstance(existing[i], dict):

			# Recurse
			merge(existing[i], new[i])

		# Else the key doesn't point to a dictionary so we store it or overwrite
		#	it
		else:

			# Add the value as is
			existing[i]	= new[i]

	# Return the existing dictionary
	return existing

# Validate function
def validate(source, expected):
	"""Validate

	Recursively checks a dictionary for any missing elements in the tree, if any
	are found they are returned in a list, else None is returned

	Args:
		source (dict): The dictionary to test
		expected (dict): The value types to test against the dictionary

	Return: list | None
	"""

	# Init the return list
	lRet	= []

	# Go through each expected element
	for sElem in expected:

		# If the element is a string
		if isinstance(sElem, basestring):

			# If it doesn't exist or is empty
			if sElem not in source or (isinstance(source[sElem], basestring) and not source[sElem]):
				lRet.append(sElem)

		# Else if the instance is a dictionary
		elif isinstance(sElem, dict):

			# Go through each item in the dictionary
			for k,l in sElem.iteritems():

				# If the item doesn't exist or is empty
				if k not in source or not source[k]:
					lRet.append(k)

				# Else, check the children
				else:

					# Call the method recursively
					lErrs	= validate(source[k], l)

					# If there's any errors, add them to the list
					if lErrs:
						for sErr in lErrs:
							lRet.append(k + '.' + sErr)

		# Invalid use of the function
		else:
			raise ValueError('%s Error: items in expected list must be strings or dicts' % sys._getframe().f_code.co_name)

	# Return the list
	return lRet
