# coding=utf8
""" Strings Module

Holds several methods for validating and manipulating strings
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-06-14"

# Import python modules
from random import randint

# The sets available for the random function
_mdRandomSets	= {
	"0x":	"0123456789abcdef",
	"0":	"01234567",
	"10":	"0123456789",
	"az":	"abcdefghijklmnopqrstuvwxyz",
	"az*":	"abcdefghijkmnopqrstuvwxyz",
	"AZ":	"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"AZ*":	"ABCDEFGHJKLMNPQRSTUVWXYZ",
	"aZ":	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
	"aZ*":	"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ",
	"!":	"!@#$%^&*-_+.?"
}

# random function
def random(length = 8, sets='_az', duplicates=True):
	"""Random

	Generates a random string. By default this function will generate an 8
	character string using lowercase letters with possible repeating characters

	Args:
		length (uint): Requested length of the password
		sets (str|str[]): A list of names from the standard sets, a string
			starting with an underscore representing one named set, or any other
			string to be used as an array of characters to chose from. If you
			want certain characters to have a greater chance of appearing, use
			them more times, e.g. twice the 'A's, "AABC", or three times the
			'B's, "ABBBC". Make sure not to turn off duplicates for this to be
			effective
		duplicates (bool): Defaults to True, allowing characters to be used more
			than once

	Sets:
		0x:		0123456789abcdef
		0:		01234567
		10:		0123456789
		az:		abcdefghijklmnopqrstuvwxyz
		az*:	abcdefghijkmnopqrstuvwxyz
		AZ:		ABCDEFGHIJKLMNOPQRSTUVWXYZ
		AZ*:	ABCDEFGHJKLMNPQRSTUVWXYZ
		aZ:		abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
		aZ*:	abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ
		!:		!@#$%^&*-_+.?

	Examples:
		> random(8, '_0x')
		"

	Returns:
		str

	Raises:
		ValueError
	"""

	# If the sets are a list
	if isinstance(sets, list):

		# If there is no count
		if not sets:
			raise ValueError('sets must contain at least one set name')

		# Init the string to be used as the allowed characters
		sChars	= ''

		# Go through the list
		for s in sets:

			# If the set doesn't exist
			if s not in _mdRandomSets:
				raise ValueError('%s is not a valid set' % s)

			# Else, add it to the allowed characters
			sChars	+= _mdRandomSets[s]

	# Else if we have a string
	elif isinstance(sets, basestring):

		# If it starts with an underscore
		if sets[0] == '_':

			# If the set doesn't exist
			if sets[1:] not in _mdRandomSets:
				raise ValueError('%s is not a valid set' % sets[1:])

			# Else, set it to the allowed characters
			sChars	= _mdRandomSets[sets[1:]]

		# Else, use the string as is
		else:
			sChars	= sets

	else:
		raise ValueError('sets is not a valid value: "%s"' % str(sets))

	# Init the return variable
	sText	= '';

	# Count the number of characters we can use
	iCount	= len(sChars)

	# Create a [length] of random character
	i	= 0
	while i < length:
		sFound		= sChars[randint(0, iCount - 1)]
		bDup		= sText.find(sFound)

		if duplicates or bDup == -1:
			sText	+= sFound
			i		+= 1

	# Return the generated string
	return sText
