# coding=utf8
"""Resize

Handles common resizing tasks
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2015-12-17"

def regionFromResize(curr_width, curr_height, bounds_width, bounds_height):
	"""Region from Resize

	Returns a new set of region points based on a current width and height and
	the bounding box

	Args:
		curr_width (uint): The current width
		curr_height (uint): The current height
		bounds_width (uint): The boundary width
		bounds_height (uint): The boundary height

	Returns:
		list [x, y, width, height]
	"""

	# Return
	lRet	= [0, 0, 0, 0]

	# If the current width is larger than the bounds width
	if curr_width > bounds_width:

		lRet[0]	= int(round((curr_width - bounds_width) / 2.0))
		lRet[1]	= 0
		lRet[2]	= int(bounds_width + round((curr_width - bounds_width) / 2.0))
		lRet[3]	= bounds_height

	# Else if the current height is larger than the bounds height
	else:

		lRet[0]	= 0
		lRet[1]	= int(round((curr_height - bounds_height) / 2.0))
		lRet[2]	= bounds_width
		lRet[3]	= int(bounds_height + round((curr_height - bounds_height) / 2.0))

	# Return the region
	return lRet

def toCrop(curr_width, curr_height, bounds_width, bounds_height):
	"""To Crop

	Resizes the current dimension so that they completely fill the boundary
	area, cropping off the parts on the longer dimension

	Args:
		curr_width (uint): The current width
		curr_height (uint): The current height
		bounds_width (uint): The boundary width
		bounds_height (uint): The boundary height

	Returns:
		list [w, h]
	"""

	# Return
	lRet	= [0,0]

	# Convert current width/height to floats
	curr_width		= float(curr_width)
	curr_height		= float(curr_height)

	# If either width or height is smaller than the boundary box, resize up
	if curr_width < bounds_width or curr_height < bounds_height:

		# Which is the side that needs to grow more?
		if (bounds_width / curr_width) > (bounds_height / curr_height):
			lRet[0]	= bounds_width
			lRet[1]	= int(round(bounds_width * (curr_height / curr_width)))
		else:
			lRet[1]	= bounds_height
			lRet[0]	= int(round(bounds_height * (curr_width / curr_height)))

	# Else if the image is already larger than the boundary, resize down
	else:

		# Which is the side that needs to shrink less?
		if (curr_width / bounds_width) > (curr_height / bounds_height):
			lRet[1]	= bounds_height
			lRet[0]	= int(round(bounds_height * (curr_width / curr_height)))
		else:
			lRet[0]	= bounds_width
			lRet[1]	= int(round(bounds_width * (curr_height / curr_width)))

	# Return the new dimensions
	return lRet

def toFit(curr_width, curr_height, bounds_width, bounds_height):
	"""To Fit

	Resizes the current dimensions so that they are no bigger than the boundary

	Args:
		curr_width (uint): The current width
		curr_height (uint): The current height
		bounds_width (uint): The boundary width
		bounds_height (uint): The boundary height

	Returns:
		list [w, h]
	"""

	# Return
	lRet	= [0,0]

	# Convert current width/height to floats
	curr_width		= float(curr_width)
	curr_height		= float(curr_height)

	# If either width or height is smaller than the boundary box, resize up
	if curr_width < bounds_width and curr_height < bounds_height:

		# Which is the side that needs to grow less?
		if (bounds_width / curr_width) > (bounds_height / curr_height):
			lRet[1]	= bounds_height
			lRet[0]	= int(round(bounds_height * (curr_width / curr_height)))
		else:
			lRet[0]	= bounds_width
			lRet[1]	= int(round(bounds_width * (curr_height / curr_width)))

	# Else if the image is already larger than the boundary, resize down
	else:

		# Which is the side that needs to shrink more?
		if (curr_width / bounds_width) > (curr_height / bounds_height):
			lRet[0]	= bounds_width
			lRet[1]	= int(round(bounds_width * (curr_height / curr_width)))
		else:
			lRet[1]	= bounds_height
			lRet[0]	= int(round(bounds_height * (curr_width / curr_height)))

	# Return the new dimensions
	return lRet
