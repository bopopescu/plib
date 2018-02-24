# coding=utf8
"""Photos

Various helper functions for manipulating photos/images
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2016-05-11"

# Import python modules
import re
from StringIO import StringIO

# Import pip modules
from PIL import Image as PilImg

# Import project modules
from shared import Resize

# exif rotation tag
ORIENTATION_TAG	= 0x0112

# Regex for valid size/thumbnail strings
SIZE_REGEX	= re.compile(r'^(?:[1-9]\d*)?x(?:[1-9]\d*)?$')

def apply_rotation_exif(image):
	"""Apply Rotation

	Transposes an image based on the EXIF rotation data, effectively making sure
	all images are in the correct orientation regardless of what meta data the
	creator may have added

	Args:
		image (str): Raw image data to be loaded and resized

	Returns:
		str
	"""

	# Load the image into a new StringIO
	sImage	= StringIO(image)
	sNewImg = StringIO('')

	# Create a new Pillow instance from the raw data
	oImg	= PilImg.open(sImage)

	# Store the format for later
	sFormat	= oImg.format

	# The sequences mapped to the orientation flag
	lSequences		= [
		[],
		[PilImg.FLIP_LEFT_RIGHT],
		[PilImg.ROTATE_180],
		[PilImg.FLIP_TOP_BOTTOM],
		[PilImg.FLIP_LEFT_RIGHT, PilImg.ROTATE_90],
		[PilImg.ROTATE_270],
		[PilImg.FLIP_TOP_BOTTOM, PilImg.ROTATE_90],
		[PilImg.ROTATE_90]
	]

	# Get the proper sequence
	try:
		lSeq	= lSequences[oImg._getexif()[ORIENTATION_TAG] - 1]

		# Transpose the image and return the new raw data
		for i in lSeq:
			oImg	= oImg.transpose(i)

		# Save the new image to a StringIO
		oImg.save(sNewImg, sFormat)

		# Pull out the raw string
		sReturn	= sNewImg.getvalue()

	# If there's none, there's nothing to do, return the raw image
	except Exception as e:

		print('%s: %s' % (
			e.__class__.__name__,
			', '.join([str(s) for s in e.args])
		))

		sReturn	= image

	# Cleanup
	oImg.close()
	sImage.close()
	sNewImg.close()

	# Return the altered (or non-altered) image
	return sReturn

def convert_to_jpeg(image):
	"""Convert To JPEG

	Method to convert non-standard images to JPEG

	Args:
		image (str): The raw image data

	Returns:
		str
	"""

	# Load the image into a new StringIO
	sImage = StringIO(image)

	# Create an empty StringIO for the new image
	sNewImg	= StringIO('')

	# Create a new Pillow instance from the raw data
	oImg = PilImg.open(sImage)

	# If the mode is not valid
	if oImg.mode not in ('1','L','RGB','RGBA'):
		oImg = oImg.convert('RGB');

	# Save the new image as a JPEG
	oImg.save(sNewImg, 'JPEG')

	# Pull out the raw string
	sRet = sNewImg.getvalue()

	# Close the image
	oImg.close()

	# Return the new image
	return sRet

def details(image):
	"""Details

	Returns details about an image: resolution; length; type, and mime

	Args:
		image (str): Raw image data to get the details for

	Returns:
		dict
	"""

	# Load the image into a new StringIO
	sImage	= StringIO(image)

	# Create a new Pillow instance from the raw data
	oImg	= PilImg.open(sImage)

	# Get the details
	dRet	= {
		"length":		len(image),
		"mime":			PilImg.MIME[oImg.format],
		"resolution":	oImg.size,
		"type":			oImg.format
	}

	# Check for exif rotation data
	try:
		dRet['orientation']	= oImg._getexif()[ORIENTATION_TAG]
	except Exception:
		dRet['orientation'] = False

	# Cleanup
	sImage.close()
	oImg.close()

	# Return the details
	return dRet

def resize(image, size, crop=False):
	"""Resize

	Given raw data and a size, a new image is created and returned as raw data

	Args:
		image (str): Raw image data to be loaded and resized
		size (str): Size in the format WWWxHHH
		crop (bool): If true, photo is cropped to fit rather than adding white
			space

	Returns:
		str
	"""

	# Split the size into width and height
	lSize	= [i for i in size.split('x')]

	# Load the image into a new StringIO
	sImage	= StringIO(image)
	sNewImg	= StringIO('')

	# Create a new Pillow instance from the raw data
	oImg	= PilImg.open(sImage)

	# If we have both dimensions
	if lSize[0] and lSize[1]:

		# Turn them into ints
		lSize[0]	= int(lSize[0])
		lSize[1]	= int(lSize[1])

		# Create a new blank image
		oNewImg	= PilImg.new(oImg.mode, lSize, (255,255,255,255))

		# If we are cropping,
		if crop:
			lResize = Resize.toCrop(oImg.width, oImg.height, lSize[0], lSize[1])

		# Else get the largest dimensions that can fit in the bounding box
		else:
			lResize	= Resize.toFit(oImg.width, oImg.height, lSize[0], lSize[1])

		# Resize the image
		oImg.thumbnail(lResize, PilImg.ANTIALIAS)

		# Get the offsets
		lOffset	= ((lSize[0] - lResize[0]) / 2, (lSize[1] - lResize[1]) / 2)

		# Paste the thumb onto the new canvas
		oNewImg.paste(oImg, lOffset)

		# Save the new image to a StringIO
		oNewImg.save(sNewImg, oImg.format)

		# Pull out the raw string
		sReturn	= sNewImg.getvalue()

		# Close the new image
		oNewImg.close()

	# Else if we only have the width
	else:

		# Get the size to use
		iSize	= lSize[0] and int(lSize[0]) or int(lSize[1])

		# Get the size needed to scale the image
		lResize	= Resize.toCrop(oImg.size[0], oImg.size[1], iSize)

		# Scale the image based on the resize values
		oImg.thumbnail(lResize, PilImg.ANTIALIAS)

		# Save theresized image to a StringIO
		oImg.save(sNewImg)

		# Pull out the raw string
		sReturn	= sNewImg.getvalue()

	# Close everything else
	oImg.close()
	sImage.close()

	# Return the new string
	return sReturn
