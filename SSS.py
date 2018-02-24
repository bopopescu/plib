# coding=utf8
"""SSS (S3) Module

Various helper functions for using S3
"""

# Import future
from __future__ import print_function, absolute_import

__author__		= "Chris Nasr"
__copyright__	= "OuroborosCoding"
__maintainer__	= "Chris Nasr"
__email__		= "ouroboroscode@gmail.com"
__created__		= "2017-12-05"

# Import python modules
from time import sleep

# Import pip modules
import boto3
import botocore.session
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from botocore.vendored.requests.packages.urllib3.exceptions import ReadTimeoutError

# Module variables
_c = None
_r = None
_s3 = None

# Module constants
MAX_TIMEOUTS = 5

# delete method
def delete(key, bucket=None):
	"""Delete

	Deletes an existing object off S3

	Args:
		key: The key the object is stored under
		bucket: Optional bucket (needed if not passed in init)

	Returns:
		None
	"""

	# If init wasn't called
	if not _s3:
		raise RuntimeError('SSS.init must be called before SSS.delete')

	# If there's a bucket
	if bucket:
		sBucket = bucket
	elif _s3['bucket']:
		sBucket = _s3['bucket']
	else:
		raise ValueError('Must specify bucket in delete or init')

	# If there's a path, prepend it
	if _s3['path']:
		key = _s3['path'] + key

	# Keep trying if we get timeout errors
	iTimeouts = 0
	while True:

		try:

			# Attempt to delete the object
			_r.Object(sBucket, key).delete()

		except ClientError as e:
			raise SSSException(e.args, sBucket, key)

		except ReadTimeoutError as e:
			iTimeouts += 1					# increment the timeout count
			if iTimeouts >= MAX_TIMEOUTS:	# if we've timed out the max times
				raise SSSException('S3 not available', str(e))
			sleep(1)						# else sleep for a second
			continue						# and try again

		except Exception as e:
			raise SSSException('Unknown S3 exception', str(e))

# get method
def get(key, bucket=None):
	"""Get

	Gets an existing object off S3

	Args:
		key: The key the object is stored under
		bucket: Optional bucket (needed if not passed in init)

	Returns:
		str
	"""

	# If init wasn't called
	if not _s3:
		raise RuntimeError('SSS.init must be called before SSS.get')

	# If there's a bucket
	if bucket:
		sBucket = bucket
	elif _s3['bucket']:
		sBucket = _s3['bucket']
	else:
		raise ValueError('Must specify bucket in get or init')

	# If there's a path, prepend it
	if _s3['path']:
		key = _s3['path'] + key

	# Keep trying if we get timeout errors
	iTimeouts = 0
	while True:

		try:

			# Attempt to fetch the object
			dBlob = _r.Object(sBucket, key).get()

			# Return the body
			return dBlob['Body'].read()

		except ClientError as e:
			raise SSSException(e.args, sBucket, key)

		except ReadTimeoutError as e:
			iTimeouts += 1					# increment the timeout count
			if iTimeouts >= MAX_TIMEOUTS:	# if we've timed out the max times
				raise SSSException('S3 not available', str(e))
			sleep(1)						# else sleep for a second
			continue						# and try again

		except Exception as e:
			raise SSSException('Unknown S3 exception', str(e))

# Initialise method
def init(conf, bucket=None, path=None):
	"""Init

	Initialises the module so that it can be used

	Args:
		conf: The configuration parameters, see boto3.resource for more info
		bucket: Optional bucket (needed if not passed in init)
		path: Optional path to prepend to all keys

	Returns:
		None
	"""

	# Pull in the module variable
	global _c, _r, _s3

	# Init the conf
	_s3 = {
		"conf": BotoConfig(**conf),
		"bucket": bucket,
		"path": path
	}

	# Get an S3 resource
	_r = boto3.resource('s3', config=_s3['conf'])

	# Get a client
	_c = botocore.session.get_session().create_client('s3')

# put method
def put(key, content, bucket=None, acl='private', headers={}, metadata={}):
	"""Put

	Puts an object on S3

	Args:
		key: The key to store the content under
		content: The content of the upload
		bucket: The name of the bucket to upload the key to
		acl: The ACL mode of the upload
		headers: Misc. headers that can be set, see boto3.S3.Object.put for reference
		metadata: Misc. metadata that will be associated with the object

	Returns:
		None

	Raises:
		RuntimeError: If init is not called prior to calling upload
		ValueError: If fields are missing
		SSSException: To pass along uncatchable errors
	"""

	# If init wasn't called
	if not _s3:
		raise RuntimeError('SSS.init must be called before SSS.put')

	# If there's a bucket
	if bucket:
		sBucket = bucket
	elif _s3['bucket']:
		sBucket = _s3['bucket']
	else:
		raise ValueError('Must specify bucket in put or init')

	# Add the ACL, Body and Metdata to the headers variable to simplify our life
	headers['ACL'] = acl
	headers['Body'] = content
	headers['Metadata'] = metadata

	# Calculate the size of the object
	headers['ContentLength'] = len(content)

	# If there's a path, prepend it
	if _s3['path']:
		key = _s3['path'] + key

	# Keep trying if we get timeout errors
	iTimeouts = 0
	while True:

		# Create new object and upload it
		try:
			return _r.Object(sBucket, key).put(**headers)

		# Check for client Errors
		except ClientError as e:
			raise SSSException(e.args, sBucket, key)

		except ReadTimeoutError as e:
			iTimeouts += 1					# increment the timeout count
			if iTimeouts >= MAX_TIMEOUTS:	# if we've timed out the max times
				raise SSSException('S3 not available', str(e))
			sleep(1)						# else sleep for a second
			continue						# and try again

		except Exception as e:
			raise SSSException('Unknown S3 exception', str(e))

#url method
def url(key, expires, bucket=None, headers={}):
	"""URL

	Generates a presigned URL

	Args:
		key: The key the object is stored under
		expires: The seconds the URL should be valid
		bucket: The name of the bucket the key is in
		headers: Misc. headers that can be set, see boto3.S3.Client.generate_presigned_url

	Returns:
		string

	Raises:
		SSSException
	"""

	# If init wasn't called
	if not _s3:
		raise RuntimeError('SSS.init must be called before SSS.url')

	# If there's a bucket
	if bucket:
		sBucket = bucket
	elif _s3['bucket']:
		sBucket = _s3['bucket']
	else:
		raise ValueError('Must specify bucket in url or init')

	# If there's a path, prepend it
	if _s3['path']:
		key = _s3['path'] + key

	# Add the bucket and key to the headers to simplify our life
	headers['Bucket'] = sBucket
	headers['Key'] = key

	# Generate and return the URL
	return _c.generate_presigned_url(
		'get_object',
		headers,
		ExpiresIn=expires
	)

class SSSException(Exception):
	"""SSSException class

	Used for raising s3 specific exceptions

	Extends:
		Exception
	"""
	pass