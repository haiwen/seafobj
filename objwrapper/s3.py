import boto3
from botocore.exceptions import ClientError

class S3Conf(object):
    def __init__(self, key_id, key, bucket_name, host, port, use_v4_sig, aws_region, use_https, path_style_request, sse_c_key):
        self.key_id = key_id
        self.key = key
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.use_v4_sig = use_v4_sig
        self.aws_region = aws_region
        self.use_https = use_https
        self.path_style_request = path_style_request
        self.sse_c_key = sse_c_key


class SeafS3Client(object):
    """Wraps a s3 connection and a bucket"""
    def __init__(self, conf):
        self.conf = conf
        self.client = None
        self.bucket = None
        self.do_connect()

    def do_connect(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
        addressing_style = 'virtual'
        if self.conf.path_style_request:
            addressing_style = 'path'
        if self.conf.use_v4_sig:
            config = boto3.session.Config(signature_version='s3v4', s3={'addressing_style':addressing_style})
        else:
            config = boto3.session.Config(signature_version='s3',s3={'addressing_style':addressing_style})

        if self.conf.host is None:
            self.client = boto3.client('s3',
                                       region_name=self.conf.aws_region,
                                       aws_access_key_id=self.conf.key_id,
                                       aws_secret_access_key=self.conf.key,
                                       use_ssl=self.conf.use_https,
                                       config=config)
        else:
            # https://github.com/boto/boto3/blob/master/boto3/session.py#L265
            endpoint_url = 'https://%s' % self.conf.host if self.conf.use_https else 'http://%s' % self.conf.host
            if self.conf.port:
                endpoint_url = '%s:%s' % (endpoint_url, self.conf.port)
            self.client = boto3.client('s3',
                                       aws_access_key_id=self.conf.key_id,
                                       aws_secret_access_key=self.conf.key,
                                       endpoint_url=endpoint_url,
                                       config=config)

        self.bucket = self.conf.bucket_name

    def read_object_content(self, obj_id):
        if self.conf.sse_c_key:
            obj = self.client.get_object(Bucket=self.bucket, Key=obj_id, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            obj = self.client.get_object(Bucket=self.bucket, Key=obj_id)
        return obj.get('Body').read()


    def read_obj_raw(self, real_obj_id):
        data = self.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'S3 storage backend'

    def list_objs(self, repo_id=None):
        start_after = ''
        while True:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
            if repo_id:
                objects = self.client.list_objects_v2(Bucket=self.bucket, StartAfter=start_after,
                                                                Prefix=repo_id)
            else:
                objects = self.client.list_objects_v2(Bucket=self.bucket, StartAfter=start_after)

            if len(objects.get('Contents', [])) == 0:
                break

            for content in objects.get('Contents', []):
                tokens = content.get('Key', '').split('/')
                if len(tokens) == 2:
                    repo_id = tokens[0]
                    obj_id = tokens[1]
                    obj = [repo_id, obj_id, content.get('Size', 0)]
                    yield obj

            # The 'Contents' of response is a list, each element is a dict,
            # and each dict must contain the 'Key'.
            # Use the 'Key' of the last dict as the 'StartAfter' parameter of the next list_objects_v2().
            # If the dict does not contain 'Key', terminate the loop,
            # otherwise will fall into an infinite loop
            start_after = objects.get('Contents', [])[-1].get('Key', '')
            if not objects.get('IsTruncated', False) or not start_after:
                break

    def obj_exists(self, s3_path):
        bucket = self.bucket
        try:
            if self.conf.sse_c_key:
                self.client.head_object(Bucket=bucket, Key=s3_path, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
            else:
                self.client.head_object(Bucket=bucket, Key=s3_path)
            exists = True
        except ClientError:
            exists = False

        return exists

    def write_obj(self, data, s3_path):
        bucket = self.bucket
        if self.conf.sse_c_key:
            self.client.put_object(Bucket=bucket, Key=s3_path, Body=data, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            self.client.put_object(Bucket=bucket, Key=s3_path, Body=data)

    def remove_obj(self, s3_path):
        bucket = self.bucket
        self.client.delete_object(Bucket=bucket, Key=s3_path)

    def stat_raw(self, s3_path):
        bucket = self.bucket
        if self.conf.sse_c_key:
            obj = self.client.get_object(Bucket=bucket, Key=s3_path, SSECustomerKey=self.conf.sse_c_key, SSECustomerAlgorithm='AES256')
        else:
            obj = self.client.get_object(Bucket=bucket, Key=s3_path)
        size = int(obj.get('ContentLength'))
        return size