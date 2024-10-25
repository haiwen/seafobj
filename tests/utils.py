import os
from objwrapper.s3 import S3Conf
from objwrapper.s3 import SeafS3Client
from objwrapper.alioss import OSSConf 
from objwrapper.alioss import SeafOSSClient

envs = os.environ

oss_key = envs.get("OSS_ACCESS_KEY")
oss_key_id = envs.get("OSS_ACCESS_KEY_ID")
oss_bucket = envs.get("OSS_BUCKET")
oss_region = envs.get("OSS_REGION")

s3_key = envs.get("S3_ACCESS_KEY")
s3_key_id = envs.get("S3_ACCESS_KEY_ID")
s3_bucket = envs.get("S3_BUCKET")
s3_region = envs.get("S3_REGION")

def get_s3_client(sse_c_key):
    conf = S3Conf(s3_key_id, s3_key, s3_bucket, None, None, True, s3_region, True, False, sse_c_key)
    client = SeafS3Client(conf)
    return client

def get_oss_client():
    host = f'oss-{oss_region}.aliyuncs.com'
    conf = OSSConf(oss_key_id, oss_key, oss_bucket, host, True)
    client = SeafOSSClient(conf)
    return client
