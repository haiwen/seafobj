import pytest
import uuid
from tests.utils import get_s3_client, get_oss_client

data1 = 'test file content'
data2 = "second test file content"
data3 = "third test file content"

repo_id1 = str(uuid.uuid4())
repo_id2 = str(uuid.uuid4())

def read_and_write_client(client):
    key1 = f'{repo_id1}/249408dcc7aaba6e0948cb2d1950aaf4c86078b0'
    key2 = f'{repo_id1}/8228f1d3877efa3395475ccccd065f87d7727e29'
    key3 = f'{repo_id2}/97c5a757b1aa4de4a9d7c07f3d66648e43c562e7'
    client.write_obj(data1, key1)
    client.write_obj(data2, key2)
    client.write_obj(data3, key3)
    assert client.obj_exists(key1) == True
    assert client.obj_exists(key2) == True
    assert client.obj_exists(key3) == True
    assert client.stat_obj(key1) == len(data1)
    assert client.stat_obj(key2) == len(data2)
    assert client.stat_obj(key3) == len(data3)

    objs = client.list_objs()
    num = 0
    for obj in objs:
        if obj[0] == repo_id1 or obj[0] == repo_id2:
            num += 1
    assert num == 3

    objs = client.list_objs(repo_id1)
    num = 0
    for obj in objs:
        assert obj[0] == repo_id1
        assert obj[1] == '249408dcc7aaba6e0948cb2d1950aaf4c86078b0' or obj[1] == '8228f1d3877efa3395475ccccd065f87d7727e29'
        num += 1
    assert num == 2

    objs = client.list_objs(repo_id2)
    num = 0
    for obj in objs:
        assert obj[0] == repo_id2
        assert obj[1] == '97c5a757b1aa4de4a9d7c07f3d66648e43c562e7'
        num += 1
    assert num == 1

    raw = client.read_obj(key1)
    assert raw == data1.encode("utf-8")
    raw = client.read_obj(key2)
    assert raw == data2.encode("utf-8")
    raw = client.read_obj(key3)
    assert raw == data3.encode("utf-8")

    client.remove_obj(key1)
    assert client.obj_exists(key1) == False
    client.remove_obj(key2)
    assert client.obj_exists(key2) == False
    client.remove_obj(key3)
    assert client.obj_exists(key3) == False

@pytest.mark.parametrize(
    "sse_c_key",
    [None, 'I6gkJwqfGiXRNx7wW7au23iznCy///Q='],
)
def test_obj_wrapper_s3 (sse_c_key):
    client = get_s3_client(sse_c_key)
    assert client.get_name() == "S3 storage backend"
    read_and_write_client(client)
def test_obj_wrapper_oss ():
    client = get_oss_client()
    assert client.get_name() == "OSS storage backend"
    read_and_write_client(client)

