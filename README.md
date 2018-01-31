## Test

### Prepare

#### Environment

`add seafobj to PYTHONPATH env`
```
export PYTHONPATH=...../seafobj
```

#### Dependent

```
pip install -r test-requirements.txt
sudo apt-get install python-ceph python-rados
pip install boto
```

#### Configure File

update `./test/functional/data/ceph/seafile.conf` configure file if you want test ceph storage system.

### Run

run command `python run_test.py --sotrage {{fs|ceph}}`

### Note

Test data will be migrate to ceph storage system when run ceph test case.
So make sure it doesn't affect you.
