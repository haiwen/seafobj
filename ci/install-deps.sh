add-apt-repository ppa:fkrull/deadsnakes-python2.7
apt-get update  -y
apt-get install python2.7 python-pip python-dev build-essential python-ceph -y
apt install python-rados
pip install --upgrade pip 

pip install -r ./requirements.txt
