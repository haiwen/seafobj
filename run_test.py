import os
import subprocess

file_path = os.path.abspath(__file__)
path = os.path.join(os.path.dirname(file_path), 'test', 'functional', 'data')
with open(os.path.join(path, 'seafile.ini'), 'w') as fd:
    fd.write(os.path.join(path, 'storage'))

os.environ['SEAFILE_CONF_DIR'] = path
os.environ['CCNET_CONF_DIR'] = ''
args = ['pytest', '-sv', 'test']
subprocess.check_call(args, env=os.environ)
