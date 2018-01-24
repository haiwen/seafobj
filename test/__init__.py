import os

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'functional','data')
os.environ['SEAFILE_CONF_DIR'] = data_dir
with open(os.path.join(data_dir, 'seafile.ini'), 'w') as fd:
    fd.write(os.path.join(data_dir, 'storage'))
