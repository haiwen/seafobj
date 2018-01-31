import os
import sys
import argparse
import subprocess


file_path = os.path.abspath(__file__)
storages = ['local', 'ceph']
conf_dir = os.path.join(os.path.dirname(file_path), 'test', 'functional', 'data')

def prepare(storage, env):
    """ migrate data and set env after migrate data.
    """
    if storage == 'ceph':
        if 'seafobj' not in os.environ.get('PYTHONPATH', ''):
            sys.stdout.write('please set env first. \n')
            sys.exit(0)
        args = ['python', 'test/seafobj_migrate.py']
        subprocess.check_call(args, env=env)
        with open(os.path.join(conf_dir, 'ceph', 'seafile.conf')) as fr:
            with open(os.path.join(conf_dir, 'seafile.conf'), 'w') as fw:
                fw.write(fr.read())

    os.environ['SEAFILE_CONF_DIR'] = os.path.join(conf_dir)
    os.environ['CCNET_CONF_DIR'] = ''

def clear():
    """ clear seafile.conf content
    """
    with open(os.path.join(conf_dir, 'seafile.conf'), 'r+') as fw:
        fw.truncate()
    #os.remove(os.path.join(conf_dir, 'seafile.ini'))

def set_env(storage):
    """ set env for migrate and run local file system test
    """
    os.environ['CCNET_CONF_DIR'] = ''
    os.environ['SEAFILE_CONF_DIR'] = conf_dir
    if storage == 'fs':
        os.environ['CEPH_SEAFILE_CENTRAL_CONF_DIR'] = ''
    elif storage == 'ceph':
        os.environ['CEPH_SEAFILE_CENTRAL_CONF_DIR'] = os.path.join(conf_dir, 'ceph')

    #with open(os.path.join(conf_dir, 'seafile.ini'), 'w') as fd:
    #    fd.write(os.path.join(conf_dir, 'storage'))

    prepare(storage, os.environ)

    return os.environ

def run(storage):
    if storage == 'all':
        for storage in storages:
            env = set_env(storage)
            msg = 'Start the standard %s filesystem test.' % storage
            print(msg.center(180))
            #args = ['pytest', '--ignore=test/functional/crypto_test.py','-sv', 'test']
            args = ['pytest', '-sv', 'test']
            try:
                subprocess.check_call(args, env=env)
            except:
                pass
            clear()
    else:
        env = set_env(storage)
        #args = ['pytest', '--ignore=test/functional/crypto_test.py','-sv', 'test']
        args = ['pytest', '-sv', 'test']
        try:
            subprocess.check_call(args, env=env)
        except:
            pass
        clear()

def main():
    parser = argparse.ArgumentParser(
        description='seafevents main program')
    parser.add_argument('--storage', help='backend storeage of file system')
    args = parser.parse_args()

    storage = 'fs' 
    if args.storage and args.storage in ['fs', 'ceph', 'all']:
        storage = args.storage

    run(storage)

if __name__ == '__main__':
    main()
