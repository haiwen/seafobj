#!/usr/bin/env python

'''Copy test data from a real library storage'''

import os

repo_id = '413c175b-0f7d-4616-8298-22bc147af43c'

seafile_dir = '/data/data/seafile-data'

test_storage_dir = os.path.join(os.path.dirname(__file__), 'test/functional/data/storage')

def copy_folder(src, dst):
    if not os.path.exists(dst):
        os.makedirs(dst)

    cmd = 'cp -arf %s %s' % (src, dst)
    # print 'running ' + cmd
    # return
    if os.system(cmd) < 0:
        raise Exception('failed to run ' + cmd)

# from: seafile-data/storage/blocks/413c175b-0f7d-4616-8298-22bc147af43c
# to:   test/functional/data/storage/blocks/413c175b-0f7d-4616-8298-22bc147af43c

def do_copy():
    for name in ('commits', 'fs', 'blocks'):
        src = os.path.join(seafile_dir, 'storage', name, repo_id)
        dst = os.path.join(test_storage_dir, name)
        copy_folder(src, dst)

def main():
    do_copy()

if __name__ == '__main__':
    main()
