#!/usr/bin/env python

import os
import argparse
import ConfigParser
from seafobj.db import Base
from seafobj.db import init_db_session_class
from seafobj.objstore_factory import SeafObjStoreFactory

def get_head_commits_from_db(session):
    Branch = Base.classes.Branch
    head_commits = session.query(Branch.repo_id, Branch.commit_id).all()

    return head_commits

def check_if_repo_corrupted(head_commit_from_db, commits_store_obj):
    repo_id = head_commit_from_db.repo_id
    head_commit_id = head_commit_from_db.commit_id

    return not commits_store_obj.obj_exists(repo_id, head_commit_id)

def count_corrupted_files(session, repo_id):
    '''If this repo is corrupted then all the files in this repo are count as corrupted files.'''
    RepoFileCount = Base.classes.RepoFileCount
    files_count = 0
    res = session.query(RepoFileCount.file_count).filter(RepoFileCount.repo_id == repo_id).all()
    if (res):
        files_count = res[0].file_count

    return files_count

def count_total_files(session):
    RepoFileCount = Base.classes.RepoFileCount
    total_files_count = 0
    rows = session.query(RepoFileCount.file_count).all()
    for row in rows:
        total_files_count += row.file_count

    return total_files_count

def main ():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', required=True, help='path to seafile.conf')
    args = parser.parse_args()
    confdir = args.c
    seafile_conf = os.path.join(confdir, 'seafile.conf')

    cfg = ConfigParser.ConfigParser()
    cfg.read(seafile_conf)
    Session = init_db_session_class(cfg)
    session = Session()

    objstore_factory = SeafObjStoreFactory()
    commits_obj_store = objstore_factory.get_obj_store('commits')

    head_commits_from_db = get_head_commits_from_db(session)
    corrupted_files_count = 0
    for head_commit_from_db in head_commits_from_db:
        repo_corrupted = check_if_repo_corrupted(head_commit_from_db, commits_obj_store)
        if repo_corrupted:
            corrupted_files_count += count_corrupted_files(session, head_commit_from_db.repo_id)
            print ('Library %s is corrupted.' % head_commit_from_db.repo_id)
    total_files_count = count_total_files(session)
    if corrupted_files_count == 0:
        print 0
    else:
        print ('%d/%d' % (corrupted_files_count, total_files_count))

main()
