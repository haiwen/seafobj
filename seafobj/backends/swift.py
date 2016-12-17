#coding: utf-8

import httplib
import urllib2
import requests
import json
import logging
import hashlib
from swiftclient.client import Connection
from swiftclient.client import ClientException

from seafobj.backends.base import AbstractObjStore
from seafobj.exceptions import GetObjectError, SwiftAuthenticateError

class MismatchException(Exception):
    pass

class SwiftConf(object):
    def __init__(self, user_name, password, container, auth_host, auth_ver, tenant, use_https):
        self.user_name = user_name
        self.password = password
        self.container = container
        self.auth_host = auth_host
        self.auth_ver = auth_ver
        self.tenant = tenant
        self.use_https = use_https

class SeafSwiftClient(object):

    def __init__(self, swift_conf):
        self.swift_conf = swift_conf
        self.conn = None

        if swift_conf.use_https:
            self.base_url = 'https://%s' % swift_conf.auth_host
        else:
            self.base_url = 'http://%s' % swift_conf.auth_host

        if swift_conf.auth_ver == 'v1.0':
            self.auth_ver = '1'
            self.authurl = '%s/' % self.base_url
        elif swift_conf.auth_ver == 'v2.0':
            self.auth_ver = '2'
            self.authurl = '%s/v2.0/' % self.base_url
        elif swift_conf.auth_ver == 'v3.0':
            self.auth_ver = '3'
            self.authurl = '%s/v3/' % self.base_url

    def authenticated(self):
        if self.conn is not None:
            return True
        return False

    def authenticate(self):
        if self.auth_ver == '3':
            _os_options = {
                    'user_domain_name': 'Default',
                    'project_domain_name': 'Default',
                    'project_name': 'Default'}
            self.conn = Connection(
                    authurl = self.authurl,
                    user = self.swift_conf.user_name,
                    key = self.swift_conf.password,
                    tenant_name = self.swift_conf.tenant,
                    os_options=_os_options,
                    auth_version = self.auth_ver )
        else:
            self.conn = Connection(
                    authurl = self.authurl,
                    user = self.swift_conf.user_name,
                    key = self.swift_conf.password,
                    tenant_name = self.swift_conf.tenant,
                    auth_version = self.auth_ver )

    def read_object_content(self, obj_id):
        retries = 2
        i = 0
        while i<retries: 
            if not self.authenticated():
                self.authenticate()
            resp_headers, obj_contents = self.conn.get_object(self.swift_conf.container, obj_id)
            if int(resp_headers['content-length']) != len(obj_contents):
                i+=1
                continue
            m = hashlib.md5()
            m.update(obj_contents)
            Etag = m.hexdigest()
            if Etag != resp_headers['etag']:
                raise MismatchException('Digest mismatched')
            return obj_contents

class SeafObjStoreSwift(AbstractObjStore):
    '''Swift backend for seafile objecs'''
    def __init__(self, compressed, swift_conf, crypto=None):
        AbstractObjStore.__init__(self, compressed, crypto)
        self.swift_client = SeafSwiftClient(swift_conf)

    def read_obj_raw(self, repo_id, version, obj_id):
        real_obj_id = '%s/%s' % (repo_id, obj_id)
        data = self.swift_client.read_object_content(real_obj_id)
        return data

    def get_name(self):
        return 'Swift storage backend'

    def list_objs(self):
        obj_list = []
        if not self.swift_client.authenticated():
            self.swift_client.authenticate()

        conn = self.swift_client.conn
        container = self.swift_client.swift_conf.container
        resp_header, objs = conn.get_container(container, full_listing = True)
        for obj_info in objs:
            token = obj_info['name'].split('/')
            if len(token) == 2:
                repo_id = token[0]
                obj_id = token[1]
                obj = [repo_id, obj_id]
                obj_list.append(obj)

        return obj_list

    def obj_exists(self, repo_id, obj_id):
        if not self.swift_client.authenticated():
            self.swift_client.authenticate()

        obj = '%s/%s' % (repo_id, obj_id)
        container = self.swift_client.swift_conf.container
        try:
            self.swift_client.conn.head_object(container, obj)
            return True
        except ClientException as e:
            if e.http_status == 404:
                return False
            else:
                raise
    
    def write_obj(self, data, repo_id, obj_id):
        if not self.swift_client.authenticated():
            self.swift_client.authenticate()

        obj = '%s/%s' % (repo_id, obj_id)
        container = self.swift_client.swift_conf.container
        m = hashlib.md5()
        m.update(data)
        Etag = m.hexdigest()
        self.swift_client.conn.put_object(container,
                                          obj,
                                          contents = data,
                                          etag = Etag,
                                          content_type='application/octet-stream')
