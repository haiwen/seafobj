#coding: utf-8

import http.client
import urllib.request, urllib.error, urllib.parse
import json
from seafobj.backends.base import AbstractObjStore
from seafobj.exceptions import GetObjectError, SwiftAuthenticateError

class SwiftConf(object):
    def __init__(self, user_name, password, container, auth_host, auth_ver, tenant, use_https, region, domain):
        self.user_name = user_name
        self.password = password
        self.container = container
        self.auth_host = auth_host
        self.auth_ver = auth_ver
        self.tenant = tenant
        self.use_https = use_https
        self.region = region
        self.domain = domain

class SeafSwiftClient(object):
    MAX_RETRY = 2

    def __init__(self, swift_conf):
        self.swift_conf = swift_conf
        self.token = None
        self.storage_url = None
        if swift_conf.use_https:
            self.base_url = 'https://%s' % swift_conf.auth_host
        else:
            self.base_url = 'http://%s' % swift_conf.auth_host

    def authenticated(self):
        if self.token is not None and self.storage_url is not None:
            return True
        return False

    def authenticate(self):
        if self.swift_conf.auth_ver == 'v1.0':
            self.authenticate_v1()
        elif self.swift_conf.auth_ver == "v2.0":
            self.authenticate_v2()
        else:
            self.authenticate_v3()

    def authenticate_v1(self):
        url = '%s/auth/%s' % (self.base_url, self.swift_conf.auth_ver)

        hdr = {'X-Storage-User': self.swift_conf.user_name,
               'X-Storage-Pass': self.swift_conf.password}
        req = urllib.request.Request(url, None, hdr)
        try:
            resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %d.' %
                                         (SeafSwiftClient.MAX_RETRY, e.getcode()))
        except urllib.error.URLError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %s.' %
                                         (SeafSwiftClient.MAX_RETRY, e.reason))

        ret_code = resp.getcode()
        if ret_code == http.client.OK or ret_code == http.client.NON_AUTHORITATIVE_INFORMATION:
            self.storage_url = resp.headers['x-storage-url']
            self.token = resp.headers['x-auth-token']
        else:
            raise SwiftAuthenticateError('[swift] Unexpected code when authenticate: %d' %
                                         ret_code)
        if self.storage_url == None:
            raise SwiftAuthenticateError('[swift] Failed to authenticate.')

    def authenticate_v2(self):
        url = '%s/%s/tokens' % (self.base_url, self.swift_conf.auth_ver)
        hdr = {'Content-Type': 'application/json'}
        auth_data = {'auth': {'passwordCredentials': {'username': self.swift_conf.user_name,
                                                      'password': self.swift_conf.password},
                              'tenantName': self.swift_conf.tenant}}

        req = urllib.request.Request(url, json.dumps(auth_data).encode('utf8'), hdr)
        try:
            resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %d.' %
                                         (SeafSwiftClient.MAX_RETRY, e.getcode()))
        except urllib.error.URLError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %s.' %
                                         (SeafSwiftClient.MAX_RETRY, e.reason))

        ret_code = resp.getcode()
        ret_data = resp.read()

        if ret_code == http.client.OK or ret_code == http.client.NON_AUTHORITATIVE_INFORMATION:
            data_json = json.loads(ret_data)
            self.token = data_json['access']['token']['id']
            catalogs = data_json['access']['serviceCatalog']
            for catalog in catalogs:
                if catalog['type'] == 'object-store':
                    if self.swift_conf.region:
                        for endpoint in catalog['endpoints']:
                            if endpoint['region'] == self.swift_conf.region:
                                self.storage_url = endpoint['publicURL']
                                return
                    else:
                        self.storage_url = catalog['endpoints'][0]['publicURL']
                        return
        else:
            raise SwiftAuthenticateError('[swift] Unexpected code when authenticate: %d' %
                                         ret_code)
        if self.swift_conf.region and self.storage_url == None:
            raise SwiftAuthenticateError('[swift] Region \'%s\' not found.' % self.swift_conf.region)

    def authenticate_v3(self):
        url = '%s/v3/auth/tokens' % self.base_url
        hdr = {'Content-Type': 'application/json'}

        if  self.swift_conf.domain:
            domain_value = self.swift_conf.domain
        else:
            domain_value = 'default'
        auth_data = {'auth': {'identity': {'methods': ['password'],
                                           'password': {'user': {'domain': {'id': domain_value},
                                                                 'name': self.swift_conf.user_name,
                                                                 'password': self.swift_conf.password}}},
                              'scope': {'project': {'domain': {'id': domain_value},
                                                    'name': self.swift_conf.tenant}}}}

        req = urllib.request.Request(url, json.dumps(auth_data).encode('utf8'), hdr)
        try:
            resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %d.' %
                                         (SeafSwiftClient.MAX_RETRY, e.getcode()))
        except urllib.error.URLError as e:
            raise SwiftAuthenticateError('[swift] Failed to authenticate: %s.' %
                                         (SeafSwiftClient.MAX_RETRY, e.reason))

        ret_code = resp.getcode()
        ret_data = resp.read()

        if  ret_code == http.client.OK or ret_code == http.client.NON_AUTHORITATIVE_INFORMATION or ret_code == http.client.CREATED:
            self.token = resp.headers['X-Subject-Token']
            data_json = json.loads(ret_data)
            catalogs = data_json['token']['catalog']
            for catalog in catalogs:
                if catalog['type'] == 'object-store':
                    if self.swift_conf.region:
                        for endpoint in catalog['endpoints']:
                            if endpoint['region'] == self.swift_conf.region and endpoint['interface'] == 'public':
                                self.storage_url = endpoint['url']
                                return
                    else:
                        for endpoint in catalog['endpoints']:
                            if endpoint ['interface'] == 'public':
                                self.storage_url = endpoint['url']
                                return
        else:
            raise SwiftAuthenticateError('[swift] Unexpected code when authenticate: %d' %
                                         ret_code)
        if self.swift_conf.region and self.storage_url == None:
            raise SwiftAuthenticateError('[swift] Region \'%s\' not found.' % self.swift_conf.region)

    def read_object_content(self, obj_id):
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            if not self.authenticated():
                self.authenticate()

            url = '%s/%s/%s' % (self.storage_url, self.swift_conf.container, obj_id)
            hdr = {'X-Auth-Token': self.token}
            req = urllib.request.Request(url, headers=hdr)
            try:
                resp = urllib.request.urlopen(req)
            except urllib.error.HTTPError as e:
                err_code = e.getcode()
                if err_code == http.client.UNAUTHORIZED:
                    # Reset token and storage_url
                    self.token = None
                    self.storage_url = None
                    i += 1
                    continue
                else:
                    raise GetObjectError('[swift] Failed to read %s: %d' % (obj_id, err_code))
            except urllib.error.URLError as e:
                raise GetObjectError('[swift] Failed to read %s: %s' % (obj_id, e.reason))

            ret_code = resp.getcode()
            ret_data = resp.read()

            if ret_code == http.client.OK:
                return ret_data
            else:
                raise GetObjectError('[swift] Unexpected code when read %s: %d' %
                                     (obj_id, ret_code))
        raise GetObjectError('[swift] Failed to read %s: quit after %d unauthorized retries.' %
                             (obj_id, SeafSwiftClient.MAX_RETRY))

    def list_objs(self):
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            if not self.authenticated():
                self.authenticate()

            url = '%s/%s' % (self.storage_url, self.swift_conf.container)
            hdr = {'X-Auth-Token': self.token}
            req = urllib.request.Request(url, headers=hdr)
            try:
                resp = urllib.request.urlopen(req)
            except urllib.error.HTTPError as e:
                err_code = e.getcode()
                if err_code == http.client.UNAUTHORIZED:
                    # Reset token and storage_url
                    self.token = None
                    self.storage_url = None
                    i += 1
                    continue
                else:
                    raise GetObjectError('[swift] Failed to list objs %s' % err_code)
            except urllib.error.URLError as e:
                raise GetObjectError('[swift] Failed to list objs' % e.reason)

            ret_code = resp.getcode()
            ret_data = resp.read()

            if ret_code == http.client.OK:
                return ret_data
            else:
                raise GetObjectError('[swift] Unexpected code when list objs %s' % ret_code)
        raise GetObjectError('[swift] Failed to list objs: quit after %d unauthorized retries.' %
                            SeafSwiftClient.MAX_RETRY)

    def remove_obj(self, obj_id):
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            if not self.authenticated():
                self.authenticate()

            url = '%s/%s/%s' % (self.storage_url, self.swift_conf.container, obj_id)
            hdr = {'X-Auth-Token': self.token}
            req = urllib.request.Request(url, headers=hdr, method='DELETE')
            try:
                resp = urllib.request.urlopen(req)
            except urllib.error.HTTPError as e:
                err_code = e.getcode()
                if err_code == http.client.UNAUTHORIZED:
                    # Reset token and storage_url
                    self.token = None
                    self.storage_url = None
                    i += 1
                    continue
                else:
                    raise GetObjectError('[swift] Failed to remove %s: %d' % (obj_id, err_code))
            except urllib.error.URLError as e:
                raise GetObjectError('[swift] Failed to remove %s: %s' % (obj_id, e.reason))

            ret_code = resp.getcode()
            return ret_code
        raise GetObjectError('[swift] Failed to remove obj %s: quit after %d unauthorized retries.' %
                             (obj_id, SeafSwiftClient.MAX_RETRY))
    
    def stat_obj(self, obj_id):
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            if not self.authenticated():
                self.authenticate()

            url = '%s/%s/%s' % (self.storage_url, self.swift_conf.container, obj_id)
            hdr = {'X-Auth-Token': self.token}
            req = urllib.request.Request(url, headers=hdr, method='HEAD')
            try:
                resp = urllib.request.urlopen(req)
            except urllib.error.HTTPError as e:
                err_code = e.getcode()
                if err_code == http.client.UNAUTHORIZED:
                    # Reset token and storage_url
                    self.token = None
                    self.storage_url = None
                    i += 1
                    continue
                else:
                    raise GetObjectError('[swift] Failed to remove %s: %d' % (obj_id, err_code))
            except urllib.error.URLError as e:
                raise GetObjectError('[swift] Failed to remove %s: %s' % (obj_id, e.reason))

            for k, v in resp.headers.items():
                if k == 'Content-Length':
                    return int(v)
        raise GetObjectError('[swift] Failed to remove obj %s: quit after %d unauthorized retries.' %
                             (obj_id, SeafSwiftClient.MAX_RETRY))

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

    def list_objs(self, repo_id=None):
        objs = self.swift_client.list_objs().decode('utf8').split('\n')
        if repo_id:
            for obj in objs:
                tokens = obj.split('/')
                if tokens[0] == repo_id and len(tokens) == 2:
                    yield [tokens[0], tokens[1], 0]
        else:
            for obj in objs:
                tokens = obj.split('/')
                if len(tokens) == 2:
                    yield [tokens[0], tokens[1], 0]

    def remove_obj(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        self.swift_client.remove_obj(key)
    
    def stat_raw(self, repo_id, obj_id):
        key = '%s/%s' % (repo_id, obj_id)

        return self.swift_client.stat_obj(key)
