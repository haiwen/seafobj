#coding: utf-8

import httplib
import urllib2
import json
from seafobj.backends.base import AbstractObjStore
from seafobj.exceptions import GetObjectError, SwiftAuthenticateError

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
    MAX_RETRY = 3

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
        url = '%s/%s/tokens' % (self.base_url, self.swift_conf.auth_ver)
        hdr = {'Content-Type': 'application/json'}
        auth_data = {'auth': {'passwordCredentials': {'username': self.swift_conf.user_name,
                                                      'password': self.swift_conf.password},
                              'tenantName': self.swift_conf.tenant}}
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            req = urllib2.Request(url, json.dumps(auth_data), hdr)
            resp = urllib2.urlopen(req)
            ret_code = resp.getcode()
            ret_data = resp.read()

            if ret_code == httplib.OK or ret_code == httplib.NON_AUTHORITATIVE_INFORMATION:
                data_json = json.loads(ret_data)
                self.token = data_json['access']['token']['id']
                catalogs = data_json['access']['serviceCatalog']
                for catalog in catalogs:
                    if catalog['type'] == 'object-store':
                        self.storage_url = catalog['endpoints'][0]['publicURL']
                        return
            elif ret_code == httplib.UNAUTHORIZED or ret_code >= httplib.INTERNAL_SERVER_ERROR:
                # Retry
                i += 1
            else:
                raise SwiftAuthenticateError('[swift] Failed to authenticate: [%d]%s' %
                                             (ret_code, ret_data))
        raise SwiftAuthenticateError('[swift] Failed to authenticate on retry %d times' %
                                     SeafSwiftClient.MAX_RETRY)

    def read_object_content(self, obj_id):
        if not self.authenticated():
            self.authenticate()

        url = '%s/%s/%s' % (self.storage_url, self.swift_conf.container, obj_id)
        hdr = {'X-Auth-Token': self.token}
        i = 0
        while i <= SeafSwiftClient.MAX_RETRY:
            req = urllib2.Request(url, headers=hdr)
            resp = urllib2.urlopen(req)
            ret_code = resp.getcode()
            ret_data = resp.read()

            if ret_code == httplib.OK:
                return ret_data
            elif ret_code == httplib.UNAUTHORIZED or ret_code >= httplib.INTERNAL_SERVER_ERROR:
                # Retry
                i += 1
            else:
                raise GetObjectError('[swift] Failed to read %s: [%d]%s' %
                                     (obj_id, ret_code, ret_data))
        raise GetObjectError('[swift] Failed to read %s on retry %d times' %
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
