#coding: UTF-8

class AbstractObjStore(object):
    '''Base class of seafile object backend'''
    def read_obj(self, repo_id, version, obj_id):
        '''Read the content of the object from the backend. Each backend
        subclass should have their own implementation

        '''
        raise NotImplementedError
        
    def get_name(self):
        raise NotImplementedError
