#coding: UTF-8

class SeafObjException(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg

    def __str__(self):
        return self.msg

class InvalidConfigError(SeafObjException):
    '''This Exception is rasied when error happens during parsing
    seafile.conf

    '''
    pass

class ObjectFormatError(SeafObjException):
    '''This Exception is rasied when error happened during parse object
    format

    '''
    pass