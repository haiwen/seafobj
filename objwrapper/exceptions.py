#coding: UTF-8

class ObjWrapperException(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = str(msg)

    def __str__(self):
        return self.msg

class InvalidConfigError(ObjWrapperException):
    '''This Exception is rasied when error happens during parsing
    seafile.conf

    '''
    pass
