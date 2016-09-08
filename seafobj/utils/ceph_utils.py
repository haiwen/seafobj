#coding: UTF-8
#
import rados

from ctypes import c_char_p

def ioctx_set_namespace(ioctx, namespace):
    '''Python rados client has no binding for rados_ioctx_set_namespace, we
    add it here.

    '''
    ioctx.require_ioctx_open()
    if isinstance(namespace, unicode):
        namespace = namespace.encode('UTF-8')

    if not isinstance(namespace, str):
        raise TypeError('namespace must be a string')

    if hasattr(ioctx, 'set_namespace'):
        ioctx.set_namespace(namespace)
    else:
        # A hack to set namespace for older version (<= 0.94.x) rados python lib.
        rados.run_in_thread(ioctx.librados.rados_ioctx_set_namespace,
                            (ioctx.io, c_char_p(namespace)))
