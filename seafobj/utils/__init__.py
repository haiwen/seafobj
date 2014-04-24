
def to_utf8(s):
    if isinstance(s, unicode):
        s = s.encode('utf-8')

    return s