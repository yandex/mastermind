
def import_object(s):
    parts = [p.encode('utf-8') for p in s.rsplit('.', 1)]
    if len(parts) == 1:
        return __import__(s)
    else:
        mod = __import__(parts[0], fromlist=[parts[1]])
        return getattr(mod, parts[1])
