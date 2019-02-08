class Properties(object):
    def __init__(self, filepath=None):
        self.ready = True
        self.err = ''
        self.d = {}
        if filepath is None:
            return
        try:
            f = open(filepath)
            for line in f:
                line = line.rstrip()
                if "#" in line or "=" not in line:
                    continue
                loc = line.find("=")
                if loc > 0 and loc < len(line)-1:
                    key = line[0:loc]
                    val = line[loc+1:]
                    self.d[key] = val
            f.close()
        except IOError as ioe:
            self.err = "%s" % ioe
            self.ready = False

    def get_value(self, key, default_value):
        if self.d.has_key(key):
            return self.d[key]
        else:
            return default_value
        
    def set_value(self, key, value):
        self.d[key] = value

    def isReady(self):
        return self.ready

    def getLastError(self):
        return self.err
        