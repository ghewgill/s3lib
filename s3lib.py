import base64
import calendar
import hmac
import httplib
import re
import sha
import time
import urllib
import xml.dom.minidom

def makestruct(e, arrays = {}):
    r = {}
    r['_tag'] = e.tagName
    for a in arrays:
        r[a] = []
    for m in e.childNodes:
        if m.nodeType == xml.dom.Node.ELEMENT_NODE:
            if m.tagName in arrays:
                if arrays[m.tagName] is not None:
                    r[m.tagName] = [makestruct(x) for x in m.getElementsByTagName(arrays[m.tagName])]
                else:
                    r[m.tagName] += [makestruct(m)]
            else:
                r[m.tagName] = makestruct(m)
        elif m.nodeType == xml.dom.Node.TEXT_NODE:
            return m.data
    return r

def parsetime(ts):
    m = re.match(r"(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.(\d+))?Z$", ts)
    return calendar.timegm([int(m.group(i)) for i in range(1, 7)])

class S3Exception(Exception):
    def __init__(self, r):
        Exception.__init__(self)
        self.status = r.status
        doc = xml.dom.minidom.parseString(r.read())
        self.info = makestruct(doc.documentElement)

    def __str__(self):
        if 'BucketName' in self.info:
            return "%s: %s (%s)" % (self.info['Code'], self.info['Message'], self.info['BucketName'])
        else:
            return "%s: %s" % (self.info['Code'], self.info['Message'])

class HTTPResponseLogger:
    def __init__(self, r, logfile, format):
        self.r = r
        self.logfile = logfile
        self.format = format
        self.total = 0

    def read(self, n = None):
        if n is None:
            buf = self.r.read()
            log = open(self.logfile, "a")
            print >>log, self.format % len(buf)
            log.close()
        else:
            buf = self.r.read(n)
            self.total += len(buf)
            if buf is None:
                log = open(self.logfile, "a")
                print >>log, self.format % self.total
                log.close()
        return buf

    def __getattr__(self, name):
        return self.r.__dict__[name]

class S3Store:
    def __init__(self, access, secret, logfile):
        self.access = access
        self.secret = secret
        self.logfile = logfile
        self.server = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)

    def create(self, bucket):
        r = self._exec("PUT", "/"+urllib.quote(bucket))
        if r.status != 200:
            raise S3Exception(r)
        r.read()
        return r

    def list(self, bucket, query = ""):
        ret = None
        marker = None
        while True:
            q = query
            if marker is not None:
                if len(q) == 0:
                    q += "?"
                else:
                    q += "&"
                q += "marker=" + marker
            r = self.get(bucket, q)
            data = r.read()
            doc = xml.dom.minidom.parseString(data)
            if doc.documentElement.tagName == "ListAllMyBucketsResult":
                assert bucket == ""
                return makestruct(doc.documentElement, {'Buckets': "Bucket"})
            elif doc.documentElement.tagName == "ListBucketResult":
                assert bucket != ""
                s = makestruct(doc.documentElement, {'Contents': None, 'CommonPrefixes': None})
                if ret is None:
                    ret = s
                else:
                    ret['Contents'] += s['Contents']
                    ret['CommonPrefixes'] += s['CommonPrefixes']
                if s['IsTruncated'] == "false":
                    break
                if 'NextMarker' in s:
                    marker = s['NextMarker']
                else:
                    marker = s['Contents'][-1]['Key']
            else:
                raise Exception("s3c: Error: Unexpected element: %s" % doc.documentElement.tagName)
        return ret

    def get(self, name, query = ""):
        r = self._exec("GET", "/"+urllib.quote(name), query = query)
        if r.status != 200:
            raise S3Exception(r)
        return r

    def put(self, name, data):
        r = self._exec("PUT", "/"+urllib.quote(name), data)
        if r.status != 200:
            raise S3Exception(r)
        r.read()
        return r

    def delete(self, name):
        r = self._exec("DELETE", "/"+urllib.quote(name))
        if r.status != 204:
            raise S3Exception(r)
        r.read()
        return r

    def _exec(self, method, name, data = None, headers = None, query = ""):
        if headers is None:
            headers = {}
        if not 'Date' in headers:
            headers['Date'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        sig = method + "\n"
        if 'Content-MD5' in headers:
            sig += headers['Content-MD5']
        sig += "\n"
        if 'Content-Type' in headers:
            sig += headers['Content-Type']
        sig += "\n"
        sig += headers['Date'] + "\n"
        # TODO: headers
        sig += name
        headers['Authorization'] = "AWS %s:%s" % (self.access, base64.encodestring(hmac.new(self.secret, sig, sha).digest()).strip())
        tries = 0
        delay = 0.1
        while True:
            self.server.request(method, name+query, data, headers)
            r = self.server.getresponse()
            if r.status < 300:
                break
            e = S3Exception(r)
            if e.info['Code'] != "InternalError":
                raise e
            #print >>sys.stderr, "got InternalError, sleeping", delay
            time.sleep(delay)
            delay *= 2
            tries += 1
            if tries >= 5:
                raise e
        if self.logfile is not None:
            line = "%s %d %s %s%s" % (self.access, time.time(), method, name, query)
            if method == "GET":
                r = HTTPResponseLogger(r, self.logfile, line + " %d")
            elif method == "PUT":
                log = open(self.logfile, "a")
                print >>log, line, len(data)
                log.close()
            else:
                log = open(self.logfile, "a")
                print >>log, line
                log.close()
        return r

