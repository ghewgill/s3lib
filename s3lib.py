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

class S3Store:
    def __init__(self, access, secret):
        self.access = access
        self.secret = secret
        self.server = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)

    def create(self, bucket):
        r = self._exec("PUT", "/"+bucket)
        if r.status != 200:
            print >>sys.stderr, "s3c: Error:", r.status
            print r.read()
            sys.exit(1)
        return r

    def list(self, bucket, query = ""):
        r = self.get(bucket, query)
        data = r.read()
        doc = xml.dom.minidom.parseString(data)
        if doc.documentElement.tagName == "ListAllMyBucketsResult":
            assert bucket == ""
            return makestruct(doc.documentElement, {'Buckets': "Bucket"})
        elif doc.documentElement.tagName == "ListBucketResult":
            assert bucket != ""
            return makestruct(doc.documentElement, {'Contents': None, 'CommonPrefixes': None})
        print >>sys.stderr, "s3c: Error: Unexpected element:", doc.documentElement.tagName
        sys.exit(1)

    def get(self, name, query = ""):
        r = self._exec("GET", "/"+urllib.quote(name), query = query)
        if r.status != 200:
            print >>sys.stderr, "s3c: Error:", r.status
            print r.read()
            sys.exit(1)
        return r

    def put(self, name, data):
        r = self._exec("PUT", "/"+name, data)
        if r.status != 200:
            print >>sys.stderr, "s3c: Error:", r.status
            print r.read()
            sys.exit(1)
        return r

    def delete(self, name):
        r = self._exec("DELETE", "/"+name)
        if r.status != 204:
            print >>sys.stderr, "s3c: Error:", r.status
            print r.read()
            sys.exit(1)
        return r

    def _exec(self, method, name, data = None, headers = {}, query = ""):
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
        self.server.request(method, name+query, data, headers)
        return self.server.getresponse()

