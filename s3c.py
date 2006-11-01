import base64
import calendar
import hmac
import httplib
import os
import re
import sha
import shutil
import sys
import time
import urllib
import xml.dom.minidom

from pprint import pprint

# TODO

Access = None
Secret = None

def readConfig():
    global Access, Secret
    fn = ".s3crc"
    if 'HOME' in os.environ:
        fn = os.environ['HOME']+os.path.sep+fn
    elif 'HOMEDRIVE' in os.environ and 'HOMEPATH' in os.environ:
        fn = os.environ['HOMEDRIVE']+os.environ['HOMEPATH']+os.path.sep+fn
    f = None
    try:
        f = open(fn)
        for s in f:
            m = re.match(r"(\w+)\s+(\S+)", s)
            if not m:
                continue
            if m.group(1) == "access":
                Access = m.group(2)
            elif m.group(1) == "secret":
                Secret = m.group(2)
            else:
                continue
        f.close()
    except:
        if f is not None:
            f.close()
    if 'S3ACCESS' in os.environ:
        Access = os.environ['S3ACCESS']
    if 'S3SECRET' in os.environ:
        Secret = os.environ['S3SECRET']

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

def humantime(ts):
    m = re.match(r"(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.(\d+))Z$", ts)
    t = calendar.timegm([int(m.group(i)) for i in range(1, 7)])
    if time.time() - t < 180*86400:
        return time.strftime("%b %d %H:%M", time.localtime(t))
    else:
        return time.strftime("%b %d  %Y", time.localtime(t))

def print_columns(align, data):
    maxwidth = [reduce(max, [len(str(x[c])) for x in data], 0) for c in range(len(align))]
    for d in data:
        s = ""
        for c in range(len(align)):
            if align[c]:
                s += " "*(maxwidth[c]-len(str(d[c]))) + str(d[c]) + " "
            elif c+1 < len(align):
                s += str(d[c]) + " "*(maxwidth[c]-len(str(d[c]))) + " "
            else:
                s += str(d[c])
        print s

class S3Store:
    def __init__(self, access, secret):
        self.access = access
        self.secret = secret
        self.server = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)

    def _exec(self, method, name, data = None, headers = {}, query = None):
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

def _get(name, query):
    r = s3._exec("GET", "/"+name, query = query)
    if r.status == 200:
        return r
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def create(name):
    r = s3._exec("PUT", "/"+name)
    if r.status == 200:
        print "Created:", r.getheader("Location")
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def list(name):
    r = _get(name)
    data = r.read()
    doc = xml.dom.minidom.parseString(data)
    if doc.documentElement.tagName == "ListAllMyBucketsResult":
        s = makestruct(doc.documentElement, {'Buckets': "Bucket"})
        for b in s['Buckets']:
            print b['Name']
    elif doc.documentElement.tagName == "ListBucketResult":
        s = makestruct(doc.documentElement, {'Contents': None})
        for c in s['Contents']:
            print c['Key']

def ls(name):
    m = re.match(r"(.*?)/(.*)", name)
    prefix = ""
    if m:
        bucket = m.group(1)
        prefix = m.group(2) + "/"
        r = _get(bucket, "?prefix="+urllib.quote(prefix)+"&delimiter=/")
    else:
        r = _get(name, "?delimiter=/")
    data = r.read()
    doc = xml.dom.minidom.parseString(data)
    if doc.documentElement.tagName == "ListAllMyBucketsResult":
        s = makestruct(doc.documentElement, {'Buckets': "Bucket"})
        for b in s['Buckets']:
            print b['Name']
    elif doc.documentElement.tagName == "ListBucketResult":
        s = makestruct(doc.documentElement, {'Contents': None, 'CommonPrefixes': None})
        items = [('-rw-------',1,c['Owner']['DisplayName'],c['Owner']['DisplayName'],c['Size'],humantime(c['LastModified']),c['Key'][len(prefix):]) for c in s['Contents']] + [('drw-------',1,'-','-',0,0,c['Prefix']) for c in s['CommonPrefixes']]
        items.sort(lambda x, y: cmp(x[6], y[6]))
        print_columns((False,True,False,False,True,False,False), items)

def put(name):
    data = sys.stdin.read()
    r = s3._exec("PUT", "/"+name, data)
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def get(name):
    r = _get(name)
    shutil.copyfileobj(r, sys.stdout)

def delete(name):
    r = s3._exec("DELETE", "/"+name)
    if r.status == 204:
        print "Deleted:", name
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def main():
    readConfig()
    global Access, Secret, s3
    a = 1
    command = None
    while a < len(sys.argv):
        if sys.argv[a][0] == "-":
            if sys.argv[a] == "-a" or sys.argv[a] == "--access":
                a += 1
                Access = sys.argv[a]
            elif sys.argv[a] == "-s" or sys.argv[a] == "--secret":
                a += 1
                Secret = sys.argv[a]
            else:
                print >>sys.stderr, "Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            command = sys.argv[a]
            a += 1
            break
    if Access is None or Secret is None:
        print >>sys.stderr, "Need access and secret"
        sys.exit(1)
    s3 = S3Store(Access, Secret)
    if command == "create":
        create(sys.argv[a])
    elif command == "list":
        if a < len(sys.argv):
            list(sys.argv[a])
        else:
            list("")
    elif command == "ls":
        if a < len(sys.argv):
            ls(sys.argv[a])
        else:
            ls("")
    elif command == "put":
        put(sys.argv[a])
    elif command == "get":
        get(sys.argv[a])
    elif command == "delete":
        delete(sys.argv[a])
    else:
        print >>sys.stderr, "Unknown command:", command

if __name__ == "__main__":
    main()
