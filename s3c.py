import base64
import hmac
import httplib
import os
import re
import sha
import sys
import time
import xml.dom.minidom

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

class S3Store:
    def __init__(self, access, secret):
        self.access = access
        self.secret = secret
        self.server = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)

    def _exec(self, method, name, data = None, headers = {}):
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
        self.server.request(method, name, data, headers)
        return self.server.getresponse()

def create(name):
    r = s3._exec("PUT", "/"+name)
    if r.status == 200:
        print r.getheader("Location")
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def list(name):
    r = s3._exec("GET", "/"+name)
    if r.status == 200:
        data = r.read()
        #print data
        doc = xml.dom.minidom.parseString(data)
        if doc.documentElement.tagName == "ListAllMyBucketsResult":
            for b in doc.getElementsByTagName("Bucket"):
                print b.getElementsByTagName("Name")[0].firstChild.data
        elif doc.documentElement.tagName == "ListBucketResult":
            for b in doc.getElementsByTagName("Contents"):
                print b.getElementsByTagName("Key")[0].firstChild.data
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

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
    r = s3._exec("GET", "/"+name)
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def delete(name):
    r = s3._exec("DELETE", "/"+name)
    if r.status == 204:
        sys.stdout.write(r.read())
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
