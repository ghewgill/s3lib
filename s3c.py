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
    f = open(os.environ['HOME']+"/.s3crc")
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
    if 'S3ACCESS' in os.environ:
        Access = os.environ['S3ACCESS']
    if 'S3SECRET' in os.environ:
        Secret = os.environ['S3SECRET']

def now():
    return time.strftime("%a, %d %b %Y %T GMT", time.gmtime())

def create(name):
    t = now()
    s3.request("PUT", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+Access+":"+base64.encodestring(hmac.new(Secret, "PUT\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        print r.getheader("Location")
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def list(name):
    t = now()
    s3.request("GET", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+Access+":"+base64.encodestring(hmac.new(Secret, "GET\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
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
    t = now()
    data = sys.stdin.read()
    s3.request("PUT", "/"+name, data, headers = {'Date': t, 'Content-type': "text/html", 'x-amz-acl': "public-read", 'Authorization': "AWS "+Access+":"+base64.encodestring(hmac.new(Secret, "PUT\n\ntext/html\n"+t+"\nx-amz-acl:public-read\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def get(name):
    t = now()
    s3.request("GET", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+Access+":"+base64.encodestring(hmac.new(Secret, "GET\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def delete(name):
    t = now()
    s3.request("DELETE", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+Access+":"+base64.encodestring(hmac.new(Secret, "DELETE\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 204:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        print r.read()
        sys.exit(1)

def main():
    readConfig()
    global Access, Secret, s3
    s3 = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)
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

if __name__ == "__main__":
    main()
