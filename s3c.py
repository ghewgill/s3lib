import base64
import hmac
import httplib
import os
import re
import sha
#import SOAPpy
import sys
import time

access = None
secret = None

def readConfig():
    global access, secret
    f = open(os.environ['HOME']+"/.s3crc")
    for s in f:
        m = re.match(r"(\w+)\s+(\S+)", s)
        if not m:
            continue
        if m.group(1) == "access":
            access = m.group(2)
        elif m.group(1) == "secret":
            secret = m.group(2)
        else:
            continue
    f.close()
    if 'S3ACCESS' in os.environ:
        access = os.environ['S3ACCESS']
    if 'S3SECRET' in os.environ:
        secret = os.environ['S3SECRET']

#   SOAP stuff:

#   def now():
#       return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

#   def create(name):
#       p = {
#           'Bucket': name,
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"CreateBucket"+p['Timestamp'], sha).digest())
#       print s3.CreateBucket(**p)

#   def list(name):
#       p = {
#           'Bucket': name,
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"ListBucket"+p['Timestamp'], sha).digest())
#       print s3.ListBucket(**p)

#   def listbuckets():
#       p = {
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"ListAllMyBuckets"+p['Timestamp'], sha).digest())
#       print s3.ListAllMyBuckets(**p)

#   def put(name):
#       m = re.match(r"([^/]+)/(.+)", name)
#       bucket = m.group(1)
#       key = m.group(2)
#       data = sys.stdin.read()
#       p = {
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#           'Bucket': bucket,
#           'Key': key,
#           'Data': base64.encodestring(data),
#           'ContentLength': len(data),
#           'Metadata': {'Name': "Content-Type", 'Value': "text/plain"},
#           #'AccessControlList': {},
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"PutObjectInline"+p['Timestamp'], sha).digest())
#       print s3.PutObjectInline(**p)

#   def get(name):
#       m = re.match(r"([^/]+)/(.+)", name)
#       bucket = m.group(1)
#       key = m.group(2)
#       p = {
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#           'Bucket': bucket,
#           'Key': key,
#           'GetData': True,
#           'InlineData': True,
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"GetObject"+p['Timestamp'], sha).digest())
#       print s3.GetObject(**p)

#   def delete(name):
#       m = re.match(r"([^/]+)/(.+)", name)
#       bucket = m.group(1)
#       key = m.group(2)
#       p = {
#           'AWSAccessKeyId': access,
#           'Timestamp': now(),
#           'Bucket': bucket,
#           'Key': key,
#       }
#       p['Signature'] = base64.encodestring(hmac.new(secret, "AmazonS3"+"DeleteObject"+p['Timestamp'], sha).digest())
#       print s3.DeleteObject(**p)

def now():
    return time.strftime("%a, %d %b %Y %T GMT", time.gmtime())

def listbuckets():
    t = now()
    s3.request("GET", "/", headers = {'Date': t, 'Authorization': "AWS "+access+":"+base64.encodestring(hmac.new(secret, "GET\n\n\n"+t+"\n/", sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        sys.exit(1)

def create(name):
    t = now()
    s3.request("PUT", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+access+":"+base64.encodestring(hmac.new(secret, "PUT\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        sys.exit(1)

def list(name):
    t = now()
    s3.request("GET", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+access+":"+base64.encodestring(hmac.new(secret, "GET\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        sys.exit(1)

def put(name):
    t = now()
    data = sys.stdin.read()
    s3.request("PUT", "/"+name, data, headers = {'Date': t, 'Content-type': "text/html", 'x-amz-acl': "public-read", 'Authorization': "AWS "+access+":"+base64.encodestring(hmac.new(secret, "PUT\n\ntext/html\n"+t+"\nx-amz-acl:public-read\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        sys.exit(1)

def get(name):
    t = now()
    s3.request("GET", "/"+name, headers = {'Date': t, 'Authorization': "AWS "+access+":"+base64.encodestring(hmac.new(secret, "GET\n\n\n"+t+"\n/"+name, sha).digest()).strip()})
    r = s3.getresponse()
    if r.status == 200:
        sys.stdout.write(r.read())
    else:
        print >>sys.stderr, "Error:", r.status
        sys.exit(1)

def main():
    readConfig()
    global access, secret, s3
    #s3 = SOAPpy.SOAPProxy("https://s3.amazonaws.com/soap", namespace="http://s3.amazonaws.com/doc/2006-03-01/", config = SOAPpy.SOAPConfig(dumpSOAPOut = True))
    s3 = httplib.HTTPSConnection("s3.amazonaws.com")
    a = 1
    command = None
    while a < len(sys.argv):
        if sys.argv[a][0] == "-":
            if sys.argv[a] == "-a" or sys.argv[a] == "--access":
                a += 1
                access = sys.argv[a]
            elif sys.argv[a] == "-s" or sys.argv[a] == "--secret":
                a += 1
                secret = sys.argv[a]
            else:
                print >>sys.stderr, "Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            command = sys.argv[a]
            a += 1
            break
    if command == "listbuckets":
        listbuckets()
    elif command == "create":
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
