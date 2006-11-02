import os
import re
import shutil
import sys
import time
import urllib

import s3lib

Access = None
Secret = None

def readConfig():
    global Access, Secret
    fn = ".s3crc"
    if 'S3CRC' in os.environ:
        fn = os.environ['S3CRC']
    elif 'HOME' in os.environ:
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

def humantime(t):
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

def create(argv):
    for a in range(len(argv)):
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        if "/" in name:
            print >>sys.stderr, "s3c: bucket name cannot contain /"
            sys.exit(1)
        r = s3.create(name)
        print "s3c: Created", r.getheader("Location")

def list(argv):
    if len(argv) == 0:
        argv = ["/"]
    for a in range(len(argv)):
        if len(argv) > 1:
            print "%s:" % argv[a]
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        if "/" in name:
            print >>sys.stderr, "s3c: bucket name cannot contain /"
            sys.exit(1)
        s = s3.list(name)
        if s['_tag'] == "ListAllMyBucketsResult":
            for b in s['Buckets']:
                print b['Name']
        elif s['_tag'] == "ListBucketResult":
            for c in s['Contents']:
                print c['Key']
        if len(argv) > 1:
            print

def ls(argv):
    if len(argv) == 0:
        argv = ["/"]
    for a in range(len(argv)):
        if len(argv) > 1:
            print "%s:" % argv[a]
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        m = re.match(r"(.*?)/(.+)", name)
        prefix = ""
        if m:
            bucket = m.group(1)
            prefix = m.group(2)
            if prefix[len(prefix)-1] != "/":
                prefix += "/"
            s = s3.list(bucket, "?prefix="+urllib.quote(prefix)+"&delimiter=/")
        else:
            bucket = name
            s = s3.list(bucket, "?delimiter=/")
        if s['_tag'] == "ListAllMyBucketsResult":
            items = []
            for b in s['Buckets']:
                objects = s3.list(b['Name'])['Contents']
                items += [(
                    'drw-------',
                    1,
                    s['Owner']['DisplayName'],
                    s['Owner']['DisplayName'],
                    sum([int(x['Size']) for x in objects]),
                    humantime(max([s3lib.parsetime(x['LastModified']) for x in objects])),
                    b['Name']
                )]
            items.sort(lambda x, y: cmp(x[6], y[6]))
            print_columns((False,True,False,False,True,True,False), items)
        elif s['_tag'] == "ListBucketResult":
            items = []
            if len(s['CommonPrefixes']) > 0:
                bucketowner = s3.list("")['Owner']['DisplayName']
                for c in s['CommonPrefixes']:
                    objects = s3.list(bucket, "?prefix="+urllib.quote(c['Prefix']))['Contents']
                    items += [(
                        'drw-------',
                        1,
                        bucketowner,
                        bucketowner,
                        sum([int(x['Size']) for x in objects]),
                        humantime(max([s3lib.parsetime(x['LastModified']) for x in objects])),
                        c['Prefix']
                    )]
            items += [(
                '-rw-------',
                1,
                c['Owner']['DisplayName'],
                c['Owner']['DisplayName'],
                c['Size'],
                humantime(s3lib.parsetime(c['LastModified'])),
                c['Key'][len(prefix):]
            ) for c in s['Contents']]
            items.sort(lambda x, y: cmp(x[6], y[6]))
            print_columns((False,True,False,False,True,True,False), items)
        if len(argv) > 1:
            print

def get(argv):
    name = argv[0]
    if name[0] == "/":
        name = name[1:]
    r = s3.get(name)
    shutil.copyfileobj(r, sys.stdout)

def put(argv):
    name = argv[0]
    if name[0] == "/":
        name = name[1:]
    data = sys.stdin.read()
    r = s3.put(name, data)
    print "s3c: Put %s (%d bytes, md5 %s)" % (name, len(data), r.getheader("ETag"))

def delete(argv):
    for a in range(len(argv)):
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        s3.delete(name)
        print "s3c: Deleted", name

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
                print >>sys.stderr, "s3c: Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            command = sys.argv[a]
            a += 1
            break
    if Access is None or Secret is None:
        print >>sys.stderr, "s3c: Need access and secret"
        sys.exit(1)
    s3 = s3lib.S3Store(Access, Secret)
    if command == "create":
        create(sys.argv[a:])
    elif command == "list":
        list(sys.argv[a:])
    elif command == "ls":
        ls(sys.argv[a:])
    elif command == "get":
        get(sys.argv[a:])
    elif command == "put":
        put(sys.argv[a:])
    elif command == "delete":
        delete(sys.argv[a:])
    else:
        print >>sys.stderr, "s3c: Unknown command:", command

if __name__ == "__main__":
    main()
