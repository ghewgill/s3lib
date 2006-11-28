#!/usr/bin/env python

import os
import re
import shutil
import sys
import time
import urllib

import s3lib

s3 = None

def humantime(t):
    if time.time() - t < 180*86400:
        return time.strftime("%b %d %H:%M", time.localtime(t))
    else:
        return time.strftime("%b %d  %Y", time.localtime(t))

suffixes = ['B','K','M','G','T','P','E','Z','Y']
def metricsuffix(x):
    e = 0
    f = 1
    while len(str(int(x/f))) > 3 and e+1 < len(suffixes):
        e += 1
        f *= 1024
    if len(str(int(x/f))) == 1 and f > 1:
        return "%.1f%s" % (1.0*x/f, suffixes[e])
    else:
        return "%d%s" % (x/f, suffixes[e])

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

def do_create(argv):
    for a in range(len(argv)):
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        if "/" in name:
            print >>sys.stderr, "s3c: bucket name cannot contain /"
            sys.exit(1)
        r = s3.create(name)
        print "s3c: Created", r.getheader("Location")

def do_list(argv):
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

def do_ls(argv):
    # -r for recursive (list all subdirs flat)
    # -s for dir sizes (summary of dir sizes)
    # -a for acls
    # -h for metric size units
    recursive = False
    subdirs = False
    metric = False
    args = []
    for a in argv:
        if a == "-h":
            metric = True
            continue
        if a == "-r":
            recursive = True
            continue
        if a == "-s":
            subdirs = True
            continue
        args += [a]
    if metric:
        sizeconvert = lambda x: metricsuffix(x)
    else:
        sizeconvert = lambda x: x
    if len(args) == 0:
        args = ["/"]
    for a in args:
        if len(args) > 1:
            print "%s:" % a
        name = a
        if name[0] == "/":
            name = name[1:]
        m = re.match(r"(.*?)/(.+)", name)
        prefix = ""
        query = ""
        if m:
            bucket = m.group(1)
            prefix = m.group(2)
            if prefix[len(prefix)-1] != "/":
                prefix += "/"
            query = "?prefix="+urllib.quote(prefix)
        else:
            bucket = name
        if not recursive:
            if len(query) > 0:
                query += "&delimiter=/"
            else:
                query += "?delimiter=/"
        s = s3.list(bucket, query)
        if s['_tag'] == "ListAllMyBucketsResult":
            items = []
            for b in s['Buckets']:
                objects = []
                if subdirs:
                    objects = s3.list(b['Name'])['Contents']
                items += [(
                    'drw-------',
                    1,
                    s['Owner']['DisplayName'],
                    s['Owner']['DisplayName'],
                    sizeconvert(sum([int(x['Size']) for x in objects])),
                    humantime(reduce(max, [s3lib.parsetime(x['LastModified']) for x in objects], 0)),
                    b['Name']
                )]
            items.sort(lambda x, y: cmp(x[6], y[6]))
            print_columns((False,True,False,False,True,True,False), items)
        elif s['_tag'] == "ListBucketResult":
            items = []
            if len(s['CommonPrefixes']) > 0:
                bucketowner = s3.list("")['Owner']['DisplayName']
                for c in s['CommonPrefixes']:
                    objects = []
                    if subdirs:
                        objects = s3.list(bucket, "?prefix="+urllib.quote(c['Prefix']))['Contents']
                    items += [(
                        'drw-------',
                        1,
                        bucketowner,
                        bucketowner,
                        sizeconvert(sum([int(x['Size']) for x in objects])),
                        humantime(reduce(max, [s3lib.parsetime(x['LastModified']) for x in objects], 0)),
                        c['Prefix'][len(prefix):]
                    )]
            items += [(
                '-rw-------',
                1,
                c['Owner']['DisplayName'],
                c['Owner']['DisplayName'],
                sizeconvert(int(c['Size'])),
                humantime(s3lib.parsetime(c['LastModified'])),
                c['Key'][len(prefix):]
            ) for c in s['Contents']]
            items.sort(lambda x, y: cmp(x[6], y[6]))
            print_columns((False,True,False,False,True,True,False), items)
        if len(args) > 1:
            print

def do_get(argv):
    name = argv[0]
    filename = None
    if name[0] == "/":
        name = name[1:]
    if len(argv) > 1:
        filename = argv[1]
    outf = None
    if filename is not None:
        outf = file(filename, "wb")
    r = s3.get(name)
    if outf is None:
        shutil.copyfileobj(r, sys.stdout)
    else:
        shutil.copyfileobj(r, outf)
        outf.close()

def do_put(argv):
    neveroverwrite = False
    filename = None
    name = None
    for a in range(len(argv)):
        if argv[a] == "-n":
            neveroverwrite = True
            continue
        if name is None:
            name = argv[a]
        elif filename is None:
            filename = name
            name = argv[a]
        else:
            print >>sys.stderr, "s3c: Too many parameters for PUT"
            sys.exit(1)
    if name is None:
        print >>sys.stderr, "s3c: Name required on PUT"
        sys.exit(1)
    if name[0] == "/":
        name = name[1:]
    if "/" not in name:
        print >>sys.stderr, "s3c: Name for PUT must contain /"
        sys.exit(1)
    if neveroverwrite:
        m = re.match(r"(.*?)/(.+)", name)
        if m is None:
            print >>sys.stderr, "s3c: Name for PUT must contain /"
            sys.exit(1)
        bucket = m.group(1)
        prefix = m.group(2)
        try:
            # TODO: limit to 1 response?
            r = s3.list(bucket, "?prefix="+urllib.quote(prefix))
            if prefix in [x['Key'] for x in r['Contents']]:
                print >>sys.stderr, "s3c: file already exists and -n specified:", name
                sys.exit(1)
        except s3lib.S3Exception, e:
            if e.info['Code'] != "NoSuchKey":
                raise e
    if filename is not None:
        data = file(filename, "rb")
        r = s3.put(name, data)
        data.seek(0, 2)
        print "s3c: Put %s as %s (%d bytes, md5 %s)" % (filename, name, data.tell(), r.getheader("ETag"))
        data.close()
    else:
        MAX_SIZE = 1000000
        data = sys.stdin.read(MAX_SIZE)
        # create temporary file if it's too big to hold in memory
        tf = None
        if len(data) >= MAX_SIZE:
            tf = os.tmpfile()
            tf.write(data)
            shutil.copyfileobj(sys.stdin, tf)
            data = tf
        r = s3.put(name, data)
        if tf is not None:
            print "s3c: Put %s (%d bytes, md5 %s)" % (name, tf.tell(), r.getheader("ETag"))
            tf.close()
        else:
            print "s3c: Put %s (%d bytes, md5 %s)" % (name, len(data), r.getheader("ETag"))

def do_delete(argv):
    force = False
    for a in range(len(argv)):
        if argv[a] == "-f":
            force = True
            continue
        name = argv[a]
        if name[0] == "/":
            name = name[1:]
        try:
            s3.delete(name)
        except s3lib.S3Exception, e:
            if force and e.info['Code'] == "BucketNotEmpty":
                keys = s3.list(name)
                for k in [x['Key'] for x in keys['Contents']]:
                    s3.delete(name+"/"+k)
                s3.delete(name)
            else:
                raise e
        print "s3c: Deleted", name

def main():
    global s3
    access = None
    secret = None
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
                print >>sys.stderr, "s3c: Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            command = sys.argv[a]
            a += 1
            break
    try:
        s3 = s3lib.S3Store(access, secret)
        if command == "create":
            do_create(sys.argv[a:])
        elif command == "list":
            do_list(sys.argv[a:])
        elif command == "ls":
            do_ls(sys.argv[a:])
        elif command == "get":
            do_get(sys.argv[a:])
        elif command == "put":
            do_put(sys.argv[a:])
        elif command == "delete":
            do_delete(sys.argv[a:])
        else:
            print >>sys.stderr, "s3c: Unknown command:", command
            sys.exit(1)
    except s3lib.S3Exception, e:
        print >>sys.stderr, "s3c: Error %s: %s" % (e.info['Code'], e.info['Message'])
        sys.exit(1)

if __name__ == "__main__":
    main()
