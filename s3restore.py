#!/usr/bin/env python

import cPickle
import hmac
import md5
import os
import re
import sha
import shutil
import stat
import sys
import urllib

import s3lib

from pprint import pprint

class Config:
    def __init__(self):
        self.DryRun = False

Config = Config()
s3 = None

def shellquote(s):
    return "'" + s.replace("'", "'\\''") + "'"

def xor(x, y):
    assert len(x) == len(y)
    r = ""
    for i in range(len(x)):
        r += chr(ord(x[i]) ^ ord(y[i]))
    return r

def karn_encrypt(s, k):
    K = sha.new(k).digest()
    assert len(K) == 20
    K1 = K[:10]
    K2 = K[10:]
    if len(s) % 20 > 0:
        s += ' ' * (20 - len(s) % 20)
    r = ""
    i = 0
    while i < len(s):
        T1 = s[i:i+10]
        T2 = s[i+10:i+20]
        C2 = xor(sha.new(K1+T1).digest()[:10], T2)
        C1 = xor(sha.new(K2+C2).digest()[:10], T1)
        r += C1 + C2
        i += 20
    return r

def karn_decrypt(s, k):
    K = sha.new(k).digest()
    assert len(K) == 20
    K1 = K[:10]
    K2 = K[10:]
    assert len(s) % 20 == 0
    r = ""
    i = 0
    while i < len(s):
        C1 = s[i:i+10]
        C2 = s[i+10:i+20]
        T1 = xor(sha.new(K2+C2).digest()[:10], C1)
        T2 = xor(sha.new(K1+T1).digest()[:10], C2)
        r += T1 + T2
        i += 20
    return r

def md5file(fn):
    h = md5.new()
    f = open(fn, "rb")
    while True:
        buf = f.read(16384)
        if not buf:
            break
        h.update(buf)
    f.close()
    return h

def main():
    global s3
    access = None
    secret = None
    source = None
    dest = None
    a = 1
    while a < len(sys.argv):
        if sys.argv[a][0] == "-":
            if sys.argv[a] == "-a" or sys.argv[a] == "--access":
                a += 1
                access = sys.argv[a]
            elif sys.argv[a] == "-s" or sys.argv[a] == "--secret":
                a += 1
                secret = sys.argv[a]
            elif sys.argv[a] == "--dry-run":
                Config.DryRun = True
            else:
                print >>sys.stderr, "s3mirror: Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            if source is None:
                source = sys.argv[a]
            elif dest is None:
                dest = sys.argv[a]
            else:
                print >>sys.stderr, "s3restore: too many arguments"
                sys.exit(1)
        a += 1
    s3 = s3lib.S3Store(access, secret)
    i = source.find("/")
    prefix = source[i+1:]
    if not prefix.endswith("/"):
        prefix += "/"
    query = "?prefix="+urllib.quote(prefix)
    bucket = source[:i]
    files = s3.list(bucket, query)
    dirs = {}
    for f in files['Contents']:
        if f['Key'].endswith(".bz2"):
            fn = f['Key'][:-4]
            fn = fn[len(prefix):]
            pfn = os.path.join(dest, fn)
            print pfn
            dir = os.path.dirname(pfn)
            if dir not in dirs:
                try:
                    os.makedirs(dir)
                except OSError:
                    pass
                dirs[dir] = True
            outf = os.popen("bunzip2 >"+shellquote(pfn), "wb")
            r = s3.get(source+"/"+fn+".bz2")
            shutil.copyfileobj(r, outf)
            outf.close()

if __name__ == "__main__":
    main()
