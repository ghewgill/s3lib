#!/usr/bin/env python

import cPickle
import hmac
import md5
import os
import re
import sha
import stat
import sys

import s3lib

from pprint import pprint

class Config:
    def __init__(self):
        self.Access = None
        self.Secret = None
        self.Logfile = None
        self.Encrypt = None
        self.EncryptNames = False
        self.IgnoreManifest = False

Config = Config()
s3 = None

def readConfig():
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
                Config.Access = m.group(2)
            elif m.group(1) == "secret":
                Config.Secret = m.group(2)
            elif m.group(1) == "logfile":
                Config.Logfile = m.group(2)
            else:
                continue
        f.close()
    except:
        if f is not None:
            f.close()
    if 'S3ACCESS' in os.environ:
        Config.Access = os.environ['S3ACCESS']
    if 'S3SECRET' in os.environ:
        Config.Secret = os.environ['S3SECRET']

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
    f = open(fn)
    while True:
        buf = f.read(16384)
        if not buf:
            break
        h.update(buf)
    return h

def main():
    global s3
    readConfig()
    a = 1
    source = None
    dest = None
    while a < len(sys.argv):
        if sys.argv[a][0] == "-":
            if sys.argv[a] == "-a" or sys.argv[a] == "--access":
                a += 1
                Config.Access = sys.argv[a]
            elif sys.argv[a] == "-s" or sys.argv[a] == "--secret":
                a += 1
                Config.Secret = sys.argv[a]
            elif sys.argv[a] == "--encrypt":
                a += 1
                Config.Encrypt = sys.argv[a]
            elif sys.argv[a] == "--encrypt-names":
                Config.EncryptNames = True
            elif sys.argv[a] == "--ignore-manifest":
                Config.IgnoreManifest = True
            else:
                print >>sys.stderr, "s3mirror: Unknown option:", sys.argv[a]
                sys.exit(1)
        else:
            if source is None:
                source = sys.argv[a]
            elif dest is None:
                dest = sys.argv[a]
            else:
                print >>sys.stderr, "s3mirror: too many arguments"
                sys.exit(1)
        a += 1
    if Config.Access is None or Config.Secret is None:
        print >>sys.stderr, "s3mirror: Need access and secret"
        sys.exit(1)
    s3 = s3lib.S3Store(Config.Access, Config.Secret, Config.Logfile)
    for base, dirs, files in os.walk(source):
        assert base.startswith(source)
        prefix = base[len(source):]
        if not prefix.startswith("/"):
            prefix = "/"+prefix
        if not prefix.endswith("/"):
            prefix += "/"
        manifest = None
        if not Config.IgnoreManifest:
            try:
                mf = open(os.path.join(base, ".s3mirror-MANIFEST"))
                mfdata = mf.read()
                mf.close()
                manifest = cPickle.loads(mfdata)
            except IOError:
                pass
            if manifest is None:
                try:
                    print "s3mirror: Fetching %s" % (dest+prefix+".s3mirror-MANIFEST")
                    mfdata = s3.get(dest+prefix+".s3mirror-MANIFEST").read()
                    if Config.EncryptNames:
                        mfdata = karn_decrypt(mfdata, Config.Secret)
                    manifest = cPickle.loads(mfdata)
                except s3lib.S3Exception:
                    pass
                #current = s3.list(dest, query = "?prefix="+prefix)
                #mfi = [x for x in current['Contents'] if x['Key'] == prefix+".s3mirror-MANIFEST"]
                #if len(mfi):
                #    try:
                #        mf = open(os.path.join(base, ".s3mirror-MANIFEST"))
                #        mfdata = mf.read()
                #        mf.close()
                #        hash = md5.new(mfdata).hexdigest()
                #        if hash in mfi[0]['ETag']:
                #            manifest = cPickle.loads(mfdata)
                #        else:
                #            manifest = cPickle.loads(s3.get(dest+prefix+".s3mirror-MANIFEST").read())
                #    except IOError:
                #        manifest = cPickle.loads(s3.get(dest+prefix+".s3mirror-MANIFEST").read())
        if manifest is None:
            manifest = {}
        changed = False
        for name in files:
            if name == ".s3mirror-MANIFEST":
                continue
            fn = os.path.join(base, name)
            t = os.stat(fn)[stat.ST_MTIME]
            h = md5file(fn).hexdigest()
            if name not in manifest or h != manifest[name]['h']:
                print fn
                if Config.Encrypt:
                    f = os.popen("bzip2 <\""+fn+"\" | gpg --encrypt -r "+Config.Encrypt)
                else:
                    f = os.popen("bzip2 <\""+fn+"\"")
                if Config.EncryptNames:
                    sname = dest+"/"+hmac.new(Config.Secret, prefix+name, sha).hexdigest()
                elif Config.Encrypt is not None:
                    sname = dest+prefix+name+".bz2.gpg"
                else:
                    sname = dest+prefix+name+".bz2"
                r = s3.put(sname, f.read())
                manifest[name] = {
                    't': t,
                    'h': h,
                    'm': r.getheader("ETag")
                }
                changed = True
        if changed:
            mfdata = cPickle.dumps(manifest)
            if Config.EncryptNames:
                s3.put(dest+prefix+".s3mirror-MANIFEST", karn_encrypt(mfdata, Config.Secret))
            else:
                s3.put(dest+prefix+".s3mirror-MANIFEST", mfdata)
            try:
                mf = open(os.path.join(base, ".s3mirror-MANIFEST"), "w")
                mf.write(mfdata)
                mf.close()
            except IOError:
                pass

if __name__ == "__main__":
    main()
