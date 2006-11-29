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

import s3lib

from pprint import pprint

class Config:
    def __init__(self):
        self.Encrypt = None
        self.EncryptNames = False
        self.IgnoreManifest = False

Config = Config()
s3 = None

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
    f.close()
    return h

def scanfiles(source, dest):
    todo = []
    for base, dirs, files in os.walk(source):
        tododir = {'dir': base, 'files': []}
        assert base.startswith(source)
        prefix = base[len(source):]
        if not prefix.startswith("/"):
            prefix = "/"+prefix
        if not prefix.endswith("/"):
            prefix += "/"
        tododir['prefix'] = prefix
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
                    mfdata = s3.get(dest+prefix+".s3mirror-MANIFEST").read()
                    print "s3mirror: Fetching %s" % (dest+prefix+".s3mirror-MANIFEST")
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
        tododir['manifest'] = manifest
        changed = False
        for name in files:
            if name == ".s3mirror-MANIFEST":
                continue
            fn = os.path.join(base, name)
            try:
                st = os.stat(fn)
                t = st[stat.ST_MTIME]
                if not stat.S_ISREG(st.st_mode):
                    continue
                h = md5file(fn).hexdigest()
            except (IOError, OSError):
                print >>sys.stderr, "s3mirror: File vanished:", fn
                continue
            if name not in manifest or h != manifest[name]['h']:
                tododir['files'].append({
                    'name': name,
                    'size': st[stat.ST_SIZE],
                    't': t,
                    'h': h,
                })
        if len(tododir['files']) > 0:
            todo.append(tododir)
    return todo

def sendfiles(todo, dest):
    total = sum([sum([f['size'] for f in d['files']]) for d in todo])
    done = 0
    for dir in todo:
        base = dir['dir']
        prefix = dir['prefix']
        manifest = dir['manifest']
        for finfo in dir['files']:
            name = finfo['name']
            t = finfo['t']
            h = finfo['h']
            fn = os.path.join(base, name)
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
            MAX_SIZE = 1000000
            data = f.read(MAX_SIZE)
            tf = None
            if len(data) >= MAX_SIZE:
                tf = os.tmpfile()
                tf.write(data)
                shutil.copyfileobj(f, tf)
                data = tf
            r = s3.put(sname, data)
            if tf is not None:
                tf.close()
            manifest[name] = {
                't': t,
                'h': h,
                'm': r.getheader("ETag")
            }
            mfdata = cPickle.dumps(manifest)
            try:
                mf = open(os.path.join(base, ".s3mirror-MANIFEST"), "w")
                mf.write(mfdata)
                mf.close()
            except IOError:
                pass
            done += finfo['size']
            print done, '/', total
        mfdata = cPickle.dumps(manifest)
        if Config.EncryptNames:
            s3.put(dest+prefix+".s3mirror-MANIFEST", karn_encrypt(mfdata, Config.Secret))
        else:
            s3.put(dest+prefix+".s3mirror-MANIFEST", mfdata)

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
    s3 = s3lib.S3Store(access, secret)
    todo = scanfiles(source, dest)
    sendfiles(todo, dest)

if __name__ == "__main__":
    main()
