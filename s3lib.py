#!/usr/bin/env python

import base64
import calendar
import hmac
import httplib
import os
import re
import sha
import socket
import sys
import time
import urllib
import xml.dom.minidom

from collections import defaultdict

def readConfig():
    access = None
    secret = None
    logfile = None
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
                access = m.group(2)
            elif m.group(1) == "secret":
                secret = m.group(2)
            elif m.group(1) == "logfile":
                logfile = m.group(2)
            else:
                continue
        f.close()
    except:
        if f is not None:
            f.close()
    if 'S3ACCESS' in os.environ:
        access = os.environ['S3ACCESS']
    if 'S3SECRET' in os.environ:
        secret = os.environ['S3SECRET']
    return access, secret, logfile

def makestruct(e, arrays = {}):
    """Construct a Python data structure from an XML fragment."""
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
    """Convert an ISO 8601 date string into a time value."""
    m = re.match(r"(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.(\d+))?Z$", ts)
    return calendar.timegm([int(m.group(i)) for i in range(1, 7)])

class S3Exception(Exception):
    """Encapsulate an S3 exception."""

    def __init__(self, r, readdata = True):
        Exception.__init__(self)
        self.status = r.status
        if readdata:
            doc = xml.dom.minidom.parseString(r.read())
            self.info = makestruct(doc.documentElement)
        else:
            self.info = {'Code': self.status, 'Message': ""}

    def __str__(self):
        if 'BucketName' in self.info:
            return "%s: %s (%s)" % (self.info['Code'], self.info['Message'], self.info['BucketName'])
        else:
            return "%s: %s" % (self.info['Code'], self.info['Message'])

class Monitor:
    def __init__(self):
        self._attempt = defaultdict(int)
        self._request = defaultdict(int)
        self._bytesin = 0
        self._bytesout = 0

    def attempt(self, method):
        self._attempt[method] += 1

    def request(self, method):
        self._request[method] += 1

    def bytesin(self, n):
        self._bytesin += n

    def bytesout(self, n):
        self._bytesout += n

def cost(monitor):
    GB = 2**30
    return (
        0.100 * monitor._bytesin / GB +
        0.170 * monitor._bytesout / GB +
        0.01 * monitor._request['PUT'] / 1000 +
        0.01 * sum(v for k, v in monitor._request.items() if k != 'DELETE') / 10000
    )

class HTTPResponseLogger:
    """Provide logging facilities for an HTTP response object."""

    def __init__(self, r, logfile, format):
        self.r = r
        self.logfile = logfile
        self.format = format
        self.total = 0

    def read(self, n = None):
        if n is None:
            buf = self.r.read()
            log = open(self.logfile, "a")
            print >>log, self.format % len(buf)
            log.close()
        else:
            buf = self.r.read(n)
            self.total += len(buf)
            if buf is None:
                log = open(self.logfile, "a")
                print >>log, self.format % self.total
                log.close()
        return buf

    def __getattr__(self, name):
        return self.r.__dict__[name]

class S3Store:
    """Provide access to Amazon S3."""

    def __init__(self, access = None, secret = None, logfile = None):
        """Initialise an instance."""
        self.access = access
        self.secret = secret
        self.logfile = logfile
        self.monitors = []
        a, s, l = readConfig()
        if self.access is None:
            self.access = a
        if self.secret is None:
            self.secret = s
        if self.logfile is None:
            self.logfile = l
        self.server = httplib.HTTPSConnection("s3.amazonaws.com", strict = True)

    def addmonitor(self, monitor):
        if monitor in self.monitors:
            return False
        self.monitors.append(monitor)
        return True

    def removemonitor(self, monitor):
        self.monitors = [x for x in self.monitors if x != monitor]

    def create(self, bucket):
        """Create a new bucket."""
        r = self._exec("PUT", "/"+urllib.quote(bucket))
        if r.status != 200:
            raise S3Exception(r)
        r.read()
        return r

    def list(self, bucket, query = "", callback = None):
        """List contents of a bucket."""
        ret = None
        marker = None
        while True:
            q = query
            if marker is not None:
                if len(q) == 0:
                    q += "?"
                else:
                    q += "&"
                q += "marker=" + marker
            r = self.get(bucket, q)
            data = r.read()
            doc = xml.dom.minidom.parseString(data)
            if doc.documentElement.tagName == "ListAllMyBucketsResult":
                assert bucket == ""
                return makestruct(doc.documentElement, {'Buckets': "Bucket"})
            elif doc.documentElement.tagName == "ListBucketResult":
                assert bucket != ""
                s = makestruct(doc.documentElement, {'Contents': None, 'CommonPrefixes': None})
                if ret is None:
                    ret = s
                else:
                    ret['Contents'] += s['Contents']
                    ret['CommonPrefixes'] += s['CommonPrefixes']
                if callback is not None:
                    callback(count = len(ret['Contents']))
                if s['IsTruncated'] != "true":
                    break
                if 'NextMarker' in s:
                    marker = s['NextMarker']
                else:
                    marker = s['Contents'][-1]['Key']
            else:
                raise Exception("s3c: Error: Unexpected element: %s" % doc.documentElement.tagName)
        return ret

    def get(self, name, query = "", method = "GET"):
        """Get an object from a bucket."""
        r = self._exec(method, "/"+urllib.quote(name), query = query)
        if r.status != 200:
            raise S3Exception(r)
        return r

    def put(self, name, data):
        """Put an object into a bucket."""
        r = self._exec("PUT", "/"+urllib.quote(name), data)
        if r.status != 200:
            raise S3Exception(r)
        r.read()
        return r

    def delete(self, name):
        """Delete an object from a bucket."""
        r = self._exec("DELETE", "/"+urllib.quote(name))
        if r.status != 204:
            raise S3Exception(r)
        r.read()
        return r

    def _exec(self, method, name, data = None, headers = None, query = ""):

        # It is necessary to force a disconnect and reconnect to avoid
        # problems with keepalive HTTP connections. If the server has timed
        # out we won't know that, and will get IOError exceptions when
        # we try to send data.
        #
        # This should be improved by handling IOError exceptions in the
        # retry logic.
        self.server.close()
        self.server.connect()

        if headers is None:
            headers = {}
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
        tries = 0
        delay = 0.1
        while True:
            for m in self.monitors:
                m.attempt(method)
            try:
                self.server.putrequest(method, name+query)
                if data is not None:
                    if isinstance(data, str):
                        datasize = len(data)
                    else:
                        data.seek(0, 2)
                        datasize = data.tell()
                        data.seek(0)
                    self.server.putheader('Content-Length', str(datasize))
                for hdr, value in headers.iteritems():
                    self.server.putheader(hdr, value)
                self.server.endheaders()
                if data is not None:
                    if isinstance(data, str):
                        self.server.send(data)
                    else:
                        while True:
                            buf = data.read(16*1024)
                            if not buf:
                                break
                            self.server.send(buf)
                r = self.server.getresponse()
                if r.status < 300:
                    break
                if method == "HEAD":
                    e = S3Exception(r, False)
                else:
                    e = S3Exception(r)
                if e.info['Code'] != "InternalError":
                    raise e
            except httplib.HTTPException, e:
                print >>sys.stderr, "got httplib.HTTPException:", e, ", retrying"
                pass
            except socket.error, e:
                print >>sys.stderr, "got socket.error:", e, ", retrying"
                pass
            #print >>sys.stderr, "got InternalError, sleeping", delay
            self.server.close()
            time.sleep(delay)
            delay *= 2
            tries += 1
            if tries >= 5:
                raise e
            self.server.connect()
        for m in self.monitors:
            m.request(method)
            if data is not None:
                m.bytesin(datasize)
        if self.logfile is not None:
            line = "%s %d %s %s%s" % (self.access, time.time(), method, name, query)
            if method == "GET":
                r = HTTPResponseLogger(r, self.logfile, re.sub("%", "%%", line) + " %d")
            elif method == "PUT":
                log = open(self.logfile, "a")
                if data is not None:
                    print >>log, line, datasize
                else:
                    print >>log, line
                log.close()
            else:
                log = open(self.logfile, "a")
                print >>log, line
                log.close()
        return r

