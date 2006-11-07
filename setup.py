#!/usr/bin/env python

from distutils.core import setup

setup(
    name = "s3lib",
    version = "0.1",
    description = "Amazon S3 Interface Library",
    author = "Greg Hewgill",
    author_email = "greg@hewgill.com",
    url = "http://hewgill.com/software/s3lib/",
    py_modules = ["s3lib"],
    scripts = ["s3c.py", "s3mirror.py"],
)
