#!/usr/bin/env python
# Copyright 2011 Canonical Ltd.
#
# This file is part of u1db.
#
# u1db is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License version 3
# as published by the Free Software Foundation.
#
# u1db is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with u1db.  If not, see <http://www.gnu.org/licenses/>.

import sys


def config():
    import u1db
    ext = []
    kwargs = {
        "name": "u1db",
        "version": u1db.__version__,
        "description": "Simple syncable document storage",
        "url": "https://launchpad.net/u1db",
        "license": "GNU LGPL v3",
        "author": "Ubuntu One team",
        "author_email": "u1db-discuss@lists.launchpad.net",
        "download_url": "https://launchpad.net/u1db/+download",
        "packages": ["u1db", "u1db.backends", "u1db.remote",
                     "u1db.commandline", "u1db.compat"],
        "package_data": {'': ["*.sql"]},
        "scripts": ['u1db-client', 'u1db-serve'],
        "ext_modules": ext,
        "install_requires": ["paste", "simplejson", "routes", "pyxdg"],
        # informational
        "tests_require": ["testtools", "testscenarios", "Cython",
                          "pyOpenSSL"],
        "classifiers": [
            'Development Status :: 4 - Beta',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: '
                'GNU Library or Lesser General Public License (LGPL)',
            'Operating System :: OS Independent',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX',
            'Programming Language :: Python',
            'Programming Language :: Cython',
            'Topic :: Software Development :: Debuggers',
        ],
        "long_description": """\
A simple syncable JSON document store.

This allows you to get, retrieve, index, and update JSON documents, and
synchronize them with other stores.
"""
    }

    try:
        from setuptools import setup, Extension
    except ImportError:
        from distutils.core import setup, Extension

    try:
        from Cython.Distutils import build_ext
    except ImportError, e:
        print "Unable to import Cython, to test the C implementation"
    else:
        kwargs["cmdclass"] = {"build_ext": build_ext}
        extra_libs = []
        extra_defines = []
        if sys.platform == 'win32':
            # Used for the random number generator
            extra_libs.append('advapi32')
            extra_libs.append('libcurl_imp')
            extra_libs.append('libeay32')
            extra_defines = [('_CRT_SECURE_NO_WARNINGS', 1)]
        else:
            extra_libs.append('curl')
        extra_libs.append('json')
        ext.append(Extension(
            "u1db.tests.c_backend_wrapper",
            ["u1db/tests/c_backend_wrapper.pyx"],
            include_dirs=["include"],
            library_dirs=["src"],
            libraries=['u1db', 'sqlite3', 'oauth'] + extra_libs,
            define_macros=[] + extra_defines,
            ))


    setup(**kwargs)

if __name__ == "__main__":
    config()
