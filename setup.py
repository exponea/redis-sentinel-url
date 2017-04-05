#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='Redis-Sentinel-Url',
    py_modules=['redis_sentinel_url'],
    version='1.0.1',
    install_requires=['redis>=2.10.3'],
    tests_require=['mock', 'nose'],
    test_suite='nose.collector',
    description='A factory for redis connection that supports using Redis Sentinel',
    url='https://github.com/exponea/redis-sentinel-url',
    author='Martin Sucha',
    author_email='martin.sucha@exponea.com',
    license='Apache 2.0',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
