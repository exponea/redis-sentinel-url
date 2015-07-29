#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='Flask-Redis-Sentinel',
    py_modules=['flask_redis_sentinel'],
    version='0.1.0',
    install_requires=['Flask>=0.10.1', 'redis>=2.10.3'],
    description='Redis-Sentinel integration for Flask',
    url='https://github.com/infinario/flask-redis-sentinel',
    author='Martin Sucha',
    author_email='martin.sucha@infinario.com',
    license='Apache 2.0',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

