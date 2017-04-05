# Copyright 2015, 2016 Exponea s r.o. <info@exponea.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple
try:
    import urllib.parse as urlparse
except ImportError:  # pragma: no cover
    import urlparse
import redis
import redis.sentinel  # requires redis-py 2.9.0+
import sys


if sys.version_info[0] == 2:  # pragma: no cover
    # Python 2.x
    _string_types = basestring  # noqa: F821

    def iteritems(d):
        return d.iteritems()
else:  # pragma: no cover
    # Python 3.x
    _string_types = str

    def iteritems(d):
        return d.items()


SentinelUrlParseResult = namedtuple('SentinelUrlParseResult', ['hosts', 'sentinel_options', 'client_options',
                                                               'default_client'])

DefaultClient = namedtuple('DefaultClient', ['type', 'service'])


def parse_sentinel_url(url, sentinel_options=None, client_options=None):
    """Parse a URL listing sentinel options.

    :param url: redis+sentinel://host:port[,host2:port2,...][/service_name[/db]][?socket_timeout=0.1]
    :param sentinel_options: default sentinel options as dict, the ones in url always win
    :param client_options: default client options as dict, the ones in url always win
    :return: SentinelUrlParseResult
    """

    if isinstance(url, _string_types):
        url = urlparse.urlparse(url)

    if url.scheme != 'redis+sentinel':
        raise ValueError('Unsupported scheme: {}'.format(url.scheme))

    def parse_host(s):
        if ':' in s:
            host, port = s.split(':', 1)
            port = int(port)
        else:
            host = s
            port = 26379

        return host, port

    if '@' in url.netloc:
        auth, hostspec = url.netloc.split('@', 1)
    else:
        auth = None
        hostspec = url.netloc

    if auth and ':' in auth:
        _, password = auth.split(':', 1)
    else:
        password = None

    hosts = [parse_host(s) for s in hostspec.split(',')]

    global_option_types = {
        'min_other_sentinels': int,
        'db': int,
        'service': str,
        'client_type': str,
    }

    option_types = {
        'socket_timeout': float,
        'socket_connect_timeout': float,
    }

    sentinel_url_options = {}
    if sentinel_options:
        sentinel_url_options.update(sentinel_options)
    url_options = {}
    if client_options:
        url_options.update(client_options)

    if password is not None:
        url_options['password'] = password

    for name, value in iteritems(urlparse.parse_qs(url.query)):
        if name in global_option_types:
            option_name = name
            option_store = url_options
            option_type = global_option_types[option_name]
        else:
            if name.startswith('sentinel_'):
                option_name = name[9:]
                option_store = sentinel_url_options
            else:
                option_name = name
                option_store = url_options

            if option_name not in option_types:
                continue

            option_type = option_types[option_name]

        if len(value) > 1:
            raise ValueError('Multiple values specified for {}'.format(name))

        option_store[option_name] = option_type(value[0])

    path = url.path
    if path.startswith('/'):
        path = path[1:]
    if path == '':
        path_parts = []
    else:
        path_parts = path.split('/')

    if 'service' in url_options:
        service_name = url_options.pop('service')
    elif len(path_parts) >= 1:
        service_name = path_parts[0]
    else:
        service_name = None

    client_type = url_options.pop('client_type', 'master')
    if client_type not in ('master', 'slave'):
        raise ValueError('Client type must be either master or slave, got {!r}')

    default_client = DefaultClient(client_type, service_name)

    if 'db' not in url_options:
        if len(path_parts) >= 2:
            url_options['db'] = int(path_parts[1])
        else:
            url_options['db'] = 0

    return SentinelUrlParseResult(hosts, sentinel_url_options, url_options, default_client)


def connect(url, sentinel_class=redis.sentinel.Sentinel, sentinel_options=None, client_class=redis.StrictRedis,
            client_options=None):
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme not in ['redis', 'rediss', 'unix', 'redis+sentinel']:
        raise ValueError('Unsupported redis URL scheme: {}'.format(parsed_url.scheme))

    if sentinel_options is None:
        sentinel_options = {}

    if client_options is None:
        client_options = {}

    if parsed_url.scheme != 'redis+sentinel':
        return None, client_class.from_url(url, **client_options)

    sentinel_url = parse_sentinel_url(url, sentinel_options=sentinel_options, client_options=client_options)

    sentinel = sentinel_class(sentinel_url.hosts, sentinel_kwargs=sentinel_url.sentinel_options,
                              **sentinel_url.client_options)
    client = None

    if sentinel_url.default_client:
        if sentinel_url.default_client.type == 'master':
            client = sentinel.master_for(sentinel_url.default_client.service, redis_class=client_class)
        else:
            client = sentinel.slave_for(sentinel_url.default_client.service, redis_class=client_class)

    return sentinel, client
