# Copyright 2015 7Segments s r.o. <info@7segments.com>
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
import inspect
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
from flask import current_app
import warnings
import redis
import redis.sentinel  # requires redis-py 2.9.0+
import sys
from werkzeug.local import LocalProxy
from werkzeug.utils import import_string


if sys.version_info[0] == 2:  # pragma: no cover
    # Python 2.x
    _string_types = basestring

    def iteritems(d):
        return d.iteritems()
else:  # pragma: no cover
    # Python 3.x
    _string_types = str

    def iteritems(d):
        return d.items()

_EXTENSION_KEY = 'redissentinel'


SentinelUrlParseResult = namedtuple('SentinelUrlParseResult', ['hosts', 'sentinel_options', 'connection_options',
                                                               'default_connection'])


def parse_sentinel_url(url):
    """Parse a URL listing sentinel options.

    :param url: redis+sentinel://host:port[,host2:port2,...]/service_name/db[?socket_timeout=0.1]
    :return: (redis.sentinel.Sentinel, service_name)
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
        'slave': bool,
    }

    option_types = {
        'socket_timeout': float,
        'socket_connect_timeout': float,
    }

    sentinel_url_options = {}
    url_options = {}

    if password is not None:
        sentinel_url_options['password'] = password

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

    if 'slave' in url_options:
        slave = url_options.pop('slave')
    else:
        slave = False

    default_connection = {
        'service': service_name,
        'slave': slave
    }

    if 'db' not in url_options:
        if len(path_parts) >= 2:
            url_options['db'] = int(path_parts[1])
        else:
            url_options['db'] = 0

    return SentinelUrlParseResult(hosts, sentinel_url_options, url_options, default_connection)


def sentinel_from_url(url, sentinel_class=redis.sentinel.Sentinel):
    parsed = parse_sentinel_url(url)
    return (sentinel_class(parsed.hosts, sentinel_kwargs=parsed.sentinel_options, **parsed.connection_options),
            parsed.default_connection)


class _ExtensionData(object):
    def __init__(self, client_class, sentinel=None, default_connection=None):
        self.client_class = client_class
        self.sentinel = sentinel
        self.default_connection = default_connection
        self.master_connections = {}
        self.slave_connections = {}

    def master_for(self, service_name, **kwargs):
        if self.sentinel is None:
            raise ValueError('Cannot get master {} using non-sentinel configuration'.format(service_name))
        if service_name not in self.master_connections:
            self.master_connections[service_name] = self.sentinel.master_for(service_name, redis_class=self.client_class,
                                                                             **kwargs)
        return self.master_connections[service_name]

    def slave_for(self, service_name, **kwargs):
        if self.sentinel is None:
            raise ValueError('Cannot get slave {} using non-sentinel configuration'.format(service_name))
        if service_name not in self.slave_connections:
            self.slave_connections[service_name] = self.sentinel.slave_for(service_name, redis_class=self.client_class,
                                                                           **kwargs)
        return self.slave_connections[service_name]


class _ExtensionProxy(LocalProxy):
    __slots__ = ('__sentinel',)

    def __init__(self, sentinel, local, name=None):
        object.__setattr__(self, '_ExtensionProxy__sentinel', sentinel)
        super(_ExtensionProxy, self).__init__(local, name=name)

    def _get_current_object(self):
        app = current_app._get_current_object()
        if _EXTENSION_KEY not in app.extensions or self.__sentinel.config_prefix not in app.extensions[_EXTENSION_KEY]:
            raise ValueError('RedisSentinel extension with config prefix {} was not initialized for application {}'.
                             format(self.__sentinel.config_prefix, app.import_name))
        ext_data = app.extensions[_EXTENSION_KEY][self.__sentinel.config_prefix]

        local = object.__getattribute__(self, '_LocalProxy__local')

        return local(ext_data)


class SentinelExtension(object):
    """Flask extension that supports connections to master using Redis Sentinel.

    Supported URL types:
      redis+sentinel://
      redis://
      rediss://
      unix://
    """
    def __init__(self, app=None, config_prefix=None, client_class=None, sentinel_class=None):
        self.config_prefix = None
        self.client_class = client_class
        self.sentinel_class = sentinel_class
        if app is not None:
            self.init_app(app, config_prefix=config_prefix)
        self.default_connection = _ExtensionProxy(self, lambda ext_data: ext_data.default_connection)
        self.sentinel = _ExtensionProxy(self, lambda ext_data: ext_data.sentinel)

    def init_app(self, app, config_prefix=None, client_class=None, sentinel_class=None):
        if _EXTENSION_KEY not in app.extensions:
            app.extensions[_EXTENSION_KEY] = {}

        extensions = app.extensions[_EXTENSION_KEY]

        if config_prefix is None:
            config_prefix = 'REDIS'

        if config_prefix in extensions:
            raise ValueError('Config prefix {} already registered'.format(config_prefix))

        self.config_prefix = config_prefix

        def key(suffix):
            return '{}_{}'.format(config_prefix, suffix)

        url = app.config.get(key('URL'))

        if client_class is not None:
            pass
        elif self.client_class is not None:
            client_class = self.client_class
        else:
            client_class = app.config.get(key('CLASS'), redis.StrictRedis)
            if isinstance(client_class, _string_types):
                client_class = import_string(client_class)

        if sentinel_class is not None:
            pass
        elif self.sentinel_class is not None:
            sentinel_class = self.sentinel_class
        else:
            sentinel_class = app.config.get(key('SENTINEL_CLASS'), redis.sentinel.Sentinel)
            if isinstance(sentinel_class, _string_types):
                sentinel_class = import_string(sentinel_class)

        data = _ExtensionData(client_class)

        if url:
            parsed_url = urlparse.urlparse(url)
            if parsed_url.scheme not in ['redis', 'rediss', 'unix', 'redis+sentinel']:
                raise ValueError('Unsupported redis URL scheme: {}'.format(parsed_url.scheme))

            if parsed_url.scheme == 'redis+sentinel':
                sentinel, default_connnection = sentinel_from_url(parsed_url, sentinel_class=sentinel_class)
                data.sentinel = sentinel
                if default_connnection:
                    if default_connnection['slave']:
                        data.default_connection = sentinel.slave_for(default_connnection['service'], redis_class=client_class)
                    else:
                        data.default_connection = sentinel.master_for(default_connnection['service'], redis_class=client_class)
            else:
                data.default_connection = client_class.from_url(url)
        else:
            # Stay compatible with Flask-And-Redis for a while
            warnings.warn('Setting redis connection via separate variables is deprecated. Please use REDIS_URL.',
                          DeprecationWarning)
            kwargs = self._config_from_variables(app.config, client_class)
            data.default_connection = client_class(**kwargs)

        extensions[config_prefix] = data

    @staticmethod
    def _config_from_variables(config, client_class, config_prefix='REDIS'):
        def key(suffix):
            return '{}_{}'.format(config_prefix, suffix)

        host = config.get(key('HOST'))
        if host and (host.startswith('file://') or host.startswith('/')):
            config.pop(key('HOST'))
            config[key('UNIX_SOCKET_PATH')] = host

        # Create the instance with keyword arguments
        args = inspect.getargspec(client_class.__init__).args
        args.remove('self')

        def get_config(suffix):
            value = config[key(suffix)]
            if suffix == 'PORT':
                return int(value)
            return value

        return {arg: get_config(arg.upper()) for arg in args if key(arg.upper()) in config}

    def master_for(self, service_name, **kwargs):
        return _ExtensionProxy(self, lambda ext_data: ext_data.master_for(service_name, **kwargs))

    def slave_for(self, service_name, **kwargs):
        return _ExtensionProxy(self, lambda ext_data: ext_data.slave_for(service_name, **kwargs))