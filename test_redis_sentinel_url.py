# Copyright 2016 Exponea s r.o. <info@exponea.com>
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

from unittest import TestCase
from mock import MagicMock
import redis
from redis_sentinel_url import parse_sentinel_url, DefaultClient, connect


class TestUrlParsing(TestCase):
    def test_basic_url(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname:7000')
        self.assertEqual(parsed.hosts, [('hostname', 7000)])
        self.assertEqual(parsed.sentinel_options, {})
        self.assertEqual(parsed.client_options, {'db': 0})
        self.assertEqual(parsed.default_client, DefaultClient('master', None))

    def test_without_port(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname')
        self.assertEqual(parsed.hosts, [('hostname', 26379)])
        self.assertEqual(parsed.sentinel_options, {})
        self.assertEqual(parsed.client_options, {'db': 0})
        self.assertEqual(parsed.default_client, DefaultClient('master', None))

    def test_multiple_hosts(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000,hostname2:7002,hostname3:7003')
        self.assertEqual(parsed.hosts, [('hostname', 7000), ('hostname2', 7002), ('hostname3', 7003)])
        self.assertEqual(parsed.sentinel_options, {})
        self.assertEqual(parsed.client_options, {'db': 0})
        self.assertEqual(parsed.default_client, DefaultClient('master', None))

    def test_service_name_in_path(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/themaster')
        self.assertEqual(parsed.default_client, DefaultClient('master', 'themaster'))

    def test_service_name_in_query(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/theother?service=themaster')
        self.assertEqual(parsed.default_client, DefaultClient('master', 'themaster'))

    def test_slave_service(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/theslave?client_type=slave')
        self.assertEqual(parsed.default_client, DefaultClient('slave', 'theslave'))

    def test_ssl(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?ssl=1')
        self.assertDictEqual(parsed.client_options, {'db': 0, 'ssl': True})

        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?ssl=0')
        self.assertDictEqual(parsed.client_options, {'db': 0, 'ssl': False})

        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?sentinel_ssl=1')
        self.assertDictEqual(parsed.sentinel_options, {'ssl': True})

        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?sentinel_ssl=0')
        self.assertDictEqual(parsed.sentinel_options, {'ssl': False})

        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?sentinel_ssl=1&ssl=1')
        self.assertDictEqual(parsed.client_options, {'db': 0, 'ssl': True})
        self.assertDictEqual(parsed.sentinel_options, {'ssl': True})

    def test_invalid_client_type(self):
        with self.assertRaises(ValueError):
            parse_sentinel_url(
                'redis+sentinel://hostname:7000/theslave?client_type=whatever')

    def test_db_in_path(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/themaster/2')
        self.assertEqual(parsed.client_options['db'], 2)

    def test_db_in_query(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/themaster/2?db=4')
        self.assertEqual(parsed.client_options['db'], 4)

    def test_socket_timeout(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/themaster/2?socket_timeout=0.1')
        self.assertEqual(parsed.client_options['socket_timeout'], 0.1)

    def test_sentinel_socket_timeout(self):
        parsed = parse_sentinel_url(
            'redis+sentinel://hostname:7000/themaster/2?sentinel_socket_timeout=0.1')
        self.assertEqual(parsed.sentinel_options['socket_timeout'], 0.1)

    def test_no_value_for_option(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?socket_timeout')
        self.assertNotIn('sentinel_socket_timeout', parsed.client_options)

    def test_no_value_for_option2(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?socket_timeout=')
        self.assertNotIn('sentinel_socket_timeout', parsed.client_options)

    def test_multiple_values_for_option(self):
        with self.assertRaisesRegexp(ValueError, 'Multiple values specified for sentinel_socket_timeout'):
            parse_sentinel_url('redis+sentinel://hostname:7000/?sentinel_socket_timeout=0.1&sentinel_socket_timeout=2')

    def test_unknown_option(self):
        parsed = parse_sentinel_url('redis+sentinel://hostname:7000/?unknown_option=1')
        self.assertNotIn('unknown_option', parsed.client_options)

    def test_unsupported_scheme(self):
        with self.assertRaisesRegexp(ValueError, 'Unsupported scheme: redis'):
            parse_sentinel_url('redis://hostname:7000/')

    def test_with_password(self):
        parsed = parse_sentinel_url('redis+sentinel://:thesecret@hostname:7000')
        self.assertEquals(parsed.sentinel_options, {})
        self.assertEquals(parsed.client_options['password'], 'thesecret')


class FakeRedis(MagicMock):
    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
                 ssl=False, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs=None, ssl_ca_certs=None):
        super(FakeRedis, self).__init__()
        self.kwargs = {
            'host': host,
            'port': port,
            'db': db,
            'password': password,
            'socket_timeout': socket_timeout,
            'socket_connect_timeout': socket_connect_timeout,
            'socket_keepalive': socket_keepalive,
            'socket_keepalive_options': socket_keepalive_options,
            'connection_pool': connection_pool,
            'unix_socket_path': unix_socket_path,
            'encoding': encoding,
            'encoding_errors': encoding_errors,
            'charset': charset,
            'errors': errors,
            'decode_responses': decode_responses,
            'retry_on_timeout': retry_on_timeout,
            'ssl': ssl,
            'ssl_keyfile': ssl_keyfile,
            'ssl_certfile': ssl_certfile,
            'ssl_cert_reqs': ssl_cert_reqs,
            'ssl_ca_certs': ssl_ca_certs
        }

    @classmethod
    def from_url(cls, url, **kwargs):
        i = cls()
        i.kwargs = {'url': url}
        i.kwargs.update(kwargs)
        return i


class FakeSentinel(object):
    def __init__(self, sentinels, min_other_sentinels=0, sentinel_kwargs=None,
                 **connection_kwargs):
        self._sentinels = sentinels
        self.min_other_sentinels = min_other_sentinels
        self.sentinel_kwargs = sentinel_kwargs
        self.connection_kwargs = connection_kwargs

    def _update_kwargs(self, kwargs, connection_kwargs):
        connection = dict(self.connection_kwargs)
        connection.update(connection_kwargs)
        kwargs['connection_kwargs'] = connection

    def master_for(self, service_name, redis_class=redis.StrictRedis,
                   connection_pool_class=redis.sentinel.SentinelConnectionPool, **kwargs):
        i = FakeRedis()
        i.kwargs = {
            'is_master': True,
            'service_name': service_name,
            'redis_class': redis_class,
            'connection_pool_class': connection_pool_class
        }
        self._update_kwargs(i.kwargs, kwargs)
        return i

    def slave_for(self, service_name, redis_class=redis.StrictRedis,
                  connection_pool_class=redis.sentinel.SentinelConnectionPool, **kwargs):
        i = FakeRedis()
        i.kwargs = {
            'is_master': False,
            'service_name': service_name,
            'redis_class': redis_class,
            'connection_pool_class': connection_pool_class
        }
        self._update_kwargs(i.kwargs, kwargs)
        return i


class TestConnecting(TestCase):
    def test_default_client_redis_url(self):
        sentinel, client = connect('redis://hostname:7001/3', client_class=FakeRedis)
        self.assertEqual(client.kwargs['url'], 'redis://hostname:7001/3')

    def test_default_client_sentinel_url_master(self):
        sentinel, client = connect('redis+sentinel://hostname:7001/mymaster/3', client_class=FakeRedis,
                                   sentinel_class=FakeSentinel, client_options={'decode_responses': True},
                                   sentinel_options={'connect_timeout': 0.3})

        self.assertIsNotNone(sentinel)
        self.assertEquals(sentinel.sentinel_kwargs['connect_timeout'], 0.3)

        self.assertEqual(client.kwargs['is_master'], True)
        self.assertEqual(client.kwargs['service_name'], 'mymaster')
        self.assertEqual(client.kwargs['connection_kwargs']['db'], 3)
        self.assertEqual(client.kwargs['connection_kwargs']['decode_responses'], True)

    def test_default_client_sentinel_url_slave(self):
        sentinel, client = connect('redis+sentinel://hostname:7001/myslave/3?client_type=slave', client_class=FakeRedis,
                                   sentinel_class=FakeSentinel, client_options={'decode_responses': True},
                                   sentinel_options={'connect_timeout': 0.3})

        self.assertIsNotNone(sentinel)
        self.assertEquals(sentinel.sentinel_kwargs['connect_timeout'], 0.3)

        self.assertEqual(client.kwargs['is_master'], False)
        self.assertEqual(client.kwargs['service_name'], 'myslave')
        self.assertEqual(client.kwargs['connection_kwargs']['db'], 3)
        self.assertEqual(client.kwargs['connection_kwargs']['decode_responses'], True)

    def test_unsupported_url_scheme(self):
        with self.assertRaisesRegexp(ValueError, r'Unsupported redis URL scheme: redis\+something'):
            connect('redis+something://hostname:7001/myslave/3?slave=true', client_class=FakeRedis,
                    sentinel_class=FakeSentinel, client_options={'decode_responses': True})
