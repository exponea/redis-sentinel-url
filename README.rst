Redis-Sentinel-Url
==================

.. image:: https://travis-ci.org/exponea/redis-sentinel-url.svg
    :target: https://travis-ci.org/exponea/redis-sentinel-url
    :alt: Travis CI

Redis-Sentinel-Url provides parser and connection factory for `redis://` and `redis+sentinel://` URLs (the latter
being defined by this package).

* Supports Python 2.7 and 3.3+
* Licensed using Apache License 2.0

Installation
------------

Install with pip::

    pip install Redis-Sentinel-Url


URL scheme for connecting via Sentinel
--------------------------------------

This package defines `redis+sentinel://` scheme for connecting to Redis via Sentinel::

    redis+sentinel://[:password@]host:port[,host2:port2,...][/service_name[/db]][?param1=value1[&param2=value=2&...]]

- You can specify multiple sentinel host:port pairs separated by comma.
- If `service_name` is provided, it is used to create a default client
- `service_name` and `db` can also be specified as URL parameters (URL parameters take precedence)
- Client options (keyword arguments to `redis.StrictRedis`) are specified as URL parameters
- Options for connecting to Sentinel (keyword arguments to `redis.sentinel.Sentinel`) are specified
  with `sentinel_` prefix
- There is special `client_type` option to specify whether the default client should be `master` (the default) or
  `slave` service when connecting via Sentinel

Basic usage
-----------

Supports schemes supported by `redis.StrictRedis.from_url` and also `redis+sentinel://` scheme described above:

.. code-block:: python

    import redis_sentinel_url

    sentinel, client = redis_sentinel_url.connect('redis://localhost/0')
    # None, StrictRedis(...)

    sentinel, client = redis_sentinel_url.connect('rediss://localhost/0')
    # None, StrictRedis(...)

    sentinel, client = redis_sentinel_url.connect('unix://[:password]@/path/to/socket.sock?db=0')
    # None, StrictRedis(...)

    sentinel, client = redis_sentinel_url.connect('redis+sentinel://localhost:26379,otherhost:26479/mymaster/0')
    # Sentinel(...), StrictRedis(...)
