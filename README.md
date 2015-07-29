# Flask-Redis-Sentinel

![](https://travis-ci.org/Infinario/flask-redis-sentinel.svg)

Flask-Redis-Sentinel provides support for connecting to Redis using Sentinel and also supports connecting to Redis
without it.

* Supports Python 2.7 and 3.3+
* Licensed using Apache License 2.0

## Basic usage

```python
from flask.ext.redis_sentinel import SentinelExtension

redis_sentinel = SentinelExtension()
redis_connection = redis_sentinel.default_connection

# Later when you create application
app = Flask(...)
redis_sentinel.init_app(app)
```

You can configure Redis connection parameters using `REDIS_URL` Flask configuration variable with `redis+sentinel`
URL scheme:

```
redis+sentinel://localhost:26379[,otherhost:26379,...]/mymaster/0
redis+sentinel://localhost:26379[,otherhost:26379,...]/mymaster/0?socket_timeout=0.1
redis+sentinel://localhost:26379[,otherhost:26379,...]/mymaster/0?sentinel_socket_timeout=0.1
redis+sentinel://:sentinel-secret-password@localhost:26379[,otherhost:26379,...]/mymaster/0?sentinel_socket_timeout=0.1
```

The extension also supports URL schemes as supported by redis-py for connecting to an instance directly without Sentinel:

```
redis://[:password]@localhost:6379/0
rediss://[:password]@localhost:6379/0
unix://[:password]@/path/to/socket.sock?db=0
```

Flask-And-Redis style config variables are also supported for easier migration, but the extension will
log a `DeprecationWarning`:

```
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
```

In case both `REDIS_URL` and other variables are present, the URL is used.

## Creating multiple connection pools using a single Sentinel cluster

```python
from flask.ext.redis_sentinel import SentinelExtension

redis_sentinel = SentinelExtension()
master1 = redis_sentinel.master_for('service1')
master2 = redis_sentinel.master_for('service2')
slave1 = redis_sentinel.master_for('service1')
```

## Accessing redis-py's Sentinel instance

```python
from flask.ext.redis_sentinel import SentinelExtension
from flask import jsonify, Flask

app = Flask('test')

redis_sentinel = SentinelExtension(app=app)

@app.route('/'):
def index():
  slaves = redis_sentinel.sentinel.discover_slaves('service1')
  return jsonify(slaves=slaves)

```
