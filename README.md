redis-sentinel-proxy
====================

Small command utility that:

* Given a redis sentinel server listening on `SENTINEL_PORT`, keeps asking it for the address of a master named `NAME`

* Proxies all tcp requests that it receives on master `PORT` to that master

* Also listens on replica `PORT` and proxies requests to replicas with load balancing (round-robin or least connections)

Usage:

`./redis-sentinel-proxy -listen IP:MASTER_PORT -replica-listen IP:REPLICA_PORT -sentinel :SENTINEL_PORT -master NAME -balancing round-robin --resolve-retries 10`

testing
============
- install `docker` and `docker-compose`.
- run `make tests-unit tests-intergration`