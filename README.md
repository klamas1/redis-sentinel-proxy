redis-sentinel-proxy
====================

Small command utility that:

* Given a redis sentinel server listening on `SENTINEL_PORT`, keeps asking it for the address of a master named `NAME`

* Proxies all tcp requests that it receives on master `PORT` to that master

* Also listens on replica `PORT` and proxies requests to replicas with load balancing (round-robin or least connections)

Usage:

`./redis-sentinel-proxy -listen IP:MASTER_PORT -replica-listen IP:REPLICA_PORT -sentinel :SENTINEL_PORT -master NAME -balancing round-robin -debug --resolve-retries 10`

testing
============
- install `docker` and `docker-compose`.
- run `make tests-unit tests-intergration`

026/01/17 15:08:38 [DEBUG] Sentinel slaves response: "*2\r\n*42\r\n$4\r\nname\r\n$18\r\n10.111.47.148:6379\r\n$2\r\nip\r\n$13\r\n10.111.47.148\r\n$4\r\nport\r\n$4\r\n6379\r\n$5\r\nrunid\r\n$40\r\n805d6e466c82dd6378ca35fdf421cc202d87540b\r\n$5\r\nflags\r\n$5\r\nslave\r\n$21\r\nlink-pending-commands\r\n$1\r\n0\r\n$13\r\nlink-refcount\r\n$1\r\n1\r\n$14\r\nlast-ping-sent\r\n$1\r\n0\r\n$18\r\nlast-ok-ping-reply\r\n$1\r\n0\r\n$15\r\nlast-ping-reply\r\n$1\r\n0\r\n$23\r\ndown-after-milliseconds\r\n$4\r\n5000\r\n$12\r\ninfo-refresh\r\n$4\r\n1127\r\n$13\r\nrole-reported\r\n$5\r\nslave\r\n$18\r\nrole-reported-time\r\n$8\r\n13726917\r\n$21\r\nmaster-link-down-time\r\n$1\r\n0\r\n$18\r\nmaster-link-status\r\n$2\r\nok\r\n$11\r\nmaster-host\r\n$13\r\n10.111.40.184\r\n$11\r\nmaster-port\r\n$4\r\n6379\r\n$14\r\nslave-priority\r\n$3\r\n100\r\n$17\r\nslave-repl-offset\r\n$9\r\n636946898\r\n$17\r\nreplica-announced\r\n$1\r\n1\r\n*42\r\n$4\r\nname\r\n$18\r\n10.111.35.248:6379\r\n$2\r\nip\r\n$13\r\n10.111.35.248\r\n$4\r\nport\r\n$4\r\n6379\r\n$5\r\nrunid\r\n$40\r\n4fd63d5c4521cf6662f3bb1d1d0c9aa7914e11a8\r\n$5\r\nflags\r\n$5\r\nslave\r\n$21\r\nlink-pending-commands\r\n$1\r\n2\r\n$13\r\nlink-refcount\r\n$1\r\n1\r\n$14\r\nlast-ping-sent\r\n$1\r\n3\r\n$18\r\nlast-ok-ping-reply\r\n$4\r\n1026\r\n$15\r\nlast-ping-reply\r\n$4\r\n1026\r\n$23\r\ndown-after-milliseconds\r\n$4\r\n5000\r\n$12\r\ninfo-refresh\r\n$4\r\n1127\r\n$13\r\nrole-reported\r\n$5\r\nslave\r\n$18\r\nrole-reported-time\r\n$8\r\n13726909\r\n$21\r\nmaster-link-down-time\r\n$1\r\n0\r\n$18\r\nmaster-link-status\r\n$2\r\nok\r\n$11\r\nmaster-host\r\n$13\r\n10.111.40.184\r\n$11\r\nmaster-port\r\n$4\r\n6379\r\n$14\r\nslave-priority\r\n$3\r\n100\r\n$17\r\nslave-repl-offset\r\n$9\r\n636946898\r\n$17\r\nreplica-announced\r\n$1\r\n1\r\n"
2026/01/17 15:08:38 [DEBUG] Parts: [*2 *42 $4 name $18 10.111.47.148:6379 $2 ip $13 10.111.47.148 $4 port $4 6379 $5 runid $40 805d6e466c82dd6378ca35fdf421cc202d87540b $5 flags $5 slave $21 link-pending-commands $1 0 $13 link-refcount $1 1 $14 last-ping-sent $1 0 $18 last-ok-ping-reply $1 0 $15 last-ping-reply $1 0 $23 down-after-milliseconds $4 5000 $12 info-refresh $4 1127 $13 role-reported $5 slave $18 role-reported-time $8 13726917 $21 master-link-down-time $1 0 $18 master-link-status $2 ok $11 master-host $13 10.111.40.184 $11 master-port $4 6379 $14 slave-priority $3 100 $17 slave-repl-offset $9 636946898 $17 replica-announced $1 1 *42 $4 name $18 10.111.35.248:6379 $2 ip $13 10.111.35.248 $4 port $4 6379 $5 runid $40 4fd63d5c4521cf6662f3bb1d1d0c9aa7914e11a8 $5 flags $5 slave $21 link-pending-commands $1 2 $13 link-refcount $1 1 $14 last-ping-sent $1 3 $18 last-ok-ping-reply $4 1026 $15 last-ping-reply $4 1026 $23 down-after-milliseconds $4 5000 $12 info-refresh $4 1127 $13 role-reported $5 slave $18 role-reported-time $8 13726909 $21 master-link-down-time $1 0 $18 master-link-status $2 ok $11 master-host $13 10.111.40.184 $11 master-port $4 6379 $14 slave-priority $3 100 $17 slave-repl-offset $9 636946898 $17 replica-announced $1 1]

