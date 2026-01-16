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

2026/01/16 13:59:16 [DEBUG] Parts: [*2 *42 $4 name $18 10.111.47.136:6379 $2 ip $13 10.111.47.136 $4 port $4 6379 $5 runid $40 b9a5503c0a6d698518218619038f42c4c3c7e717 $5 flags $5 slave $21 link-pending-commands $1 0 $13 link-refcount $1 1 $14 last-ping-sent $1 0 $18 last-ok-ping-reply $3 774 $15 last-ping-reply $3 774 $23 down-after-milliseconds $4 5000 $12 info-refresh $4 6573 $13 role-reported $5 slave $18 role-reported-time $8 25108909 $21 master-link-down-time $1 0 $18 master-link-status $2 ok $11 master-host $12 10.111.35.50 $11 master-port $4 6379 $14 slave-priority $3 100 $17 slave-repl-offset $9 618098047 $17 replica-announced $1 1 *42 $4 name $17 10.111.40.28:6379 $2 ip $12 10.111.40.28 $4 port $4 6379 $5 runid $40 3096e4ead704d1bfae0c32daa53d41c969ed335e $5 flags $5 slave $21 link-pending-commands $1 0 $13 link-refcount $1 1 $14 last-ping-sent $1 0 $18 last-ok-ping-reply $3 775 $15 last-ping-reply $3 775 $23 down-after-milliseconds $4 5000 $12 info-refresh $4 6574 $13 role-reported $5 slave $18 role-reported-time $8 25108916 $21 master-link-down-time $1 0 $18 master-link-status $2 ok $11 master-host $12 10.111.35.50 $11 master-port $4 6379 $14 slave-priority $3 100 $17 slave-repl-offset $9 618098047 $17 replica-announced $1 1]
2026/01/16 13:59:16 [DEBUG] j=0, index=87
2026/01/16 13:59:16 [DEBUG] j=1, index=91
2026/01/16 13:59:16 [DEBUG] j=2, index=95
2026/01/16 13:59:16 [DEBUG] j=3, index=99
2026/01/16 13:59:16 [DEBUG] j=4, index=103
2026/01/16 13:59:16 [DEBUG] j=5, index=107
2026/01/16 13:59:16 [DEBUG] j=6, index=111
2026/01/16 13:59:16 [DEBUG] j=7, index=115
2026/01/16 13:59:16 [DEBUG] j=8, index=119
2026/01/16 13:59:16 [DEBUG] j=9, index=123
2026/01/16 13:59:16 [DEBUG] j=10, index=127
2026/01/16 13:59:16 [DEBUG] j=11, index=131
2026/01/16 13:59:16 [DEBUG] j=12, index=135
2026/01/16 13:59:16 [DEBUG] j=13, index=139
2026/01/16 13:59:16 [DEBUG] j=14, index=143
2026/01/16 13:59:16 [DEBUG] j=15, index=147
2026/01/16 13:59:16 [DEBUG] j=16, index=151
2026/01/16 13:59:16 [DEBUG] j=17, index=155
2026/01/16 13:59:16 [DEBUG] j=18, index=159
2026/01/16 13:59:16 [DEBUG] j=19, index=163
2026/01/16 13:59:16 [DEBUG] j=20, index=167
2026/01/16 13:59:16 [DEBUG] j=21, index=171
2026/01/16 13:59:16 [DEBUG] Not enough parts for j=21: index=171, required=174, len=171
2026/01/16 13:59:16 [DEBUG] Parts from index 171: []
2026/01/16 13:59:16 [DEBUG] Found replica 10.111.40.28:6379 after 21 fields
2026/01/16 13:59:16 [DEBUG] Replica 10.111.40.28:6379 accessible
2026/01/16 13:59:16 [DEBUG] Total replicas found: 2
2026/01/16 13:59:16 [DEBUG] UpdateReplicas before filter: replicas=[10.111.47.136:6379 10.111.40.28:6379], masterAddr=10.111.35.50:6379
2026/01/16 13:59:16 [DEBUG] Checking replica 10.111.47.136:6379 against master 10.111.35.50:6379
2026/01/16 13:59:16 [DEBUG] Checking replica 10.111.40.28:6379 against master 10.111.35.50:6379
2026/01/16 13:59:16 [DEBUG] UpdateReplicas after filter: filtered=[10.111.47.136:6379 10.111.40.28:6379]
2026/01/16 13:59:16 [DEBUG] Initial setup: master 10.111.35.50:6379, replicas [10.111.47.136:6379 10.111.40.28:6379]
2026/01/16 13:59:16 Waiting for connections...
2026/01/16 13:59:16 Waiting for connections...