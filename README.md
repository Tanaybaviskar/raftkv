# RaftKV

A distributed key-value store built from scratch in Go, implementing the
Raft consensus algorithm without external consensus libraries.

## Goal

Most general-purpose distributed databases (etcd, Cassandra, CockroachDB)
optimize for steady-state throughput. This project is optimized specifically
for **fast failover** — recovering quickly when a node crashes — and aims to
benchmark that recovery time directly against etcd under identical conditions.

## Status: Work in progress (Day 1 of 20)

- [x] Leader election with randomized timeouts, terms, and majority voting
- [x] Verified correct behavior under simulated node failures (majority-safety holds)
- [ ] Log replication
- [ ] Real network transport (TCP/gRPC)
- [ ] Persistence (write-ahead log)
- [ ] Fast-failover optimization (incremental state transfer)
- [ ] Benchmarks vs etcd

## Why this project

Built as a deep dive into distributed systems fundamentals — consensus,
replication, and fault tolerance — rather than relying on a managed database
or existing consensus library.