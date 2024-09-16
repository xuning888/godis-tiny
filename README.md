# Godis-tiny - Redis Server in golang

[中文](./README_ZH.md)
## Introduction
This project was created during my study of [redis3.2](https://github.com/redis/redis/tree/3.2) with the aim of gaining a deeper understanding of the implementation principles of the Redis server and Go programming. This project is mainly a learning-oriented project for self-learning and exploration.

## Current Features
- Implementation of several Redis commands
- Modified command handling to single-threaded consumption, simplifying thread safety and locking mechanisms
- TTL mechanism shifted from a time-wheel to a priority queue sorted by expiration time, combining scheduled and random cleanup for expired keys
- Network handling using [gnet](https://github.com/panjf2000/gnet)
- Support for AOF (Append Only File) and AOF Rewrite

## Planned Features
- Implementation of the five basic Redis data structures
- Support for RDB (Redis Database File) persistence
- Exploration and implementation of Redis master-slave replication

## Supported Operating Systems
- Linux
- macOS
- Windows (Performance may be limited due to partial gnet support)

## Acknowledgements
- HDT3213's [godis](https://github.com/HDT3213/godis)
- panjf2000's [gnet](https://github.com/panjf2000/gnet)