# Godis - Redis Server Implementation in Go

[中文](./README_ZH.md)
## Introduction
This project is a learning exercise inspired by [HDT3213's godis](https://github.com/HDT3213/godis). The goal is to gain a deeper understanding of the inner workings of a Redis server and Go programming by examining and implementing parts of the godis codebase. This is primarily a study project for educational purposes.

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

## Note
This project is derived from my study of [HDT3213's godis](https://github.com/HDT3213/godis). It is intended for learning and educational purposes. Contributions and feedback are welcome, but please note this is a personal study project and may not be suitable for production use.