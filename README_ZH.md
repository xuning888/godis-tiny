# Godis - 用 Go 实现的 Redis 服务器

## 简介
这个项目是我在学习 [HDT3213 的 godis](https://github.com/HDT3213/godis) 源码过程中创建的，目的是更深入地理解 Redis 服务器的实现原理和 Go 语言编程。这个项目主要是一个学习性质的项目，供自我学习和探讨使用。

## 当前已实现的功能
- 实现了部分 Redis 命令
- 将命令处理方式改为单线程消费，简化了线程安全和锁的实现
- 将 TTL 机制由时钟轮改为按过期时间排序的优先队列，通过定时清理和主动随机清理相结合的方式处理过期 key
- 使用 [gnet](https://github.com/panjf2000/gnet) 作为网络库
- 支持 AOF 及 AOF 重写

## 计划实现的功能
- 实现 Redis 的五种基本数据结构
- 实现 RDB（Redis 数据库文件）持久化
- 探索和实现 Redis 的主从复制模式

## 支持的操作系统
- Linux
- macOS
- Windows（由于 gnet 对 Windows 的支持不完全，性能可能受到影响）

## 说明
这个项目是出于学习目的而创建的，灵感和基础来自 [HDT3213 的 godis](https://github.com/HDT3213/godis)。欢迎有兴趣的朋友一起讨论和学习，但请注意这是一个个人学习项目。