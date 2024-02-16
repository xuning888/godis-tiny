# 简介
发现了一个[大佬](https://github.com/HDT3213)使用golang语言实现一个redis服务器的项目[godis](https://github.com/HDT3213/godis)。
对这个项目非常感兴趣，所以就学习一下。

在此记录一下学习记录 

# 已经实现的部分
* redis的部分命令
* 把 [godis](https://github.com/HDT3213/godis) 中的命令处理方式改成了单协程消费,这样我就不用太多的关注内部数据结构的线程安全和锁实现
* 把 [godis](https://github.com/HDT3213/godis) 中ttl的实现方式由时钟轮改成了按照过期时间排序的优先队列，采用定时清理和主动随机清理结合的方式来处理过期key
* 使用[netpoll](https://github.com/cloudwego/netpoll)、[gnet](https://github.com/panjf2000/gnet)作为的网络库

# 即将学习和实现的部分
* redis 的5种基本的数据结构
* aof & rdb
* redis主从模式
