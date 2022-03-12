# easy-redis-by-python

一个用Python实现的简单的Redis

A simple redis coded by python

###1.设计思路

EasyRedis：使用Python3实现的简易的Redis。

具体思路：EasyRedis使用python中的低级别的网络服务支持基本的Socket，去访问底层操作系统 Socket 接口。使用其创建服务端和客户端，服务端可以监听客户端发送的请求并创建连接。创建连接时，服务端会每个客户端创建一个进程，在进程中维持与客户端的通讯，服务端有一个连接列表，服务端每创建一个连接都会将所创建的进行放入其中。服务端与客户端之间通讯协议严格遵守Redis2.0以后使用的Redis服务其通讯新标准协议。
数据存储实现：

EasyRedis 使用一个名为er的全局字典存放所有数据

字符串：在er中以键值对的形式进行存储，键值都为字符串数据类型，并实现set、get、exist、incr指令

列表：在er中存放列表，列表名为字符串数据类型，列表的值使用Python已经封装好的列表数据类型，并实现lpush、rpop、lrange指令

集合：在er中存放集合，集合名为字符串数据类型，集合的值使用Python已经封装好的集合数据类型，并实现sadd、smembers指令

哈希：在er中存放哈希表，哈希表名为字符串数据类型，用Python的字典数据类型存放，并实现哈希命令：hset，hget

持久化：通过save命令，将er全局字典通过序列化为二进制数据，将数据存放进文件中。启动EasyRedis时先将数从文件中读取，并赋值给er，实现持久化数据。

###2.采用的语言、框架、通信协议说明

开发语言：
Python 3.7.4

使用框架：
Python中低级别的网络服务支持基本的 Socket

通信协议：
严格遵守redis2.0中Redis服务器通讯的新标准协议。

持久化协议：
二进制序列化
二进制序列化框架：
Python pickle模块

####分享学习，谢谢支持。