import config
import socket
import threading
import re
import os
import pickle

# 配置信息
HOST = config.host
PORT = config.port
FILE_NAME = config.file_name

# 全局字典存放各种类型的数据
global er


def init():
    """
    初始化EasyRedis
    初始化全局字典存数据
    或将持久化文件的数据读取到全局字典中
    """
    global er
    er = {}
    if os.path.exists(FILE_NAME):  # 若持久化文件存在则读取文件并设置为全局自字典
        with open(FILE_NAME, 'rb') as f:  # 打开文件
            er = pickle.load(f)  # 将二进制文件对象转换成 Python 对象


def string_set(key, value):
    """
    字符数据类型的set方法 设置key的值为value
    :param key: key值
    :param value: value值
    :return: 设置成功返回 1 ，若参数为空返回 错误提示信息
    """
    if value:
        er[key] = value[0]
        return respond_handle('integers', '1')
    else:
        return respond_handle('errors', '2')


def string_get(key, value=None):
    """
    字符数据类型的get方法 获取key的值
    :param key: key值
    :param value: value值
    :return: 值存在返回值，不存在返回nil
    """
    if key not in er:
        return respond_handle('bulk_strings', 'nil')
    if isinstance(er[key], str):
        return respond_handle('bulk_strings', er[key])
    else:
        return respond_handle('errors', '3')


def exist(key, value=None):
    """
    字符数据类型的exist方法 判断key是否存在值
    :param key: key值
    :return: 存在返回1，不存在返回0
    """
    if key in er:
        return respond_handle('integers', '1')
    else:
        return respond_handle('integers', '0')


def string_incr(key, value=None):
    """
    字符数据类型的incr方法
    若key的值为数字类型的值，则值自增 1
    若key的值不为数字类型，返回错误信息
    若key不存在，则设置key的值为 1
    :param key: key值
    :return: 成功自增返回 1 ，失败返回 错误信息
    """
    if key not in er:
        er[key] = '1'
        return respond_handle('integers', '1')
    if not isinstance(er[key], str):
        return respond_handle('errors', '3')
    if er[key].isdigit():
        er[key] = str(int(er[key]) + 1)
        return respond_handle('integers', '1')
    return respond_handle('errors', '4')



def list_lpush(key, value):
    """
    将所有指定的值插入到存于 key 的列表的头部。
    如果 key 不存在，那么在进行 push 操作前会创建一个空列表。
    如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
    可以使用一个命令把多个元素 push 进入列表，只需在命令末尾加上多个指定的参数。
    元素是从最左端的到最右端的、一个接一个被插入到 list 的头部。
    所以对于这个命令例子 LPUSH mylist a b c，
    返回的列表是 c 为第一个元素， b 为第二个元素， a 为第三个元素。
    :param key: key值
    :param value: 列表值，lpush的值
    :return:
    """
    if key not in er:
        er[key] = []
    if not isinstance(er[key], list):
        return respond_handle('errors', 3)
    if value:
        for i in value:
            er[key].insert(0, i)
        return respond_handle('integers', len(value))
    return respond_handle('errors', 2)


def list_rpop(key, value=None):
    """
    移除并返回存于 key 的 list 的最后一个元素。
    :param key: key值
    :return: 最后一个元素的值，或者当 key 不存在的时候返回 nil。
    """
    if key not in er:
        return respond_handle('bulk_strings', 'nli')
    if not isinstance(er[key], list):
        return respond_handle('errors', 3)
    return respond_handle('bulk_strings', er[key].pop(-1))


def list_lrange(key, value=None):
    """
    返回存储在 key 的列表里指定范围内的元素。
    start 和 end 偏移量都是基于0的下标，即list的第一个元素下标是0（list的表头），
    第二个元素下标是1，以此类推。
    :param key: key值
    :return: 指定范围里的列表元素。
    """
    if key not in er:
        return respond_handle('bulk_strings', 'nli')
    if not isinstance(er[key], list):
        return respond_handle('errors', 3)
    start = 0
    end = len(er[key]) + 1
    if len(value) == 2 and str(value[0]).isdigit() and str(value[1]).isdigit():
        start = int(value[0])
        end = int(value[1]) + 1
    l = er[key][start:end]
    return respond_handle('arrays', l)


def set_sadd(key, value):
    """
    添加一个或多个指定的member元素到集合的 key中.指定的一个或者多个元素member
    如果已经在集合key中存在则忽略.如果集合key
    不存在，则新建集合key,并添加member元素到集合key中.
    如果key 的类型不是集合则返回错误.
    :param key: key值
    :param value: value列表
    :return: 添加成功返回添加数量
    """
    if key not in er:
        er[key] = set()
    if not isinstance(er[key], set):
        return respond_handle('errors', 3)
    temp = er[key].copy()
    for i in value:
        er[key].add(i)
    return respond_handle('integers', len(er[key] - temp))


def set_smembers(key, value):
    if key not in er:
        return respond_handle('bulk_strings', 'nli')
    if not isinstance(er[key], set):
        return respond_handle('errors', '3')
    data = list()
    for i in er[key]:
        data.append(i)
    return respond_handle('arrays', data)


def hash_hset(key, value):
    """
    设置 key 指定的哈希集中指定字段的值。
    如果 key 指定的哈希集不存在，会创建一个新的哈希集并与 key 关联。
    如果字段在哈希集中存在，它将被重写。
    :param key: key值
    :param value: value列表
    :return: 成功返回 1 ，失败返回错误
    """
    if key not in er:
        er[key] = dict()
    if not isinstance(er[key], dict):
        return respond_handle('errors', 3)
    if len(value) != 2:
        return respond_handle('errors', 2)  # 返回参数错误
    field = value.pop(0)
    value = value.pop(0)
    er[key][field] = value
    return respond_handle('integers', 1)


def hash_hget(key, value=None):
    """
    返回 key 指定的哈希集中该字段所关联的值
    :param key: key值
    :return: 该字段所关联的值。当字段不存在或者 key 不存在时返回nil。
    """
    if key not in er:
        return respond_handle('bulk_strings', 'nil')
    if len(value) != 1:
        return respond_handle('errors', '2')
    if isinstance(er[key], dict) and value[0] in er[key]:
        return respond_handle('bulk_strings', er[key][value[0]])
    return respond_handle('bulk_strings', 'nil')


def er_save(key=None, value=None):
    """
    持久化指令处理函数
    :return: 返回simple_strings OK
    """
    with open(FILE_NAME, 'wb') as f:  # 打开文件
        pickle.dump(er, f)  # 用 dump 函数将 Python 对象转成二进制对象文件
    return respond_handle('simple_strings', 'OK')


def er_keys(key=None, value=None):
    """
    获取当前数据库的所有key
    :return: 返回当前数据库所有key
    """
    l = er.keys()
    return respond_handle('arrays',l)

# 指令对应的处理函数
switch = {
    'set': string_set,
    'get': string_get,
    'exist': exist,
    'incr': string_incr,
    'lpush': list_lpush,
    'rpop': list_rpop,
    'lrange': list_lrange,
    'sadd': set_sadd,
    'smembers': set_smembers,
    'hset': hash_hset,
    'hget': hash_hget,
    'save': er_save,
    'keys': er_keys,
}


def data_handle(command: str, key=None, value=None) -> str:
    """
    指令处理函数
    :param command: 指令
    :param key: key值
    :param value: value值
    :return: 返回操作结果信息
    """
    try:
        return switch[command](key, value)
    except KeyError:
        return respond_handle('errors', 1)


def request_handle(data: str) -> bytes:
    """
    消息处理函数
    :param data:
    :return:
    """
    pattern = '(.*)\r\n'
    l = re.findall(pattern, data)  # 正则表达式解析协议内容
    c_l = []  # 将数据再次解析到列表方便 后续操作
    for i in range(int(l[0][1:])):  # 根据 *x x的个数获取传过来的参数
        if int(l[2 * i + 1][1:]) == len(l[2 * i + 2]):  # 根据 $x x的大小判断 参数值是否正确
            c_l.append(l[2 * i + 2])  # 将数据再次解析到列表方便
    if len(c_l) > 1:  # 多个参数的指令
        message = data_handle(c_l[0], c_l[1], c_l[2:])
    elif c_l[0] == 'save' or c_l[0] == 'keys':  # 无参指令
        message = data_handle(c_l[0])
    elif c_l[0] in switch:  # 需要参数的指令，但无参数传入，返回错误信息
        message = respond_handle('errors', '2')
    elif c_l[0] not in switch:  # 未知指令
        message = respond_handle('errors', '1')
    else:  # 兜底
        message = respond_handle('errors', '5')
    return message.encode()  # 解析成byte数据 为socket 传输用


def respond_handle(type: str, value=None) -> str:
    """
    响应处理函数，将数据协议化后返回给客户端
    :param type: 五种：'simple_strings'、'integers'、'bulk_strings'、'arrays'、'errors'
    :param value: 需要协议化的数据
    :return: 返回协议化后数据
    """
    if type == 'simple_strings':  # 简单
        return '+{}\r\n'.format(value)
    if type == 'integers':  # 整数
        return ':{}\r\n'.format(value)
    if type == 'bulk_strings':  # 单行
        return '${}\r\n{}\r\n'.format(len(value), value)
    if type == 'arrays':  # 批数据
        message = str()
        message += '*{}\r\n'.format(len(value))  # 获取字符串列表长度 得到 *len 第一个参数
        for i in value:  # 获取每个参数长度 得到 $len
            message += '${}\r\n{}\r\n'.format(len(i), i)
        return message
    if type == 'errors':  # 错误
        return '-{}\r\n'.format(value)
    return '-(error)\r\n'

def thread_conn(conn, address):
    """
    每个连接的线程的
    处理函数
    :param conn:  socket连接
    :param address:  连接IP
    :return:
    """
    print('Connected by', address)  # 输出客户端的IP地址
    while 1:
        try:
            data = conn.recv(1024)  # 把接收的数据实例化
        except (ConnectionResetError, ConnectionAbortedError):  # 客户端关闭连接
            break
        data = data.decode()
        if str(data).strip() == 'quit':  # 客户端退出连接
            break

        # 处理客户端发来请求 , 返回bytes响应数据
        message = request_handle(str(data))
        conn.sendall(message)  # 把结果发送给客户端

    # 上面循环断开说明连接断开，下面打印连接断开，并关闭连接
    print('The connection of {} is closed.'.format(address))
    conn.close()  # 关闭连接


def start():
    """
    主线程
    """
    init()  # 初始化EasyRedis
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(20)  # 设置最大的监听数量
    list_conn_threading = []  # 连接线程存放列表
    print('The server starts waiting for the connection.')
    while True:
        conn, address = s.accept()  # 接受客户端连接
        # 将客户端信息包装成字典数据
        kwargs = {
            'conn': conn,
            'address': address
        }
        thread = threading.Thread(target=thread_conn, kwargs=kwargs)  # 创建线程去处理每一个客户端发来的请求
        thread.start()  # 启动线程
        list_conn_threading.append(thread)  # 将每条线程放如一个list中


start()
