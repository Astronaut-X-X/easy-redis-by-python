import config
import socket
import re

HOST = config.host
PORT = config.port


def respond_handle(data: str) -> str:
    if data[0] == '+':
        l = re.findall('\+(.*)\r\n', data)
        return l[0]
    if data[0] == ':':
        l = re.findall(':(.*)\r\n', data)
        return '(integer)'+l[0]
    if data[0] == '$':
        l = re.findall('(.*)\r\n', data)
        return l[1] if int(l[0][1:]) == len(l[1]) else 'error'
    if data[0] == '*':
        pattern = '(.*)\r\n'
        l = re.findall(pattern, data)  # 正则表达式解析协议内容
        c_l = []  # 将数据再次解析到列表方便 后续操作
        for i in range(int(l[0][1:])):  # 根据 *x x的个数获取传过来的参数
            if int(l[2 * i + 1][1:]) == len(l[2 * i + 2]):  # 根据 $x x的大小判断 参数值是否正确
                c_l.append(l[2 * i + 2])  # 将数据再次解析到列表方便
        return c_l
    if data[0] == '-':
        l = re.findall('-(.*)\r\n', data)
        if l[0] == '1':
            return '(error) ERR unknown command'
        elif l[0] == '2':
            return '(error) ERR wrong number of arguments'
        elif l[0] == '3':
            return '(error) WRONGTYPE Operation against a key holding the wrong kind of value'
        elif l[0] == '4':
            return '(error) ERR value is not an integer or out of range'
        else:
            return '(error)'


def is_quit(str_cmd: str) -> bool:
    if str_cmd.strip() == 'quit':
        return True
    return False


def transform_input(str_cmd: str) -> str:
    message = ''  # 定义一个返回字符串
    temp = str_cmd.strip()  # 去除两端多余空格
    list_cmd = re.findall('\S+', temp)  # 根据空格转化为字符串列表
    message += '*{}\r\n'.format(len(list_cmd))  # 获取字符串列表长度 得到 *len 第一个参数
    for i in list_cmd:  # 获取每个参数长度 得到 $len
        message += '${}\r\n{}\r\n'.format(len(i), i)
    return message


def start():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 定义socket类型，网络通信，TCP
    try:
        s.connect((HOST, PORT))  # 要连接的IP与端口
    except ConnectionRefusedError:
        print('由于目标计算机积极拒绝，无法连接。')  # 无法连接服务器
        return  # 停止程序
    while 1:
        cmd = input("EasyRedis->:")  # 与人交互，输入命令
        if is_quit(cmd):  # 判断是否退出客户端
            break
        cmd = transform_input(cmd)  # 将输入的命令协议化
        s.sendall(cmd.encode())  # 把命令转为二进制 并 发送给对服务器端
        data = s.recv(1024)  # 把接收的数据定义为变量
        print(respond_handle(data.decode()))  # 把接受数据转为字符串 并 输出


start()
