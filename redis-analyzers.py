# -*- coding: utf-8 -*-

# @{PROJECT_NAME}
# @Author ：TLQ
# @Time ：2021/3/5 18:36

# Version:v1.1  #1.改进，排序方式改为降序。 #2.大于5天的数据没有，不能pass，应该返回为0。



import struct
import xlwt
import operator
import prettytable as pt
import json
import configparser
import time

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

try:
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

from rdbtools import RdbParser, MemoryCallback

from redis import StrictRedis
from redis.exceptions import ConnectionError, ResponseError



sizes_list = []
key_num = 0.00

def print_memory_for_key(key, host='localhost', port=6379, db=0, password=None):
    redis = connect_to_redis(host, port, db, password)
    reporter = PrintMemoryUsage()
    callback = MemoryCallback(reporter, 64)
    parser = RdbParser(callback, filters={})
    #  DUMP command only return the key data, so we hack RdbParser to inject key name as parsed bytes.
    parser._key = key.encode('utf-8')

    raw_dump = redis.execute_command('dump', key)
    if not raw_dump:
        sys.stderr.write('Key %s does not exist\n' % key)
        sys.exit(-1)

    stream = BytesIO(raw_dump)
    data_type = read_unsigned_char(stream)
    #parser.read_object(stream, data_type)
    parser.read_object(stream, data_type)


def connect_to_redis(host, port, db, password):
    try:
        redis = StrictRedis(host=host, port=port, db=db, password=password)
        if not check_redis_version(redis):
            sys.stderr.write('This script only works with Redis Server version 2.6.x or higher\n')
            sys.exit(-1)
    except ConnectionError as e:
        sys.stderr.write('Could not connect to Redis Server : %s\n' % e)
        sys.exit(-1)
    except ResponseError as e:
        sys.stderr.write('Could not connect to Redis Server : %s\n' % e)
        sys.exit(-1)
    return redis

def check_redis_version(redis):
    server_info = redis.info()
    version_str = server_info['redis_version']
    version = tuple(map(int, version_str.split('.')))

    if version[0] > 2 or (version[0] == 2 and version[1] >= 6) :
        return True
    else:
        return False

def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]



class PrintMemoryUsage(object):
    def next_record(self, record) :
        global sizes_list
        sizes_list.append(record.bytes)
        if record.type in ('set', 'list', 'sortedset', 'hash'):#
            key_size = record.__sizeof__()
            #print("size1")
            sizes_list.append(key_size)

num_string = 0
num_hash = 0
num_zset = 0
num_list = 0
num_set = 0


def scan_redis(pool,host,port,db, password):

    global sizes_list
    global key_num
    global num_string
    global num_hash
    global num_zset
    global num_list
    global num_set

    datas_list = []
    key_num = pool.dbsize()
    cursor,counts = 0,0
    while True:
        cursor,keys = pool.scan(cursor,match="*",count=10)
        counts += len(keys)
        for key in keys:
            datas_dict={}
            if pool.type(key) == 'hash':
                value_size = pool.hscan(key,cursor,match="*",count=1)[1].__sizeof__()
                num_hash += 1
            elif pool.type(key) == 'list':
                list_s=[]
                for i in range(pool.llen(key)):
                    list_s.append(pool.lindex(key,i))
                value_size = list_s.__sizeof__()
                num_list += 1
            elif pool.type(key) == 'set':
                value_size = pool.sscan(key)[1].__sizeof__()
                num_set += 1
            elif pool.type(key) =='zset':
                value_size = pool.zscan(key)[1].__sizeof__()
                num_zset += 1
                # print pool.lrange(key,0,-1)
                # value_size = pool.lrange(key,0,-1).__sizeof__()
            else:
                #key_size = key.decode("utf-8").__sizeof__()
                value_size = sys.getsizeof(pool.get(key.decode("utf-8", "replace")))
                num_string += 1
            print_memory_for_key(key.decode("utf-8", "replace"), host, port,
                                 db, password)
            key_size=sizes_list[0]
            sizes_list=[]
            key_ttl = str(pool.ttl(key.decode("utf-8", "replace")))
            datas_dict["key"]=key
            datas_dict["key_size"]=key_size
            datas_dict["key_type"]=pool.type(key)
            datas_dict["value_size"]=value_size
            datas_dict["key_ttl"]=key_ttl
            datas_list.append(datas_dict)
        Rate = len(datas_list)/float(key_num)
        print "\r 一共有%d个key,Scanf的进度是：%.3f%%"%(key_num,Rate*100),
        if len(datas_list) == int(key_num):
            break

        if cursor == 0:
            break
    print("\n已经Scanf完成了，数据正在生成。")
    return datas_list

def get_excel(datas):

    wbk = xlwt.Workbook()
    sheet = wbk.add_sheet('sheet 1')
    header=["key","key_type","key_size","value.size","TTL"]

    for i in range(len(header)):
        sheet.write(0,i,header[i])#第0行第一列写入内容
    for j in range(len(datas)):
        how = j+1
        sheet.write(how,0,datas.keys()[j])
        sheet.write(how,1,datas[datas.keys()[j]][0])
        sheet.write(how,2,datas[datas.keys()[j]][1])
        sheet.write(how,3,datas[datas.keys()[j]][2])
        sheet.write(how,4,datas[datas.keys()[j]][3])
    wbk.save('Domains-3.xls')


def data_update(datas,types):
    sorted_x = sorted(datas, key=operator.itemgetter(types),reverse=True)
    return sorted_x

def get_num_data(datas_1,num):
    datas = []
    j = 0
    num = int(num)
    if num <= len(datas_1):
        while j < num:
            datas.append(datas_1[j])
            j = j+1
    else:
        return datas_1
    return datas

def application_1(datas,types,num,strings):   #输出前n个，以xxx开头的key的信息,默认输出前100个key。

    datas_1 = []
    for i in datas:
        if strings:
            if i.get("key").startswith(strings):
                datas_1.append(i)
            else:
                pass
        else:
            datas_1.append(i)
    datas = data_update(datas_1,types)
    datas = get_num_data(datas,num)
    return datas

def application_2(datas,types,num):   #输出所有没有ttl的key.
    datas_1 = []
    for i in datas:
        if i.get("key_ttl") == "-1":
            datas_1.append(i)
    datas = data_update(datas_1,types)
    datas = get_num_data(datas,num)
    return datas

def application_3(datas,time,num,types):   #输出ttl小于n的key，默认n=1d
    datas_1 = []
    for i in datas:
        if i.get("key_ttl") != "-1":
            if time:
                if int(i.get("key_ttl")) > int(int(time)*86400):
                    datas_1.append(i)
                else:
                    pass
            else:
                if int(i.get("key_ttl")) > int(86400):
                    datas_1.append(i)
                else:
                    pass
        else:
            pass
    datas = data_update(datas_1,types)
    datas = get_num_data(datas,num)
    return datas

def application_4(datas,key_size,types,num):    #输出key_size大于n的前m个key，默认size为10M，前100个。
    datas_1 = []
    for i in datas:
        if key_size:
            if int(i.get("key_size")) > int(float(key_size)*1024*1024):
                datas_1.append(i)
            else:
                pass
        else:
            if int(i.get("key_size")) > int(1024*1024):
                datas_1.append(i)
            else:
                pass
    datas = data_update(datas_1,types)
    datas = get_num_data(datas,num)

    return datas

def application_5(datas,value_size,types,num):    #输出value_size大于n的前m个key，默认size为10M，前100个。
    datas_1 = []
    for i in datas:
        if value_size:
            if int(i.get("value_size")) > int(float(value_size)*1024*1024):
                datas_1.append(i)
            else:
                pass
        else:
            if int(i.get("value_size,")) > int(1024*1024):
                datas_1.append(i)
            else:
                pass

    datas = data_update(datas_1,types)
    datas = get_num_data(datas,num)

    return datas


def get_table(datas):

    tb = pt.PrettyTable()
    tb.padding_width = 3
    if len(datas) == 0:
        tb.field_names = ["key_type","key_ttl","value_size","key","key_size"]
        print tb
    else:
        tb.field_names = datas[0].keys()
        for i in datas:
            tb.add_row(i.values())
        print tb

#def main_1():
#     usage = """usage: %prog [options] redis-key
# Examples :
# %prog user:13423
# %prog -s localhost -p 6379 user:13423
# """
#
#     parser = OptionParser(usage=usage)
#     parser.add_option("-s", "--server", dest="host", default="192.168.81.100",
#                       help="Redis Server hostname. Defaults to 127.0.0.1")
#     parser.add_option("-p", "--port", dest="port", default=6379, type="int",
#                       help="Redis Server port. Defaults to 6379")
#     parser.add_option("-a", "--password", dest="password",
#                       help="Password to use when connecting to the server")
#     parser.add_option("-d", "--db", dest="db", default=0,
#                       help="Database number, defaults to 0")


    # (options, args) = parser.parse_args()
    #print_memory_for_key(redis_key, host=options.host, port=options.port,
    #db=options.db, password=options.password)
    # redis=connect_to_redis(host='192.168.81.100', port=6379, db=0, password=None)
    #
    # datas = scan_redis(redis, host=options.host, port=options.port, db=options.db, password=options.password)
    # #get_excel(datas)
    #
    # datas_json = json.dumps(datas, sort_keys=False, ensure_ascii=False, indent=4,
    #                         separators=(',', ': '))
    # f = open('datas_json', 'w')
    # f.write(datas_json)
    # return datas

def get_json_dict(host):
    try:
        load_json = open('datas_json', 'r')
        datas = json.load(load_json)

    except IOError:
        datas =get_scanf_datas(host)
    return datas


def apps(host):

    datas = get_json_dict(host)
    print ("""
        功能菜单
        1.输出前n个，以XXX开头的key的信息。
        2.输出没有ttl的key的信息,默认500条。
        3.输出ttl大于n的key的信息。
        4.输出key_size大于n的前m个key的信息。
        5.输出value_size大于n的前m个key的信息。
        0.退出
        """
           )
    youinput = int(raw_input("Input num is:"))
    if youinput == 1:
        num = int(raw_input("pl input n:") or 50)
        strings = raw_input("pl input XXX:") or None
        types = raw_input("pl input sort by type:") or "key"
        datas = application_1(datas,types,num,strings)
        get_table(datas)
    elif youinput == 2:
        num = int(raw_input("pl input n:") or 500)
        types = raw_input("pl input sort by type:") or "key"
        datas = application_2(datas,types,num)
        get_table(datas)
    elif youinput == 3:
        time = raw_input("pl input times:") or None
        num = int(raw_input("pl input n:") or 500)
        types = raw_input("pl input sort by type:") or "key_ttl"
        datas = application_3(datas,time,num,types)
        get_table(datas)
    elif youinput == 4:
        key_size = raw_input("pl input key_size:") or None
        num = int(raw_input("pl input n:") or 50)
        types = raw_input("pl input sort by type:") or "key_size"
        datas = application_4(datas,key_size,types,num)
        get_table(datas)

    elif youinput == 5:
        value_size = raw_input("pl input value_size:") or None
        num = int(raw_input("pl input n:") or 50)
        types = raw_input("pl input sort by type:") or "value_size"
        datas = application_4(datas,value_size,types,num)
        get_table(datas)
    elif youinput == 0:
        exit()

def get_scanf_datas(host):

    redis=connect_to_redis(host=host, port=6379, db=0, password=None)
    datas = scan_redis(redis, host=host, port="6379", db=0, password=None)
    datas_json = json.dumps(datas, sort_keys=False, ensure_ascii=False, indent=4,
                            separators=(',', ': '))
    f = open('datas_json', 'w')
    f.write(datas_json)
    return datas


def get_access(tokens):

    conf = configparser.ConfigParser()
    try:
        conf.read("./access.ini", encoding="utf8")
        if tokens == "ttl":
            time = conf.get('ttl','ttl_time')
            return time
        elif tokens == "key":
            key_size = conf.get('key', 'key_size')
            return key_size
        elif tokens == "value":
            value_size = conf.get('value', 'value_size')
            return value_size
    except Exception as e:
        print "请检查配置文件./access.ini是否存在。"
        sys.exit()


ttl_null = 0
ttl_1 = 0
ttl_2 = 0
key_1 =0
key_2 =0
value_1 = 0
value_2 = 0



class ttl_key_value_datas:
    def apps_datas(self,i,types):
        global ttl_null
        global ttl_1
        global ttl_2
        global key_1
        global key_2
        global value_1
        global value_2
        time = int(get_access(types))

        if types == "ttl":
            num = 3600*24
            if int(i.get("key_ttl")) == int(-1):
                ttl_null += 1
            elif int(i.get("key_ttl")) <= int(time*int(num)) :#ttl大于1d
                ttl_1 += 1
            elif int(i.get("key_ttl")) > int(time*int(num)) :
                ttl_2 += 1
            else:
                pass
        elif types == "key":
            num = 1024*1024
            if int(i.get("key_size")) <= int(time*int(num)) :
                key_1 += 1
            elif int(i.get("key_size")) > int(time*int(num)) :
                key_2 += 1
            else:
                pass
        else:
            num = 1024*1024
            if int(i.get("value_size")) <= int(time*int(num)) :
                value_1 += 1
            elif int(i.get("value_size")) > int(time*int(num)) :
                value_2 += 1
            else:
                pass

    def ttl_datas(self,i):
        self.apps_datas(i,"ttl")


    def key_datas(self,i):
        self.apps_datas(i,"key")


    def value_datas(self,i):
        self.apps_datas(i,"value")




def checkup(host):

    ttls = get_access("ttl")
    keys = get_access("key")
    values = get_access("value")

    datas = get_json_dict(host)

    apps = ttl_key_value_datas()
    global key_num
    if key_num == 0:
        key_num = len(datas)
    else:
        pass

    global num_string
    global num_hash
    global num_zset
    global num_list
    global num_set

    j = 0
    for i in datas:
        j += 1
        apps.ttl_datas(i)
        apps.key_datas(i)
        apps.value_datas(i)
        Rate = j/float(key_num)
        print "\r 数据生成的进度是：%.3f%%"%(Rate*100),

    print "all keys num : %d" %(key_num)
    print "List类型的key个数：%d" %(num_list)
    print "String类型的key个数：%d" %(num_string)
    print "Hash类型的key个数：%d" %(num_hash)
    print "Set类型的key个数：%d" %(num_set)
    print "Zset类型的key个数：%d" %(num_zset)
    print "keys ttl null : %d" %(ttl_null)
    print "keys ttl 小于 %s 天的个数为: %d" %(ttls,ttl_1)
    print "keys ttl 大于 %s 天的个数为: %d" %(ttls,ttl_2)
    print "keys size 小于%s MB的个数为: %d" %(keys,key_1)
    print "keys size 大于%s MB的个数为: %d" %(keys,key_2)
    print "value size 小于%s MB的个数为: %d" %(values,value_1)
    print "value size 大于%s MB的个数为: %d" %(values,value_2)


def main():

    start =time.clock()
    try:
        host = sys.argv[1]
        argv = sys.argv[2]
        if argv == "checkup":
            checkup(host)
            end = time.clock()
            print 'Running time: %s Seconds'%(end-start)
        elif argv == "apps":
            apps(host)
            end = time.clock()
            print 'Running time: %s Seconds'%(end-start)
        else:
            print ("Please Input checkup or apps")
            exit()
    except IndexError:
        print ("请在脚本名后面输入 $Host [健康检查：checkup | 应用：apps]")

main()



