import threading
from influxdb import InfluxDBClient
from WindPy import *
from queue import Queue
import time
from datetime import datetime

influxdb_url = "influxdb-staging.apps.fedge.cn"
port = 80
database = "test"
username = "root"
password = "root"

client = InfluxDBClient(host=influxdb_url, port=port, username=username, password=password, database=database,
                        timeout=10)

w.start();

au_code = "au1912.SHF"
codes = [au_code, "518880.SH"]
raw_queue = Queue()

# 缓存行情数据0-4是ask1-5, 5-9是bid1-5, 10-14是asize1-5, 15-19是bsize1-5
field_map = {"RT_ASK1": 0, "RT_ASK2": 1, "RT_ASK3": 2, "RT_ASK4": 3, "RT_ASK5": 4, "RT_BID1": 5, "RT_BID2": 6,
             "RT_BID3": 7, "RT_BID4": 8, "RT_BID5": 9, "RT_ASIZE1": 10, "RT_ASIZE2": 11, "RT_ASIZE3": 12,
             "RT_ASIZE4": 13, "RT_ASIZE5": 14, "RT_BSIZE1": 15, "RT_BSIZE2": 16, "RT_BSIZE3": 17, "RT_BSIZE4": 18,
             "RT_BSIZE5": 19}
etf_buffer = [-1 for i in range(20)]
au_buffer = [-1 for i in range(20)]

# 平均价格缓存
etf_price = []
au_price = []


def myCallback(indata):
    """
    用于处理行情的回调函数

    :param indata:
    :return:
    """

    print(indata)

    if indata.ErrorCode != 0:
        print('error code:' + str(indata.ErrorCode) + '\n')
        return

    # 获取推送行情代码
    Codes = indata.Codes
    # 获取推送行情指标
    Fields = indata.Fields
    # 获取推送行情数据
    data = indata.Data
    # 获取接收到推送行情的日期时间
    receive_time = indata.Times[0]

    for i in range(len(Codes)):
        # 取出该品种对应的数据
        new_data = [d[i] for d in data]

        if "RT_TIME" in Fields:
            # 如果包含实时时间，更新缓存后，加入到数据库中
            utc_datetime = get_datetime(receive_time, new_data[0])
            update_cache(Codes[i], Fields[1:], new_data[1:])
            add_data(Codes[i], utc_datetime)
        else:
            # 如果不包含实时时间，只更新缓存
            update_cache(Codes[i], Fields, new_data)


def get_datetime(receive_time, real_time):
    """
    根据receive_time得到日期，根据real_time得到行情时间，拼接为行情对应的日期时间戳 (UTC时区)

    :param receive_time:
    :param real_time:
    :return:
    """

    # 取出日期 receive_time的类型是datetime.datetime
    date = receive_time.strftime("%Y-%m-%d")
    # 将日期转化为时间戳格式
    date_stamp = time.mktime(time.strptime(date, "%Y-%m-%d"))

    # real_time是用浮点数来表示时间 e.g. 111340.0
    # 获取秒数
    second = real_time % 100
    # 获取分钟
    minute = int((real_time / 100) % 100)
    # 获取小时
    hour = int((real_time / 10000) % 100)
    # 计算real_time对应多少秒
    time_stamp = second + minute * 60 + hour * 3600
    # 日期+时间对应的时间戳
    stamp = date_stamp + time_stamp

    utc_datetime = datetime.utcfromtimestamp(stamp)

    return utc_datetime


def update_cache(code, fields, new_data):
    if code == "518880.SH":
        for i in range(len(fields)):
            # 获取行情指标在缓存中的索引
            field_index = field_map[fields[i]]
            # 更新缓存中的指标数据
            etf_buffer[field_index] = new_data[i]
    else:
        for i in range(len(fields)):
            field_index = field_map[fields[i]]
            au_buffer[field_index] = new_data[i]


def add_data(code, utc_datetime):
    """
    把数据加到缓存队列里

    :param code:
    :param utc_datetime:
    :param ask1_price:
    :param bid1_price:
    :return:
    """

    # 非交易时间的数据不收录
    t = utc_datetime.strftime("%H%M%S")
    t = int(t)

    # 不包括十点十五到十点半的数据,一点到一点半的也不包括
    # if t < 13000 or (21500 < t < 23000) or (33000 < t < 53000) or t > 70000:
    #     print(t)
    #     return

    if t < 13000 or (33000 < t < 50000) or t > 70000:
        print(t)
        return

    if code == "518880.SH":
        cache_data = etf_buffer
    else:
        cache_data = au_buffer

    raw_data = {
        "measurement": "au_md_wind_v2",
        "tags": {
            "symbol": code
        },
        "time": utc_datetime,
        "fields": {
            "ask_1": cache_data[0],
            "ask_2": cache_data[1],
            "ask_3": cache_data[2],
            "ask_4": cache_data[3],
            "ask_5": cache_data[4],
            "bid_1": cache_data[5],
            "bid_2": cache_data[6],
            "bid_3": cache_data[7],
            "bid_4": cache_data[8],
            "bid_5": cache_data[9],
            "ask_v_1": cache_data[10],
            "ask_v_2": cache_data[11],
            "ask_v_3": cache_data[12],
            "ask_v_4": cache_data[13],
            "ask_v_5": cache_data[14],
            "bid_v_1": cache_data[15],
            "bid_v_2": cache_data[16],
            "bid_v_3": cache_data[17],
            "bid_v_4": cache_data[18],
            "bid_v_5": cache_data[19],
            "avg_price": round((cache_data[0] + cache_data[5]) / 2, 4)
        }
    }

    raw_queue.put(raw_data)
    print("添加数据{}".format(raw_data))


def process_data():
    """
    并发处理数据，写入数据库
    :return: Null
    """

    while True:
        raw_data = process_raw_data()
        put_avg_price(raw_data)
        diff = calculate_diff()

        # 如果有基差数据
        if diff:
            w_data = []

            for d in diff:
                w_data.append({
                    "measurement": "au_diff_wind",
                    "tags": {
                        "symbol": "au",
                    },
                    "time": d["time"],
                    "fields": {
                        "etf_avg_price": float(d["etf_avg_price"]),
                        "etf_avg_price*100": float(d["etf_avg_price*100"]),
                        "au_avg_price": float(d["au_avg_price"]),
                        "diff": float(d["diff"])
                    }
                })

            print("Diff data:")
            print(w_data)

            try:
                client.write_points(w_data)
                print("Write diff data")
            except Exception as err:
                print("Fail to write diff data!")
                print(str(err))

        time.sleep(3)

    return


def process_raw_data():
    """
    把原始数据写入到数据库中
    :return: Null
    """

    write_data = []

    while True:
        if raw_queue.empty():
            break

        write_data.append(raw_queue.get())

    if write_data:
        # 把原始数据写入到表中
        try:
            client.write_points(write_data)
            print("Write raw data")
        except Exception as err:
            print("Fail to write raw data!")
            print(str(err))

    return write_data


def put_avg_price(raw_data):
    """
    把平均价格放到两个对应的数组里
    :param raw_data:
    :return:
    """
    # print(len(raw_data))

    for data in raw_data:
        # 计算基差用的平均价格
        # avg_price = round((data["fields"]["bid_1"] + data["fields"]["ask_1"]) / 2, 3)
        # price_data = {"avg_price": avg_price, "time": data["time"]}
        # 获取平均价格
        price_data = {"avg_price": data["fields"]["avg_price"], "time": data["time"]}

        symbol = data["tags"]["symbol"]
        type = str.split(symbol, ".")[1]

        # 把价格数据放到对应的数组里，每个数组维护长度为25
        if type == "SHF":
            au_price.append(price_data)
            if len(au_price) == 26:
                au_price.pop(0)
        elif type == "SH":
            etf_price.append(price_data)
            if len(etf_price) == 26:
                etf_price.pop(0)


def calculate_diff():
    """
    计算基差
    :return: 基差数据
    """

    etf_copy = etf_price.copy()
    au_copy = au_price.copy()

    # 获取时间同步的数据
    etf_data, au_data, length = sync_timestamp(etf_copy, au_copy)

    basis = []

    # 无时间同步的数据
    if length == 0:
        return

    # 这里的现货和期货数据是排好序的
    for i in range(0, length):
        t = etf_data[i]["time"]
        etf_avg_price = float(etf_data[i]["avg_price"])
        etf_avg_price_100 = round(etf_avg_price * 100, 4)

        au_avg_price = 0
        for d in au_data:
            if d["time"] == t:
                print("time {} | {}".format(d["time"], t))
                au_avg_price = float(d["avg_price"])
                break

        diff = round(etf_avg_price_100 - au_avg_price, 4)
        basis.append({"time": t, "etf_avg_price": etf_avg_price, "etf_avg_price*100": etf_avg_price_100,
                      "au_avg_price": au_avg_price, "diff": diff})

    return basis


def sync_timestamp(etf_data, au_data):
    """
    将查询到的现货和期货数据在时间上进行同步
    :param etf_data: 查询到的现货数据
    :param au_data: 查询到的期货数据
    :return: 时间同步以后的现货和期货数据
    """

    etf_time = []
    au_time = []

    for d in etf_data:
        etf_time.append(d["time"])

    for d in au_data:
        au_time.append(d["time"])

    etf_time = set(etf_time)
    au_time = set(au_time)

    # 同步的时间点
    comm_time = etf_time & au_time

    new_etf_data = remove_data(etf_data, comm_time)
    new_au_data = remove_data(au_data, comm_time)

    return new_etf_data, new_au_data, len(comm_time)


def remove_data(data, comm_time):
    """
    从原始数据中移除不同步的数据点
    :param data: 原始数据
    :param comm_time: 同步的时间点
    :return: 移除不同步的时间点后的数据
    """

    new_data = []

    for d in data:
        if d["time"] in comm_time:
            new_data.append(d)

    return new_data


if __name__ == '__main__':
    # 订阅行情
    w.wsq(codes,
          "rt_time, rt_ask1, rt_ask2, rt_ask3, rt_ask4, rt_ask5, rt_bid1, rt_bid2, rt_bid3, rt_bid4, rt_bid5, "
          "rt_asize1, rt_asize2, rt_asize3, rt_asize4, rt_asize5, rt_bsize1, rt_bsize2, rt_bsize3, rt_bsize4, "
          "rt_bsize5", func=myCallback)

    t = threading.Thread(target=process_data)
    t.setDaemon(True)
    t.start()

    while (1):
        info = "这个while循环主要是防止IDE在运行或者debug时，运行w.wsq()语句后就退出，从而导致行情推送过来后，回调函数无法运行！"
