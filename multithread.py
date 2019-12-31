# encoding: UTF-8


# 导入框架提供的函数，我们这里先引入订阅股票回调函数，订阅股票，下单
from strategy_platform.api import (sub_realmd, register_realmd_cb)
from strategy_platform.api import (submit_order)
from strategy_platform.api import (add_argument)

from influxdb import InfluxDBClient
import datetime
import time
from queue import Queue
import threading
import copy

# 标的
universe = ["au1912.SHFE", "518880.SH"]

# 账户类型和账户名称， 在策略启动时输入
acct_type = "S0"
acct = "s000001"

# 策略开始和结束时间，格式为"hh:mm:ss" 在该时间段之内才运行策略
# 如果start_time不输入则策略脚本运行时里面启动策略的运行
# 如果end_time不输入则表示策略一直不退出，直到关闭该运行框架
start_time = "09:30:00"
end_time = "20:55:00"

# 启动时配置账号类型和账号等信息
add_argument("acct_type", str, 0, acct_type)
add_argument("acct", str, 0, acct)
add_argument("symbol", list, 0, universe)
add_argument("start_time", str, 0, start_time)
add_argument("end_time", str, 2, end_time)

influxdb_url = "influxdb-staging.apps.fedge.cn"
port = 80
database = "test"
username = "root"
password = "root"

client = InfluxDBClient(host=influxdb_url, port=port, username=username, password=password, database=database,
                        timeout=10)

raw_queue = Queue()
etf_price = []
au_price = []
# 行情缓存
etf_last_time = datetime.datetime.utcfromtimestamp(0)
au_last_time = datetime.datetime.utcfromtimestamp(0)
etf_cache = None
au_cache = None


def on_realmd(realmk_obj, cb_arg):
    """
    订阅"标的"的回调函数，
    :param realmk_obj: 标的 RealMKData 对象
    :param cb_arg: register_realmd_cb注册时传入的变量，一般不会用到
    :return:
    """

    # 获取时间,早九点的时间是9XX不是09XX
    if len(realmk_obj.time) == 8:
        # 过滤半秒的数据
        # if str(realmk_obj.time)[5] != '0':
        #     return
        t = str(realmk_obj.date)[:4] + "-" + str(realmk_obj.date)[4:6] + "-" + str(realmk_obj.date)[6:] + " " + str(
            realmk_obj.time)[:1] + ":" + str(realmk_obj.time)[1:3] + ":" + str(realmk_obj.time)[3:5]
    else:
        # if str(realmk_obj.time)[6] != '0':
        #     return
        t = str(realmk_obj.date)[:4] + "-" + str(realmk_obj.date)[4:6] + "-" + str(realmk_obj.date)[6:] + " " + str(
            realmk_obj.time)[:2] + ":" + str(realmk_obj.time)[2:4] + ":" + str(realmk_obj.time)[4:6]

    # 将本地时间转化为UTC时间
    timeArray = time.strptime(t, "%Y-%m-%d %H:%M:%S")
    stamp = time.mktime(timeArray)
    utc_time = datetime.datetime.utcfromtimestamp(stamp)

    data = {
        "measurement": "au_md_test",
        "tags": {
            "symbol": str(realmk_obj.symbol),
        },
        "time": utc_time,
        "fields": {
            "stop_mark": int(realmk_obj.stopMark),
            "last_price": float(realmk_obj.lastPrice),
            "last_volume": int(realmk_obj.lastVolume),
            "bid_1": float(realmk_obj.bidPrice1),
            "bid_v_1": int(realmk_obj.bidVolume1),
            "bid_2": float(realmk_obj.bidPrice2),
            "bid_v_2": int(realmk_obj.bidVolume2),
            "bid_3": float(realmk_obj.bidPrice3),
            "bid_v_3": int(realmk_obj.bidVolume3),
            "bid_4": float(realmk_obj.bidPrice4),
            "bid_v_4": int(realmk_obj.bidVolume4),
            "bid_5": float(realmk_obj.bidPrice5),
            "bid_v_5": int(realmk_obj.bidVolume5),
            "bid_6": float(realmk_obj.bidPrice6),
            "bid_v_6": int(realmk_obj.bidVolume6),
            "bid_7": float(realmk_obj.bidPrice7),
            "bid_v_7": int(realmk_obj.bidVolume7),
            "bid_8": float(realmk_obj.bidPrice8),
            "bid_v_8": int(realmk_obj.bidVolume8),
            "bid_9": float(realmk_obj.bidPrice9),
            "bid_v_9": int(realmk_obj.bidVolume9),
            "bid_10": float(realmk_obj.bidPrice10),
            "bid_v_10": int(realmk_obj.bidVolume10),
            "ask_1": float(realmk_obj.askPrice1),
            "ask_v_1": int(realmk_obj.askVolume1),
            "ask_2": float(realmk_obj.askPrice2),
            "ask_v_2": int(realmk_obj.askVolume2),
            "ask_3": float(realmk_obj.askPrice3),
            "ask_v_3": int(realmk_obj.askVolume3),
            "ask_4": float(realmk_obj.askPrice4),
            "ask_v_4": int(realmk_obj.askVolume4),
            "ask_5": float(realmk_obj.askPrice5),
            "ask_v_5": int(realmk_obj.askVolume5),
            "ask_6": float(realmk_obj.askPrice6),
            "ask_v_6": int(realmk_obj.askVolume6),
            "ask_7": float(realmk_obj.askPrice7),
            "ask_v_7": int(realmk_obj.askVolume7),
            "ask_8": float(realmk_obj.askPrice8),
            "ask_v_8": int(realmk_obj.askVolume8),
            "ask_9": float(realmk_obj.askPrice9),
            "ask_v_9": int(realmk_obj.askVolume9),
            "ask_10": float(realmk_obj.askPrice10),
            "ask_v_10": int(realmk_obj.askVolume10),
            "flag": 0
        }
    }

    interpolate_value(data, utc_time)

    raw_queue.put(data)

    return


def interpolate_value(raw_data, utc_time):
    """
    插值间隔为一分钟以内的数据
    :param raw_data:
    :param utc_time:
    :return:
    """

    global etf_last_time, etf_cache, au_last_time, au_cache

    if raw_data["tags"]["symbol"] == "518880.SH":
        # 计算两次的行情时间间隔
        interval = (utc_time - etf_last_time).seconds
        if 3 < interval < 60:
            diff = int(interval / 3)
            for i in range(diff - 1):
                # 这里需要深拷贝
                data = copy.deepcopy(etf_cache)
                # t = datetime.datetime.strftime(etf_last_time + datetime.timedelta(seconds=3 * (i + 1)),
                #                                "%Y-%m-%dT%H:%M:%SZ")
                # # 转化为datetime对象，把插值的时间格式与原始数据同步
                # t = datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
                t = etf_last_time + datetime.timedelta(seconds=3 * (i + 1))
                data["time"] = t
                # 修改标记位
                data["fields"]["flag"] = 1
                raw_queue.put(data)

        # 更新缓存
        etf_last_time = utc_time
        etf_cache = raw_data
    else:
        interval = (utc_time - au_last_time).seconds
        if 1 < interval < 60:
            for i in range(interval - 1):
                data = copy.deepcopy(au_cache)
                # t = datetime.datetime.strftime(au_last_time + datetime.timedelta(seconds=(i + 1)),
                #                                "%Y-%m-%dT%H:%M:%SZ")
                # # 转化为datetime对象，在时间同步时需要
                # t = datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
                t = au_last_time + datetime.timedelta(seconds=(i + 1))
                data["time"] = t
                # 修改标记位
                data["fields"]["flag"] = 1
                raw_queue.put(data)

        # 更新缓存
        au_last_time = utc_time
        au_cache = raw_data


def process_data():
    """
    并发处理数据，写入数据库
    :return: Null
    """

    while True:
        raw_data = process_raw_data()
        calculate_avg_price(raw_data)
        diff = calculate_diff()

        # 如果有基差数据
        if diff:
            w_data = []

            for d in diff:
                w_data.append({
                    "measurement": "au_diff",
                    "tags": {
                        "symbol": "au",
                    },
                    "time": d["time"],
                    "fields": {
                        "etf_avg_price": float(d["etf_avg_price"]),
                        "etf_avg_price*100": float(d["etf_avg_price*100"]),
                        "au_avg_price": float(d["au_avg_price"]),
                        "diff": float(d["diff"]),
                        "flag": d["flag"]
                    }
                })

            log.info("Diff data:")
            log.info(w_data)

            try:
                client.write_points(w_data)
                log.info("Write diff data")
            except Exception as err:
                log.info("Fail to write diff data!")
                log.info(str(err))

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
            log.info("Enpty queue")
            break

        write_data.append(raw_queue.get())

    # 把原始数据写入到表中
    try:
        client.write_points(write_data)
        print("Write raw data")
    except Exception as err:
        print("Fail to write raw data!")
        print(str(err))
        on_fini()

    return write_data


def calculate_avg_price(raw_data):
    """
    根据原始数据计算平均价格
    :param raw_data:
    :return:
    """
    print(len(raw_data))

    for data in raw_data:
        # 计算基差用的平均价格
        avg_price = round((data["fields"]["bid_1"] + data["fields"]["ask_1"]) / 2, 4)
        price_data = {"avg_price": avg_price, "time": data["time"], "flag": data["fields"]["flag"]}

        symbol = data["tags"]["symbol"]
        type = str.split(symbol, ".")[1]

        # 把价格数据放到对应的数组里，每个数组维护长度为25
        if type == "SHFE":
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
                log.info("time {} | {}".format(d["time"], t))
                au_avg_price = float(d["avg_price"])
                flag = etf_data[i]["flag"] | d["flag"]
                break

        diff = round(etf_avg_price_100 - au_avg_price, 4)

        basis.append({"time": t, "etf_avg_price": etf_avg_price, "etf_avg_price*100": etf_avg_price_100,
                      "au_avg_price": au_avg_price, "diff": diff, "flag": flag})

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


def config_argument(argument_dict):
    for k, v in argument_dict.items():
        log.info("argument: [key: {}, value: {}]".format(k, v))
        if k == "acct":
            global acct
            acct = v
        elif k == "acct_type":
            global acct_type
            acct_type = v
        # elif k == "symbol":
        #     global universe
        #     universe = v
        elif k == "start_time":
            global start_time
            start_time = v
        elif k == "end_time":
            global end_time
            end_time = v


def on_init(argument_dict):
    """
    策略启动入口函数，如果不实现，则该策略立马退出
    :param argument_dict: 为一个dict，是策略启动时可能需要的参数，该参数可以通过add_argument进行增加
    :return: 无返回值，该函数执行失败时，需要抛出异常
    """

    config_argument(argument_dict)

    # 注册订阅标的的回调函数
    register_realmd_cb(on_realmd, None)

    # 订阅标的
    sub_realmd(universe)

    thread = threading.Thread(target=process_data)
    thread.setDaemon(True)
    thread.start()

    return


def on_fini():
    """
    策略退出时调用该函数， 如果不实现则默认函数只打印一条日志
    :return: 无返回值，如果失败需要抛出异常
    """
    log.info("on_fini called ......")
    # 等待子线程完成
    time.sleep(10)


def on_update(dict):
    """
    更新策略时调用该函数；
    如果不实现该函数，则系统提供一个默认的空函数
    start_time和end_time在系统内部默认会处理，用户函数也可以处理
    :param dict: 参数为dict，value见add_argument函数说明
    :return: 需要2个返回值，第一个返回值表示成功或失败，第二个表示原因
       True, "", 成功
       False, "because xxx" 失败
    """
    log.info("on_update called ......")
    return True, ""
