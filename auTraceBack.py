from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from Strategy import Strategy
from influxdb import InfluxDBClient
import datetime
import pandas as pd
import smtplib
import calculate_equity

_DIR = './record/'


def calculate_diff(etf_data, au_data):
    """
    计算基差数据

    :param etf_data:
    :param au_data:
    :return:
    """

    basis = []
    # 获取时间同步后的数据
    syc_etf_data, syc_au_data, length = sync_timestamp(etf_data, au_data)

    if length == 0:
        return

    for i in range(0, length):
        # 取出时间
        t = syc_etf_data[i]["time"]

        # etf均价
        etf_avg_price = round((float(syc_etf_data[i]["ask_1"]) + float(syc_etf_data[i]["bid_1"])) / 2, 4)
        etf_avg_price_100 = round(etf_avg_price * 100, 4)

        # 期货均价
        au_avg_price = round((float(syc_au_data[i]["ask_1"]) + float(syc_au_data[i]["bid_1"])) / 2, 4)

        # 基差
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


def divide_data(md_data):
    """
    根据品种划分数据

    :param md_data:
    :return:
    """

    etf_data = []
    au_data = []

    for d in md_data:
        if d["symbol"] == "518880.SH":
            etf_data.append(d)
        else:
            au_data.append(d)

    return etf_data, au_data


def interpolate_value(origin_etf_data, origin_au_data):
    """
    插值缺失的行情数据，并移除非交易时间的数据点

    :param origin_etf_data:
    :param origin_au_data:
    :return:
    """

    au_symbol = "au2002.SHFE"

    new_etf_data = []
    new_au_data = []

    for i in range(0, len(origin_etf_data) - 1):
        # 比较当前行情数据点的时间和后一个行情数据点的时间差，判断是否需要插值
        new_etf_data.append(origin_etf_data[i])

        time_f = datetime.datetime.strptime(origin_etf_data[i]["time"], "%Y-%m-%dT%H:%M:%SZ")
        time_l = datetime.datetime.strptime(origin_etf_data[i + 1]["time"], "%Y-%m-%dT%H:%M:%SZ")
        interval = (time_l - time_f).seconds

        # 补充缺失值，行情间隔为3s,行情间隔超过五分钟的不进行插值
        if 3 < interval < 300:
            diff = int(interval / 3)
            # print(diff)
            # ask_1 = origin_etf_data[i]["ask_1"]
            # bid_1 = origin_etf_data[i]["bid_1"]
            # print(origin_etf_data[i])

            for j in range(0, diff - 1):
                t = datetime.datetime.strftime(time_f + datetime.timedelta(seconds=3 * (j + 1)), "%Y-%m-%dT%H:%M:%SZ")

                new_etf_data.append(
                    {"time": t, "bid_1": origin_etf_data[i]["bid_1"], "bid_v_1": origin_etf_data[i]["bid_v_1"],
                     "bid_2": origin_etf_data[i]["bid_2"], "bid_v_2": origin_etf_data[i]["bid_v_2"],
                     "ask_1": origin_etf_data[i]["ask_1"], "ask_v_1": origin_etf_data[i]["ask_v_1"],
                     "ask_2": origin_etf_data[i]["ask_2"], "ask_v_2": origin_etf_data[i]["ask_v_2"],
                     "symbol": "518880.SH"})
    # 插入最后一个点
    new_etf_data.append(origin_etf_data[-1])
    # 移除非交易时间的行情点
    new_etf_data = remove_points(new_etf_data)

    for i in range(0, len(origin_au_data) - 1):
        new_au_data.append(origin_au_data[i])

        time_f = datetime.datetime.strptime(origin_au_data[i]["time"], "%Y-%m-%dT%H:%M:%SZ")
        time_l = datetime.datetime.strptime(origin_au_data[i + 1]["time"], "%Y-%m-%dT%H:%M:%SZ")
        interval = (time_l - time_f).seconds

        # 补充缺失值,行情间隔为1s，行情间隔超过五分钟的不进行插值
        if 1 < interval < 300:
            # ask_1 = origin_au_data[i]["ask_1"]
            # bid_1 = origin_au_data[i]["bid_1"]

            for j in range(0, interval - 1):
                t = datetime.datetime.strftime(time_f + datetime.timedelta(seconds=(j + 1)), "%Y-%m-%dT%H:%M:%SZ")

                new_au_data.append(
                    {"time": t, "bid_1": origin_au_data[i]["bid_1"], "bid_v_1": origin_au_data[i]["bid_v_1"],
                     "bid_2": origin_au_data[i]["bid_2"], "bid_v_2": origin_au_data[i]["bid_v_2"],
                     "ask_1": origin_au_data[i]["ask_1"], "ask_v_1": origin_au_data[i]["ask_v_1"],
                     "ask_2": origin_au_data[i]["ask_2"], "ask_v_2": origin_au_data[i]["ask_v_2"],
                     "symbol": au_symbol})
    # 插入最后一个点
    new_au_data.append(origin_au_data[-1])
    # 移除非交易时间的行情点
    new_au_data = remove_points(new_au_data)
    # print(len(new_au_data))

    return new_etf_data, new_au_data


def remove_points(origin_data):
    """
    移除不在交易时间里的点

    :param origin_data:
    :return:
    """

    new_data = []

    for d in origin_data:
        t = datetime.datetime.strptime(d["time"], "%Y-%m-%dT%H:%M:%SZ").time().strftime("%H%M%S")
        t = int(t)

        # 去掉非交易时间的数据，对应的是UTC的时间
        if t < 13000 or (21500 < t < 23000) or (33000 < t < 53000) or t > 70000:
            # origin_data.remove(d)
            pass
        else:
            # t += 80000
            # print(t)
            new_data.append(d)

    return new_data


def get_rt_data(time):
    """
    根据时间点获取对应的实时行情数据
    Note: 选择的数据库是au_md_trace_back,可能是插值的数据

    :param time:
    :return:
    """
    # 转化为对应时间的UTC时间
    time = time - datetime.timedelta(hours=8)
    time = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    query_sql = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md_trace_back WHERE time ='" + time + "'"
    # print(query_sql)
    data = list(client.query(query_sql))[0]
    # print(data)

    for d in data:
        if d["symbol"] == "518880.SH":
            etf_data = d
        else:
            au_data = d

    return etf_data, au_data


def write_trace_back_md_data(etf_md_data, au_md_data):
    """
    把插值后的行情数据写进另一个数据库里
    Note：没有标记插值

    :param etf_md_data:
    :param au_md_data:
    :return:
    """
    w_data = []
    raw_md_data = etf_md_data + au_md_data

    for d in raw_md_data:
        w_data.append({
            "measurement": "au_md_trace_back",
            "tags": {
                "symbol": d["symbol"]
            },
            "time": d["time"],
            "fields": {
                "ask_1": float(d["ask_1"]),
                "ask_2": float(d["ask_2"]),
                "bid_1": float(d["bid_1"]),
                "bid_2": float(d["bid_2"]),
                "ask_v_1": d["ask_v_1"],
                "ask_v_2": d["ask_v_2"],
                "bid_v_1": d["bid_v_1"],
                "bid_v_2": d["bid_v_2"]
            }
        })

    # print(raw_md_data)

    try:
        client.write_points(w_data)
    except Exception as err:
        print(str(err))


if __name__ == '__main__':
    influxdb_url = ""
    port = 80
    database = "test"
    username = "root"
    password = "root"

    client = InfluxDBClient(host=influxdb_url, port=port, username=username, password=password, database=database,
                            timeout=10)

    # TODO: 判断是否是交易日，作为定时任务进行回测
    # 历史数据点不足3900的时候，要用前两天的，不然盘头的信号触发不了交易
    start_date = "2019-12-26"
    end_date = "2019-12-30"
    # 取出昨日数据和今日数据
    query_history = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md where time >= '" + start_date + "' and time < '" + end_date + "' ORDER BY time ASC"
    query_today = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md where time >= '" + end_date + "'"
    # query_today = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md where time >= '" + end_date + "' and time < '2019-12-21'"

    # print(query_history)
    # print(query_today)

    try:
        history_md_data = list(client.query(query_history))[0]
        today_md_data = list(client.query(query_today))[0]
    except Exception as err:
        print(str(str))

    # 根据品种划分数据
    history_etf_data, history_au_data = divide_data(history_md_data)
    today_etf_data, today_au_data = divide_data(today_md_data)

    # 插值历史数据，计算基差
    history_etf_data, history_au_data = interpolate_value(history_etf_data, history_au_data)
    # 记录补充缺失值以后的历史行情数据
    write_trace_back_md_data(history_etf_data, history_au_data)
    # 获取同步行情数据和同步数据的长度
    history_diff_data = calculate_diff(history_etf_data, history_au_data)
    print("etf data len: {} | diff data len {}".format(len(history_etf_data), len(history_diff_data)))

    # 插值今日数据，同步数据
    today_etf_data, today_au_data = interpolate_value(today_etf_data, today_au_data)
    # 记录补充缺失值以后的今日行情数据
    write_trace_back_md_data(today_etf_data, today_au_data)
    # 获取同步行情数据和同步数据的长度
    today_etf_data, today_au_data, length = sync_timestamp(today_etf_data, today_au_data)
    print("today: etf data len: {} | au data len {}".format(len(today_etf_data), len(today_au_data)))

    strategy = Strategy()
    # 喂历史基差数据
    strategy.get_history_prem(history_diff_data)
    print(strategy.price_log["prem"])

    # 添加昨日未平仓交易记录
    history_open_t = input("历史开仓时间：\n")
    if history_open_t:
        try:
            # 开仓时间
            open_t = datetime.datetime.strptime(history_open_t, "%Y-%m-%dT%H:%M:%SZ")
            ETF_p = input("开仓ETF价格：")
            F_p = input("开仓期货价格：")
            side = input("开仓方向(LONG/SHORT)：")
            strategy.get_history_open(open_t, ETF_p, F_p, side)
        except Exception as err:
            print("错误的时间格式")
            print(str(err))

    for i in range(0, length):
        t = today_etf_data[i]["time"]
        # UTC时间转为北京时间
        time = datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(hours=8)
        # 喂今日实时行情数据，进行回测
        strategy.on_market_update(time, today_etf_data[i]["bid_1"], today_etf_data[i]["ask_1"],
                                  today_au_data[i]["bid_1"],
                                  today_au_data[i]["ask_1"])

    columns = ["时间", "etf价格", "au价格", "方向", "仓位", "ETF卖一", "ETF卖一量", "ETF卖二", "ETF卖二量", "ETF买一", "ETF买一量", "ETF买二",
               "ETF买二量", "期货卖一", "期货卖一量", "期货卖二", "期货卖二量", "期货买一", "期货买一量", "期货买二", "期货买二量"]
    record = pd.DataFrame(columns=columns)

    # 保存交易日志
    trade_map = {"LONG": "买入现货，卖出期货", "SHORT": "卖出现货，买入期货"}
    log = strategy.log
    for i in range(len(strategy.log)):
        # 获取开平仓时间
        open_t = log[i][0]
        close_t = log[i][4]

        # 保存开平仓交易日志，开仓和平仓记录分开保存
        # 获取对应时间的实时行情数据
        etf_data, au_data = get_rt_data(open_t)
        record.loc[2 * i] = [open_t, log[i][1], log[i][2], trade_map[log[i][3]], 1, etf_data["ask_1"],
                             etf_data["ask_v_1"], etf_data["ask_2"], etf_data["ask_v_2"], etf_data["bid_1"],
                             etf_data["bid_v_1"], etf_data["bid_2"], etf_data["bid_v_2"], au_data["ask_1"],
                             au_data["ask_v_1"], au_data["ask_2"], au_data["ask_v_2"], au_data["bid_1"],
                             au_data["bid_v_1"], au_data["bid_2"], au_data["bid_v_2"]]
        etf_data, au_data = get_rt_data(close_t)
        record.loc[2 * i + 1] = [close_t, log[i][5], log[i][6], trade_map[log[i][7]], 0, etf_data["ask_1"],
                                 etf_data["ask_v_1"], etf_data["ask_2"], etf_data["ask_v_2"], etf_data["bid_1"],
                                 etf_data["bid_v_1"], etf_data["bid_2"], etf_data["bid_v_2"], au_data["ask_1"],
                                 au_data["ask_v_1"], au_data["ask_2"], au_data["ask_v_2"], au_data["bid_1"],
                                 au_data["bid_v_1"], au_data["bid_2"], au_data["bid_v_2"]]

    # 如果有未平的单子，添加到日志
    open_log = strategy.open_log
    if len(open_log) > len(log):
        open_t = open_log[-1][0]
        etf_data, au_data = get_rt_data(open_t)
        record.loc[2 * (len(open_log) - 1)] = [open_t, open_log[-1][1], open_log[-1][2], trade_map[open_log[-1][3]], 1,
                                               etf_data["ask_1"],
                                               etf_data["ask_v_1"], etf_data["ask_2"], etf_data["ask_v_2"],
                                               etf_data["bid_1"],
                                               etf_data["bid_v_1"], etf_data["bid_2"], etf_data["bid_v_2"],
                                               au_data["ask_1"],
                                               au_data["ask_v_1"], au_data["ask_2"], au_data["ask_v_2"],
                                               au_data["bid_1"],
                                               au_data["bid_v_1"], au_data["bid_2"], au_data["bid_v_2"]]

    # 交易日志
    print("交易日志：", log)
    # 开仓日志
    print("开仓日志：", open_log)
    print(record)

    # 保存的文件名
    file_name = "Record-%s-3std.xlsx" % end_date
    # 保存的文件路径
    file_path = _DIR + file_name
    # 有开仓的情况下
    if len(open_log) > 0:
        # 保存为Excel文件
        record.to_excel(file_path, index=False, encoding="utf-8")
        # 计算当日权益
        take_net = float(input("昨日take净资产："))
        make_net = float(input("昨日make净资产："))
        cal = calculate_equity.calculate_equity(file_path, take_net, make_net)
        cal.calculate()
        cal.save_result(file_path)

        # 发送附件
        message = MIMEMultipart('related')
        input("---------------------")

        # 读取附件
        # file_name = "Record-" + end_date + "-3std.xlsx"
        message_xlsx = MIMEText(open(file_path, "rb").read(), "base64", "utf-8")

        # 设置附件的名字
        message_xlsx["Content-Disposition"] = "attachment;filename= %s" % file_name
        message.attach(message_xlsx)

        # 设置邮件信息
        message["From"] = "Fedge"
        message["To"] = "Fedge"
        message["Subject"] = end_date + " Record"

        # 连接邮箱服务器
        smtp = smtplib.SMTP_SSL()
        smtp.connect(host="smtp.exmail.qq.com", port="465")
        print("connect")

        # 登录
        username = input("Please input the username\n")
        password = input("Please input the password\n")
        smtp.login(user=username, password=password)
        print("login")

        # 发送邮件
        smtp.sendmail(username, "", message.as_string())
        smtp.sendmail(username, "", message.as_string())
        smtp.sendmail(username, "", message.as_string())

        print("send")

        # 关闭服务
        smtp.quit()
