from auTraceBack import *
from influxdb import InfluxDBClient
import numpy as np

influxdb_url = ""
port = 80
database = "test"
username = "root"
password = "root"

client = InfluxDBClient(host=influxdb_url, port=port, username=username, password=password, database=database,
                        timeout=10)

boll_data = []
window_length = 3900  # 滚动窗口长度
std_mutiplier = 3  # 标准差倍数
w_data = []


def trace_back_boll(diff_data, raw_diff_data):
    """
    回测布林带
    :return: Null
    """

    length = len(diff_data)

    for i in range(window_length, length):
        # 取4800的时间窗口的数据
        diff_list = diff_data[i - window_length:i]

        mean = np.mean(diff_list)
        std = np.std(diff_list)
        up = mean + std_mutiplier * std
        dn = mean - std_mutiplier * std
        recent_time = raw_diff_data[i]["time"]

        w_data.append({
            "measurement": "verify_trace_back",
            "tags": {
                "symbol": "au"
            },
            "time": recent_time,
            "fields": {
                "mean": mean,
                "std": std,
                "up": up,
                "dn": dn,
                "diff": diff_data[i]
            }
        })


def write_data():
    try:
        client.write_points(w_data)
    except Exception as err:
        print(str(err))


if __name__ == '__main__':
    # TODO: 判断是否是交易日，作为定时任务进行回测
    start_date = "2019-12-26"
    end_date = "2019-12-27"
    query_history = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md where time > '" + start_date + "' and time < '" + end_date + "' ORDER BY time ASC"
    query_today = "SELECT ask_1, ask_2, bid_1, bid_2, ask_v_1, ask_v_2, bid_v_1, bid_v_2, symbol FROM au_md where time > '" + end_date + "'"

    print(query_history)
    print(query_today)
    try:
        history_md_data = list(client.query(query_history))[0]
        today_md_data = list(client.query(query_today))[0]
    except Exception as err:
        print(str(err))

    # 划分数据
    history_etf_data, history_au_data = divide_data(history_md_data)
    today_etf_data, today_au_data = divide_data(today_md_data)

    # 插值历史数据，计算基差
    history_etf_data, history_au_data = interpolate_value(history_etf_data, history_au_data)
    history_diff_data = calculate_diff(history_etf_data, history_au_data)

    # 插值今日数据，计算击差
    today_etf_data, today_au_data = interpolate_value(today_etf_data, today_au_data)
    today_diff_data = calculate_diff(today_etf_data, today_au_data)

    raw_diff_data = history_diff_data + today_diff_data
    # 取出基差数据
    diff_data = []
    for d in raw_diff_data:
        diff_data.append(d["diff"])

    print(diff_data)

    trace_back_boll(diff_data, raw_diff_data)
    # print(w_data)
    write_data()
