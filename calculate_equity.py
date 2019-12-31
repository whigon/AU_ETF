import datetime

import numpy as np
import pandas as pd
import os

_DIR = "./record/"


class calculate_equity(object):
    def __init__(self, file, take_net=400000, make_net=400000):
        self.df = pd.read_excel(file)
        # 昨日净值
        self.take_net = take_net
        self.make_net = make_net

        self.times = self._count_()  # 交易次数
        self.au_c = 6  # 期货的手续费
        self.etf_c = 0.00015  # 现货的手续费率
        self.start_money = 400000

        if self.times % 2 == 1:
            self.etf_close_price = float(input("今日ETF收盘价：")) * 100 * 1000
            self.au_close_price = float(input("今日黄金期货收盘价：")) * 1000

        # 未平仓的收益，今日平仓后，需减去
        print("如昨日有未平仓的单子今日平仓，请输入。")
        self.open_take_profit = float(input("昨日未平仓交易收益(take)："))
        self.open_make_profit = float(input("昨日未平仓交易收益(make)："))

        if self.open_take_profit is None:
            self.open_take_profit = 0
        if self.open_make_profit is None:
            self.open_make_profit = 0

    def _count_(self):
        # 根据开仓方向的计数统交易次数
        direction = self.df.columns[3]
        return self.df[direction].count()

    def calculate(self):
        # 计算收益
        take_profit = self.calculate_take()
        make_profit = self.calculate_make()

        # 汇总收益
        take_total_profit = np.sum(take_profit)
        make_total_profit = np.sum(make_profit)

        self.df = self.df.append(pd.DataFrame({"etf价格": "双take", "au价格": "ETF make"}, index=[0]), ignore_index=True,
                                 sort=False)

        # 添加每次交易的盈亏
        for i in range(len(make_profit)):
            self.df = self.df.append(
                pd.DataFrame({"时间": "交易{}".format(i + 1), "etf价格": take_profit[i], "au价格": make_profit[i]}, index=[0]),
                ignore_index=True, sort=False)

        # 添加今日的总盈亏
        self.df = self.df.append(
            pd.DataFrame({"时间": "总计", "etf价格": take_total_profit, "au价格": make_total_profit}, index=[0]),
            ignore_index=True, sort=False)

        # 添加昨日净资产
        self.df = self.df.append(
            pd.DataFrame({"时间": "昨日净资产", "etf价格": self.take_net, "au价格": self.make_net}, index=[0]),
            ignore_index=True, sort=False)

        # 计算今日净资产:昨日净资产-今日收益-昨日计入收益中的未平仓收益
        self.take_net = self.take_net + take_total_profit - self.open_take_profit
        self.make_net = self.make_net + make_total_profit - self.open_make_profit

        # 添加今日净资产
        self.df = self.df.append(
            pd.DataFrame({"时间": "今日净资产", "etf价格": self.take_net, "au价格": self.make_net}, index=[0]),
            ignore_index=True, sort=False)

        # 添加今日净值
        self.df = self.df.append(
            pd.DataFrame(
                {"时间": "净值", "etf价格": self.take_net / self.start_money, "au价格": self.make_net / self.start_money},
                index=[0]), ignore_index=True, sort=False)
        print(self.df)

    def calculate_take(self):
        profit = []

        if self.times % 2 == 0:
            for i in range(0, self.times, 2):
                etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_price_(i)

                profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                                etf_sell_c))
        else:
            for i in range(0, self.times - 1, 2):
                etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_price_(i)

                profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                                etf_sell_c))

            # 有未平仓的单子
            etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_all_price_()

            profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                            etf_sell_c))

        return profit

    def calculate_make(self):
        profit = []

        if self.times % 2 == 0:
            for i in range(0, self.times, 2):
                etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_price_(i,
                                                                                                                     True)
                profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                                etf_sell_c))
        else:
            for i in range(0, self.times - 1, 2):
                etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_price_(i,
                                                                                                                     True)
                profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                                etf_sell_c))

            # 有未平仓的单子
            etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c = self._get_all_price_(
                True)
            profit.append(self._cal_profit_(etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c,
                                            etf_sell_c))

        return profit

    def _get_price_(self, i, is_make=False):
        """
        获取用于计算的相关价格数据
        :param i: 第几次交易
        :param is_make:
        :return:
        """
        # make计算方法用买一卖一数据
        if is_make:
            etf_buy_price_index = 9
            etf_sell_price_index = 5
        else:
            etf_buy_price_index = 1
            etf_sell_price_index = 1

        if self.df.iloc[i][3] == "买入现货，卖出期货":
            etf_buy_price = self.df.iloc[i][etf_buy_price_index] * 100 * 1000
            au_sell_price = self.df.iloc[i][2] * 1000
            etf_buy_c = etf_buy_price * self.etf_c

            etf_sell_price = self.df.iloc[i + 1][etf_sell_price_index] * 100 * 1000
            au_buy_price = self.df.iloc[i + 1][2] * 1000
            etf_sell_c = etf_sell_price * self.etf_c
        else:
            etf_sell_price = self.df.iloc[i][etf_sell_price_index] * 100 * 1000
            au_buy_price = self.df.iloc[i][2] * 1000
            etf_sell_c = etf_sell_price * self.etf_c

            etf_buy_price = self.df.iloc[i + 1][etf_buy_price_index] * 100 * 1000
            au_sell_price = self.df.iloc[i + 1][2] * 1000
            etf_buy_c = etf_buy_price * self.etf_c

        return etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c

    def _get_open_price_(self, is_make=False):
        """
        获取未平仓的价格
        :param is_make:
        :return:
        """
        # make计算方法用买一卖一数据
        if is_make:
            etf_buy_price_index = 9
            etf_sell_price_index = 5
        else:
            etf_buy_price_index = 1
            etf_sell_price_index = 1

        if self.df.iloc[self.times - 1][3] == "买入现货，卖出期货":
            etf_open_price = self.df.iloc[self.times - 1][etf_buy_price_index] * 100 * 1000
            au_open_price = self.df.iloc[self.times - 1][2] * 1000
            etf_open_c = etf_open_price * self.etf_c
        else:
            etf_open_price = self.df.iloc[self.times - 1][etf_sell_price_index] * 100 * 1000
            au_open_price = self.df.iloc[self.times - 1][2] * 1000
            etf_open_c = etf_open_price * self.etf_c

        return etf_open_price, au_open_price, etf_open_c

    def _get_all_price_(self, is_make=False):
        """
        获取未平仓的单子的计算价格
        :param is_make:
        :return:
        """
        etf_close_c = self.etf_close_price * self.etf_c

        # 获取开仓价格
        etf_open_price, au_open_price, etf_open_c = self._get_open_price_(is_make)

        if self.df.iloc[self.times - 1][3] == "买入现货，卖出期货":
            etf_buy_price = etf_open_price
            au_sell_price = au_open_price
            etf_buy_c = etf_open_c

            etf_sell_price = self.etf_close_price
            au_buy_price = self.au_close_price
            etf_sell_c = etf_close_c
        else:
            etf_sell_price = etf_open_price
            au_buy_price = au_open_price
            etf_sell_c = etf_open_c

            etf_buy_price = self.etf_close_price
            au_sell_price = self.au_close_price
            etf_buy_c = etf_close_c

        return etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c

    def _cal_profit_(self, etf_sell_price, etf_buy_price, au_sell_price, au_buy_price, etf_buy_c, etf_sell_c):
        return round(etf_sell_price - etf_buy_price + au_sell_price - au_buy_price - etf_buy_c - etf_sell_c - self.au_c,
                     3)

    def save_result(self, file_name):
        self.df.to_excel(file_name, index=False, encoding="utf-8")


if __name__ == '__main__':
    date = "2019-11-11"

    if date is None:
        date = datetime.date.today()

    file = _DIR + "Record-{}-3std-cats.xlsx".format(date)

    if os.path.exists(file):
        cal = calculate_equity(file, 400000, 400000)
        cal.calculate()
    else:
        print("No such file")
