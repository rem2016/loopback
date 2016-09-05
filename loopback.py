"""
@file: loopback.py
@time: 16-9-4 下午6:22
@author: rem

Any issues or improvements please contact: remch183@outlook.com
"""
# encoding: utf-8
import pandas as pd
import numpy as np
from pandas import DataFrame, Series
import copy
import datetime
import pickle
from abc import abstractmethod

__all__ = ('Handler', 'User', 'Loopbacker', 'Strategy')
ef = pd.ExcelFile('data/final_data_HHD.xlsx')
hs300 = ef.parse()
ef.close()
with open('../myAstock/data/two_year_panel.pickle', 'rb') as f:
    # two years stock data
    # type: Panel
    two_year = pickle.load(f)

two_year.set_axis(1, two_year.iloc[0].index.to_datetime())


class Handler:
    """
    methods:
        order(security, amount)
            security: 股票码
            amount: 交易数量(以股为单位), 正买负卖
            is_price: 当其为True时交易以价格(元)为单位
    """

    @abstractmethod
    def order(self, security, amount, is_price=False): pass


class Strategy:
    def __init__(self, history_length=30, frequent='day', chosen=None):
        self.history_length = history_length
        self.frequent = frequent
        self.chosen = chosen
        if chosen is None:
            self.chosen = ['000300']

    @abstractmethod
    def handle_data(self, data, context, handler): pass


oneday = datetime.timedelta(days=1)


class User:
    def __init__(self, total_money):
        self.__slots__ = ('balance', 'assets', 'stocks', 'trades', 'state')
        self.balance = total_money
        self.assets = total_money
        self.stocks = DataFrame(columns=['总价', '盈亏', '成本', '现价', '持仓'])
        self.trades = DataFrame(columns=['security', 'amount'])
        # index 为投资日期
        self.state = DataFrame(columns=['总额', '盈亏', '仓库', '现金'])


class Loopbacker:
    stocks = copy.copy(two_year)
    stocks['000300'] = hs300
    calenda = list(hs300.index)
    TOTAL_MONEY = 5e6

    class BalanceNotEnoughError(ValueError):
        pass

    def __init__(self, strategy):
        assert issubclass(type(strategy), Strategy)
        self.stocks = copy.copy(self.stocks)
        self.user = User(self.TOTAL_MONEY)
        self._strategy = strategy
        # 该日期不可用,需要调用move_day()之后才可以使用
        self._today = datetime.datetime(2014, 9, 1) + (strategy.history_length - 1) * oneday
        self._history_len = strategy.history_length
        self.handler = self._get_handler()
        self._trans_stocks = self.stocks.transpose(1, 0, 2)
        self._t = self._trans_stocks
        # 删除没有被选中的股票
        droped = self.stocks.keys().isin(strategy.chosen)
        droped = self.stocks.keys()[droped]
        self.stocks = self.stocks.drop(droped)

    def start(self):
        udf = self.user.state
        user = self.user
        _s = self._trans_stocks
        _s = _s[_s.keys() <= self._today]
        while self.move_day():
            self._refresh_user_state()
            udf.ix[self._today] = [user.assets, 100 * (user.assets / self.TOTAL_MONEY - 1), user.assets - user.balance,
                                   user.balance]
            data = self._get_today_data()
            if data is None: break
            self._strategy.handle_data(data, user, self.handler)

    def move_day(self):
        if self._today in self.calenda:
            try:
                self._today = self.calenda[self.calenda.index(self._today) + 1]
                return True
            except IndexError:
                return False

        def bad():
            nonlocal self
            return self._today < self.calenda[-1] and (self._today not in self.calenda)

        while bad():
            self._today += oneday
        return bad()

    def _order(self, security, amount, is_price=False):
        """
        对成本的更新需要注意,避免犯错
        """
        value = amount
        price = self._t.iloc[self._today].ix[security]
        if not is_price:
            value *= price
        else:
            amount = value // price
            value = amount * price
        if self.user.balance < value:
            raise self.BalanceNotEnoughError("In buying security %s, need %f yuan. However, balance is %f" %
                                             (security, value, self.user.balance))
        self.user.balance -= value
        st = self.user.stocks
        if security in st.index:
            se = st.ix[security]
            if value < 0 and amount + se.持仓 < 0:
                raise self.BalanceNotEnoughError\
                    ("In selling  security %s, the remain price is %f. You can't sell at %f yuan" %
                     (security, se.总价, value))
            se.总价 += value
            se.现价 = price
            if value > 0:
                se.成本 = (se.成本 * se.持仓 + value) / (se.持仓 + amount)
                se.盈亏 = (se.现价 - se.成本) / se.成本 * 100
            se.持仓 += amount
        else:
            if value < 0:
                raise self.BalanceNotEnoughError("You didn't buy security %s" % (security, ))
            st.ix[security] = [value, 0, price, price, amount]
        td = self.user.trades
        td.ix[len(td)] = (security, amount)
        return True


    def _get_today_data(self):
        _s = self._trans_stocks
        try:
            _s[self._today] = self._t[self._today]
        except KeyError:
            return None
        return _s

    def _get_handler(self):
        outer_order = self._order

        class MyHandler(Handler):
            def __init__(self):
                pass

            def order(self, security, amount, is_price=False):
                nonlocal outer_order
                return outer_order(security, amount, is_price)

        return MyHandler()

    def _refresh_user_state(self):
        """
        通过 user.stocks (index,成本价,持仓 , 现价(updated))刷新
         user.stocks, user.assets
        """
        st = self.user.stocks
        for key in st.index:
            price = self._t.iloc[self._today].ix[key]
            se = st.ix[key]
            se.现价 = price
            newZ = se.持仓 * se.现价
            self.user.assets += - se.总价 + newZ
            se.总价 = newZ
            se.盈亏 = (se.现价 - se.成本) / se.成本 * 100

    def _get_data(self, date):
        """ """
        pass
