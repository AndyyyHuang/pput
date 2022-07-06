#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/1/6 13:49
# @Author  : dy
# @contact : aidabloc@163.com
# @File    : mongodb.py
# @Desc    :
from datetime import datetime

from dateutil.parser import parse
from numpy import unique
from pandas import DataFrame, concat
from pymongo import MongoClient
# from tqdm import tqdm


# 对日内数据添加交易日
def _map_get_trading_date(x: datetime):
    return x.strftime("%Y-%m-%d")


class DataAPI(object):
    def __init__(self, ip_config: str = None):
        option_symbol_map_dict = dict()
        option_symbol_map_dict["000016"] = ["510050", False]
        option_symbol_map_dict["510050"] = ["510050", False]
        option_symbol_map_dict["510300"] = ["510300", False]
        option_symbol_map_dict["000300"] = ["io", True]
        option_symbol_map_dict["159919"] = ["159919", False]
        self.option_symbol_map_dict = option_symbol_map_dict

        if ip_config is None:
            self.ip_config = "mongodb://localhost:27017/"
            # self.ip_config = "mongodb://192.168.1.170:27017/"
        else:
            self.ip_config = ip_config

    def get_day_stock(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                      end_date: str = None) -> DataFrame or None:
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :return:
        """
        if symbol[:3] == "159":
            label = "etf"
        elif symbol[:3] == "510":
            label = "etf"
        elif symbol[:3] == "000":
            label = "index"
        else:
            label = None

        if label is None:
            return None
        else:
            return self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                                label=label, db_name="day_stock")

    def get_day_stock_future(self, symbol: str = "IF", count: int = None, start_date: str = "20150101",
                             end_date: str = None) -> DataFrame or None:
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :return:
        """
        future_map_dict = {
            "IH": "stock_future",
            "ih": "stock_future",
            "IF": "stock_future",
            "if": "stock_future",
            "IC": "stock_future",
            "ic": "stock_future"
        }

        if symbol in future_map_dict:
            label = future_map_dict[symbol]
            symbol = symbol.lower()
        else:
            label = None

        if label is None:
            return
        else:
            return self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                                label=label, db_name="day_stock")

    def get_day_equity_option_atm_iv(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                                     end_date: str = None,
                                     is_index_option: bool = False) -> DataFrame:
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param is_index_option:
        :return:
        """

        if is_index_option:
            label_list = ["M1", "M2", "M3", "Q1", "Q2", "Q3"]
        else:
            label_list = ["M1", "M2", "Q1", "Q2"]

        data = self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="atm_iv", db_name="day_option")
        return self._reformat_data(data, label_list, False)

    def get_day_equity_option_delta_iv(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                                       end_date: str = None,
                                       is_index_option: bool = False) -> DataFrame:
        """
        Delta-IV  and Time Weighted IV

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param is_index_option:
        :return:
        """
        if is_index_option:
            label_list = ["M1", "M2", "M3", "Q1", "Q2", "Q3"] + ["Day120", "Day160", "Day20", "Day40", "Day60", "Day90"]
        else:
            label_list = ["M1", "M2", "Q1", "Q2"] + ["Day20", "Day40", "Day60", "Day90"]

        data = self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="delta_iv", db_name="day_option")
        return self._reformat_data(data, label_list, False)

    def get_day_equity_option_md_iv(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                                    end_date: str = None) -> DataFrame:
        """
        api for market quotes and  implied volatility under the assumption of no arbitrage equilibrium

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param is_index_option:
        :return:
        """
        data = self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="md_iv", db_name="day_option")
        return data

    def get_day_equity_option_market_data(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                                          end_date: str = None) -> DataFrame:
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :return:
        """
        return self._get_data_from_day_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="day_cross", db_name="day_cross_option")

    def _get_data_from_day_data(self, symbol: str = "510050", count: int = None, start_date: str = "20150101",
                                end_date: str = None, label: str = "atm_iv",
                                db_name: str = "day_option"):
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param label:
        :param db_name:
        :return:
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        with MongoClient(self.ip_config, connect=False) as client:
            db = client[db_name]
            collection_name = "{}_{}".format(label, symbol)
            collection = db[collection_name]

            if count is not None:
                date_list = DataFrame.from_records(collection.find({}, ["TradingDay"]))
                if date_list.empty:
                    return
                date_list = unique(date_list.drop(columns="_id", axis=1))
                date_list = date_list[date_list <= end_date]
                start_date = date_list[-count]
            data = DataFrame.from_records(
                collection.find({'TradingDay': {'$gte': start_date, '$lte': end_date}}))

            if data.empty:
                return
            return data.drop(columns="_id", axis=1)

    def get_min_equity_option_atm_iv(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                                     is_index_option=False,
                                     use_filter: bool = False) -> DataFrame:
        """
        获取ATM-IV 表格数据

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param is_index_option:
        :param use_filter:
        :return:
        """
        if is_index_option:
            label_list = ["M1", "M2", "M3", "Q1", "Q2", "Q3"]
        else:
            label_list = ["M1", "M2", "Q1", "Q2"]

        data = self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="atm_iv_", db_name="min_option")
        data = self._reformat_data(data, label_list, True)

        if use_filter:
            return self._filter_data(data)
        else:
            return data

    def get_min_equity_option_delta_iv(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                                       is_index_option=False, use_filter: bool = False) -> DataFrame:
        """
        Delta-IV  and Time Weighted IV

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param is_index_option:
        :param use_filter:
        :return:
        """
        if is_index_option:
            label_list = ["M1", "M2", "M3", "Q1", "Q2", "Q3"] + ["Day120", "Day160", "Day20", "Day40", "Day60", "Day90"]
        else:
            label_list = ["M1", "M2", "Q1", "Q2"] + ["Day20", "Day40", "Day60", "Day90"]

        data = self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="delta_iv_", db_name="min_option")
        data = self._reformat_data(data, label_list, True)
        if use_filter:
            return self._filter_data(data)
        else:
            return data

    def get_min_equity_option_md_iv(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                                    use_filter: bool = False) -> DataFrame:
        """
        api for market quotes and  implied volatility under the assumption of no arbitrage equilibrium
        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param use_filter:
        :return:
        """
        data = self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                            label="md_iv_", db_name="min_option")
        data["TradingDay"] = data["CrossTimestamp"].map(_map_get_trading_date)

        if use_filter:
            return self._filter_data(data)
        else:
            return data

    def get_min_equity_option_market_data(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                                          use_filter: bool = False) -> DataFrame:
        """

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param use_filter:
        :return:
        """
        if use_filter:
            return self._filter_data(
                self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                             label="min_cross_", db_name="min_cross_option"))
        else:
            return self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                                label="min_cross_", db_name="min_cross_option")

    def get_min_stock(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                      use_filter: bool = False):
        if use_filter:
            return self._filter_data(
                self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                             label="min_", db_name="min_stock"))
        else:
            return self._get_data_from_min_data(symbol=symbol, count=count, start_date=start_date, end_date=end_date,
                                                label="min_", db_name="min_stock")

    def _get_data_from_min_data(self, symbol="510050", count=None, start_date="20150101", end_date=None,
                                label="delta_iv_", db_name="min_cross_option"):
        """
        从min-option 提取数据

        :param symbol:
        :param count:
        :param start_date:
        :param end_date:
        :param label:
        :return:
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        with MongoClient(self.ip_config, connect=False) as client:
            db = client[db_name]

            table_list = db.list_collection_names()
            table_list = [i for i in table_list if label in i]
            table_list = [i for i in table_list if symbol in i]
            table_list = [i for i in table_list if i.split("_")[3] <= end_date]
            table_list.sort()

            if count is not None:
                table_list = table_list[-count:]
            else:
                table_list = [i for i in table_list if start_date <= i.split("_")[3] <= end_date]

            data_list = []
            for table_name in table_list:
                data = DataFrame.from_records(db[table_name].find({}))
                data_list.append(data)
            data = concat(data_list, axis=0, sort=True)
        return data.drop(columns="_id", axis=1).reset_index(drop=True)

    @staticmethod
    def _reformat_data(data, label_list, use_cross=True):
        """
        对数据获取数据重新格式化

        :param data:
        :return:
        """
        if use_cross:
            data["TradingDay"] = data["CrossTimestamp"].map(_map_get_trading_date)
            use_time_point = "CrossTimestamp"
        else:
            use_time_point = "TradingDay"

        df_list = []
        # for trading_date, row in tqdm(data.groupby("TradingDay")):
        for trading_date, row in data.groupby("TradingDay"):
            # 标签选择 ETF期权最后三个交易日没有spot 插值数据
            if len(list(set(row["ExpirationDate"]))) == 7:
                use_list = label_list[1:]
            # 标签选择 指数期权最后三个交易日没有spot 插值数据
            elif len(list(set(row["ExpirationDate"]))) == 11:
                use_list = label_list[1:]
            else:
                use_list = label_list
            row_list = []

            for (_, ser), label in zip(row.groupby("ExpirationDate"), use_list):
                ser = ser.set_index(use_time_point)
                ser.columns = [[label] * ser.shape[1], ser.columns]
                row_list.append(ser)
            row = concat(row_list, axis=1, sort=True)
            df_list.append(row)
        return concat(df_list, axis=0, sort=True)

    @staticmethod
    def _filter_data(data: DataFrame) -> DataFrame:
        for date in unique(data["TradingDay"]):
            filter_time: str = "{} 09:30:00".format(parse(date).strftime("%Y-%m-%d"))
            data = data.loc[data["CrossTimestamp"] != filter_time]
            filter_time: str = "{} 09:31:00".format(parse(date).strftime("%Y-%m-%d"))
            data = data.loc[data["CrossTimestamp"] != filter_time]
            filter_time = "{} 11:30:00".format(parse(date).strftime("%Y-%m-%d"))
            data = data.loc[data["CrossTimestamp"] != filter_time]
            filter_time = "{} 13:00:00".format(parse(date).strftime("%Y-%m-%d"))
            data = data.loc[data["CrossTimestamp"] != filter_time]
        return data


if __name__ == "__main__":
    client = DataAPI()

    print(client.get_min_equity_option_atm_iv(symbol="510050").head())
    print(client.get_min_equity_option_atm_iv(symbol="510300").head())
    print(client.get_min_equity_option_atm_iv(symbol="_io_").head())
    print(client.get_min_equity_option_atm_iv(symbol="159919").head())

    print(client.get_min_equity_option_delta_iv(symbol="510050").head())
    print(client.get_min_equity_option_delta_iv(symbol="510300").head())
    print(client.get_min_equity_option_delta_iv(symbol="_io_").head())
    print(client.get_min_equity_option_delta_iv(symbol="159919").head())
