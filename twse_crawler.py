import datetime
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Dir"] = (
        df["Dir"]
        .str.split(">")
        .str[1]
        .str.split("<")
        .str[0]
    )
    df["Change"] = (
        df["Dir"] + df["Change"]
    )
    df["Change"] = (
        df["Change"]
        .str.replace(" ", "")
        .str.replace("X", "")
        .astype(float)
    )
    df = df.fillna("")
    df = df.drop(["Dir"], axis=1)
    for col in [
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "Open",
        "Max",
        "Min",
        "Close",
        "Change",
    ]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "")
            .str.replace("X", "")
            .str.replace("+", "")
            .str.replace("----", "0")
            .str.replace("---", "0")
            .str.replace("--", "0")
        )
    return df

def gen_date_list(start_date: str, end_date: str) -> typing.List[str]:

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    days = (end_date - start_date).days + 1
    date_list = [str(start_date + datetime.timedelta(days=day)) for day in range(days)]

    return date_list

def twse_header():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",  
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "www.twse.com.tw",
        "Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }

def colname_zh2en(df: pd.DataFrame, colname: typing.List) -> pd.DataFrame:
    taiwan_stock_price = {
        "證券代號": "StockID",
        "證券名稱": "",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "Open",
        "最高價": "Max",
        "最低價": "Min",
        "收盤價": "Close",
        "漲跌(+/-)": "Dir",
        "漲跌價差": "Change",
        "最後揭示買價": "",
        "最後揭示買量": "",
        "最後揭示賣價": "",
        "最後揭示賣量": "",
        "本益比": "",
    }
    df.columns = [taiwan_stock_price[name] for name in colname]
    df = df.drop([""], axis=1)
    return df


def crawler_twse(date: str) -> pd.DataFrame:
    """
    證交所網址
    https://www.twse.com.tw/zh/trading/historical/mi-index.html
    """
    url = ("https://www.twse.com.tw/rwd/zh/afterTrading/"
           "MI_INDEX?date={date}&type=ALL&response=json&_=1721446566467")
    url = url.format(date=date.replace("-", ""))

    time.sleep(5)

    res = requests.get(url, headers=twse_header())
    if res.json()["stat"] == '很抱歉，沒有符合條件的資料!':
        return pd.DataFrame()
    
    df = res.json()["tables"][8]
    colname=df["fields"]
    df = pd.DataFrame(df["data"])
    df = colname_zh2en(df.copy(), colname)
    df["date"] = date

    return df


class TaiwanStockPrice(BaseModel):
    StockID: str
    TradeVolume: int
    Transaction: int
    TradeValue: int
    Open: float
    Max: float
    Min: float
    Close: float
    Change: float
    date: str

def check_schema(df: pd.DataFrame) -> pd.DataFrame:
    df_dict = df.to_dict("records")
    df_schema = [TaiwanStockPrice(**dd).__dict__ for dd in df_dict]
    df = pd.DataFrame(df_schema)
    return df


def main(start_date: str, end_date: str):
    
    date_list = gen_date_list(start_date, end_date)
    print(date_list)
    for date in date_list:
        logger.info(date)
        df = crawler_twse(date)
        if len(df) > 0:
            # 清理資料
            df = clean_data(df.copy())
            # 檢查資料型態
            df = check_schema(df.copy())
            df.to_csv(f"taiwan_stock_price_twse_{date}.csv", index=False)


if __name__ == "__main__":
    start_date, end_date = sys.argv[1:]
    main(start_date, end_date)