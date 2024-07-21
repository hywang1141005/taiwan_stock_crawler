import datetime
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
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
            .str.replace(" ", "")
            .str.replace("除權息", "0")
            .str.replace("除息", "0")
            .str.replace("除權", "0")
        )
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
    df_schema = [
        TaiwanStockPrice(**dd).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df


def gen_date_list(start_date: str, end_date: str) -> typing.List[str]:
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    days = (end_date - start_date).days + 1
    date_list = [str(start_date + datetime.timedelta(days=day)) for day in range(days)]
    return date_list

def convert_date(date: str) -> str:
    year, month, day = date.split("-")
    year = int(year) - 1911
    return f"{year}/{month}/{day}"

def tpex_header():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def set_column(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        "StockID",
        "Close",
        "Change",
        "Open",
        "Max",
        "Min",
        "TradeVolume",
        "TradeValue",
        "Transaction",
    ]
    return df

def crawler_tpex(date: str) -> pd.DataFrame:
    """
    櫃買中心網址
    https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw
    """

    url = ("https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/"
           "stk_wn1430_result.php?l=zh-tw&d={date}&se=AL")
    url = url.format(date=convert_date(date))
    
    time.sleep(5)
    res = requests.get(url, headers=tpex_header())
    data = res.json()["aaData"]
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if len(df) == 0:
        return pd.DataFrame()
    df = df[[0, 2, 3, 4, 5, 6, 7, 8, 9]]
    df = set_column(df.copy())
    df["date"] = date
    return df

def main(start_date: str, end_date: str):
    date_list = gen_date_list(start_date, end_date)
    for date in date_list:
        logger.info(date)
        df = crawler_tpex(date)
        if len(df) > 0:
            df = clean_data(df.copy())
            df = check_schema(df.copy())
            df.to_csv(
                f"taiwan_stock_price_tpex_{date}.csv",
                index=False,
            )


if __name__ == "__main__":
    start_date, end_date = sys.argv[1:]
    main(start_date, end_date)