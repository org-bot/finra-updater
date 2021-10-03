import asyncio
import os

import aiohttp
import ijson
import requests

from datetime import datetime
from datetime import timedelta
from io import StringIO
from aiofile import async_open
from os import path

import pandas as pd
import numpy as np

BASE_URL = "https://cdn.finra.org/equity/regsho/daily/"
SOURCES = (
    ("TRF_NYSE", "TRF NYSE", "FNYXshvol"),
    ("TRF_NASDAQ_Car", "TRF NASDAQ Carteret", "FNSQshvol"),
    ("TRF_NASDAQ_Chi", "TRF NASDAQ Chicago", "FNQCshvol"),
    ("ADF", "ADF", "FNRAshvol"),
    ("ORF", "ORF", "FORFshvol"),
    ("NMS", "NMS", "CNMSshvol")
)

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateAll())

async def updateAll():
    async with aiohttp.ClientSession() as session:
        tasks = [
            # updateSource(SOURCES[0]),
            # updateSource(SOURCES[1]),
            # updateSource(SOURCES[2]),
            # updateSource(SOURCES[3]),
            # updateSource(SOURCES[4]),
            updateSource(SOURCES[5]),
        ]
        await asyncio.gather(*tasks)

async def updateSource(source):
    symbols = set()
    date = datetime.today() + timedelta(days=-2)
    data = pd.DataFrame()
    while date < datetime.today():
        if date.weekday() < 5:
            url = BASE_URL + source[2] + date.strftime("%Y%m%d") + ".txt"
            r = requests.get(url)

            values = pd.read_csv(StringIO(r.text), sep='|', engine='python')
            index = 0
            while index < len(values) - 1:
                symbols.add(values.loc[index].Symbol)

                # ticker = values.loc[index].Symbol + source[0] + "TOTAL"
                # desc = values.loc[index].Symbol + source[0] + "Total Volume"
                # if ticker not in tickerSet:
                #     tickerSet.add(ticker)
                #     tickers[ticker] = desc
                #
                # if "ShortExemptVolume" in values.columns:
                #     ticker = values.loc[index].Symbol + source[0] + "SHORT_EXEMPT"
                #     desc = values.loc[index].Symbol + source[0] + "Short Exempt Volume"
                #     if ticker not in tickerSet:
                #         tickerSet.add(ticker)
                #         tickers[ticker] = desc
                index += 1

            data = data.append(values)
        date = date + timedelta(days=1)
    directory = "../repo/data/finra/"
    os.makedirs(directory, exist_ok=True)
    for  symbol in symbols:
        symbolData = data[data["Symbol"] == symbol]
        symbolData = symbolData.copy()
        symbolData["Date"] = pd.to_datetime(symbolData["Date"], format="%Y%m%d")
        symbolData.set_index("Date", inplace=True)
        symbol = symbol.replace("/", "_").upper()
        filename = symbol + "_" + source[0] + "_SHORT"
        symbolData = symbolData["ShortVolume"].to_frame()
        values = symbolData.copy()
        symbolData.insert(1, 'high', values.copy(), True)
        symbolData.insert(2, 'low', values.copy(), True)
        symbolData.insert(3, 'close', values.copy(), True)
        symbolData.insert(4, 'volume', np.zeros(values.size, dtype=int), True)

        if path.isfile(directory + filename):
            async with async_open(directory + filename, "r", encoding="UTF-8") as f:
                d = await f.read()
                storedData = pd.read_csv(StringIO(d), names=["Date", *symbolData.columns.values])
                storedData["Date"] = storedData["Date"].map(lambda x: datetime.strptime(x, "%Y%m%dT"))
                storedData.set_index("Date", inplace=True)
                symbolData = storedData.append(symbolData)

        async with async_open(directory + filename + ".csv", "w+") as f:
            s = StringIO()
            symbolData.to_csv(s, header=False, date_format="%Y%m%dT")
            await f.write(s.getvalue())

        # filename = symbol + source[0] + "TOTAL"
        # with open("../data/" + filename + ".csv", "w") as f:
        #     symbolData["TotalVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        #
        # if len(symbolData["ShortExemptVolume"]) > 0:
        #     filename = symbol + source[0] + "SHORT_EXEMPT"
        #     with open("../data/" + filename + ".csv", "w") as f:
        #         symbolData["ShortExemptVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        print(symbol + " -------- updated")

def tickers():
    path = os.path.join('..', 'repo', 'symbol_info', 'finra.json')
    with open(path, 'r', encoding="UTF-8") as f:
        objects = ijson.items(f, 'ticker.item')
        tickers = (o for o in objects)
        resultSet = set()
        resultList = []
        for t in tickers:
            parts = t.split("_")[0]
            t = "_".join(parts[0:-2])
            resultSet.add(t)
            resultList.append(t)

        return resultSet, resultList

def descriptions():
    path = os.path.join('..', 'repo', 'symbol_info', 'finra.json')
    with open(path, 'r', encoding="UTF-8") as f:
        objects = ijson.items(f, 'description.item')
        descriptions = (o for o in objects)
        result = []
        for t in descriptions:
            result.append(t)
        return result

if __name__ == "__main__":
    main()
