import asyncio
import os
import urllib.parse

import aiohttp
import ijson
import re
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
    tickers = set()
    daysENV = os.getenv("DAYS")
    if daysENV is None or daysENV == '': days = 3
    else: days = int(daysENV)
    if days < 3: days = 3
    date = datetime.today() + timedelta(days=-days)
    data = pd.DataFrame()
    while date < datetime.today():
        if date.weekday() < 5:
            url = BASE_URL + source[2] + date.strftime("%Y%m%d") + ".txt"
            r = requests.get(url)

            values = pd.read_csv(StringIO(r.text), sep='|', engine='python')
            index = 0
            while index < len(values) - 1:
                tickers.add(values.loc[index].Symbol)
                index += 1

            data = data.append(values)
        date = date + timedelta(days=1)
    directory = "./repo/data/finra/"
    os.makedirs(directory, exist_ok=True)
    for ticker in tickers:
        symbolData = data[data["Symbol"] == ticker]
        symbolData = symbolData.copy()
        symbolData["Date"] = pd.to_datetime(symbolData["Date"], format="%Y%m%d")
        symbolData.set_index("Date", inplace=True)

        symbol = ticker.replace('/WS', '/W')
        if re.search(r'/[A-VX-Z]', symbol) is not None:
            symbol = symbol.replace("/", ".")
        symbol = symbol.replace("p", "/P")
        symbol = symbol.replace("r", "/R")
        symbol = symbol.replace("w", "/W/I")

        filename = symbol + "_SHORT_VOLUME.csv"
        filename = urllib.parse.quote(filename, safe='')
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
                symbolData = pd.concat([storedData, symbolData])
                # Taken from here: https://stackoverflow.com/a/34297689
                # As more efficient way to deal with duplicates
                symbolData = symbolData[~symbolData.index.duplicated(keep='first')]
                symbolData = symbolData.sort_values(by='index')

        async with async_open(directory + filename, "w") as f:
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

if __name__ == "__main__":
    main()
