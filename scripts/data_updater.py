import asyncio
import math
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
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(update_all())


async def update_all():
    async with aiohttp.ClientSession() as session:
        tasks = [
            # updateSource(SOURCES[0]),
            # updateSource(SOURCES[1]),
            # updateSource(SOURCES[2]),
            # updateSource(SOURCES[3]),
            # updateSource(SOURCES[4]),
            update_source(SOURCES[5]),
        ]
        await asyncio.gather(*tasks)


async def update_source(source):
    tickers = set()
    days_env = os.getenv("DAYS")
    if days_env is None or days_env == '':
        days = 3
    else:
        days = int(days_env)
    if days < 3:
        days = 3
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
        # special case for NA ticker, looks like a bug in csv parsing
        if type(ticker) is not str and math.isnan(ticker):
            ticker = "NA"

        symbol_data = data[data["Symbol"] == ticker]
        symbol_data = symbol_data.copy()
        symbol_data["Date"] = pd.to_datetime(symbol_data["Date"], format="%Y%m%d")
        symbol_data.set_index("Date", inplace=True)

        symbol = ticker.replace('/WS', '/W')
        if re.search(r'/[A-VX-Z]', symbol) is not None:
            symbol = symbol.replace("/", ".")
        symbol = symbol.replace("p", "/P")
        symbol = symbol.replace("r", "/R")
        symbol = symbol.replace("w", "/W/I")

        filename = symbol + "_SHORT_VOLUME.csv"
        filename = urllib.parse.quote(filename, safe='')
        symbol_data = symbol_data["ShortVolume"].to_frame()
        values = symbol_data.copy()
        symbol_data.insert(1, 'high', values.copy(), True)
        symbol_data.insert(2, 'low', values.copy(), True)
        symbol_data.insert(3, 'close', values.copy(), True)
        symbol_data.insert(4, 'volume', np.zeros(values.size, dtype=int), True)

        if path.isfile(directory + filename):
            async with async_open(directory + filename, "r", encoding="UTF-8") as f:
                d = await f.read()
                stored_data = pd.read_csv(StringIO(d), names=["Date", *symbol_data.columns.values])
                stored_data["Date"] = stored_data["Date"].map(lambda x: datetime.strptime(x, "%Y%m%dT"))
                stored_data.set_index("Date", inplace=True)
                symbol_data = pd.concat([stored_data, symbol_data])
                # Taken from here: https://stackoverflow.com/a/34297689
                # As more efficient way to deal with duplicates
                symbol_data = symbol_data[~symbol_data.index.duplicated(keep='last')]
                symbol_data.reset_index(inplace=True)
                symbol_data = symbol_data.sort_values(by=['Date'])
                symbol_data.set_index("Date", inplace=True)

        async with async_open(directory + filename, "w") as f:
            s = StringIO()
            symbol_data.to_csv(s, header=False, date_format="%Y%m%dT")
            await f.write(s.getvalue())

        # filename = symbol + source[0] + "TOTAL"
        # with open("../data/" + filename + ".csv", "w") as f:
        #     symbol_data["TotalVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        #
        # if len(symbol_data["ShortExemptVolume"]) > 0:
        #     filename = symbol + source[0] + "SHORT_EXEMPT"
        #     with open("../data/" + filename + ".csv", "w") as f:
        #         symbol_data["ShortExemptVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        print(symbol + " -------- updated")


if __name__ == "__main__":
    main()
