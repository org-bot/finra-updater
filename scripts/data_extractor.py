import asyncio
import aiohttp
import requests

from datetime import datetime
from datetime import timedelta
from io import StringIO

import pandas as pd

BASE_URL = "https://cdn.finra.org/equity/regsho/daily/"
SOURCES = (
    ("TRF_NYSE", "TRF NYSE", "FNYXshvol", "20090803"),
    ("TRF_NASDAQ_Car", "TRF NASDAQ Carteret", "FNSQshvol", "20090803"),
    ("TRF_NASDAQ_Chi", "TRF NASDAQ Chicago", "FNQCshvol", "20180910"),
    ("ADF", "ADF", "FNRAshvol", "20090803"),
    ("ORF", "ORF", "FORFshvol", "20090803"),
    ("NMS", "NMS", "CNMSshvol", "20180801")
)

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(processAll())

async def processAll():
    async with aiohttp.ClientSession() as session:
        tasks = [
            # processSource(SOURCES[0]),
            # processSource(SOURCES[1]),
            # processSource(SOURCES[2]),
            # processSource(SOURCES[3]),
            # processSource(SOURCES[4]),
            processSource(SOURCES[5]),
        ]
        await asyncio.gather(*tasks)

async def processSource(source):
    tickerSet = set()
    tickers = {}
    symbols = set()
    date = datetime.strptime(source[3], "%Y%m%d")
    data = pd.DataFrame()
    while date < datetime.today():
        if date.weekday() < 5:
            url = BASE_URL + source[2] + date.strftime("%Y%m%d") + ".txt"
            r = requests.get(url)

            values = pd.read_csv(StringIO(r.text), sep='|', engine='python')
            index = 0
            while index < len(values) - 1:
                ticker = values.loc[index].Symbol + "_" + source[0] + "_SHORT"
                desc = values.loc[index].Symbol + source[0] + "Short Volume"
                symbols.add(values.loc[index].Symbol)
                if ticker not in tickerSet:
                    tickerSet.add(ticker)
                    tickers[ticker] = desc

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
            print(source[0] + " ----- " + date.strftime("%Y%m%d") + " -------- " + str(len(tickerSet)))
        date = date + timedelta(days=1)
    print(data)
    i = 1
    for  symbol in symbols:
        symbolData = data[data["Symbol"] == symbol]
        symbolData = symbolData.copy()
        symbolData["Date"] = pd.to_datetime(symbolData["Date"], format="%Y%m%d")
        symbolData.set_index("Date", inplace=True)
        symbol = symbol.replace("/", "_")
        filename = symbol + "_" + source[0] + "_SHORT"
        with open("./data/finra/" + filename + ".csv", "w+") as f:
            symbolData["ShortVolume"].to_csv(f, header=False, date_format="%Y%m%dT")

        # filename = symbol + source[0] + "TOTAL"
        # with open("../data/" + filename + ".csv", "w") as f:
        #     symbolData["TotalVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        #
        # if len(symbolData["ShortExemptVolume"]) > 0:
        #     filename = symbol + source[0] + "SHORT_EXEMPT"
        #     with open("../data/" + filename + ".csv", "w") as f:
        #         symbolData["ShortExemptVolume"].to_csv(f, header=False, date_format="%Y%m%dT")
        print(str(i) + " ----- " + symbol + " -------- done")
        i += 1

    with open(source[0] + '.txt', 'w+') as f:
        for item in tickerSet:
            f.write("%s\n" % item.replace("/", "_"))


if __name__ == "__main__":
    main()
