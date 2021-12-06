import asyncio
import json
import os
import re

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
SOURCE = ("NMS", "NMS", "CNMSshvol")


def main():
    symbols = tickers()
    descs = descriptions()

    date = datetime.today() + timedelta(days=-5)
    while date < datetime.today():
        if date.weekday() < 5:
            url = BASE_URL + SOURCE[2] + date.strftime("%Y%m%d") + ".txt"
            r = requests.get(url)
            values = pd.read_csv(StringIO(r.text), sep='|', engine='python')
            index = 0
            while index < len(values) - 1:

                ticker = values.loc[index].Symbol
                symbol = ticker.replace('/WS', '/W')
                if re.search(r'/[A-VX-Z]', symbol) is not None:
                    symbol = symbol.replace("/", ".")
                symbol = symbol.replace("p", "/P")
                symbol = symbol.replace("r", "/R")
                symbol = symbol.replace("w", "/W/I")
                ticker = symbol
                symbol = symbol + "_" + SOURCE[0] + "_SHORT"

                if symbol not in symbols:
                    symbols.append(symbol)
                    descs.append(ticker + ' FINRA Consolidated NMS Short Volume')
                index += 1
        date = date + timedelta(days=1)

    with open("symbol_info_template.json", "r") as template_file:
        symbol_info = template_file.read()
        symbol_info = symbol_info % (json.dumps(descs), json.dumps(symbols), json.dumps(symbols))
        with open("repo/symbol_info/finra.json", "w") as symbol_info_file:
            symbol_info_file.write(symbol_info)


def tickers():
    path = os.path.join('repo', 'symbol_info', 'finra.json')
    with open(path, 'r', encoding="UTF-8") as f:
        objects = ijson.items(f, 'symbol.item')
        tickers = (o for o in objects)
        resultList = []
        for t in tickers:
            resultList.append(t)
        return resultList

def descriptions():
    path = os.path.join('repo', 'symbol_info', 'finra.json')
    with open(path, 'r', encoding="UTF-8") as f:
        objects = ijson.items(f, 'description.item')
        descriptions = (o for o in objects)
        result = []
        for t in descriptions:
            result.append(t)
        return result

if __name__ == "__main__":
    main()
