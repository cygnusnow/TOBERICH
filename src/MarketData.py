__author__ = 'cyang'


# coding=utf-8
import datetime

import tushare as ts
import pymongo
import numpy as np
import pandas as pd
import json


# 设置通联数据账户的token凭证码
from pandas import DataFrame

ts.set_token('e36a4a971d6f30cd7a528e36bbd00dad7faf53ae8e7c6100ca49d337245e6f6d')

# Connect to the db
client = pymongo.MongoClient("localhost", 27017)

#
db_stockInfo = client.stockInfo
db_stockInfo_coll = db_stockInfo['stockInfo_coll']


ts.get_hist_data('600848')  # 一次性获取全部日k线数据

# 得到交易日
tlMaster = ts.Master()
# 上海，深圳交易所数据 XSHG,XSHE'
df = tlMaster.TradeCal(exchangeCD='XSHE,XSHG', beginDate='20140928', endDate='20151010')
js_df = df[1:].to_json(orient="records")
x = json.loads(js_df)

db_stockInfo_coll.drop()
db_stockInfo_coll.insert(x)
print("######Downnloaded the calendar from the stock exhange!###### %d" %(db_stockInfo_coll.find().count()))

openDate = df[df.isOpen > 0]
openDate = openDate['calendarDate'].apply(lambda x:x.replace('-',''))
js_df = openDate[1:].to_json(orient="records")
calendarDate = json.loads(js_df)
print(calendarDate)

# 市场行情数据
## 沪深股票日行情
db_MktEqud_coll = db_stockInfo['MktEqud_coll']
## 期货日行情(主力以持仓量计算)
db_MktFutd_coll = db_stockInfo['MktFutd']
## 指数日行情
db_MktIdxd_coll = db_stockInfo['MktIdxd']
## 沪深大宗交易
db_MktBlockd_coll = db_stockInfo.MktBlockd
## 债券回购交易日行情
db_MktRepod_coll = db_stockInfo.MktRepod
## 债券日行情
db_MktBondd_coll = db_stockInfo.MktBondd
## 港股日行情
db_MktHKEqud_coll = db_stockInfo.MktHKEqud
## 获取最新市场信息快照
db_TickRTSnapshot_coll = db_stockInfo.TickRTSnapshot
## 获取指数成分股的最新市场信息快照
db_TickRTSnapshotIndex_coll = db_stockInfo.TickRTSnapshotIndex
## 获取期货最新市场信息快照
db_FutureTickRTSnapshot_coll = db_stockInfo.FutureTickRTSnapshot

# 获取历史某一日股票行情数据，包括了停牌股票（停牌的报价都是0）
tlMarket = ts.Market()
db_MktEqud_coll.drop()
#for i in calendarDate:
for data in calendarDate:
    #df = tlMarket.MktEqud(tradeDate=data)
    #js_df = df[1:].to_json(orient="records")
    #mktEqud = json.loads(js_df)
    #print("#########", mktEqud[1:])
    #db_MktEqud_coll.insert(mktEqud)

    df = tlMarket.MktIdxd(tradeDate=data)
    js_df = df[1:].to_json(orient="records")
    mktIdxd = json.loads(js_df)
    db_MktIdxd_coll.insert(mktIdxd)



