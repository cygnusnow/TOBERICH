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

DB_GETRICH = client.GETRICH

db_SecID_coll                   = DB_GETRICH.SecID                     ## 证券编码及基本上市信息
db_TradeCal_coll                = DB_GETRICH.TradeCal                  ## 交易所交易日历
db_SecTypeRel_coll              = DB_GETRICH.SecTypeRel                ## 证券板块成分
db_EquInfo_coll                 = DB_GETRICH.EquInfo                   ## 沪深股票键盘精灵
db_SecTypeRegionRel_coll        = DB_GETRICH.SecTypeRegionRel          ## 沪深股票地域分类
db_SecType_coll                 = DB_GETRICH.SecType                   ## 证券板块
db_SecTypeRegion_coll           = DB_GETRICH.SecTypeRegion             ## 地域分类

# 得到交易日
tlMaster = ts.Master()
# 证券编码及基本上市信息
stocksType = ['E','B','F','IDX','FU','OP']  # E 股票,B 债券,F 基金,IDX 指数,FU 期货,OP 期权；
for types in stocksType:
    df = tlMaster.SecID(assetClass=types)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_SecID_coll.insert(db_json)

# 交易所交易日历
# XSHG表示上海证券交易所，XSHE表示深圳证券交易所，CCFX表示中国金融期货交易所，XDCE表示大连商品交易所，XSGE表示上海期货交易所，XZCE表示郑州商品交易所，XHKG表示香港证券交易所
#exchangeType = ['XSHG','XSHE','CCFX','XDCE','XSGE','XZCE','XHKG']
exchangeType = ['XSHG','XSHE']
for types in exchangeType:
    df = tlMaster.TradeCal(exchangeCD=types, beginDate='20150928', endDate='20151010')
    #df = df['calendarDate'].apply(lambda x: x.replace('-', ''))
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_TradeCal_coll.insert(db_json)

# 证券板块成分
df = tlMaster.SecTypeRel()
js_df = df[1:].to_json(orient="records")
db_json = json.loads(js_df)
db_SecTypeRel_coll.insert(db_json)

# 沪深股票地域分类
df = tlMaster.SecTypeRegionRel()
js_df = df[1:].to_json(orient="records")
db_json = json.loads(js_df)
db_SecTypeRegionRel_coll.insert(db_json)

# 证券板块
df = tlMaster.SecType()
js_df = df[1:].to_json(orient="records")
db_json = json.loads(js_df)
db_SecType_coll.insert(db_json)

# 地域分类
df = tlMaster.SecTypeRegion()
js_df = df[1:].to_json(orient="records")
db_json = json.loads(js_df)
db_SecTypeRegion_coll.insert(db_json)


# 市场行情数据
db_MktEqud_coll = DB_GETRICH.MktEqud            ## 沪深股票日行情
db_MktFutd_coll = DB_GETRICH.MktFutd            ## 期货日行情(主力以持仓量计算)
db_MktIdxd_coll = DB_GETRICH.MktIdxd            ## 指数日行情
db_MktBlockd_coll = DB_GETRICH.MktBlockd        ## 沪深大宗交易
db_MktRepod_coll = DB_GETRICH.MktRepod          ## 债券回购交易日行情
db_MktBondd_coll = DB_GETRICH.MktBondd          ## 债券日行情
db_MktHKEqud_coll = DB_GETRICH.MktHKEqud        ## 港股日行情
db_TickRTSnapshot_coll = DB_GETRICH.TickRTSnapshot              ## 获取最新市场信息快照
db_TickRTSnapshotIndex_coll = DB_GETRICH.TickRTSnapshotIndex    ## 获取指数成分股的最新市场信息快照
db_FutureTickRTSnapshot_coll = DB_GETRICH.FutureTickRTSnapshot  ## 获取期货最新市场信息快照

# 获取历史某一日股票行情数据，包括了停牌股票（停牌的报价都是0）
tlMarket = ts.Market()
print("Get market data")
calendarDate = db_TradeCal_coll.find()
for i in calendarDate:
    print(i.replace(lambda x:x.))

#for date in db_TradeCal_coll.find({"calendarDate"}):
for date in ['20160316']:
    df = tlMarket.MktEqud(tradeDate=date)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_MktEqud_coll.insert(db_json)

    df = tlMarket.MktIdxd(tradeDate=date)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_MktIdxd_coll.insert(db_json)

    df = tlMarket.MktBlockd(tradeDate=date)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_MktBlockd_coll.insert(db_json)

    df = tlMarket.MktRepod(tradeDate=date)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_MktRepod_coll.insert(db_json)

    df = tlMarket.MktBondd(tradeDate=date)
    js_df = df[1:].to_json(orient="records")
    db_json = json.loads(js_df)
    db_MktBondd_coll.insert(db_json)

    #df = tlMarket.MktHKE#qud(tradeDate=date)
    #js_df = df[1:].to_json(orient="records")
    #db_json = json.loads(js_df)
    #db_MktHKEqud_coll.insert(db_json)



# 股票信息
db_Equ_coll          = DB_GETRICH.Equ         ## 股票基本信息
db_EquAllot_coll     = DB_GETRICH.EquAllot    ## 股票配股信息
db_EquDiv_coll       = DB_GETRICH.EquDiv      ## 股票分红信息
db_EquIndustry_coll  = DB_GETRICH.EquIndustry ## 股票行业分类
db_EquIPO_coll       = DB_GETRICH.EquIPO      ## 股票首次上市信息
db_EquRef_coll       = DB_GETRICH.EquRef      ## 股票股权分置
db_EquRetud_coll     = DB_GETRICH.EquRetud    ## 股票每日回报率
db_EquSplits_coll    = DB_GETRICH.EquSplits   ## 股票拆股信息
db_FstTotal_coll     = DB_GETRICH.FstTotal    ## 沪深融资融券每日汇总信息
db_FstDetail_coll    = DB_GETRICH.FstDetail   ## 沪深融资融券每日交易明细信息
db_EquShare_coll     = DB_GETRICH.EquShare    ## 公司股本变动

# 得到所有上市公司的ticker



