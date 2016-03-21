__author__ = 'cyang'


# coding=utf-8
import datetime

import tushare as ts
import pymongo
import numpy as np
import pandas as pd
import json


# ����ͨ�������˻���tokenƾ֤��
from pandas import DataFrame

ts.set_token('e36a4a971d6f30cd7a528e36bbd00dad7faf53ae8e7c6100ca49d337245e6f6d')

# Connect to the db
client = pymongo.MongoClient("localhost", 27017)

#
db_stockInfo = client.stockInfo
db_stockInfo_coll = db_stockInfo['stockInfo_coll']


ts.get_hist_data('600848')  # һ���Ի�ȡȫ����k������

# �õ�������
tlMaster = ts.Master()
# �Ϻ������ڽ��������� XSHG,XSHE'
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

# �г���������
## �����Ʊ������
db_MktEqud_coll = db_stockInfo['MktEqud_coll']
## �ڻ�������(�����Գֲ�������)
db_MktFutd_coll = db_stockInfo['MktFutd']
## ָ��������
db_MktIdxd_coll = db_stockInfo['MktIdxd']
## ������ڽ���
db_MktBlockd_coll = db_stockInfo.MktBlockd
## ծȯ�ع�����������
db_MktRepod_coll = db_stockInfo.MktRepod
## ծȯ������
db_MktBondd_coll = db_stockInfo.MktBondd
## �۹�������
db_MktHKEqud_coll = db_stockInfo.MktHKEqud
## ��ȡ�����г���Ϣ����
db_TickRTSnapshot_coll = db_stockInfo.TickRTSnapshot
## ��ȡָ���ɷֹɵ������г���Ϣ����
db_TickRTSnapshotIndex_coll = db_stockInfo.TickRTSnapshotIndex
## ��ȡ�ڻ������г���Ϣ����
db_FutureTickRTSnapshot_coll = db_stockInfo.FutureTickRTSnapshot

# ��ȡ��ʷĳһ�չ�Ʊ�������ݣ�������ͣ�ƹ�Ʊ��ͣ�Ƶı��۶���0��
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



