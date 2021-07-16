# -*- coding: utf-8 -*-
"""
Created on Wed May 12 09:06:16 2020

@author: Yaoxingqi
"""

import pandas as pd
import numpy as np
# SQL扩展
# PandaSQL
# Pandas有一些SQL扩展，例如pandasql，它允许在数据帧之上执行SQL查询。通过pandasql，可以直接查询数据框对象，就好像它们是数据库表一样。
import pandasql as pdsql

import xlrd
import xlwt
# import xlutils
import xlsxwriter
import requests

# 注意到datetime是模块， datetime模块还包含一个datetime类， 通过from datetime import datetime导入的才是datetime这个类。
# 如果仅导入import datetime， 则必须引用全名datetime.datetime。
import datetime
from datetime import datetime,date,time

from threading import Thread
import json

import pyodbc
import pymssql
import sqlite3
import sqlalchemy as sqla
from sqlalchemy import create_engine

from urllib.request import urlopen, Request


pd.set_option("display.width",120,"display.max_columns",999)

with pymssql.connect('(local)', 'sa', 'P@ssw0rd520', 'kaoqin-01', charset = "GBK") as conn:
    with conn.cursor(as_dict=True) as cursor:  # 数据存放到字典中
        # cursor.execute('SELECT U.NAME,C.CHECKTIME FROM USERINFO U,CHECKINOUT C WHERE U.USERID=C.USERID')
        # cursor.execute('SELECT U.NAME,C.CHECKTIME FROM USERINFO U,(SELECT * FROM CHECKINOUT C WHERE C.CHECKTIME BETWEEN ''2021-04-01'' AND ''2021-04-30'') C WHERE U.USERID=C.USERID')
        cursor.execute('SELECT u.USERID,u.NAME "姓名",datename(weekday,C.CHECKTIME) "星期",convert(varchar(10),C.CHECKTIME,120) "日期",convert(varchar(8),C.CHECKTIME,114) "时间" FROM USERINFO U,CHECKINOUT C WHERE U.USERID=C.USERID')
        data=cursor.fetchall()
        # for row in cursor.fetchall():
            # print(type(row['NAME']),type(row['CHECKTIME']))
            # print(row)

df=pd.DataFrame(data)
print(df)

# 按照姓名和日期分组，计算时间的最大值和最小值，最大值是下班时间，最小值是上班时间
df_date = df.groupby(['日期','姓名']).agg(上班时间=('时间', np.min), 下班时间=('时间',np.max))
df_date["上班时间"]=df_date["上班时间"].dt.time
df_date["下班时间"]=df_date["下班时间"].dt.time
work_time=datetime.time(8,30,0)
rest_time=datetime.time(17,30,0)

def to_hour(x):
    y=x.hour+x.minute/60+x.second/3600
    return np.round(y,2)
    
df_date["上班时间_a"]=df_date["上班时间"].mask(df_date["上班时间"]<work_time,work_time)
# df_date["下班时间_a"]=np.where(df_date["下班时间"]>=work_time,work_time,df_date["下班时间"])
df_date["工作时长"]=df_date["下班时间"].apply(to_hour)-df_date["上班时间_a"].apply(to_hour)-1
df_date["加班时长"]=df_date["下班时间"].apply(to_hour)-18
df_date["工作时长"]=df_date["工作时长"].mask(df_date['上班时间']>work_time,"迟到")
df_date["工作时长"]=df_date["工作时长"].mask(df_date['下班时间']<rest_time,"早退")
df_date["工作时长"]=df_date["工作时长"].mask((df_date['下班时间']<rest_time) & (df_date['上班时间']>work_time),"迟到+早退")
df_date["工作时长"]=df_date["工作时长"].mask((df_date['上班时间']==df_date['下班时间'])&(df_date['上班时间']>datetime.time(12,0,0)),"缺上午卡")
df_date["工作时长"]=df_date["工作时长"].mask((df_date['上班时间']==df_date['下班时间'])&(df_date['下班时间']<datetime.time(12,0,0)),"缺下午卡")
df_date["工作时长"]=df_date["工作时长"].mask((df_date['下班时间']>=rest_time) & (df_date['上班时间']<=work_time),"正常")
# print(df_date)

table=df_date.drop(columns='上班时间_a').stack().unstack('打卡日期')
print(table)
table.index.names=["姓名","类型"]
table.to_csv("E:\\code\\考勤\\出勤情况表.csv", encoding="gbk", index=True,sep=',')