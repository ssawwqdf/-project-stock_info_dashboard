# -------- data
import pandas as pd
import numpy as np

import sqlalchemy as sa

# -------- str
import re
# -------- date
from datetime import datetime, date

# -------- craw
from bs4 import BeautifulSoup # 클래스라 생성자 만들어야 함
import requests

# -------- flask
from flask import Flask, make_response, jsonify, request, render_template
from flask_cors import CORS, cross_origin
import json

engine = sa.create_engine('oracle://ai:1111@localhost:1521/XE')
conn = engine.connect()
conn.close()


# -----------------------
# -----------------------
# -----------------------
# -----------------------

# -----------------네이버 company list
url = "http://comp.fnguide.com/XML/Market/CompanyList.txt"
nv_com_res = requests.get(url)

# print(nv_com_res.encoding)  # ISO-8859-1
nv_com_res.encoding = "utf-8-sig"  # 그냥 utf-8하면 에러 날 수도 있다
nv_com_json = json.loads(nv_com_res.text)  # { "Co": [ {},{},{} ] }

nv_com_df = pd.DataFrame(data=nv_com_json["Co"])
# print(com_df)
# print(nv_com_df[nv_com_df['gb']=='701'])

nv_com_df['gb']=nv_com_df['gb'].astype('int')
# print(nv_com_df.head())
# nv_com_df.to_sql('temptemp', conn)

# -----------------------
# -----------------------
# -----------------------
# -----------------------


# ---------------- listed company

listed_comp=pd.read_csv('C:\\Users\\LHL\\Desktop\\상장법인목록.csv')

listed_comp.columns=['corp_name', 'stock_code', 'industry', 'main_product','listed_date',  'settle_month', 'presid', 'hpage', 'region']
listed_comp.info()

# listed_comp.to_sql('listed_comp', conn)

nv_com_df2=nv_com_df[nv_com_df['gb']==701]
# nv_com_df2.info()
com_df=pd.merge(nv_com_df2, listed_comp, how='outer', left_on= 'nm', right_on= 'corp_name')


print(com_df)
# com_df.info()
# print(com_df.iloc[:,1:4])
# print(com_df.loc[com_df['nm'].isna(), ['nm', 'corp_name']])
# print(com_df.loc[com_df['corp_name'].isna(), ['nm', 'corp_name']])
# -----------------------
# -----------------------
# -----------------------
# -----------------------



    # ----------- 검색할 때 필요할까봐 남겨둠
    # Series.str.contains 외에도 start_with, end_with 등등 있다
    # print(com_df[com_df['nm'].str.contains('카카오')][['cd', 'nm']])
    # print(com_df[com_df['nm'].str.contains('카카오')]['cd'].tolist())


# -----------------------
# -----------------------
# -----------------------
# -----------------------

gicode='A265520'
url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?NewMenuID=103&gicode={gicode}"
headers = {'User-Agent': 'Mozilla'}

# res = requests.get(url, headers=headers)
# soup = BeautifulSoup(res.text, "html.parser")

# box_list = soup.select("#divDaechaY > table > tbody > tr")
# print(box_list)

#divDaechaY > table > tbody > tr:nth-child(1)
#p_grid2_2
#divDaechaY > table > tbody > tr:nth-child(3)
#divDaechaY > table > tbody > tr:nth-child(4)

# for box in box_list:
#     print(box)
#     haha=box.select('tr')
#     print(haha)

table_list = pd.read_html(url, encoding='UTF-8')
# print(len(table_list))
# print(table_list[2].info())
# print(table_list[2])


############ 네이버 금융 ~~에 참여한 계정 펼치기 부분 삭제
# pri_bs=table_list[0] # 연간이냐 분기냐에 따라 달라짐
# # print(pri_bs['IFRS(연결)'])
# for i, idx in enumerate(pri_bs['IFRS(연결)']):
#     print(idx)
#     m=idx.rstrip('에 참여한 계정 펼치기')
#     pri_bs.iloc[i,0]=m
# print(pri_bs['IFRS(연결)'])


# df=df.iloc[0,1:6]



# tot_list = []
    # print(box_list)
    #
    # for box in box_list:
    #     print(box)
    #     for item in box:
    #         title = box.select_one('th > div').text
    #         print(item)
    # list=[]
    # price1 = box.select_one("th > div").text
    # price2 = box.select_one("").text
    #
    # list.append(price1)
    # list.append(price2)
    #
    # tot_list.append(list)

    # 프레임 만드는 게 주 목적이면 df=pd.DataFrame(data=tot_list) 하고 return df
    # df = pd.DataFrame(data=tot_list)
    # return tot_list  # [[],[],[]]

# ------------------------ # ------------------------
# ------------------------ 네이버 금융 여러 표# ------------------------
# ------------------------ # ------------------------

# gcode='005930'
# url = f"https://finance.naver.com/item/main.naver?code={gcode}"
# headers = {'User-Agent': 'Mozilla'}

# res = requests.get(url, headers=headers)
# res.encoding = 'utf-8-sig'
# soup = BeautifulSoup(res.text, "html.parser")

# table_list = pd.read_html(url, encoding='euc-kr')
#
# for i, dfdf in enumerate(table_list):
#     print("**"*30)
#     print(i)
#     print(dfdf.info())
#     print(table_list[i].head())

#
# print(table_list[0].info())

# print(table_list[1].head())

np.isnan