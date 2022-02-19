# ====================================================
#                       패키지
# ====================================================
# -------- data
import pandas as pd
import numpy as np

import sqlalchemy as sa

# -------- str
import re
# -------- date
from datetime import datetime, date
# -------- float
import math

# -------- craw
from bs4 import BeautifulSoup # 클래스라 생성자 만들어야 함
import requests

# -------- flask
from flask import Flask, make_response, jsonify, request, render_template
from flask_cors import CORS, cross_origin
import json

# -------- API
import yfinance


# --------
# ====================================================
#                      기본 설정
# ====================================================

# sql 이름과 비밀번호 수정 필요
engine = sa.create_engine('oracle://ai:1111@localhost:1521/XE')
conn = engine.connect()
conn.close() # TODO 사용 후 close 해주기
# ----------------------------------------------------

# ====================================================
#                       함  수
# ====================================================

"""
# 변수 설명
# 크롤링 데이터 : nm(회사명), cd(네이버에서 사용하는 회사 코드)
# 상장 기업 리스트 : corp_name(회사명), stock_code(종목코드), industry(업종), main_product(주요제품), listed_date(상장일), settle_mont(	결산월), pres(대표자명), hpage(홈페이지), region(지역)
# 야후 파이낸스 : yh_code
"""

#                   ==============
#                      업종 분류
#                   ==============
# -------- 동일 업종 기업 출력
# TODO(미완성) 동일 업종 선택
def select_same_industry(corp_name):
    indus=com_df[com_df['nm']==corp_name]['industry'].values[0] # TODO(df 확인)

    # print(com_df.groupby(by='industry')['nm'].nunique().max()) # 동종업계 최대 151개 -> 151개 재무제표 크롤링?

    list_com=com_df[com_df['industry']==indus]['corp_name'].values.tolist()
    return list_com



#  -------- 네이버증권 연관기업 코드(hjh)
def relate_code_crawl(co):
    #연관 종목코드 있는 페이지 불러오기
    url='https://finance.naver.com/item/main.naver?code='+str(co)
    page=pd.read_html(url,encoding='CP949')
    #연관 종목명과 종목코드 뽑아내기(code_list[0]은 '종목명'이어서 제외)
    code_list=page[4].columns.tolist()
    code_list=code_list[1:]
    #종목코드 리스트 반환
    codes=[]
    for word in (code_list):
        codes.append(word[-6:])
    #print(codes)
    return codes

#relate_code_crawl('000660')



#                   ==============
#                  기업 이름 코드 변환
#                   ==============

# -------- 네이버 재무제표 크롤링 용 gicode로 변환
def nm_to_bs_gicode(corp_name):
    gi=com_df[com_df['nm']==corp_name]['cd']
    gi=gi.values[0]
    return gi



def stc_code_to_bs_gicode(stock_code):
    gi = com_df[com_df['stock_code'] == stock_code]['cd']
    gi = gi.values[0]
    return gi



def yh_code_to_bs_gicode(yh_code):
    gi = com_df[com_df['yh_code'] == yhcode]['cd']
    gi = gi.values[0]
    return gi



# -------- 네이버 금융 크롤링 용 gicode로 변환
def nm_to_fn_gicode(corp_name):
    gi=com_df[com_df['nm']==corp_name]['stock_code']
    gi=gi.values[0]
    return gi



def yh_code_to_fn_gicode(yh_code):
    gi=com_df[com_df['yh_code']==yh_code]['stock_code']
    gi=gi.values[0]
    return gi



# -------- 코드를 기업이름으로 변환
def stc_code_to_nm(stock_code):
    gi = com_df[com_df['stock_code'] == stock_code]['nm']
    gi = gi.values[0]
    return gi



def yh_code_to_nm(yh_code):
    gi = com_df[com_df['yh_code'] == yh_code]['nm']
    gi = gi.values[0]
    return gi



#                   ==============
#                     데이터 수집
#                   ==============


# -------- Balance Sheets API call
# def bs_api(corp_name=None, yh_code=None, stock_code=None):
#     print('haha')




# -------- Balance Sheets Crawling(재무제표 크롤링)
def bs_craw(yh_code=None, corp_name=None, stock_code=None, kind=0): # ------- 검색과 연동해서 입력 변수 설정
    """
    # kind
        : 0 (연간 포괄손익계산서),  1 (분기별 포괄손익계산서)
          2 (연간 재무상태표),     3 (분기별 재무상태표)
          4 (연간 현금흐름표),     5 (분기별 현금프름표)
    """

    # ------- 검색과 연동해서 입력되는 변수 따라 gicode(네이버에서 분류하는 기업 코드)로 변환
    gcode=0
    if yh_code!=None:
        gcode=yh_code_to_bs_gicode(yh_code)
    elif corp_name!=None:
        gcode=nm_to_bs_gicode(corp_name)
    elif stock_code!=None:
        gcode=stc_code_to_bs_gicode(stock_code)

    url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?NewMenuID=103&gicode={gcode}"
    # headers = {'User-Agent': 'Mozilla'}
    #
    # res = requests.get(url, headers=headers)
    # soup = BeautifulSoup(res.text, "html.parser")

    table_list = pd.read_html(url, encoding='UTF-8')
    new_table_list = []
    # print(len(table_list))
    # print(table_list[0].head())

    # 항목에서 불필요한 부분 제거('계산에 참여한 계정 펼치기')
    for tbl in table_list:
        for i, idx in enumerate(tbl.iloc[:,0]):
            m = idx.replace('계산에 참여한 계정 펼치기','')
            tbl.iloc[i, 0] = m
        new_table_list.append(tbl)

    return new_table_list[kind]


# ------- 네이버 금융
def fn_craw(yh_code=None, corp_name=None, stock_code=None, kind=0):
    """
       # kind
           : 0 (전일&당일 상한가, 하한가, 거래량 등) #TODO 가공 필요
             1 (증권사 별 매도 매수 정보) #TODO 가공 필요(컬럼이름)
             2 (외국인, 기관 거래 정보) #TODO 가공 필요
             3 (기업실적분석(연도별 분기별 주요재무 정보)) #TODO 가공 필요?
             4 (동일업종비교) #TODO 가공 필요?
             5 (시가총액, 주식수, 액면가 정보) #TODO 가공 필요
             6 (외국인 주식 한도, 보유 정보)
             7 (목표주가 정보) #TODO 가공 필요
             8 (PER, PBR 배당수익률 정보) (주가 따라 변동) #TODO 가공 필요
             9 (동일업종 PER, 등락률 정보) #TODO 가공 필요
             10 (호가 10단계)
             11 (인기 검색 종목: 코스피) #TODO 가공 필요
             12 (인기 검색 종목: 코스닥) #TODO 가공 필요
       """

    gcode = 0
    if yh_code!=None:
        gcode=yh_code_to_fn_gicode(yh_code)
    elif corp_name != None:
        gcode = nm_to_fn_gicode(corp_name)
    elif stock_code != None:
        gcode = str(stock_code)

    url = f"https://finance.naver.com/item/main.naver?code={gcode}"
    table_list = pd.read_html(url, encoding='euc-kr')

    return table_list[kind]






#                   ==============
#                      지표 선정
#                   ==============

# -------- 지표 선정
def idv_radar_data(yh_code=None, corp_name=None, stock_code=None):
    """
    # <지표 설명>
    # 1. 배당 분석                      -> 배당성향(배당 커버리지의 역수.)
    # 2. 유동성 분석(단기채무지급능력)    -> 당좌비율(당좌자산 / 유동부채)
    # 3. 재무건전성 분석(레버리지 비율)   -> 부채비율(총부채 / 자기자본)의 역수
    # 4. 수익성분석                      -> 매출수익성(당기순이익/매출액))
    # 5. 성장성분석                      -> 순이익성장률
    """

    if yh_code != None:
        gcode = yh_code_to_fn_gicode(yh_code)
        nm = yh_code_to_nm(yh_code)
    elif corp_name != None:
        gcode = nm_to_fn_gicode(corp_name)
        nm = corp_name
    elif stock_code != None:
        gcode = stock_code
        nm = stc_code_to_nm(stock_code)

    sil_df = fn_craw(stock_code=gcode, kind=3)

    if (sil_df.iloc[0:8, 3].isna().sum()) > 0:  # 표 안 가르고 계산하는 건 신규 상장 기업은 정보가 아예 없기 때문
        pass
    if (sil_df.iloc[0:8, 9].isna().sum()) > 0:  # 표 안 가르고 계산하는 건 신규 상장 기업은 정보가 아예 없기 때문
        pass

    else:
        # 0. 재무정보는 최신 분기 실공시 기준
        # 0. 단, 배당은 1년에 한 번 이루어지기 때문에 최신 년도 공시 기준임
        sil_df_y = sil_df['최근 연간 실적'].iloc[:, 2]  # 느리지만 .iloc으로 하는 이유는 공시 날짜가 다른 기업이 있기 때문
        sil_df_q = sil_df['최근 분기 실적'].iloc[:, 4]

        sil_df_y = sil_df_y.fillna(0)
        sil_df_q = sil_df_q.fillna(0)

        if sil_df_y.dtype == 'O':
            sil_df_y[(sil_df_y.str.len() == 1) & (sil_df_y.values == '-')] = 0
            sil_df_y = sil_df_y.astype('float')

        if sil_df_q.dtype == 'O':
            sil_df_q[(sil_df_q.str.len() == 1) & (sil_df_q.values == '-')] = 0
            sil_df_q = sil_df_q.astype('float')

        # 1. 배당성향(bd_tend)
        bd_tend = sil_df_y[15]  # 실제 배당 성향

        # 2. 유동성 분석 - 당좌비율(당좌자산/유동부채)
        #                       당좌자산 = (유동자산 - 재고자산)
        dj_rate = sil_df_q[7]  # 당좌비율

        # 3. 재무건전성 분석 - 부채비율(총부채/자기자본)의 역수
        bch_rate = sil_df_q[6] / 100  # 부채비율
        bch_rate = round((1 / bch_rate) * 100, 2)

        # 4. 수익성 분석 - 매출수익성(당기순이익/매출액) # TODO 매출액 0인 애들은?

        dg_bene = sil_df_q[2]
        mch = sil_df_q[0]

        suyk = round((dg_bene / mch) * 100, 2)

        # 5. 성장성 분석 - 순이익성장률(지속성장 가능률)
        # (1-배당성향)*자기자본순이익률(ROE)
        #    유보율

        roe = sil_df_y[5] / 100
        ubo = (100 - bd_tend) / 100
        grth = round(roe * ubo * 100, 2)

        data_list = [bd_tend, dj_rate, bch_rate, suyk, grth]
        data_dict = {nm: data_list}

        return(data_dict)



# -------- 관련 기업 지표 선정(상대적 비율 기준)

def relate_radar_data(yh_code=None, corp_name=None, stock_code=None):
    label_list = ['배당성향', '유동성', '건전성', '수익성', '성장성']
    dict_list = []

    # 주식 코드,이름으로 변환
    if yh_code != None:
        gcode = yh_code_to_fn_gicode(yh_code)
        nm = yh_code_to_nm(yh_code)
    elif corp_name != None:
        gcode = nm_to_fn_gicode(corp_name)
        nm = corp_name
    elif stock_code != None:
        gcode = stock_code
        nm = stc_code_to_nm(stock_code)

    relate_corp = relate_code_crawl(co=gcode)

    dict_list = [idv_radar_data(stock_code=stcd) for stcd in relate_corp]
    dict_list = [x for x in dict_list if x is not None]

    keys_list = [list(dict_list[i].keys())[0] for i in range(len(dict_list))]

    my_arr = np.zeros([5, 5])

    for i, dic in enumerate(dict_list):
        my_arr[i] = (np.array(dic[keys_list[i]]))

    my_arr[:, 0] = (my_arr[:, 2] / my_arr[:, 2].mean()) * 100
    my_arr[:, 1] = (my_arr[:, 2] / my_arr[:, 2].mean()) * 100
    my_arr[:, 2] = (my_arr[:, 2] / my_arr[:, 2].mean()) * 100
    my_arr[:, 3] = (my_arr[:, 3] / my_arr[:, 3].mean()) * 100
    my_arr[:, 4] = (my_arr[:, 2] / my_arr[:, 2].mean()) * 100

    for i, dic in enumerate(dict_list):
        dic[keys_list[i]] = my_arr[i, :].tolist()
        dict_list[i] = dic

    return label_list, dict_list



# -------- 관련 기업 지표 선정(원본)

# def relate_radar_data(yh_code=None, corp_name=None, stock_code=None):
#     label_list=['배당성향', '유동성', '건전성', '수익성', '성장성']
#     dict_list = []
#
#     # 주식 코드로 변환
#     gcode = 0
#     if yh_code != None:
#         gcode = yh_code_to_fn_gicode(yh_code)
#     elif corp_name != None:
#         gcode = nm_to_fn_gicode(corp_name)
#     elif stock_code != None:
#         gcode = stock_code
#
#     relate_corp = relate_code_crawl(co=gcode)
#
#     dict_list = [idv_radar_data(stock_code=stcd) for stcd in relate_corp]
#
#     dict_list = [x for x in dict_list if x is not None]
#
#
#     return label_list, dict_list



#                   ==============
#                       시각화
#                   ==============

# -------- 매출, 당기순이익 추이 그래프
def mch_dg(yh_code=None, corp_name=None, stock_code=None):
    gcode=0

    if yh_code != None:
        gcode = yh_code_to_fn_gicode(yh_code)
        nm = yh_code_to_nm(yh_code)
    elif corp_name != None:
        gcode = nm_to_fn_gicode(corp_name)
        nm = corp_name
    elif stock_code != None:
        gcode = stock_code
        nm = stc_code_to_nm(stock_code)

    bs_df=bs_craw(stock_code=gcode, kind=1)
    label_list=bs_df.columns[1:6].tolist() # 네 분기 + 전년동기
    mch_list=bs_df.loc[0, label_list].tolist() # 매출액
    dg_list=bs_df.loc[15, label_list].tolist() # 당기순이익

    return label_list, mch_list, dg_list


# -------- BS TABLE (재무상태표 필요 없다 ^^)
# def bs_table(corp_name=None, yh_code=None, stock_code=None):
#     df=bs_craw(corp_name=cor_name, yh_code=yh_code, stock_code=stock_code, kind=1)
#     df
#     """
#     # kind
#         : 0 (연간 포괄손익계산서),  1 (분기별 포괄손익계산서)
#           2 (연간 재무상태표),     3 (분기별 재무상태표)
#           4 (연간 현금흐름표),     5 (분기별 현금프름표)
#     """



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



# ====================================================
#                      데이터
# ====================================================

# -------- 병합 파일 불러오기
com_df=pd.read_csv('C:\\AI\\pythonProject\\venv\\project\\dashboard\\data\\com_df.csv',
                   dtype={'stock_code':'str', '표준코드': 'str', '단축코드' : 'str'},
                   parse_dates=['listed_date', '상장일'])


# -------- 기업별 산업 코드





# ====================================================
#                  함수 호출(test)
# ====================================================

# df=bs_craw(corp_name='삼성전자', kind=0)
# print(df)
# select_same_industry('삼성전자')




# ====================================================
#                    라우터
# ====================================================
app = Flask(__name__, template_folder='production', static_folder='build' ) # template, static 폴더 다르게 지정 가능

@app.route('/')
def index():

    # TODO: 검색에서 기업 코드/이름 할당 받음
    # ------------------테스트용 nm ,stc_code, yh_code TODO 지울 것
    nm='올리패스'
    # stc_code=nm_to_fn_gicode(nm)
    stc_code='005930' # 삼성전자 주식코드
    yh_code='035420.KS' # 네이버 야후코드
    # ------------------

    radar_label, radar_dict=relate_radar_data(yh_code=yh_code, corp_name=None, stock_code=None) # TODO: 검색에서 기업 코드/이름 할당 받음
    bar_label, bar_mch_list, bar_dg_list = mch_dg(yh_code=yh_code, corp_name=None, stock_code=None) # TODO: 데이터 없으면 0으로 처리하건 해야할듯


    return render_template("index.html",
                            RD_LABEL_LIST=radar_label,
                            RD_DATA_DICT=radar_dict,
                            BAR_LABEL_LIST=bar_label,
                            BAR_DATA_LIST_MCH=bar_mch_list,
                            BAR_DATA_LIST_DG=bar_dg_list
                           )





if __name__ == '__main__':               # 이 py 안에서 실행하면 '__main__'
    app.debug=True                       # TODO(개발 끝나면 반드시 막기)
    app.run(host='0.0.0.0', port=8899)

