# ====================================================
#                      데이터            # TODO(데이터 저장 다르게)
# ====================================================

#                   *****************
# !!!주의!!! : 야후에서 사용하는 코드의 피쳐 이름 stock_code -> yh_code로 변경됨
#                   *****************


#                   ==============
#                       불러오기
#                   ==============

# -------- 네이버 company list : nv_com_df
url = "http://comp.fnguide.com/XML/Market/CompanyList.txt"

nv_com_res = requests.get(url)
nv_com_res.encoding = "utf-8-sig"  # # ISO-8859-1

nv_com_json = json.loads(nv_com_res.text)

nv_com_df = pd.DataFrame(data=nv_com_json["Co"])
nv_com_df['gb']=nv_com_df['gb'].astype('int')

nv_com_df2=nv_com_df[nv_com_df['gb']==701] # 주식인 경우만 추출



# -------- 상장법인목록(산업 정보 포함) : listed company
listed_comp=pd.read_csv('C:\\AI\\pythonProject\\venv\\project\\dashboard\\data\\상장법인목록.csv', dtype=str) # TODO 필요 시 경로 변경
listed_comp.columns=['corp_name', 'stock_code', 'industry', 'main_product','listed_date',  'settle_month', 'presid', 'hpage', 'region']
listed_comp['listed_date']=listed_comp['listed_date'].astype('datetime64')



# -------- KOSPI, KOSDAQ 시장 : kos_kdq (기존 파일은 코스피만 있던 관계로 수정)
# !!!!!!!!!!!주의 기존 KOS code csv 파일에서는 야후 코드를 stock_code라고 사용했으나 현재는 yh_code로 바뀌었음

kos=pd.read_csv('C:\\AI\\pythonProject\\venv\\project\\dashboard\\data\\KOSPI_220.csv', dtype={'단축코드':'str'}, encoding='euc-kr') # TODO 필요 시 경로 변경
kdq=pd.read_csv('C:\\AI\\pythonProject\\venv\\project\\dashboard\\data\\KOSDAQ_220.csv', dtype={'단축코드':'str'}, encoding='euc-kr') # TODO 필요 시 경로 변경

kos['상장일']=kos['상장일'].astype('datetime64')
kdq['상장일']=kdq['상장일'].astype('datetime64')

kos['yh_code']=kos['단축코드']+'.KS'
kdq['yh_code']=kdq['단축코드']+'.KQ'

kos_kdq=pd.concat([kos, kdq], axis=0, ignore_index=True)



#                   ==============
#                     데이터전처리
#                   ==============

# -------- kos_kdq 기업 이름 추출 하기

                # : 다른 데이터 프레임과 JOIN 하기 위함
# 구분 코드 만들기
#   : 종목명이나 종목약명은 단순히 보통주/우선주/전환 보다는 1P냐 1PB냐 등에 따라 규칙이 결정되는 경우가 많음
kos_kdq['구분']=kos_kdq['영문 종목명'].str.extract(f'([\(]\w*[\)])')



# 종목약명과 종목명에서 각각 기업 이름 추출하기
#   : 동일 기업도 종목명에서 추출한 이름과 종목약명에서 추출한 이름이 다른 경우 존재(ex. F&F홀딩스 vs 에프엔에프홀딩스)

# 1) 종목약명으로 가공
# 보통주
kos_kdq.loc[kos_kdq['주식종류']=='보통주', '회사명']=kos_kdq['한글 종목약명']

# 1P
kos_kdq.loc[kos_kdq['구분']=='(1P)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[1]?우$','','{}'.format(x)))

# 1PB, 2PB, 3PB
kos_kdq.loc[kos_kdq['구분']=='(1PB)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[1]?우B$','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(2PB)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[2]?우[B]?$','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(3PB)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[3]?우[B]?$','','{}'.format(x)))

# 3PC,4PC
kos_kdq.loc[kos_kdq['구분']=='(3PC)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[3]?우\(전환\)','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(3PC)', '회사명']=kos_kdq['회사명'].apply(lambda x : re.sub('[3]?우C','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(4PC)', '회사명']=kos_kdq['한글 종목약명'].apply(lambda x : re.sub('[4]?우\(전환\)','','{}'.format(x)))

# 특수 케이스
kos_kdq['회사명']=kos_kdq['회사명'].replace('코리아써', '코리아써키트')
kos_kdq['회사명']=kos_kdq['회사명'].replace('E', 'E1')



# 2) 종목명으로 가공
# 보통주
kos_kdq.loc[kos_kdq['주식종류']=='보통주', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?보통주$','','{}'.format(x)))

# 1P
kos_kdq.loc[kos_kdq['구분']=='(1P)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[1]?우선주$','','{}'.format(x)))

# 1PB, 2PB, 3PB
kos_kdq.loc[kos_kdq['구분']=='(1PB)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[1]?우선주\(신형\)$','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(2PB)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[2]?우선주\(신형\)$','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(3PB)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[3]?우선주\(신형\)$','','{}'.format(x)))

# 3PC,4PC
kos_kdq.loc[kos_kdq['구분']=='(3PC)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[3]?우선주\(전환\)','','{}'.format(x)))
kos_kdq.loc[kos_kdq['구분']=='(4PC)', '회사명2']=kos_kdq['한글 종목명'].apply(lambda x : re.sub('[ ]?[4]?우선주\(전환\)','','{}'.format(x)))



#                   ==============
#                     프레임 병합
#                   ==============

# -------- 전체 데이터 프레임 병합
        """
        kos_kdq(코스피, 코스닥에 대한 정보와 우선주, 전환을 포함한 상장 기업 데이터프레임)  : 주식 코드 포함, 우선주 정보 포함, 기업명 포함X 대신 주식 한글 종목 명과 한글 종목 약명 포함.(ex. 네이버 우선주인 경우 네이버우) -> 기업명 추출 했음
        listed_comp(기업 산업 정보를 포함한 상장 기업 데이터프레임)                       : 주식 코드 포함, 우선주 정보 없음, 기업명(corp_name) 포함
        nv_com_df2(네이버 재무제표 주소에 접근할 수 있는 코드가 있는 기업 데이터프레임)     : 주식 코드 포함 X, 우선주 정보 없음, 기업명(nm) 포함
        """

# 코드 설명:
# listed_comp, nv_com_df: 같은 기업이어도 corp_name, nm이 다르게 표현된 경우 존재(띄어쓰기가 다르거나 영어 문자를 한글로 표현한 경우 등)
# kos_kdq: 동일 기업이어도 한글 종목명에서 기업명을 추출하느냐 한글 종목약명에서 추출하느냐에 따라 추출된 기업명 다른 경우 존재



# -------- listed_comp + kos_kdq 병합 -> com_df

# 보통주인 경우 listed_comp의 stock_code와 kos_kdq의 단축코드 일치
com_df=pd.merge(listed_comp, kos_kdq[kos_kdq['주식종류']=='보통주'], how='inner', left_on= 'stock_code', right_on= '단축코드')



# 보통주가 아닌 경우 listed_comp에는 stock_code 정보 없음 -> 회사명을 이용해 join해주어야 함.
# lited_comp의 'corp_name'과 kos_kdq의 종목약명에서 추출한 '회사명'을 기준으로 inner join 해줌
com_df2=pd.merge(listed_comp, kos_kdq[kos_kdq['주식종류']!='보통주'], how='inner', left_on= 'corp_name', right_on= '회사명')
# kos_kdq의 종목약명에서 추출한 '회사명'을 기준으로 join했을 때 join되지 않은 것은 종목명에서 추출한 회사명2를 기준으로 join해줌
com_df3=pd.merge(listed_comp, kos_kdq[(kos_kdq['주식종류']!='보통주') & (kos_kdq['회사명']!=kos_kdq['회사명2'])], how='inner', left_on= 'corp_name', right_on= '회사명2')



# 상하병합
com_df=pd.concat([com_df, com_df2, com_df3], axis=0)



# 임시 df 삭제
del([com_df2, com_df3])



# -------- com_df + nv_com_df2 병합 -> com_df

# listed_comp의 corp_name == kos_kdq의 회사명 == kos_kdq의 회사명2 인 경우 아무 이름으로 nv_com_df와 join해줌
com_df4=pd.merge(nv_com_df2, com_df[(com_df['corp_name']==com_df['회사명']) & (com_df['corp_name']==com_df['회사명2'])], how='inner', left_on= 'nm', right_on= 'corp_name')



# 하나라도 일치하지 않는 경우 각각의 경우에 따라 nv_com_df와 join해줌
com_df5=pd.merge(nv_com_df2, com_df[(com_df['corp_name']!=com_df['회사명']) | (com_df['corp_name']!=com_df['회사명2'])], how='inner', left_on= 'nm', right_on= 'corp_name')
com_df6=pd.merge(nv_com_df2, com_df[(com_df['corp_name']!=com_df['회사명']) | (com_df['corp_name']!=com_df['회사명2'])], how='inner', left_on= 'nm', right_on= '회사명')
com_df7=pd.merge(nv_com_df2, com_df[(com_df['corp_name']!=com_df['회사명']) | (com_df['corp_name']!=com_df['회사명2'])], how='inner', left_on= 'nm', right_on= '회사명2')



# 상하병합
com_df=pd.concat([com_df4, com_df5, com_df6, com_df7], axis=0)



# 불필요한 부분 제거
com_df=com_df.drop_duplicates() # 중복 제거
com_df=com_df.drop(['gb', 'presid', 'region', '시장구분', '소속부', '구분', '회사명', '회사명2'], axis=1) # 내용이 중복되어도 기존 함수에서 사용하던 컬럼은 그대로 둠



# 임시 df 삭제
del([com_df4, com_df5, com_df6, com_df7])

# stock_code -> stock_code_ori
# '단축코드' -> stock_code
com_df['stock_code_ori']=com_df['stock_code']
com_df['stock_code']=com_df['단축코드']

#                   ==============
#                     데이터 저장
#                   ==============

com_df.to_csv('C:\\AI\\pythonProject\\venv\\project\\dashboard\\data\\com_df.csv', index=False) # TODO 경로 수정