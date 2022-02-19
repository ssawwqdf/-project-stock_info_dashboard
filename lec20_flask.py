import json

import pandas as pd
from flask import Flask, make_response, jsonify, request, render_template
import cx_Oracle
import sqlalchemy as sa
from flask_cors import CORS, cross_origin
import requests
from bs4 import BeautifulSoup


# ------------------------------------
#pip install flask_cors
# import flask_cors CORS, cross_origin
# CORS(app)
# CORS(app, resources={r'*': {'origins': '*'}})
# ------------------------------------



app = Flask(__name__, template_folder="production", static_folder="build")
CORS(app)


@app.route('/')
def index():

    return render_template("index.html")


#---------------- 업체이름을 타이핑할때마다 실시간 비동기로 업체 명단을 가져와서 리턴 -----------
@app.route('/com_search_ajax', methods=['post'])
def com_search_ajax():

    str = request.form.get('search_input')
    print(str)

    com_df = pd.read_csv("com_df.csv")

    #-----------웹에서 입력한 검색어와 관련된 업체만 가져오기 -----------------
    temp = com_df[com_df['nm'].str.contains(str)][['cd', 'nm']].head()
    print(temp.values.tolist())
    return json.dumps(  temp.values.tolist()  )



if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=8088)