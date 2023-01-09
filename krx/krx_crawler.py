import requests
from flask import json
from datetime import datetime, timedelta
import pymysql
from flask_restx import Namespace,Resource
import time
# from log.logger import getPostCallApi,getErrorCallApi,getTestCallApi,getErrortestCallApi


krxApi = Namespace('krx', description='krx data crawler')

baseUrl ='http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'

header = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host' : 'data.krx.co.kr',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
    'Content-Length' : '<calculated when request is sent>'
}

@krxApi.route('/oneDayInsert')
class oneDaySelect(Resource):
    def get(self):
        '''금일 데이터 수집'''
        return oneDayKrx()

@krxApi.route('/updateKrxData')
class updateKrxDataSelect(Resource):
    def get(self):
        '''금일 순자산 업데이트'''
        return updateKrxData()

@krxApi.route('/getPdfData')
class getPdfData(Resource):
    def get(slef):
        '''pdf 저장'''
        return portfolioDepositFile()

class MysqlController:
    def __init__(self,user,passwd,host,port,database,autocommit):
        self.conn = pymysql.connect(user=user, password=passwd, host=host,port=port,database=database,autocommit=autocommit)
        self.curs = self.conn.cursor()
        self.sql = ''

    def insertEtfData(self,data):
        sql = '''
        INSERT INTO daily_etf_adj (date,ticker_symbol,close_price,prepare,change_percent,net_asset_value,open_price,high_price,low_price,volume,trading_amount,market_cap,total_net_assts,stock_registration_count,base_index_name,base_close_price,base_prepare,base_change_percent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.curs.executemany(sql,data)
        self.conn.commit()

    def updateEtfData(self,totalNetAssts,tickerSymbol,date):
        sql = '''
        UPDATE daily_etf_adj SET total_net_assts = %s WHERE ticker_symbol = %s AND DATE = %s
        '''
        self.curs.execute(sql,(totalNetAssts,tickerSymbol,date))
        self.conn.commit()

    def insertPdfDate(self,data):
        sql = '''
        INSERT INTO etf_pdf_krx (date,etf_ticker_symbol,etf_ticker_name,isin_code,ticker_symbol,ticker_name,ticker_count,set_amount,composition_ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        self.curs.executemany(sql,data)
        self.conn.commit()

    def updateClass(self,one,two,three,symbor):
        sql = '''
        UPDATE  trade_item set asset_category_krx1 = %s,asset_category_krx2 = %s,asset_category_krx3 = %s WHERE ticker_symbol = %s
        '''
        self.curs.execute(sql,(one,two,three,symbor))
        self.conn.commit()

    def selectSymbol(self,tickerSymbol):
        sql = '''
        SELECT * FROM trade_item WHERE ticker_symbol = %s
        '''
        self.curs.execute(sql,(tickerSymbol))
        row = self.curs.fetchall()
        self.conn.commit()
        return row

def etfTitle():
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    
    headers = {
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'data.krx.co.kr',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
        'Content-Length' : '<calculated when request is sent>'
    }
    data = 'locale=ko_KR&mktsel=ETF&searchText=&bld=dbms%2Fcomm%2Ffinder%2Ffinder_secuprodisu'
    res = requests.post(url,headers=headers,data=data)
    krxResult = json.loads(res.text)['block1']
    length = len(krxResult)

    print('etfTitle 길이 : ' + str(length))
    krxList = []
    #종목 갯수
    for i in range(0,length):
        krxTitleInfomation = {
            'isinCode' : krxResult[i]['full_code'],
            'tickerSymbol' : krxResult[i]['short_code'],
            'tickerName' : krxResult[i]['codeName'],
        }
        krxList.append(krxTitleInfomation)

    return krxList


def etfInfo():
    date = datetime.now()
    print(date)
    yesterday = date- timedelta(days=2)
    date = str(date).split(' ')[0].replace('-','')
    yesterday = str(yesterday).split(' ')[0].replace('-','')
    krxData = etfTitle()
    postBodyList = []
    for i in krxData:
        tickerSymbol = i['tickerSymbol']
        tickerName = i['tickerName'].replace('%','')
        isinCode = i['isinCode']

        strtDd = yesterday

        endDd = date

        body = f'bld=dbms/MDC/STAT/standard/MDCSTAT04501&locale=ko_KR&tboxisuCd_finder_secuprodisu1_1={tickerSymbol}/{tickerName}&isuCd={isinCode}&isuCd2=&codeNmisuCd_finder_secuprodisu1_0={tickerName}&param1isuCd_finder_secuprodisu1_1=&strtDd={strtDd}&endDd={endDd}&share=1&money=1&csvxls_isNo=false'
        etfInfo ={
            'tickerSymbol' : tickerSymbol,
            'body' : body
        }
        postBodyList.append(etfInfo)
    return postBodyList




### 일일 ###
def oneDayKrx():
    try:
        # getPostCallApi('krx_oneDayKrx_시작')
        today = datetime.now()
        today = str(today).split(' ')[0].replace('-','')
        data = 'bld=dbms/MDC/STAT/standard/MDCSTAT04301&locale=ko_KR&trdDd={}&share=1&money=1&csvxls_isNo=false'.format(today)
        res = requests.post(baseUrl,headers=header,data=data)
        krxResult = json.loads(res.text)['output']
 
        length = len(krxResult)
        krxList= []
        for i in range(0,length):
            date = str(today).split(' ')[0]
            tickerSymbol = str(krxResult[i]['ISU_SRT_CD'])
            closePrice = float(krxResult[i]['TDD_CLSPRC'].replace(',',''))
            prepare = float(krxResult[i]['CMPPREVDD_PRC'].replace(',',''))
            changePercent = float(krxResult[i]['FLUC_RT'].replace(',',''))
            netAssetValue =float(krxResult[i]['NAV'].replace(',',''))
            openPrice = float(krxResult[i]['TDD_OPNPRC'].replace(',',''))
            highPrice = float(krxResult[i]['TDD_HGPRC'].replace(',',''))
            lowPrice = float(krxResult[i]['TDD_LWPRC'].replace(',',''))
            tradingVolume = float(krxResult[i]['ACC_TRDVOL'].replace(',',''))
            transactionAmount = float(krxResult[i]['ACC_TRDVAL'].replace(',',''))
            marketCap = float(krxResult[i]['MKTCAP'].replace(',',''))
            totalNetAssts = float(krxResult[i]['INVSTASST_NETASST_TOTAMT'].replace(',',''))
            stockRegistrationCount = float(krxResult[i]['LIST_SHRS'].replace(',',''))


            #기초 지수
            basicIndexIndexName = krxResult[i]['IDX_IND_NM']
            if krxResult[i]['OBJ_STKPRC_IDX'] == '':
                basicIndexClosePrice = 0
            else:
                basicIndexClosePrice = float(krxResult[i]['OBJ_STKPRC_IDX'].replace(',',''))

            if krxResult[i]['CMPPREVDD_IDX'] == '':
                basicIndexPrepare = 0
            else:
                basicIndexPrepare = float(krxResult[i]['CMPPREVDD_IDX'].replace(',',''))

            if krxResult[i]['FLUC_RT1'] == '':
                baseChangePercent = 0
            else:
                baseChangePercent = float(krxResult[i]['FLUC_RT1'].replace(',',''))

            krxTuple = (date,tickerSymbol,closePrice,prepare,changePercent,netAssetValue,openPrice,highPrice,lowPrice,tradingVolume,transactionAmount,marketCap,totalNetAssts,stockRegistrationCount,basicIndexIndexName,basicIndexClosePrice,basicIndexPrepare,baseChangePercent)
            krxList.append(krxTuple)
            
        # maria_controller = MysqlController('root','root','10.100.1.191',13306,'crawl_data',False)
        maria_controller = MysqlController('root','root','10.100.1.191',13306,'ra_data',False)
        #test용 db
        #maria_controller = MysqlController('root','airi1234','10.100.0.60',33306,'log_data',False)
        # getPostCallApi('krx_oneDayKrx_sql 접속 성공')
        maria_controller.insertEtfData(krxList)
        # getPostCallApi('krx_oneDayKrx_적재 완료')

        json_result = json.loads(json.dumps(krxList, default=str, ensure_ascii=False))
        # getPostCallApi('krx_oneDayKrx_크롤링 성공')
        return json_result
    except Exception as err:
        errorMessage = str(err)
        # getErrorCallApi('krx_oneDayKrx_error',errorMessage)


def updateKrxData():
    try:
        # getPostCallApi('krx_updateKrxData_시작')
        today = getDate()
        today = str(today).split(' ')[0].replace('-','')
        data = 'bld=dbms/MDC/STAT/standard/MDCSTAT04301&locale=ko_KR&trdDd={}&share=1&money=1&csvxls_isNo=false'.format(today)
        res = requests.post(baseUrl,headers=header,data=data)
        krxResult = json.loads(res.text)['output']
        
        length = len(krxResult)
        krxList= []
        maria_controller = MysqlController('root','root','10.100.1.191',13306,'ra_data',False)
        # getPostCallApi('krx_updateKrxData_sql 접속 성공')
        for i in range(0,length):
            date =  str(today).split(' ')[0]
            tickerSymbol = str(krxResult[i]['ISU_SRT_CD'])
            totalNetAssts = float(krxResult[i]['INVSTASST_NETASST_TOTAMT'].replace(',',''))
            
            krxTuple = (date,totalNetAssts,tickerSymbol)
            krxList.append(krxTuple)

            maria_controller.updateEtfData(totalNetAssts,tickerSymbol,date)
            # getPostCallApi('krx_updateKrxData_적재 완료')
        json_result = json.loads(json.dumps(krxList, default=str, ensure_ascii=False))

        return json_result

    except Exception as err:
        errMessage = str(err)
        # getErrorCallApi('krx_updateKrxData_error',errMessage)
    

def portfolioDepositFile():
    try:
        # getPostCallApi('krx_portfolioDepositFile_실행')
        info = etfTitle()
        today =getDate()
        # today = '20220401'
        today = str(today).split(' ')[0].replace('-','')
        pdfList =[]
        num = 1
        for i in info:
            # getPostCallApi('krx_portfolioDepositFile_'+ str(num)+'/'+str(len(info)))
            # print('for 문 : '+ str(num)+'/'+str(len(info)))
            num = num +1
            etfTickerSymbol = i['tickerSymbol']
            etfIsinCode = i['isinCode']
            etfTickerName = i['tickerName'].replace('%','')
            data = f'bld=dbms/MDC/STAT/standard/MDCSTAT05001&locale=ko_KR&tboxisuCd_finder_secuprodisu1_1={etfTickerSymbol}%2F{etfTickerName}&isuCd={etfIsinCode}&isuCd2={etfIsinCode}&codeNmisuCd_finder_secuprodisu1_1={etfTickerName}&param1isuCd_finder_secuprodisu1_1=&trdDd={today}&share=1&money=1&csvxls_isNo=false'.encode('utf-8')
            res = requests.post(baseUrl,headers=header,data=data)
            krxResult = json.loads(res.text)['output']
            length = len(krxResult)
            time.sleep(3)
            for l in range(0,length):
                date = str(today).split(' ')[0]
                # date = today
                tickerSymbol = str(krxResult[l]['COMPST_ISU_CD'])
                if len(tickerSymbol) == 6:
                    if tickerSymbol == '00088K':
                        isinCode = 'KR700088K015'
                    elif tickerSymbol == '00680K':
                        isinCode = 'KR700680K019'
                    elif tickerSymbol == '00104K':
                        isinCode = 'KR700104K010'
                    elif tickerSymbol == '02826K':
                        isinCode = 'KR702826K016'
                    elif tickerSymbol == '28513K':
                        isinCode = 'KR728513K010'
                    elif tickerSymbol == '00279K':
                        isinCode = 'KR700279K010'
                    else:    
                        isinCode = makeIsin(tickerSymbol)
                else:
                    isinCode = tickerSymbol


                tickerName = krxResult[l]['COMPST_ISU_NM']

                if krxResult[l]['COMPST_ISU_CU1_SHRS'] == '-':
                    tickerCount = 0
                else:
                    tickerCount = float(krxResult[l]['COMPST_ISU_CU1_SHRS'].replace(',',''))

                if krxResult[l]['COMPST_AMT'] == '-':
                    marketCap = 0
                else:
                    marketCap = float(krxResult[l]['COMPST_AMT'].replace(',',''))
                
                if krxResult[l]['COMPST_RTO'] == '-':
                    marketCapCompositionRatio = 0
                else:
                    marketCapCompositionRatio = float(krxResult[l]['COMPST_RTO'].replace(',',''))

                pdfData = (date,etfTickerSymbol,etfTickerName,isinCode,tickerSymbol,tickerName,tickerCount,marketCap,marketCapCompositionRatio)
                pdfList.append(pdfData)

        # getPostCallApi('krx_portfolioDepositFile_sql 접속전')
        maria_controller = MysqlController('root','root','10.100.1.191',13306,'ra_data',False)
        #test용 db
        # maria_controller = MysqlController('root','airi1234','10.100.0.60',33306,'log_data',False)
        # getPostCallApi('krx_portfolioDepositFile_sql 접속 성공')
        maria_controller.insertPdfDate(pdfList)
        # getPostCallApi('krx_portfolioDepositFile_적재 완료')

        json_result = json.loads(json.dumps(pdfList, default=str, ensure_ascii=False))

        return json_result
    except Exception as err:
        errorMessage = str(err)
        # getErrorCallApi('krx_portfolioDepositFile_error',errorMessage)


def makeIsin(ticker_symbol):
    try:
        result = 0
        for i in range(0, len(ticker_symbol)):
            temp = int(ticker_symbol[i]) * (1 + (i % 2))
            if temp >= 10:
                result += int(temp / 10)
                result += int(temp % 10)
            else:
                result += temp
        result += 20
        return f'KR7{ticker_symbol}00{(10 - result % 10) % 10}'
    except Exception as err:
        errMessage = str(err)
        # getErrorCallApi('krx_makeIsin_error',errMessage)
        




def getDate():
    # getPostCallApi('krx_getDate_실행')
    date = datetime.now() - timedelta(days=1)
    yes = str(date).split(' ')[0]
    format = '%Y-%m-%d'
    date = datetime.strptime(yes,format)

    dayNumber = date.weekday()
    if dayNumber == 6:
        date = datetime.now() - timedelta(days=3)
    date = str(date).split(' ')[0]
    return date

def krxclass():
    krxData = etfTitle()
    # maria_controller = MysqlController('root','airi1234','10.100.0.60',33306,'log_data',False)
    maria_controller = MysqlController('root','root','10.100.1.191',13306,'ra_data',False)
    num = 1
    for i in krxData:
        # print(str(num) +'/' + str(len(krxData)))
        tickerSymbol = i['tickerSymbol']
        tickerName = i['tickerName'].replace('%','')
        isinCode = i['isinCode']
        selectData = maria_controller.selectSymbol(tickerSymbol)

        if len(selectData) != 0:
            dbIscode = selectData[0][1]
        
            data ='bld=dbms/MDC/STAT/standard/MDCSTAT04704&locale=ko_KR&tboxisuCd_finder_secuprodisu1_1={}/{}&isuCd={}&isuCd2={}&codeNmisuCd_finder_secuprodisu1_1={}&param1isuCd_finder_secuprodisu1_1=&csvxls_isNo=false'.format(tickerSymbol,tickerName,isinCode,isinCode,tickerName).encode('utf-8')
            time.sleep(2)
            res = requests.post(url=baseUrl,headers=header,data=data)
            krxClass = json.loads(res.text) 
            Iscode = krxClass['ISU_CD']
            assectClassification = krxClass['IDX_ASST_CLSS_NM']

            if dbIscode == Iscode:            
                
                splitData = assectClassification.split('-')
                
                if len(splitData) == 3:
                    oneData = splitData[0]
                    twoData = splitData[1]
                    threeData = splitData[2]
                    maria_controller.updateClass(oneData,twoData,threeData,tickerSymbol)
                
                if len(splitData) == 2:
                    oneData = splitData[0]
                    twoData = splitData[1]
                    threeData = ''
                    maria_controller.updateClass(oneData,twoData,threeData,tickerSymbol)

                if len(splitData) == 1:
                    oneData = splitData[0]
                    twoData = ''
                    threeData = ''
                    maria_controller.updateClass(oneData,twoData,threeData,tickerSymbol)    
        else :
            print('pass')
            pass
            