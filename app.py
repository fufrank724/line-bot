# encoding: utf-8
from flask import Flask, request, abort

from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
)
from datetime import datetime

from urllib.request import urlopen

from bs4 import BeautifulSoup
import db_operate_postgres as postgres
import _thread,time,json
from threading import Thread

app = Flask(__name__)

line_bot_api = LineBotApi('JJpLzJzbeiTxwEduA1dlKgOp1VNUGm2dMUhBlSWDr/ttQLmb/6RGaNvxPsnl3xwhQgZSxdwmosB0hnzCzFeoC3rlaVA1HE2R0n5Iw5PS0kmq26OwzLNQU9S8296C/4hwmPfxvTMmfKf03wbaY1/KcAdB04t89/1O/w1cDnyilFU=') #Your Channel Access Token
handler = WebhookHandler('a3184048d5d74fc0c724d46839623d6e') #Your Channel Secret
db=postgres.db_postgres()

strategy_operation=False
pushMessage_operation=False

_strategy_thread=None
_pushMessage_thread=None
        


@app.route('/')
def homepage():
    the_time = datetime.now().strftime("%A, %d %b %Y %l:%M %p")

    return """
    <h1>Hello heroku</h1>
    <p>It is currently {time}.</p>
    <img src="http://loremflickr.com/600/400" />
    """.format(time=the_time)

@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'OK'

                       
@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    text = event.message.text #message from user
    replytext=text
    global strategy_operation
    global pushMessage_operation
    global _strategy_thread
    global _pushMessage_thread
    if text==u"列出所有功能":
        replytext="輸入1 紀錄使用者資訊\n輸入2 查詢當前大台資訊\n輸入3 啟用策略提醒(僅有部分功能)\n\
輸入4 停用策略提醒(僅有部分功能)\n輸入5 啟用報價功能\n輸入6 停用報價功能\n輸入7 檢查所有功能狀態"
    elif text==u"1":
        replytext=db.insert_userID(event.source.user_id)

    elif text==u"2":
        #info=return_stockinfo()
        db.return_stockinfo()
        replytext=u"大台點數 "+db.price+u"\n交易量 "+str(db.vol)+u"\n運算時間："+str(db.execute_time)
        
    elif text==u"3":        
        if(_strategy_thread==None):
            replytext=u"啟用策略提醒"
            strategy_operation=True
            _strategy_thread=Thread(target=strategy)
            _strategy_thread.start()
        else:
            replytext=u"策略提醒已啟用"
            
    elif text==u"4":
        replytext=u"停用策略提醒"
        strategy_operation=False
        
    elif text==u"5":        
        if (_pushMessage_thread==None):
            pushMessage_operation=True
            replytext=u"啟用報價功能"
            _pushMessage_thread=Thread(target=push_info)
            _pushMessage_thread.start()
        else:
            replytext=u"報價功能已啟用"
            
    elif text==u"6":
        replytext=u"停用報價功能"
        pushMessage_operation=False
        
    elif text==u"7":
        if(strategy_operation):
            replytext=u"策略提醒為啟動狀態"
        else:
            replytext=u"策略提醒為停用狀態"
        if(pushMessage_operation):
            replytext+=u"\n台指推播為啟動狀態"
        else:
            replytext+=u"\n台指推播為停用狀態"
        if(db.isAlive()):
            replytext+=u"\n資料庫為啟動狀態"
        else:
            replytext+=u"\n資料庫為停用狀態"

    elif text==u"啟用資料庫":
        if(not db.isAlive()):
            db.start()
            replytext=u"已啟用資料庫"
        else:
            replytext=u"資料庫已經啟用"
    elif text==u"資料庫狀態":
        replytext=str(db.isAlive())
        
    elif text==u"測試推播功能":
        i=0
        replytext=u"測試推播"
        while(i<len(db.userID)):
            line_bot_api.push_message(db.userID[i],TextSendMessage(text=replytext))
            i+=1

    else:
        replytext=u"本機器人不提供聊天功能，若要需要協助 請輸入「列出所有功能」"
      
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=replytext)) #reply the same message from user

def time_caculate():
    now=time.strftime('%H:%M', time.localtime(time.time()))
    n = [int(x) for x in now.split(':')]
    now=100*n[0]+n[1]+800-1
    return now
    
def strategy():
    global strategy_operation
    notify=False
    while(strategy_operation):
        now=time_caculate()
        
        if(now in db.timeList):
            db.return_stockinfo()
            if (int(db.vol/1000)>2*int(db.vol_avg/1000)):
                replytext=u"交易量已超過2倍量\n"+u"交易量："+str(db.vol)+u"\n交易量平均值："+str(db.vol_avg)                    
                i=0
                while(i<len(db.userID) and not notify):
                    line_bot_api.push_message(db.userID[0],TextSendMessage(text=replytext))
                    i+=1
                notify=True
        else:
            notify= False
        #time.sleep(60)
        #now=time_caculate()
        
def push_info():
    global pushMessage_operation
    notify=False
    while(pushMessage_operation):
        i=0
        now=time_caculate()
       
        if (now in db.timeList):
            #info=return_stockinfo()
            db.return_stockinfo()
            replytext=u"當前點數： "+db.price+u"\n當前交易量： "+str(db.vol)+"\n前一個交易平均值："+str(db.vol_avg)
            #while(i<len(db.userID)):
            while(i<1)and (not notify):
                line_bot_api.push_message(db.userID[0],TextSendMessage(text=replytext))
                i+=1 
            notify=True
        else:
            notify=False
        #time.sleep(60)
    
def return_stockinfo():

    TXF_NAME = u'臺指期077'
    targets = set()
    targets.add(TXF_NAME)
    quotes = dict()
    url = 'http://info512.taifex.com.tw/Future/FusaQuote_Norl.aspx'
    with urlopen(url) as response:
        html_data = response.read()
    soup = BeautifulSoup(html_data, 'html.parser')
    trade_price=""

    rows = soup.find_all('tr', {"class": "custDataGridRow", "bgcolor": "#DADBF7"})

    for row in rows:
        # print(row)
        items = row.find_all('td')
        name = items[0].a.text.strip()
        
        if name in targets:
            trade_price = float(items[6].font.text.replace(',', ''))

    return str(trade_price)
        
if __name__ == "__main__":
    #app.run(debug=True, use_reloader=True)

    try:
        db.start()
        print("DB start!!")
    except:
        print("DB start failed!!")
    app.run()
