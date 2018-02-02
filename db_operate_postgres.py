from urllib.request import urlopen
from bs4 import BeautifulSoup
from urllib import parse 
import psycopg2,threading,time,itertools
import time

class db_postgres(threading.Thread):
    def __init__(self):
            """         set      variables         """
            self.timeList=[945,1045,1145,1245,1345]
            self.price=""
            self.vol_pre=0
            self.vol_now=0
            self.vol_list=[]
            self.vol_avg=0
            self.vol=0
            self.i=0
            self.db_operation=True
            self.result = parse.urlparse("postgres://pbzrozafoxvjfe:98eff84c7ec963ae980590a194a07cedad1a2f8ee7f44f214f29f35b99f91f95@ec2-54-225-68-71.compute-1.amazonaws.com:5432/dmuphgq6at68q")
            self.db = psycopg2.connect(
                        database = self.result.path[1:],
                        user = self.result.username,
                        password = self.result.password,
                        host = self.result.hostname
                    )
            self.cursor = self.db.cursor()
            threading.Thread.__init__(self)
            self.sql_read = 'Select "volume" FROM public."VolumeData" where "Id" < 60;'
            self.sql_update_1 ='update public."VolumeData" set "volume"=%s where "Id" = %s;'
            self.sql_Insert_user ='Insert INTO public."user"("userID") values(%s) ;'
            self.sql_search_user='select "userID" from public."user" where "userID"=%s;'
            self.sql_fetch_user='select "userID" from public."user" ;'
            self.db_read()
            self.userID=[]
            self.fetch_userID()
            self.execute_time=0
    def return_stockinfo(self):
        start_time = time.time()
        TXF_NAME = u'臺指期087'
        targets = set()
        targets.add(TXF_NAME)

        quotes = dict()

        url = 'http://info512.taifex.com.tw/Future/FusaQuote_Norl.aspx'
        with urlopen(url) as response:
            html_data = response.read()
        soup = BeautifulSoup(html_data, 'html.parser')
        trade_price=0
        trade_volume=0

        rows = soup.find_all('tr', {"class": "custDataGridRow", "bgcolor": "#DADBF7"})

        for row in rows:
            # print(row)
            items = row.find_all('td')
            name = items[0].a.text.strip()            
            if name in targets:
                trade_price=float(items[6].font.text.replace(',', ''))
                trade_volume = float(items[9].font.text.replace(',', ''))
        
        self.vol=trade_volume-self.vol_pre
        self.price=str(trade_price)
        self.execute_time=(time.time() - start_time)
        return trade_volume
       
    def convert(self,Tuple):
        new=[list(item) for item in Tuple]
        data = list(itertools.chain.from_iterable(new))
        return data
        
    def insert_userID(self,ID):
        self.cursor.execute(self.sql_search_user,(ID,))
        user=self.cursor.fetchone()
        if(user==None):
            self.cursor.execute(self.sql_Insert_user,(ID,))
            self.userID.append(ID)
            self.db.commit()
            return u"已增加使用者"
        else:
            return u"已存在該使用者"
            
    def fetch_userID(self):
        self.cursor.execute(self.sql_fetch_user)
        self.userID=self.convert(self.cursor.fetchall())

    def db_operate(self):
    #   0~59儲存交易量
    #   70儲存當前交易量
    #   71儲存volume average
        i=0
        _operate=False
        while(self.db_operation):
            #time operate
            now=time.strftime('%H:%M', time.localtime(time.time()))
            n = [int(x) for x in now.split(':')]
            now=100*n[0]+n[1]+800
            #self.cursor.execute(self.sql_read)
            #self.vol_list=self.convert(self.cursor.fetchall())
            
            #logic operate
            
            if(now in self.timeList)and (not _operate): 
                self.vol_now=self.return_stockinfo()                
                if (self.vol_now != self.vol_pre):
                    self.vol=self.vol_now-self.vol_pre
                    self.vol_list.pop(0)
                    self.vol_list.append(self.vol)
                    self.vol_avg=int(sum(self.vol_list)/60)                                        
                    self.vol_pre=self.vol_now
                operate=True
            else:
                _operate=False
                
                    
            if now==1346 :
                for index,data in enumerate(self.vol_list):
                    self.cursor.execute(self.sql_update_1,(data,index))
                self.cursor.execute(self.sql_update_1,(self.vol,70))
                self.cursor.execute(self.sql_update_1,(self.vol_avg,71))
                self.db.commit()
            #time.sleep(120)
            time.sleep(60)
        """     
        #testing code
        self.vol_now=self.return_stockinfo()
        if (self.vol_now!= self.vol_pre):
            vol=self.vol_now-self.vol_pre
            if(i<60):
                self.cursor.execute(self.sql_update_1,(vol,i))
                self.cursor.execute(self.sql_update_2,(vol,70))
                self.db.commit()  
            else:
                i=0
                self.cursor.execute(self.sql_update_1,(vol,i))
                self.cursor.execute(self.sql_update_2,(vol,70))

                self.db.commit()
            i+=1
        """
    def run (self):
          self.db_operate()
          
    def db_read(self):
        self.cursor.execute(self.sql_read)
        self.vol_list=self.convert(self.cursor.fetchall())
        self.vol_avg=sum(self.vol_list)/60
        
    def db_close(self):
        self.db.close()