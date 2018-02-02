from urllib.request import urlopen
from bs4 import BeautifulSoup
from urllib import parse 
import psycopg2,threading,time,itertools

class db_postgres:
    def __init__(self):
            """         set      variables         """
            self.timeList=[945,1045,1145,1245,1345]
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
            self.sql_read = 'Select "volume" FROM public."VolumeData" where "Id" < 60;'
            self.sql_update_1 ='update public."VolumeData" set "volume"=%s where "Id" = %s;'
            self.sql_Insert_user ='Insert INTO public."user"("userID") values(%s) ;'
            self.sql_search_user='select "userID" from public."user" where "userID"=%s;'

            self.db_read()
    def return_stockinfo(self):

        TXF_NAME = u'臺指期067'
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
                trade_price=float(items[6].font.text.replace(',', ''))
                trade_volume = float(items[9].font.text.replace(',', ''))
        self.vol=trade_volume-self.vol_pre
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
            self.db.commit()
            return u"已增加使用者"
        else:
            return u"已存在該使用者"
            

    def db_operate(self):
    #   0~59儲存交易量
    #   70儲存當前交易量
    #   71儲存volume average
        print("DB operate!")
        
        while(self.db_operation):
            #time operate
            now=time.strftime('%H:%M', time.localtime(time.time()))
            n = [int(x) for x in now.split(':')]
            now=100*n[0]+n[1]
            self.cursor.execute(self.sql_read)
            self.vol_list=self.convert(self.cursor.fetchall())
            print(time in self.timeList)
            #logic operate
            
            if(time in self.timeList): 
                self.vol_now=self.return_stockinfo()
                if (self.vol_now != self.vol_pre):
                    self.vol=self.vol_now-self.vol_pre
                    
                    if(self.i<60):                        
                        self.vol_list[i]=self.vol
                        self.cursor.execute(self.sql_update_1,(self.vol,self.i))
                        self.cursor.execute(self.sql_update_1,(self.vol,70))        
                        self.cursor.execute(self.sql_update_1,(self.vol_avg,71))                        
                        self.db.commit()  
                    else:
                        self.i=0
                        self.vol_list[i]=self.vol
                        self.cursor.execute(self.sql_update_1,(self.vol,self.i))
                        self.cursor.execute(self.sql_update_1,(self.vol,70))
                        self.cursor.execute(self.sql_update_1,(self.vol_avg,71))
                        self.db.commit()
                    self.i+=1                
                    self.vol_pre=self.vol_now
            print("DB is operating")
            time.sleep(5)
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
    def db_read(self):
        self.cursor.execute(self.sql_read)
        self.vol_list=self.convert(self.cursor.fetchall())
        self.vol_avg=sum(self.vol_list)/60
    def db_close(self):
        self.db.close()