from urllib import parse # import urllib.parse for python 3+
import psycopg2
import _thread,itertools,time
result = parse.urlparse("postgres://pbzrozafoxvjfe:98eff84c7ec963ae980590a194a07cedad1a2f8ee7f44f214f29f35b99f91f95@ec2-54-225-68-71.compute-1.amazonaws.com:5432/dmuphgq6at68q")

db = psycopg2.connect(
    database = result.path[1:],
    user = result.username,
    password = result.password,
    host = result.hostname
) 
print("db connect!")
def test():
    return "test"
cursor = db.cursor()
sql_update_1 ='update public."VolumeData" set "volume"=%s where "Id" =%s;'
sql_Insert_1 ='Insert INTO public."VolumeData"("volume","Id") values(0,71) '
sql_Insert_2 ='Insert INTO public."userID"("userID") values(%s) '
search='select "userID" from public."user"'

"""             vol in 2017/07/19           """

vol=[8972,12697,8263,59109,17538,5848,15161,14508,7430,45680,
     29025,8292,10487,10413,11824,58868,22349,13865,8473,24083,8333,57052,14566,
     11533,8470,12951,10806,56997,36981,14375,5193,16402,6518,33983,15690,5476,4098,
     9763,10337,50069,21704,9112,4853,13705,5864,33047,10922,8909,6794,13809,6712,31000,
     14121,9028,18541,9995,44452,14151,5974,8791,10979]
sql_read = 'Select "volume" FROM public."VolumeData" where "Id" < 60;'
i=0


while(i<60):
    cursor.execute(sql_update_1,(vol[i],i))
    i+=1
avg=sum(vol)/60
cursor.execute(sql_update_1,(avg,71))
print("execute!")

"""
cursor.execute(sql_read)
data=cursor.fetchall()
new=[list(item) for item in data]
data = list(itertools.chain.from_iterable(new))
for index,item in enumerate(vol):
    if(data[index]!=item):
        print(index)
print(data)
"""
db.commit()
db.close()
print("done!!")