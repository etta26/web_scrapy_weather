import urllib.request as req
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np 
import time
from datetime import timedelta
import pymysql
import getpass
import os
import sqlalchemy
from sqlalchemy import create_engine

passwd=getpass.getpass ("密碼:")
con = pymysql.connect(host='localhost', user='root', port=3306, password = passwd, database= 'data',charset='utf8')
cursor = con.cursor()
engine = create_engine('mysql+pymysql://root:user@localhost:3306/data')
con = engine.connect()

dates = pd.date_range(start='2018-01-01', end='2018-12-31', freq='D')
d = pd.DataFrame()
for dt in dates:
    url ='https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=466920&stname=%25E8%2587%25BA%25E5%258C%2597&datepicker='+dt.strftime('%Y-%m-%d')
    request = req.Request(url, headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'} )
    with req.urlopen(url) as response:
        data = response.read().decode('utf-8')
        soup = BeautifulSoup(data, 'html.parser')
        #每一日天氣溫度
        rows = soup.find('tbody').find_all('td')
        rows=[i.text.strip('\xa0') for i in rows]
        step = 17
        a = [rows[i:i+step] for i in range(0,len(rows),step) ]
        a = pd.DataFrame(a)
       
        
        ObsTime = []
        for hr in a[0]:
            if (hr <'10'):
                hr = str(hr)
            else:
                hr = str(hr)
            ObsTime.append(str(hr)+':'+'00:00')
        b = pd.Series(ObsTime)
        #將 24:00 改成 00:00
        b[23] = '00:00:00'          
        a['Obsdate'] = dt
        
        a.columns = ['obsTime','StnPres','SeaPres','Temp','Td dew','RH','WS','WD','WSGust','WDGust','Precp','PrecpHour','Sun','GloblRad','Visb','UVI','Cloud','Obsdate']
        a = a.rename(columns = {'0':'obsTime','1':'StnPres','2':'SeaPres','3':'Temp','4':'Td dew','5':'RH','6':'WS','7':'WD','8':'WSGust','9':'WDGust','10':'Precp','11':'PrecpHour','12':'Sun','13':'GloblRad','14':'Visb','15':'UVI','16':'Cloud'})
        a.drop(['obsTime'], axis = 1, inplace = True)
        
    
        c = pd.concat([a,b], axis = 1 )
        c = c.rename(columns = {'0':'ObsTime'})
        c.columns = ['StnPres','SeaPres','Temp','Td dew','RH','WS','WD','WSGust','WDGust','Precp','PrecpHour','Sun','GloblRad','Visb','UVI','Cloud','Obsdate','ObsTime']
        #凌晨12:00 要加一天
        c.iloc[23,16] = dt + 1
        c['Obsdate'] = pd.to_datetime(c['Obsdate'],format='%Y-%m-%d').dt.date 
        print(c.iloc[23,16])
        
    d = pd.concat([c,d], axis = 0, sort=False)
    d = d[['Obsdate','ObsTime','Temp','WS','WSGust','Precp','UVI','Cloud']]
    d = d.fillna(0) 
    d = d.replace({'T':0,'...':0, 'X':0})
    
#d.to_csv('weather2018.csv',index=False)
d.to_sql(name = 'weather2018', con=con, index= False)

cursor.close()
con.close()