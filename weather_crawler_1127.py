#!/usr/bin/env python
# coding: utf-8

# In[4]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
date=[]
weather=[]
# 1) reqeusts 라이브러리를 활용한 HTML 페이지 요청 
# 1-1) res 객체에 HTML 데이터가 저장되고, res.content로 데이터를 추출할 수 있음

year = input('년도를 입력하세요')
res3 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/3/Historical')
res0 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/0/Historical')
res1 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/1/Historical')
res2 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/2/Historical')

soup = BeautifulSoup(res3.content, 'html.parser')
data = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td > a ")
ww = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
for n in data:
    date.append(n.text.strip())
for n in ww:
    weather.append(n.text.strip())
    
soup = BeautifulSoup(res0.content, 'html.parser')
data = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td > a ")
ww = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
for n in data:
    date.append(n.text.strip())
for n in ww:
    weather.append(n.text.strip())
    
soup = BeautifulSoup(res1.content, 'html.parser')
data = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td > a ")
ww = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
for n in data:
    date.append(n.text.strip())
for n in ww:
    weather.append(n.text.strip())
    
soup = BeautifulSoup(res2.content, 'html.parser')
data = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td > a ")
ww = soup.select("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
for n in data:
    date.append(n.text.strip())
for n in ww:
    weather.append(n.text.strip())
    
DF=pd.DataFrame(zip(date, weather))
print(DF)

DF.to_csv(year+"weather.csv", mode='w')

