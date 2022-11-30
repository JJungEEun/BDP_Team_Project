#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
date=[]
weather=[]
portl=[]
yearl=[]

local=['146640', '146513', '145689', '145521', '145920', '145341', '145433', '146721', '146042', '145529']
year = input('년도를 입력하세요')

for i in local:
    res3 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/3/Historical')
    res0 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/0/Historical')
    res1 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/1/Historical')
    res2 = requests.get('https://weatherspark.com/h/s/'+i+'/'+year+'/2/Historical')
    
    soup = BeautifulSoup(res3.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
    for n in ww:
        weather.append(n.text.strip())
    
    soup = BeautifulSoup(res0.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
    for n in ww:
        weather.append(n.text.strip())

    soup = BeautifulSoup(res1.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
    for n in ww:
        weather.append(n.text.strip())

    soup = BeautifulSoup(res2.content, 'html.parser')
    data = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td > a ")
    ww = soup.select("#Report-Content > div:nth-child(18) > div.flex_center > table > tbody > tr > td:nth-child(2) ")
    for n in data:
        date.append(n.text.strip())
    for n in ww:
        weather.append(n.text.strip())
        
DF=pd.DataFrame(zip(date, weather))
print(DF)

DF.to_csv(year+"weather.csv", mode='w')

