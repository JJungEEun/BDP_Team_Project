#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import csv
from bs4 import BeautifulSoup
fdata=[]
# 1) reqeusts 라이브러리를 활용한 HTML 페이지 요청 
# 1-1) res 객체에 HTML 데이터가 저장되고, res.content로 데이터를 추출할 수 있음

year = input('년도를 입력하세요')
res3 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/3/Historical')
res0 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/0/Historical')
res1 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/1/Historical')
res2 = requests.get('https://weatherspark.com/h/s/26197/'+year+'/2/Historical')
# 3 winter 1 2 12 전처리할때 주의.
# 0 spring 345
# 1 summer 678
# 2 fall 9 10 11
soup = BeautifulSoup(res3.content, 'html.parser')
data = soup.select_one("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody  ")
for n in data:
    fdata.append(n.text.strip())
    
soup = BeautifulSoup(res0.content, 'html.parser')
data = soup.select_one("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody  ")
for n in data:
    fdata.append(n.text.strip())

soup = BeautifulSoup(res1.content, 'html.parser')
data = soup.select_one("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody  ")
for n in data:
    fdata.append(n.text.strip())
    
soup = BeautifulSoup(res2.content, 'html.parser')
data = soup.select_one("#Report-Content > div:nth-child(19) > div.flex_center > table > tbody  ")
for n in data:
    fdata.append(n.text.strip())
fdata = list(filter(None, fdata))

print(fdata)

with open(year+"weather.csv",'w',newline='') as f:
    writer = csv.writer(f)
    writer.writerow(fdata)

