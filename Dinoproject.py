import json
from fileinput import filename

import pandas as pd
import requests as r
import re
from io import StringIO
from bs4 import BeautifulSoup # read all datas from web page stores in structred format for processing
url = "https://en.wikipedia.org/wiki/List_of_dinosaur_genera"
headers = { # this line for Credential purpose for Accessing Data from URL
    "User-Agent": "SaroStockAnalysisBot/1.0 (https://example.com/contact)"
}
html = r.get(url, headers=headers)
#print(html.text)
soup = BeautifulSoup(html.text,'html.parser')#web page datas cannot procesed directly,
#print(soup)#we use soup to convert HTML code to Structured format to access elements
urls=soup.find_all('a', href=True) # 'a'-refers to anchor tag <a>, finding all <a href>
#print(urls)
links_and_names=[(url['href'],url.text)for url in urls] #getting all links and text
#print(len(links_and_names),"****",links_and_names)
dino_data_clean = [links_and_names[link] for link in range(len(links_and_names)) if links_and_names[link][0].startswith("/wiki/")] #olny getting link and text with links starts with /wiki/
dino_data_clean=dino_data_clean[:2370:]
#print(len(dino_data_clean),"_____________",dino_data_clean)
dino_df=pd.DataFrame(dino_data_clean,columns=['url','dinosaur'])
#print(dino_df.head())
dino_df['dinosaur']=dino_df['dinosaur'].replace('',None)#remove empty spaces
dino_df=dino_df.dropna(axis=0,subset=['dinosaur']) # important interview qns handle null values in python -dropna
dino_data_clean=dino_df.set_index('url')['dinosaur'].to_dict() #set url as index and dinosaur column as values and convert them into dictionary
#print(list(dino_data_clean.keys())[0:5])
dino_data=[('https://en.wikipedia.org'+url,dinosaur) for url,dinosaur in dino_data_clean.items()] #.items() usesd to get Key+values in dictionary
#print((dino_data)[0:5])
dino_data=dino_data[53::]
#print((dino_data)[0:2])
dino_urls=[ ele for pair in dino_data for ele in pair if ele.startswith("https://en.wikipedia.org")]# printing only urls excluding name
#print((dino_urls)[0:2])
dino_info=[]
for url in range(200):
    html=r.get(dino_urls[url],headers=headers,timeout=120)
    soup=BeautifulSoup(html.text, 'html.parser')
    para=soup.find_all('p')
    clean_para=[paragraph.text.strip() for paragraph in para]
    clean_para=clean_para[0:4]
    dino_info.append(' '.join(clean_para))
    #print(dino_info)
dino_df=pd.DataFrame(dino_data,columns=['URL','Dinosaur'])
print(dino_df)
dino_details=pd.DataFrame(dino_info,columns=['info'])
dino_df=pd.concat([dino_df,dino_details],axis=1)
#print(dino_df)
filename=r'C:\Users\rd070\OneDrive\Desktop\Data Engineering\Dino project\dino_webscrap1.csv'
dino_df.to_csv(filename,index=False)
print("Partial POC saved to Folder Successfully")
dino_info=dino_df['info'].to_dict()
dino_info=dino_info.values()

heights=[]
for ele in dino_info:
    height=re.findall(r'\d+\smeters', str(ele))#d=num,s=space,m=meters
    if height:
        heights.append(height)
    else:
        heights.append('-')

weights=[]
for ele in dino_info:
    weight= re.findall(r'\d+\s(?:tonnes|kilograms)', str(ele))
    if weight:
        weights.append(weight)
    else:
        weights.append('-')

dino_df.drop('info',axis=1,inplace=True)
height_df=pd.DataFrame(heights,columns=['Height'])
weight_df=pd.DataFrame(weights,columns=['Weight'])
dino_df=pd.concat([dino_df,height_df,weight_df],axis=1)
dino_df.columns=['URL','Dinosaur','Height','Weight']
dino_df.to_csv(r'C:\Users\rd070\OneDrive\Desktop\Data Engineering\Dino project\DinoPocWebScrape.csv' ,index=False)
print("Dinosaur POC saved to Folder Successfully")