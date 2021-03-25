import numpy as np
import time
import random
import sqlalchemy as db
import pandas as pd
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import json
conn = create_engine('mssql+pymssql://vnarang:%F;=!4hMpGMkpwYm/u7H@HEALTHSQL2:1433/HCS').connect()
#Get data
SQL= r"SELECT convert(date,eclrs_create_date) as opened_date,address FROM [HCS].[dbo].[CC_Data] where patient_type = 'confirmed' and address is not null order by convert(date,eclrs_create_date) desc"
#Run query
df = pd.read_sql_query(SQL, conn)
#x = df['Street'] +' '+df['City']+' '+df['Zipcode']+' '+df['State']
x = df['address']
x = list(x)
score=0
ind=0
geocode=[]
scr=[]
loc = []
sd= []
pipd=[]
for r in range(len(x)):
    my_dict = {'SingleLine' :x[r],'matchOutOfRange':'true','f':'pjson',}
    url_base = 'https://gis.suffolkcountyny.gov/server/rest/services/Locators/AddressMultiRole/GeocodeServer/findAddressCandidates'
    page = requests.get(url_base,params=my_dict)
    soup = BeautifulSoup(page.content, 'html.parser')
    soup = json.loads(str(soup))
    for k,v in soup.items():
        if k == 'candidates':
            if not v:
                geocode.append(x[r])
                scr.append(0)
                loc.append(0)
                sd.append(0)
                pipd.append(0)
            else:
                for i in range(len(v)):
                    for m,n in v[i].items():
                        if m == 'score':
                            if score < n:
                                score=n
                                ind=i
                            else:
                                i+1
                for key,value in v[ind].items():
                    if key == 'address':
                        geocode.append(value)
                    elif key == 'score':
                        scr.append(value)
                    elif key == 'location':
                        loc.append(value)
                        abc = str(value)
                        my_dict = {'geometry':abc,'geometryType':'esriGeometryPoint'
                                   ,'spatialRel':'esriSpatialRelIntersects','f':'pjson'}
                        url_base = 'https://gis.suffolkcountyny.gov/server/rest/services/LocalGovernmentSQLData/SanitationDistrictPolygon/FeatureServer/0/query'
                        page = requests.get(url_base,params=my_dict)
                        soup = BeautifulSoup(page.content, 'html.parser')
                        soup = json.loads(str(soup))
                        for k,v in soup.items():
                            if k == 'features':
                                if v != []:
                                    for key,values in soup.items():
                                        if key == 'features':
                                            for i,j in values[0].items():
                                                if i == 'attributes':
                                                    sd.append(j.values())
                                else:
                                    sd.append(0)
                        
                        
                        my_dict = {'geometry':abc,'geometryType':'esriGeometryPoint'
                                    ,'spatialRel':'esriSpatialRelIntersects','f':'pjson','returnGeometry':'false'}
                        url_base = 'https://gis.suffolkcountyny.gov/hosted/rest/services/Hosted/PumpStationPolygon_Covid19/FeatureServer/0/query'
                        page = requests.get(url_base,params=my_dict)
                        soup = BeautifulSoup(page.content, 'html.parser')
                        soup = json.loads(str(soup))
                        for k,v in soup.items():
                            if k == 'features':
                                if v != []:
                                    for key,values in soup.items():
                                        if key == 'features':
                                            for i,j in values[0].items():
                                                if i == 'attributes':
                                                    pipd.append(j.values())
                                else:
                                    pipd.append(0)
 
    d = {'Geo_coded_address':geocode,'Score':scr,'Location':loc,'Sewer_District':sd,'Pipe_District':pipd}
new_df = pd.DataFrame(data=d)
new_df
