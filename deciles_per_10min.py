
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pytz


# In[ ]:


data=pd.read_csv("access-beton_sf_prod-api.log", sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', 
                 engine='python', na_values=['-'],header=None,usecols=[0, 3, 4, 5, 6, 7, 8,10],names=
                 ['ip', 'time', 'request', 'status', 'size', 'referer', 'user_agent','req_time'], 
                 converters={'status': int, 'size': int,'req_time': int})
data


# In[ ]:


def read_hour(date):
    hour=float(date[13:15])
    minutes=float(date[16:18])
    return hour+minutes/100


# In[ ]:


def segment_time(date,tic):
    #renvoie le début de la tranche de tic minutes correspondant à l'heure date, 
    # par exemple, avec tic = 10, et date=[19/Sep/2018:14:59:03 +0200], le résultat, qui correspond à 14h50, sera 14.5
    # les segments sont comptés à partir de 00h00, ainsi avec un tic=25, le résultat pour l'heure 01h00 sera 0.5 (ie 0h50)
    hour=int(date[13:15])
    minutes=int(date[16:18])
    time_in_minutes=60*hour+minutes
    tic_start=time_in_minutes - time_in_minutes % tic    
    return tic_start // 60 + float(tic_start % 60)/100


# In[ ]:


data['req_time']=data['req_time'].apply(lambda x: x / 1000)


# In[ ]:


data


# In[ ]:


def time_to_minutes(x):
    return int(x[13:15])*60+int(x[16:18])

def time_segment(x,tic):
    return time_to_minutes(x)//tic

data['tics']=data['time'].apply(lambda x : time_segment(x,10))
data


# In[ ]:


deciles_by_tics=pd.DataFrame(columns=range(38,91))
deciles_by_tics


# In[ ]:


def deciles(col):
    return data[data['tics']==col]['req_time'].quantile([i/10 for i in range(10)])


# In[ ]:


for c in range(38,91):
    deciles_by_tics[c]=deciles(c)
    
deciles_by_tics


# In[ ]:


#exprimons les déciles en fonction des tics et non l'inverse :
deciles_by_tics=deciles_by_tics.T
#puis remettons les tics sous un format lisible :
deciles_by_tics.index=deciles_by_tics.index.map(lambda x:(x*10)//60 + (10*x%60)/100)


# In[ ]:


deciles_by_tics.plot(kind='bar',legend=True)


# In[ ]:


#à part pendant le pic évoqué, 80% des requêtes durent moins de 0.5s


# In[ ]:


# lorsqu'on empile les barres, ça ne va pas car les centiles se cumulent :
deciles_by_tics.plot(kind='bar',stacked=True)


# In[ ]:


#ça ne va pas car les déciles sont additionnés au lieu de se superposer...


# In[ ]:


#finallement, je trouve le plot de base plus lisible :
deciles_by_tics.plot()


# In[ ]:


deciles_by_tics

