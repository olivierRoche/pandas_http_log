
# coding: utf-8


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pytz


def load_log(log_name):
    data=pd.read_csv(log_name,
                 sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', 
                 engine='python', na_values=['-'], header=None,
                 usecols=[0, 3, 4, 5, 6, 7, 8,10],
                 names=['ip', 'time', 'request', 'status', 'size', 'referer',
                 'user_agent','req_time'], 
                 converters={'status': int, 'size': int, 'req_time': int})
    #it is more convenient to have the request time in millisecond rather than
    #microsecond :
    data['req_time'] = data['req_time'].apply(lambda x: x / 1000) 
    return data


def time_to_minutes(date):
    #returns the time given by date, as a number of minutes ellapsed since 0:00
    #eg with date = "[19/Sep/2018:06:25:05 +0200]", it is 6:25 and the
    #function returns 6*60+25=385.
    return int(date[13:15])*60+int(date[16:18])

'''
      *****TERMINOLOGY*****
We want to divide the time of the day into segments.
We call __duration__ the length of the segments (in minutes).
We call __tic__ the index of a segment. 

For example, say duration = 10 minutes, the 8th segment is [70,80]. Its tic
is 7. It is convenient to refer to this segment as the tic 7.
Hence, the tic n refers to the time segment [n*duration, (n+1)*duration].
'''

def add_tics(data,duration):
    data['tics'] = data['time'].apply(lambda x : time_to_minutes(x)//duration)


def deciles(data,tic):
    return data[data['tics']==tic]['req_time'].quantile([i/10 
                                                         for i in range(10)])

def get_deciles_by_tic(data):
    first_tic=data['tics'].min()
    last_tic=data['tics'].max()
    deciles_by_tics=pd.DataFrame(columns=range(first_tic,last_tic+1))
    for c in range(first_tic,last_tic+1):
        deciles_by_tics[c]=deciles(data,c)
    return deciles_by_tics.T

def readable_time(tic,duration):
    #returns the time at which the tic begins. If, eg, the tic begins at 5:25,
    #this will return the float 5.25.
    return duration*tic//60 + (duration*tic%60)/100
