import sys
from urllib2 import Request, urlopen, URLError
import requests
import time
from pyproj import Proj
p1 = Proj(init='epsg:26915')
import pandas as pd
import numpy as np
from xml.etree import cElementTree as ET
from bs4 import BeautifulSoup
import re

import pickle

from sql import *
import sqlite3

from matplotlib import dates, pyplot
import pytz
import datetime as DT
from datetime import datetime

import pylab as pl
from IPython import display

conAPI =  sqlite3.connect('API_Scrape.db')

query = '''SELECT rt, COUNT() as count FROM (SELECT rt, id as count FROM API_DAT GROUP BY rt, id) GROUP BY rt'''
query = '''SELECT * FROM API_DAT'''
API_test = pd.read_sql_query(query, conAPI)

conStops =  sqlite3.connect('Pattern_Stops.db')

query = '''SELECT * FROM PAT_STOPS'''
Stops_test = pd.read_sql_query(query, conStops)

key_stops = pickle.load(open('/home/vagrant/datacourse/Capstone/key_stops.pkl', 'rb') ) 


if not key_stops:
    print 'BUILDING KEY STOPS LIST'
    goroo_url = 'http://www.goroo.com/goroo/getCtaBusScheduleComplete.htm'
    rts = API_test['rt'].unique()
    key_stops = {}
    wk_key = {'02/10/16':'d','02/13/16':'a','02/14/16':'u'}
    for rt in rts:
        pids = API_test[API_test['rt'] == rt]['pid'].unique()
        dirs = Stops_test[Stops_test['pid'].isin(pids)]['rtdir'].unique()
        stops = []
        for d in dirs:
            for date in ['02/10/16', '02/13/16','02/14/16']:
                key_stops[rt+'_'+d[0]+'_'+wk_key[date]] = []
                request = requests.get(goroo_url +'?route='+rt+'&travelDate='+date+'&direction='+d[:-5])
                #response = urlopen(request)
                out = request.text
                soup = BeautifulSoup(out, 'html.parser')
                if soup.find('tr', {'class':'row2'}):
                    sub_soup = soup.find('tr', {"class" : 'row2'}).parent
                    for i in sub_soup.find_all('th', {'scope' : 'row'}):
                        key_stops[rt+'_'+d[0]+'_'+wk_key[date]].append(str(i.string))
                    stops.extend(key_stops[rt+'_'+d[0]+'_'+wk_key[date]])
        key_stops[rt] = list(set(stops))
    pickle.dump(key_stops, open('/home/vagrant/datacourse/Capstone/key_stops.pkl', 'wb'))  

anchor_stops = {}
anchor_id = {}
untagged = {}
k = [i for i in key_stops.keys() if '_' in i]
k = [i for i in k if i.split('_')[1] in ['E', 'N']]
for rt in k:#key_stops.keys():
    print rt
    anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])] = {}
    anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])] = {}
    untagged['_'.join([rt.split('_')[0],rt.split('_')[2]])] = []
    repids = []
    for pid in API_test[API_test['rt'] == rt.split('_')[0]]['pid'].unique():
        ptr = Stops_test[(Stops_test['pid'] == pid) & (Stops_test['typ'] == 'S')]
        if (ptr['stpnm'].iloc[0] == key_stops[rt][0] and ptr['stpnm'].iloc[0] not in anchor_stops.keys()) or (ptr['stpnm'].iloc[-1] == key_stops[rt][-1] and ptr['stpnm'].iloc[-1] not in anchor_stops.keys()):

            anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])] = dict(anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])], **{ptr['stpnm'].iloc[i]: float(ptr['pdist'].iloc[i]) for i in range(len(ptr)) if ptr['stpnm'].isin(key_stops[rt]).iloc[i] and
                                not ptr['stpnm'].isin(anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])]).iloc[i]})
            anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])] = dict(anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])], **{ptr['stpid'].iloc[i]: float(ptr['pdist'].iloc[i]) for i in range(len(ptr)) if ptr['stpnm'].isin(key_stops[rt]).iloc[i] and
                                not ptr['stpnm'].isin(anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])]).iloc[i]})

#        print stops
        t = [float(anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpnm'].iloc[i]]) if ptr['stpnm'].iloc[i] in anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])] else -1 for i in range(len(ptr))]
        hits = [i for i, e in enumerate(t) if e >=0 ]
        nones = [i for i, e in enumerate(t) if e ==-1]
        if len(hits)>1:
            new_id = [(ptr['stpid'].iloc[i], t[i]) for i in range(len(t)) if t[i]>0 and ptr['stpid'].iloc[i] not in anchor_id.keys()]
            anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])] = dict(anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])], **{i[0]: i[1] for i in new_id})
            for i in nones:
                if [hit for hit in hits if hit<i] and [hit for hit in hits if hit>i]:
                    below = max([hit for hit in hits if hit<i])
                    above = min([hit for hit in hits if hit>i])
#                    if ((float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below]))> 0.75*(float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))) and \
#                        ((float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below])) < 1.25*(float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))):

                    t[i] = t[below] + \
                            (float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below])  )*(t[above]-t[below])/ \
                            (float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))

                    anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpnm'].iloc[i]] = t[i]
                    anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpid'].iloc[i]] = t[i]

#            print t
        else:
            repids.append(pid)
            print 'redo', pid
    for pid in repids:
        ptr = Stops_test[(Stops_test['pid'] == pid) & (Stops_test['typ'] == 'S')]
        t = [float(anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpnm'].iloc[i]]) if ptr['stpnm'].iloc[i] in anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])] else -1 for i in range(len(ptr))]
        hits = [i for i, e in enumerate(t) if e >=0 ]
        nones = [i for i, e in enumerate(t) if e ==-1]
        if len(hits)>1:
            new_id = [(ptr['stpid'].iloc[i], t[i]) for i in range(len(t)) if t[i]>0 and ptr['stpid'].iloc[i] not in anchor_id.keys()]
            anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])] = dict(anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])], **{i[0]: i[1] for i in new_id})
            for i in nones:
                if [hit for hit in hits if hit<i] and [hit for hit in hits if hit>i]:
                    below = max([hit for hit in hits if hit<i])
                    above = min([hit for hit in hits if hit>i])
#                    if ((float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below]))> 0.75*(float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))) and \
#                        ((float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below])) < 1.25*(float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))):

                    t[i] = t[below] + \
                            (float(ptr['pdist'].iloc[i]) -float(ptr['pdist'].iloc[below])  )*(t[above]-t[below])/ \
                            (float(ptr['pdist'].iloc[above]) - float(ptr['pdist'].iloc[below]))

                    anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpnm'].iloc[i]] = t[i]
                    anchor_id['_'.join([rt.split('_')[0],rt.split('_')[2]])][ptr['stpid'].iloc[i]] = t[i]

        else:
            untagged['_'.join([rt.split('_')[0],rt.split('_')[2]])].append(pid)
            print pid+' NOT WRITTEN'
            print t
            print key_stops[rt]
            print anchor_stops['_'.join([rt.split('_')[0],rt.split('_')[2]])].keys()
            print ptr['stpnm']
    print '_'.join([rt.split('_')[0],rt.split('_')[2]])

pickle.dump(anchor_id, open('/home/vagrant/datacourse/Capstone/anchor_id_keys.pkl', 'wb'))  
pickle.dump(anchor_stops, open('/home/vagrant/datacourse/Capstone/anchor_stops.pkl', 'wb'))

