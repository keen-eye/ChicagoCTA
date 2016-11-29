
import sys
from urllib2 import Request, urlopen, URLError
import requests
import time
from pyproj import Proj
p1 = Proj(init='epsg:26915')
import matplotlib 
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns  # makes plots pretty
from pandas import *
import pandas as pd
import numpy as np
from xml.etree import cElementTree as ET
import re
from bs4 import BeautifulSoup
import pickle
from sql import *
import sqlite3
from datetime import timedelta
from datetime import datetime
from matplotlib import dates, pyplot
import matplotlib

import pylab as pl
from IPython import display
#matplotlib.rcParams['savefig.dpi'] = 2 * matplotlib.rcParams['savefig.dpi']


from datetime import datetime
import pytz
import datetime as DT

weekday_dict = {0:'sunday', 1:'monday', 2:'tuesday', 3:'wednesday', 4:'thursday', 5:'friday', 6:'saturday'}

anchor_id = pickle.load(open('/home/vagrant/datacourse/Capstone/anchor_id_keys.pkl', 'rb')) 
anchor_stops = pickle.load(open('/home/vagrant/datacourse/Capstone/anchor_stops.pkl', 'rb')) 
key_stops = pickle.load(open('/home/vagrant/datacourse/Capstone/key_stops.pkl', 'rb')) 

for rt in [x for x in key_stops.keys() if '_' not in x][[x for x in key_stops.keys() if '_' not in x].index('50'):]:
    print rt
    con =  sqlite3.connect('/home/vagrant/datacourse/Capstone/CTA_GTFS/CTA_Sched')
    query1 = '''SELECT route_id, trip_id, service_id, shape_id FROM Trips WHERE route_id == "'''+rt+'''"  '''

    query2 = '''SELECT route_id, trip_id, trip_id/100000000 AS trip_id_trunc1,(trip_id-((trip_id/100000000)*100000000))/10000 AS trip_id_trunc2, trip_id%10000 AS trip_id_trunc3, RT66.service_id AS service_id, shape_id
                FROM ('''+query1+''') AS RT66 JOIN Calendar ON RT66.service_id = Calendar.service_id WHERE wednesday == 1'''

    query3 = '''
                    SELECT * FROM (
                    SELECT route_id, MIN(arrival_time) AS start_time, MAX(arrival_time) AS end_time, trip_id_trunc1, trip_id_trunc2, trip_id_trunc3,  service_id, shape_id, StopTimes.trip_id AS trip_id
                        FROM ('''+query2+''') AS RT66Day JOIN StopTimes ON RT66Day.trip_id == StopTimes.trip_id GROUP BY RT66Day.trip_id 
                    )
                    GROUP BY trip_id_trunc2, trip_id_trunc3, start_time ORDER BY route_id, start_time ASC'''  
    Rt66Trips = pd.read_sql_query(query3, con)
    #print Rt66Trips



    con =  sqlite3.connect('/home/vagrant/datacourse/Capstone/CTA_GTFS/CTA_Sched')

    query_st = '''SELECT trip_id, departure_time, StopsSel.stop_id AS stop_id, stop_seq FROM (
                    SELECT departure_time, stop_id, trip_id, CAST(stop_sequence AS INT) AS stop_seq
                        FROM StopTimes WHERE trip_id IN ('''+','.join(Rt66Trips['trip_id'].unique())+''')
                        ) AS StopsSel JOIN Stops ON StopsSel.stop_id == Stops.stop_id ORDER BY trip_id ASC
    '''
    stops = pd.read_sql_query(query_st, con).drop_duplicates()


    fig= plt.figure(figsize = (12, 120))
    min_t = ''
    max_t = ''


    con =  sqlite3.connect('/home/vagrant/datacourse/Capstone/CTA_GTFS/CTA_Sched')

    query_st = '''SELECT trip_id, departure_time, StopsSel.stop_id AS stop_id, stop_seq FROM (
                    SELECT departure_time, stop_id, trip_id, CAST(stop_sequence AS INT) AS stop_seq
                        FROM StopTimes WHERE trip_id IN ('''+','.join(Rt66Trips['trip_id'].unique())+''')
                        ) AS StopsSel JOIN Stops ON StopsSel.stop_id == Stops.stop_id ORDER BY trip_id ASC
    '''
    stops = pd.read_sql_query(query_st, con).drop_duplicates()
    print 'SCHEDULED DATA'
    for i in stops['trip_id'].unique():
        t = stops[stops['trip_id'] == i].sort('stop_seq')
        t['pdist'] = t['stop_id'].apply(lambda x: float(anchor_id[rt+'_d'][x]) if x in anchor_id[rt+'_d'].keys() else np.nan)
        t = t[np.isfinite(t['pdist'])]
        t['t'] = t['departure_time'].apply(lambda x: datetime.strptime(x, '%H:%M:%S') if int(x[:2])<24 else datetime.strptime(str(int(x[:2])-24).zfill(2)+x[2:], '%H:%M:%S') + timedelta(days =1))
	if not t['t'].empty:
            if not min_t:
                min_t = min(t['t'])
                max_t = max(t['t'])
            else:
                min_t = min(min_t, min(t['t']))
                max_t = max(max_t, max(t['t']))
            plt.plot_date(t['pdist'], t['t'], 'k-', xdate=False, ydate=True)
    print 'REAL DATA'
    con = sqlite3.connect('API_Scrape.db')

    query = ''' SELECT * FROM API_DAT WHERE rt == "'''+rt+'''" AND sd < '20160225000000' AND sd > '20160224000000' '''

    api = pd.read_sql_query(query, con)

    patterns = {}

    for i in api['id'].unique():
        t = api[api['id'] == i]
        if len(t)>=10:

            t['t'] = t['t'].apply(lambda x: datetime.strptime(x.split()[1], '%H:%M') if x.split()[0] == t['t'].iloc[0].split()[0] else datetime.strptime(x.split()[1], '%H:%M')+timedelta(days=1))
            if max( (t['t']-t['t'].shift(1)).dropna() ).total_seconds() <= 600:
                pattern = t['pid'].iloc[0]
                t['pdist'] = t['pdist'].apply(lambda x: float(x))
                if pattern not in patterns.keys():
                    pquery = ''' SELECT stpid, pdist FROM PAT_STOPS WHERE pid == "'''+pattern+'"' 

                    conStops =  sqlite3.connect('Pattern_Stops.db')

                    stops = pd.read_sql_query(pquery, conStops)
                    patterns[pattern] = stops
                else:
                    stops = patterns[pattern]
                stops = stops[stops['stpid'] != '']
                stops['pdist'] = stops['pdist'].apply(lambda x: float(x))
                refpdist = [-1]*len(stops)
                for i in range(len(stops)):
                    if stops['stpid'].iloc[i] in anchor_id[rt+'_d'].keys():
                        refpdist[i] = anchor_id[rt+'_d'][stops['stpid'].iloc[i]]
                stops['refpdist'] = refpdist
                stops = stops[stops['refpdist'] != -1]
                refpos = [-1]*len(t)
                for j in range(len(t)):
                    if t['pdist'].iloc[j] > min(stops['pdist']) and t['pdist'].iloc[j] <max(stops['pdist']):
                        temp = stops['pdist'].apply(lambda x: x -t['pdist'].iloc[j] if x -t['pdist'].iloc[j]>0 else np.nan).argsort()
                        above = temp[temp == 0].index.tolist()[0]

                        temp = stops['pdist'].apply(lambda x: -(x -t['pdist'].iloc[j]) if -(x -t['pdist'].iloc[j])>0 else np.nan).argsort()
                        below = temp[temp == 0].index.tolist()[0]
                        if below in stops.index.tolist() and above in stops.index.tolist():
                            #print stops['pdist'].loc[below], stops['pdist'].loc[above]
                            perc = (t['pdist'].iloc[j] - stops['pdist'].loc[below]) / \
                                    (stops['pdist'].loc[above] - stops['pdist'].loc[below] )
                            refpos[j] = stops['refpdist'].loc[below] + perc*( stops['refpdist'].loc[above]-stops['refpdist'].loc[below] ) 

                t['refpdist'] = refpos
                t = t[t['refpdist']>0]
                plt.plot_date(t['refpdist'], t['t'], 'r-', xdate=False, ydate=True)

    if rt+'_E_d' in key_stops.keys():
        rtfull = rt+'_E_d'
    else:
        rtfull = rt+'_N_d'
    for j in key_stops[rtfull]:
        if j in anchor_stops[rt+'_d'].keys():
            plt.plot_date([anchor_stops[rt+'_d'][j]]*2, [min_t, max_t], 'k-', linewidth=1, xdate=False, ydate=True)
    ax = plt.gca()
    ax.xaxis.tick_top()

    plt.xticks([ anchor_stops[rt+'_d'][x] for x in key_stops[rtfull] if x in anchor_stops[rt+'_d'].keys()], key_stops[rtfull], rotation='vertical')

    plt.savefig( '/home/vagrant/datacourse/Capstone/RRFigs/'+rt+'_real.png', bbox_inches='tight', dpi=100)
    plt.close(fig)
