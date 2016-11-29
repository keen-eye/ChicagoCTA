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
anchor_id = pickle.load(open('/home/vagrant/datacourse/Capstone/anchor_id_keys.pkl', 'rb')) 
plt.ioff()
con =  sqlite3.connect('/home/vagrant/datacourse/Capstone/CTA_GTFS/CTA_Sched')
from datetime import datetime
import pytz
import datetime as DT

weekday_dict = {0:'sunday', 3:'wednesday', 6:'saturday'}
wkd = {'sunday':'u', 'weekday':'d', 'saturday':'a'}


conAPI =  sqlite3.connect('API_Scrape.db')

query = '''SELECT rt, COUNT() as count FROM (SELECT rt, id as count FROM API_DAT GROUP BY rt, id) GROUP BY rt'''
rts = pd.read_sql_query(query, conAPI)['rt'].unique()

for rt in rts:
    for weekday in weekday_dict.values():
        print rt, weekday
        query1 = '''SELECT route_id, trip_id, service_id, shape_id FROM Trips WHERE route_id == ''' + rt 

        query2 = '''SELECT route_id, trip_id, trip_id/100000000 AS trip_id_trunc1,(trip_id-((trip_id/100000000)*100000000))/10000 AS trip_id_trunc2, trip_id%10000 AS trip_id_trunc3, RT.service_id AS service_id, shape_id
                    FROM ('''+query1+''') AS RT JOIN Calendar ON RT.service_id = Calendar.service_id WHERE '''+weekday+''' == 1'''

                        
        query3 = '''
                        SELECT * FROM (
                        SELECT route_id, MIN(arrival_time) AS start_time, MAX(arrival_time) AS end_time, trip_id_trunc1, trip_id_trunc2, trip_id_trunc3,  service_id, shape_id, StopTimes.trip_id AS trip_id
                            FROM ('''+query2+''') AS RTDay JOIN StopTimes ON RTDay.trip_id == StopTimes.trip_id GROUP BY RTDay.trip_id 
                        )
                        GROUP BY trip_id_trunc2, trip_id_trunc3, start_time ORDER BY route_id, start_time ASC'''  
	try:
             RtTrips = pd.read_sql_query(query3, con)
	except:
	     continue





        query_st = '''SELECT trip_id, departure_time, StopsSel.stop_id AS stop_id, stop_seq FROM (
                        SELECT departure_time, stop_id, trip_id, CAST(stop_sequence AS INT) AS stop_seq
                            FROM StopTimes WHERE trip_id IN ('''+','.join(RtTrips['trip_id'].unique())+''')
                            ) AS StopsSel JOIN Stops ON StopsSel.stop_id == Stops.stop_id ORDER BY trip_id ASC
        '''
        stops = pd.read_sql_query(query_st, con).drop_duplicates()
	if stops.empty:
	    print 'NO DATA ON '+rt+' '+weekday
	else:
	    fig = plt.figure()#figsize(12,120))
	    fig.set_size_inches(12, 120)
            for i in stops['trip_id'].unique():
                t = stops[stops['trip_id'] == i].sort('stop_seq')
                t['pdist'] = t['stop_id'].apply(lambda x: float(anchor_id[rt+'_'+wkd['weekday']][x]) if x in anchor_id[rt+'_'+wkd['weekday']].keys() else np.nan)
                t = t[np.isfinite(t['pdist'])]
                t['t'] = t['departure_time'].apply(lambda x: datetime.strptime(x, '%H:%M:%S') if int(x[:2])<24 else datetime.strptime(str(int(x[:2])-24).zfill(2)+x[2:], '%H:%M:%S') + timedelta(days =1))
                plt.plot_date(t['pdist'], t['t'], 'k-', xdate=False, ydate=True)
            fig.savefig('/home/vagrant/datacourse/Capstone/RRFigs/'+rt+'_'+weekday+'.png', bbox_inches='tight', dpi=100)
            plt.close(fig)
