import sys
from urllib2 import Request, urlopen, URLError
import requests
import time
from pyproj import Proj
p1 = Proj(init='epsg:26915')
import pandas as pd
import numpy as np
from xml.etree import cElementTree as ET
import re

from sql import *
import sqlite3

import pytz
import datetime as DT
from datetime import datetime

import pylab as pl
from IPython import display
def p2p_dist(v, w):
    return ((v[0]-w[0])**2 + (v[1]-w[1])**2)**0.5

def write_stops_db(root):
    try:
        rt = root.find('ptr').text
    except:
        print 'READ XML ERROR', out
        return None
    wr = []
    for ptr in root.findall('ptr'):
        pid = ptr.find('pid').text
        rtdir = ptr.find('rtdir').text
        for pt in ptr.findall('pt'):
            attr = {'stpnm': '',
                    'stpid': '',
                    'typ': '',
                    'seq': '',
                    'lat': '',
                    'lon': '',
                    'x': '',
                    'y': '',
                    'pdist': ''}
            for child in pt:
                attr[child.tag] = child.text
            
            attr['x'], attr['y'] = p1(float(attr['lon']), float(attr['lat']))
            
            if attr['seq'] == '1':
                old_pdist = 0
                old_x = attr['x']
                old_y = attr['y']
            if not attr['stpid']:
                attr['pdist'] = old_pdist + p2p_dist([old_x, old_y], [attr['x'], attr['y']])
            
            old_pdist = float(attr['pdist'])
            old_x = attr['x']
            old_y = attr['y']
            tup = (pid, rtdir, attr['seq'], attr['x'], attr['y'], attr['typ'], attr['stpid'], attr['stpnm'], attr['pdist'])
            wr.append(tup)
    print tup
    conn = sqlite3.connect('Pattern_Stops.db')
    c = conn.cursor()
    c.executemany("INSERT INTO PAT_STOPS VALUES (?,?,?,?,?,?,?,?,?)", wr)
    conn.commit()
    conn.close()
    return None
    
base_url = 'http://www.ctabustracker.com/bustime/api/v1/'
pattern_str = 'getpatterns'
dev_str = '<devstring>'

#conn = sqlite3.connect('Pattern_Stops.db')

#c = conn.cursor()
#c.execute('''CREATE TABLE PAT_STOPS
#     (pid, rtdir, seq, x, y, typ, stpid, stpnm, pdist)''')
#conn.commit()
#conn.close()

while True:
    conAPI =  sqlite3.connect('API_Scrape.db')

    query = '''SELECT * FROM API_DAT'''
    API_test = pd.read_sql_query(query, conAPI)

    conStops =  sqlite3.connect('Pattern_Stops.db')


    query = '''SELECT * FROM PAT_STOPS'''
    Stops_test = pd.read_sql_query(query, conStops)

    urls = []

    pids = list(set(API_test[API_test['rt']!='992']['pid'].unique()) - \
                set(Stops_test['pid'].unique()))
    if pids:
        for x in range(0, len(pids), 10):

            urls.append(base_url+pattern_str+'?key='+dev_str + '&pid='+','.join(pids[x:x+10] ))

        for url in urls:
            print url
            request = Request(url)
            try:
                response = urlopen(request)
            except:
                print 'HTML ERROR, TRYING SUBQUERIES'
                url_root = '='.join(url.split('=')[:-1])
                ind_pids = url.split('=')[-1].split(',')
                for pid in ind_pids:
                    url = url_root + '=' + pid
                    request = Request(url)
                    try:
                        response = urlopen(request)
                    except:
                        print 'BAD PATTERN'
                        with open("bad_patterns.txt", "a") as f:
                            f.write(pid)
                            f.close()
                            break
                    out = response.read()
                    if out:
                        root = ET.fromstring(out)
                        if root.find('error'):
                            err_out = root.find('error').find('msg').text
                            if 'Transaction limit' in err_out:
                                print err_out
                                break
                        write_stops_db(root)
                    print 'PID WRITTEN'
                    time.sleep(30)
                break
                
            out = response.read()
            if out:
                root = ET.fromstring(out)
                if root.find('error'):
                    err_out = root.find('error').find('msg').text
                    if 'Transaction limit' in err_out:
                        print err_out
                        break
                write_stops_db(root)
            print 'PID WRITTEN'
            time.sleep(60)
    else:
        print 'NO PIDS WRITTEN'
    time.sleep(3600)





