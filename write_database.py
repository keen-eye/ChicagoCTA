
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

from matplotlib import dates, pyplot
import pytz
import datetime as DT
from datetime import datetime

import pylab as pl
from IPython import display
import json



def get_central_time():
    central = pytz.timezone('US/Central')
    c_time = central.localize(test).strftime('%H:%M:%S%z')
    
    m = int(c_time[3:5])
    m_o = int(c_time[-2:])
    h = int(c_time[:2])
    h_o = int(c_time[-4:-2])
    if m_o>m:
        m = 60+(m-m_o)
        h_o = h_o+1
    else:
        m = m-m_o

    if h_o>h:
        h = 24+(h-h_o)
    else:
        h = h-h_o

    c_time = '{:02d}:{:02d}:{!s}'.format(h, m, c_time[6:8])


    return c_time

def write_db(D_comp, response):
    b = False
    central = pytz.timezone('US/Central')
    test = datetime.utcnow()
    date = central.localize(test).strftime('%Y%m%d%H%M')
    out = ''
    wr = []
    try:
        out = urlopen(response.url).read()
        root = ET.fromstring(out)
    except:
        return b, D_comp
    try:
        rt = root.find('vehicle').find('rt').text
    except:
        print 'READ XML ERROR', out
        return b, D_comp
    if root.find('error'):
        err_out = root.find('error').find('msg').text
        if 'Transaction limit' in err_out:
            print err_out
            b = True
            return b, D_comp
    seen = []
    for vehicle in root.findall('vehicle'):
        seen.append(vehicle.find('rt').text+ '_'+ vehicle.find('tatripid').text )
    if not seen:
        print 'API ERROR', out
        return b, D_comp
    
    rts = set(map(lambda x: x.split('_')[0], seen))
    rm = []
    seen = []
    for vehicle in root.findall('vehicle'):
        rt = vehicle.find('rt').text
        loc_id = vehicle.find('tatripid').text
        t_id = rt +'_'+loc_id
        seen.append(t_id)
                
        hdg = vehicle.find('hdg').text
        pid = vehicle.find('pid').text
        t = vehicle.find('tmstmp').text
        pdist = vehicle.find('pdist').text
        test = datetime.utcnow()
        lt = central.localize(test).strftime('%Y%m%d%H%M')
        
        
        if t_id in D_comp.keys():
            sd = D_comp[t_id]['date']
            if float(pdist) > float(D_comp[t_id]['pdist']):
                
                spd = (float(pdist)-float(D_comp[t_id]['pdist']))/ \
                (30+(datetime.strptime(t, '%Y%m%d %H:%M') - \
                datetime.strptime(D_comp[t_id]['lastping'], '%Y%m%d %H:%M')).total_seconds())
                
                D_comp[t_id]['pdist'] = pdist
                D_comp[t_id]['lastping'] = t
            else:
                spd = 'NaN'
        else:
            spd = 0.0                                         
            D_comp[t_id] = {}
            D_comp[t_id]['date'] = date
            D_comp[t_id]['pdist'] = pdist
            D_comp[t_id]['lastping'] = t
            D_comp[t_id]['hdg'] = hdg

        lat = vehicle.find('lat').text
        lon = vehicle.find('lon').text
        x, y = p1( lon, lat )
        
        D_comp[t_id]['lat'] = lat
        D_comp[t_id]['lon'] = lon
        D_comp[t_id]['lt'] = lt
        sd = D_comp[t_id]['date']
        if vehicle.find('dly'):
            dly = 1
        else:
            dly = 0
        wr.append((sd, rt, loc_id, x, y, t,lt, spd, hdg, pid, dly, pdist))
    rt_keys = []
    for i in rts:
        rt_keys.extend([s for s in D_comp.keys() if re.match('^('+i+'_.*)$', s)])
    for t_id in rt_keys:
        if t_id not in seen:
            print t_id, D_comp[t_id]['date']
            D_comp.pop(t_id, None)
    conn = sqlite3.connect('API_Scrape.db')
    c = conn.cursor()
    c.executemany("INSERT INTO API_DAT VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", wr)
    try:
	conn.commit()
    except:
	pass
    conn.close()
    
    
    conn = sqlite3.connect('API_curr.db')

    c = conn.cursor()
    c.executemany("INSERT INTO API_CURR_DAT VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", wr)

    conn.commit()
    conn.close()

    return b, D_comp


base_url = 'http://www.ctabustracker.com/bustime/api/v1/'
route_str = 'getvehicles'
dev_str = '<devstring>'
rts_url = base_url+'getroutes'+ '?key='+dev_str

D_comp = {}
request = Request(rts_url)
response = urlopen(request)
out = response.read()
root = ET.fromstring(out)
rts_xml = root.findall("./route/rt")
urls = []
rts = [rts_xml[i].text for i in range(len(rts_xml))]
rts_str = ''
for x in range(0, len(rts), 10):
    
    urls.append(base_url+route_str+'?key='+dev_str + '&rt='+','.join(rts[x:x+10] ))
from requests_futures.sessions import FuturesSession
#conn = sqlite3.connect('API_Scrape.db')
#
#c = conn.cursor()
#c.execute('''CREATE TABLE API_DAT
#     (sd, rt, id, x, y, t,lt, spd, hdg, pid, dly, pdist)''')
#conn.commit()
#conn.close()


while True:
    session = FuturesSession(max_workers=5)
    conn = sqlite3.connect('API_curr.db')

    c = conn.cursor()
    c.execute('''DROP TABLE IF EXISTS API_CURR_DAT''')
    c.execute('''CREATE TABLE API_CURR_DAT
         (sd, rt, id, x, y, t,lt, spd, hdg, pid, dly, pdist)''')

    conn.commit()
    conn.close()
    futures = [session.get(url) for url in urls]
    for future in futures:
        try:
            result = future.result()
        except:
            b = False
        if result:
            b, D_comp = write_db(D_comp,result)
            print 'WROTE TO DB'
    
    with open('bus_loc.json', 'w') as f:
        json.dump({key: [float(D_comp[key]['lat']), float(D_comp[key]['lon']), float(D_comp[key]['hdg'])] for key in D_comp.keys()}, f)
    time.sleep(120)
    if b:
        print 'CAP LIMIT'
        time.sleep(3600)
