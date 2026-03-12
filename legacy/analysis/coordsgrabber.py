#input map and get list of tuples for coordinates
import os
import json
import sys
import sqlite3
import xlwt
from xlwt import Workbook
import csv
map = sys.argv[1]
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/test/logs/')
logslist = [log.replace('.json','') for log in logslist]
coordslist = []
wb = Workbook()
sheet1 = wb.add_sheet('Sheet 1')
a = 0
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json','r') as f:
        logtext = json.loads(f.read())
        if map in logtext["info"]["map"]:
            with open(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(log)+'.log','r') as d:
                rawtext = d.readlines()
                for line in rawtext:
                    if "victim_position" in line and not "assist" in line:
                        linelist = line.split()
                        x = linelist[-3].replace('"','')
                        y = linelist[-2]
                        z = linelist[-1].replace('"','').replace(')','')
                        coorddict = (x,y,z)
                        coordslist.append(coorddict)
                        '''sheet1.write(a,0,x)
                        sheet1.write(a,1,y)
                        sheet1.write(a,2,z)
                        a +=1
                        conn = sqlite3.connect('coords.sqlite3')
                        db = conn.cursor()
                        db.execute(f"INSERT INTO coords (x,y,z, map) VALUES ({x},{y},{z}, '{map}')")
                        conn.commit()
                        coorddict = {
                            "x":x,
                            "y":y,
                            "z":z  
                        }
                        coorddict = json.dumps(coorddict)
                        print(f'Adding {coorddict} to list.')
                        coordslist.append(coorddict)
with open(f'{map} results.json', 'w') as t:
    #t.write(coordslist)
    for item in coordslist:
        t.write(f'{item}\n')
        f.close()
        print(f'Wrote {item} to file.')
    print(f'Wrote to file {map} results.json')
with open(f'{map} results.json', 'r') as c:
    linelist = [json.loads(d) for d in c.readlines()]
    print(linelist)'''
#wb.save(f'{map}.csv')
with open(f'{map}.csv', 'w', newline='') as csvfile:
    file = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for coord in coordslist:
        file.writerow(coord)
                        