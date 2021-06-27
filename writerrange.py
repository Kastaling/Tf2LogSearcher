#!/usr/bin/python
import os
import requests
import json
import pathlib
import time
import sys
minlog = int(sys.argv[1])
maxlog = int(sys.argv[2])
logslist = sorted(set(range(maxlog, minlog)), reverse=False)
downloadedlogs = [s.replace(".json", "") for s in os.listdir(path=r'/srv/http/tf2/Tf2LogSearcher/logs')]
for i in logslist:
    if any(i == id for id in downloadedlogs):
        break
    else:
        if not os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(i)+'.json'):
            logdata = requests.get("http://logs.tf/json/" + str(i))
            logjson = json.loads(logdata.text)
            if logjson.get('success'):
                open(pathlib.Path(r'/srv/http/tf2/Tf2LogSearcher/logs/') / f"{i}.json", 'w').write(logdata.text)
                print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Wrote " + str(i))
            else:
                error = logjson.get('error')
                print(f'Invalid log {i} found. Error: {error}')
        else:
            print(f'Log {i} already downloaded!') 
