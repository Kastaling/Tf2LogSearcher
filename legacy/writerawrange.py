#!/usr/bin/python
import os
import requests
import json
import pathlib
import time
import sys
import zipfile
downloadedlogs = [s.replace(".json", "") for s in os.listdir(path=r'/srv/http/tf2/Tf2LogSearcher/logs')]
downloadedlogs = [int(s) for s in downloadedlogs]
downloadedlogs = sorted(downloadedlogs,reverse=True)
#downloadedlogs = [2886825]
for i in downloadedlogs:
    if not os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log'):
        logdata = requests.get("https://logs.tf/logs/log_"+str(i)+".log.zip")
        open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log.zip', 'wb').write(logdata.content)
        with zipfile.ZipFile(r'/srv/http/tf2/Tf2LogSearcher/logsraw/log_'+str(i)+'.log.zip', 'r') as zip_ref:
            zip_ref.extractall(r'/srv/http/tf2/Tf2LogSearcher/logsraw/')
        if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip'):
            os.remove(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip')
        print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Wrote log_" + str(i))
    else:
        print(f'Raw log {i} already downloaded!') 
