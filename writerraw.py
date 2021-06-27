#!/usr/bin/python
import os
import requests
import json
import pathlib
import time
import zipfile
def downloadlogs(offset,interval):
    response = requests.get("https://logs.tf/api/v1/log?offset=" + str(offset) + "&limit="+ str(interval))
    responselist = json.loads(response.text)
    logs = responselist.get("logs")
    logslist = []
    downloadedlogs = [s.replace(".log", "") for s in
                      os.listdir(path=r'/srv/http/tf2/Tf2LogSearcher/logs')]
    for log in logs:
        logslist.append(log.get("id"))
    print(logslist)
    for i in logslist:
        if any(i == id for id in downloadedlogs):
            break
        else:
            if not os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log'):
                logdata = requests.get("https://logs.tf/logs/log_"+str(i)+".log.zip")
                open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log.zip', 'wb').write(logdata.content)
                with zipfile.ZipFile(r'/srv/http/tf2/Tf2LogSearcher/logsraw/log_'+str(i)+'.log.zip', 'r') as zip_ref:
                    zip_ref.extractall(r'/srv/http/tf2/Tf2LogSearcher/logsraw/')
                if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip'):
                    os.remove(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip')
                print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Wrote log_" + str(i))
    # 2000 requests seems to be the limit before logs.tf times you out
i = 0
while True:
    try:
        downloadlogs(i,10000)
    except TimeoutError:
        print("Timeout error caught: Restarting in 30 seconds")
        time.sleep(30)
    i = i + 10000
