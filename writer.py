#!/usr/bin/python
import os
import requests
import json
import pathlib
import time
def downloadlogs(offset,interval):
    response = requests.get("https://logs.tf/api/v1/log?offset=" + str(offset) + "&limit="+ str(interval))
    responselist = json.loads(response.text)
    logs = responselist.get("logs")
    logslist = []
    downloadedlogs = [s.replace(".json", "") for s in
                      os.listdir(path=r'/srv/http/tf2/Tf2LogSearcher/logs')]
    for log in logs:
        logslist.append(log.get("id"))
    print(logslist)
    for i in logslist:
        if any(i == id for id in downloadedlogs):
            break
        else:
            if not os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(i)+'.json'):
                logdata = requests.get("http://logs.tf/json/" + str(i))
                open(pathlib.Path(r'/srv/http/tf2/Tf2LogSearcher/logs/') / f"{i}.json",
                     'w').write(logdata.text)
                print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Wrote " + str(i))
    # 2000 requests seems to be the limit before logs.tf times you out
i = 1111000
#i = 2000
while True:
    try:
        downloadlogs(i,10000)
    except TimeoutError:
        print("Timeout error caught: Restarting in 30 seconds")
        time.sleep(30)
    i = i + 10000
