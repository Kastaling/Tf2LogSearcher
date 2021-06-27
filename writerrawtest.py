import os
import requests
import json
import pathlib
import time
import zipfile
rawlogslist = [int(log.replace('log_','').replace('.log','')) for log in os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logsraw/')]
logslist = [int(log.replace('.json','')) for log in os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')]
logslist.sort(reverse=False)
rawlogslist.sort(reverse=False)
logstodownload = set(logslist).difference(set(rawlogslist))
for i in logstodownload:
    if not os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log'):
        logdata = requests.get("https://logs.tf/logs/log_"+str(i)+".log.zip")
        if logdata.status_code == 200:
            open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log.zip', 'wb').write(logdata.content)
            try:
                zip_ref = zipfile.ZipFile(r'/srv/http/tf2/Tf2LogSearcher/logsraw/log_'+str(i)+'.log.zip', 'r')
            except:
                if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip'):
                    os.remove(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip')
                print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Marking Log " + str(i) + " as not a valid zipfile!")
                open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log', 'w').write("invalid zip")
                continue
            zip_ref.extractall(r'/srv/http/tf2/Tf2LogSearcher/logsraw/')
            if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip'):
                os.remove(r'/srv/http/tf2/Tf2LogSearcher/logsraw//log_'+str(i)+'.log.zip')
            print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Wrote log_" + str(i))
        elif logdata.status_code == 404:
            open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log', 'w').write("404")
            print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Marking log_" + str(i) + " as 404.")
        elif logdata.status_code == 403:
            open(rf'/srv/http/tf2/Tf2LogSearcher/logsraw//log_{i}.log', 'w').write("403")
            print("[" + time.strftime("%Y/%m/%d %I:%M:%S %p") + "] Marking log_" + str(i) + " as 403.")
        else:
            print(f'{i} has received error code {logdata.status_code}')
    else:
        print(f'Raw log {i} already downloaded!') 