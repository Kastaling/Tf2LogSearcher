import sys
import requests
import os
import json
from datetime import datetime, timezone
utc_dt = datetime.now(timezone.utc)
dt = utc_dt.astimezone()
steamids = sys.argv[1].split()
def steamid3converter(steamid64):
    a = int(steamid64) - 76561197960265728
    return(f"[U:1:{a}]")
steamid3s = list(map(steamid3converter, steamids))
response = requests.get("https://logs.tf/api/v1/log?limit=10000&player="+steamids[0]).json()
response2 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=10000&player="+steamids[0]).json()
response3 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=20000&player="+steamids[0]).json()
logs = response.get("logs")+response2.get("logs")+response3.get("logs")
logslist = []
for log in logs:
    logslist.append(log.get("id"))
i = 0
for log in logslist:
    if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json'):
        with open(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json','r') as f:
            logtext = json.load(f)
        names = logtext.get("names")
        namesid = list(names.keys())
        infotext = logtext.get("info")
        if (set(namesid).issuperset(set(list(steamid3s)))) == True:
            date = int(infotext.get("date"))
            print(infotext.get("title")+"<br>")
            print(infotext.get("map")+"<br>")
            print(datetime.fromtimestamp(date).strftime('%m/%d/%Y %I:%M:%S %p %Z')+"<br>")
            b = "https://logs.tf/" + str(log)
            print(f'<a href="{b}" target="_blank">{b}</a>')
            print("<br/>")
            print("<br/>")
            i = i + 1
print(f"Total of {i} matching logs found.")