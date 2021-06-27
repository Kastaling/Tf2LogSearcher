#!/usr/bin/python3
import requests
import json
import os
import sys
word = sys.argv[1]
steamid = sys.argv[2]
a = int(steamid) - 76561197960265728
chatsteamid = f"[U:1:{a}]"
response = requests.get("https://logs.tf/api/v1/log?limit=10000&player="+steamid).json()
response2 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=10000&player="+steamid).json()
response3 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=20000&player="+steamid).json()
logs = response.get("logs")+response2.get("logs")+response3.get("logs")
logslist = []
for log in logs:
    logslist.append(log.get("id"))
i = 0
def wordfinder(chat,sid):
    banana = False
    for message in chat:
        if message.get("steamid") == chatsteamid:
            if word.lower() in message.get("msg").lower():
                banana = True
                alias = message.get("name")
                print(f"{alias} : " + message.get("msg"))
                print("<br/>")
    return banana
for log in logslist:
    if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json'):
        with open(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json','r') as f:
            logtext = json.load(f)
        chat = logtext.get("chat")
        if wordfinder(chat,chatsteamid):
            b = "https://logs.tf/"+str(log)
            print(f'<a href="{b}" target="_blank">{b}</a>')
            print("<br/>")
            print("<br/>")
            i = i + 1
print(f'Total {i} occurrences found of "{word}" from <a href="https://logs.tf/profile/{steamid}" target="_blank">{steamid}</a>.')

