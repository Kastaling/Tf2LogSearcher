#!/usr/bin/python3
import sys
import requests
import os
import json
from datetime import datetime, timezone
utc_dt = datetime.now(timezone.utc)
dt = utc_dt.astimezone()
steamid = sys.argv[1]
gamemode = sys.argv[2]
classlist = sys.argv[3].split()
a = int(steamid) - 76561197960265728
chatsteamid = f"[U:1:{a}]"
response = requests.get("https://logs.tf/api/v1/log?limit=10000&player="+steamid).json()
response2 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=10000&player="+steamid).json()
response3 = requests.get("https://logs.tf/api/v1/log?limit=10000&offset=20000&player="+steamid).json()
logs = response.get("logs")+response2.get("logs")+response3.get("logs")
logslist = []
for log in logs:
    logslist.append(log.get("id"))
if gamemode == "hl":
    for log in logslist:
        if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json'):
            with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
                logtext = json.load(f)
            names = logtext.get("names")
            namesid = list(names.keys())
            if 18 <= len(namesid):
                stats = logtext.get("players").get(f"{chatsteamid}")
                classstats = stats.get('class_stats')
                logclasslist = []
                for char in classstats:
                    logclasslist.append(char.get('type'))
                logclasslist_indexes = [logclasslist.index(e) for e in set(logclasslist) & set(classlist)]
                for index in logclasslist_indexes:
                    alias = names.get(f'{chatsteamid}')
                    character = classstats[index].get('type')
                    kills = classstats[index].get('kills')
                    assists = classstats[index].get('assists')
                    deaths = classstats[index].get('deaths')
                    if int(deaths) == 0:
                        kadr = int(kills) + int(assists)
                        kdr = int(kills)
                    else:
                        kadr = round(((int(kills) + int(assists)) / int(deaths)), 2)
                        kdr = round((int(kills) / int(deaths)), 2)
                    dmg = classstats[index].get('dmg')
                    dpm = round(((int(dmg) / classstats[index].get('total_time')) * 60), 2)
                    hs = stats.get('headshots_hit')
                    bs = stats.get('backstabs')
                    map = logtext.get('info').get('map')
                    date = logtext.get('info').get('date')
                    date = datetime.fromtimestamp(date).strftime('%I:%M:%S %p %Z %m/%d/%Y ')
                    b = "https://logs.tf/" + str(log)
                    print(f'<tr><td>{alias}</td><td>{character}</td><td>{kills}</td><td>{assists}</td><td>{deaths}</td><td>{kdr}</td><td>{kadr}</td><td>{dpm}</td><td>{dmg}</td><td>{hs}</td><td>{bs}</td><td>{map}</td><td>{date}</td><td><a href="{b}" target="_blank">{b}</a></td></tr>')
if gamemode == "7s":
    for log in logslist:
        if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json'):
            with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
                logtext = json.load(f)
            names = logtext.get("names")
            namesid = list(names.keys())
            if 14 <= len(namesid) <= 17:
                stats = logtext.get("players").get(f"{chatsteamid}")
                classstats = stats.get('class_stats')
                logclasslist = []
                for char in classstats:
                    logclasslist.append(char.get('type'))
                logclasslist_indexes = [logclasslist.index(e) for e in set(logclasslist) & set(classlist)]
                for index in logclasslist_indexes:
                    alias = names.get(f'{chatsteamid}')
                    character = classstats[index].get('type')
                    kills = classstats[index].get('kills')
                    assists = classstats[index].get('assists')
                    deaths = classstats[index].get('deaths')
                    if int(deaths) == 0:
                        kadr = int(kills) + int(assists)
                        kdr = int(kills)
                    else:
                        kadr = round(((int(kills) + int(assists)) / int(deaths)), 2)
                        kdr = round((int(kills) / int(deaths)), 2)
                    dmg = classstats[index].get('dmg')
                    dpm = round(((int(dmg) / classstats[index].get('total_time')) * 60), 2)
                    hs = stats.get('headshots_hit')
                    bs = stats.get('backstabs')
                    map = logtext.get('info').get('map')
                    date = logtext.get('info').get('date')
                    date = datetime.fromtimestamp(date).strftime('%I:%M:%S %p %Z %m/%d/%Y ')
                    b = "https://logs.tf/" + str(log)
                    print(
                        f'<tr><td>{alias}</td><td>{character}</td><td>{kills}</td><td>{assists}</td><td>{deaths}</td><td>{kdr}</td><td>{kadr}</td><td>{dpm}</td><td>{dmg}</td><td>{hs}</td><td>{bs}</td><td>{map}</td><td>{date}</td><td><a href="{b}" target="_blank">{b}</a></td></tr>')
if gamemode == "6s":
    for log in logslist:
        if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json'):
            with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
                logtext = json.load(f)
            names = logtext.get("names")
            namesid = list(names.keys())
            if 12 <= len(namesid) <= 13:
                stats = logtext.get("players").get(f"{chatsteamid}")
                classstats = stats.get('class_stats')
                logclasslist = []
                for char in classstats:
                    logclasslist.append(char.get('type'))
                logclasslist_indexes = [logclasslist.index(e) for e in set(logclasslist) & set(classlist)]
                for index in logclasslist_indexes:
                    alias = names.get(f'{chatsteamid}')
                    character = classstats[index].get('type')
                    kills = classstats[index].get('kills')
                    assists = classstats[index].get('assists')
                    deaths = classstats[index].get('deaths')
                    if int(deaths) == 0:
                        kadr = int(kills) + int(assists)
                        kdr = int(kills)
                    else:
                        kadr = round(((int(kills) + int(assists)) / int(deaths)), 2)
                        kdr = round((int(kills) / int(deaths)), 2)
                    dmg = classstats[index].get('dmg')
                    dpm = round(((int(dmg) / classstats[index].get('total_time')) * 60), 2)
                    hs = stats.get('headshots_hit')
                    bs = stats.get('backstabs')
                    map = logtext.get('info').get('map')
                    date = logtext.get('info').get('date')
                    date = datetime.fromtimestamp(date).strftime('%I:%M:%S %p %Z %m/%d/%Y ')
                    b = "https://logs.tf/" + str(log)
                    print(
                        f'<tr><td>{alias}</td><td>{character}</td><td>{kills}</td><td>{assists}</td><td>{deaths}</td><td>{kdr}</td><td>{kadr}</td><td>{dpm}</td><td>{dmg}</td><td>{hs}</td><td>{bs}</td><td>{map}</td><td>{date}</td><td><a href="{b}" target="_blank">{b}</a></td></tr>')
if gamemode == "ud":
    for log in logslist:
        if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json'):
            with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
                logtext = json.load(f)
            names = logtext.get("names")
            namesid = list(names.keys())
            if 4 <= len(namesid) <= 6:
                stats = logtext.get("players").get(f"{chatsteamid}")
                classstats = stats.get('class_stats')
                logclasslist = []
                for char in classstats:
                    logclasslist.append(char.get('type'))
                logclasslist_indexes = [logclasslist.index(e) for e in set(logclasslist) & set(classlist)]
                for index in logclasslist_indexes:
                    alias = names.get(f'{chatsteamid}')
                    character = classstats[index].get('type')
                    kills = classstats[index].get('kills')
                    assists = classstats[index].get('assists')
                    deaths = classstats[index].get('deaths')
                    if int(deaths) == 0:
                        kadr = int(kills) + int(assists)
                        kdr = int(kills)
                    else:
                        kadr = round(((int(kills) + int(assists)) / int(deaths)), 2)
                        kdr = round((int(kills) / int(deaths)), 2)
                    dmg = classstats[index].get('dmg')
                    dpm = round(((int(dmg) / classstats[index].get('total_time')) * 60), 2)
                    hs = stats.get('headshots_hit')
                    bs = stats.get('backstabs')
                    map = logtext.get('info').get('map')
                    date = logtext.get('info').get('date')
                    date = datetime.fromtimestamp(date).strftime('%I:%M:%S %p %Z %m/%d/%Y ')
                    b = "https://logs.tf/" + str(log)
                    print(
                        f'<tr><td>{alias}</td><td>{character}</td><td>{kills}</td><td>{assists}</td><td>{deaths}</td><td>{kdr}</td><td>{kadr}</td><td>{dpm}</td><td>{dmg}</td><td>{hs}</td><td>{bs}</td><td>{map}</td><td>{date}</td><td><a href="{b}" target="_blank">{b}</a></td></tr>')