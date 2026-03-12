#!/usr/bin/python3
import sys
import requests
import os
import json
import re
import xlwt
from xlwt import Workbook
word = sys.argv[1]
leaderboard = {}
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')
for log in logslist:
    if os.path.exists(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log)):
        with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log), 'r') as f:
            logtext = json.load(f)
        chat = logtext.get('chat')
        for dict in chat:
            message = dict.get('msg')
            steamid = dict.get('steamid')
            if word.lower() in message.lower():
                print("Word Found")
                if steamid in leaderboard:
                    print(f"Adding 1 to total for {steamid}")
                    leaderboard[steamid] += 1
                elif steamid.startswith("[U:"):
                    print(f"Creating key and adding 1 for {steamid}")
                    leaderboard[steamid] = 1
wb = Workbook()
sheet1 = wb.add_sheet('Sheet 1')
a = 0
for key in leaderboard:
    temp = re.split(':|]',key)
    steamid64 = int(temp[2]) + 76561197960265728

    #response = (link).json().get('response').get('players')
    #if link.status_code == 200 and len(response) > 1:
    #    link = response[0].get('profileurl')
    #    alias = response[0].get('personaname')
    #link = f"https://logs.tf/profile/{steamid64}"
    #alias = steamid64
    link = f'https://steamcommunity.com/profiles/{steamid64}/'
    alias = f"https://logs.tf/profile/{steamid64}"
    number = leaderboard[key]
    sheet1.write(a,0,link)
    sheet1.write(a,1,alias)
    sheet1.write(a,2,word)
    sheet1.write(a,3,number)
    print(f"{alias} - {number}")
    a = a+1
wb.save(f'{word} leaderboard results.xls')
