import os
import sys
import json
logslist = [log.replace('.json','') for log in os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
#logslist = logslist[:20000]
winnerdrops = 0
loserdrops = 0
neutraldrops = 0
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs/'+str(log)+'.json','r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        if logtext["success"]:
            for round in logtext["rounds"]:
                windrops = 0
                losedrops = 0
                for event in round["events"]:
                    if event["type"] == "drop":
                        if round["winner"] == event["team"]:
                            windrops += 1
                        else:
                            losedrops += 1
                if windrops < losedrops:
                    loserdrops += 1
                elif windrops > losedrops:
                    winnerdrops += 1
                elif windrops == losedrops and windrops != 0:
                    neutraldrops += 1
print(f'The percentage of games where the team that dropped more lost is: {(loserdrops / (loserdrops+winnerdrops+neutraldrops)) * 100}%.')
print(f'The percentage of games where the team that dropped more won is: {(winnerdrops / (loserdrops+winnerdrops+neutraldrops)) * 100}%.')
print(f'The percentage of games where both teams dropped the same amount is: {(neutraldrops / (loserdrops+winnerdrops+neutraldrops)) * 100}%.')