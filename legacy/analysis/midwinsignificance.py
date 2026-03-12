import os
import sys
import json
logslist = [log.replace('.json','') for log in os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
#logslist = logslist[:20000]
sameteamwin = []
diffteamwin = []
gametype = sys.argv[1]
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs/'+str(log)+'.json','r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        if logtext["success"]:
            if gametype in logtext["info"]["map"]:
                for round in logtext["rounds"]:
                    for event in round["events"]:
                        #add a break line so it doesnt go through every event dipshit
                        if "pointcap" in event["type"]:
                            if not round["winner"] is None:
                                if "Red" in event["team"]:
                                    if "Red" in round["winner"]:
                                        sameteamwin.append(1)
                                        break
                                    elif "Blue" in round["winner"]:
                                        diffteamwin.append(1)
                                        break
                                elif "Blue" in event["team"]:
                                    if "Red" in round["winner"]:
                                        sameteamwin.append(1)
                                        break
                                    elif "Blue" in round["winner"]:
                                        diffteamwin.append(1)
                                        break
print(f'Number of rounds total: {len(sameteamwin)+len(diffteamwin)} \nNumber of same team round wins: {len(sameteamwin)} \nNumber of different team round wins: {len(diffteamwin)}.')
print(f"The probability on {gametype} that the same team will win both the mid and the round is {(len(sameteamwin) / (len(sameteamwin)+len(diffteamwin))) * 100}%.")
print(f"The probability on {gametype} that one team will win the mid and then lose the round is {(len(diffteamwin) / (len(sameteamwin)+len(diffteamwin))) * 100}%.")