import os
import json
import sys
char = sys.argv[1]
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')
logslist = [log.replace('.json','') for log in logslist]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
logslist = logslist[:20000]
dpmlist = []
acclist = []
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        for player in logtext["players"]:
            stats = logtext["players"][player]["class_stats"]
            for stat in stats:
                if char in stat["type"]:
                    damage = stat["dmg"]
                    if not stat["total_time"] == 0:
                        dpm = round(int(damage) / stat["total_time"] * 60, 2)
                        dpmlist.append(dpm)
                        print(f'Adding {dpm} to list from {char} - {player}')
                        if "scattergun" in stat["weapon"]:
                            if not stat["weapon"]["scattergun"]["shots"] == 0:
                                acc = stat["weapon"]["scattergun"]["hits"]/stat["weapon"]["scattergun"]["shots"]
                                acclist.append(acc)
                                print(f'Adding accuracy of {acc*100}% from {char} - {player}')
print(f'Average {char} DPM: {round(sum(dpmlist) / len(dpmlist), 2)}')
print(f"Average {char} accuracy: {round(sum(acclist) / len(acclist) ,2) * 100}%")
print(len(dpmlist))
print(len(acclist))