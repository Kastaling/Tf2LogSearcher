import os
import sys
import json
import csv
logslist = [log.replace('.json','') for log in os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
logslist = logslist[:20000]
gamemode = sys.argv[1]
if "hl" in gamemode:
    number = 18
elif "6s" in gamemode:
    number = 12
buildtimes = []
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs/'+str(log)+'.json','r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        if logtext["success"]:
            namesid = list(logtext["names"].keys())
            if number == len(namesid):
                for player in logtext["players"]:
                    if "medicstats" in logtext["players"][player].keys():
                        if "avg_time_to_build" in logtext["players"][player]["medicstats"].keys():
                            buildtimes.append((logtext["players"][player]["medicstats"]["avg_time_to_build"],log))
with open(f'{gamemode} test uber averages.csv', 'w', newline='') as csvfile:
    file = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
    file.writerows(buildtimes)
print(f"The average time to build for {gamemode} is {sum([x[0] for x in buildtimes]) / len([x[0] for x in buildtimes])}")