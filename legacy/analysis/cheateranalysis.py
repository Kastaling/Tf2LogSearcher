import os
import json
import sys
char = sys.argv[1]
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')
logslist = [log.replace('.json','') for log in logslist]
logslist = [int(i) for i in logslist]
logslist.sort(reverse=True)
logslist = logslist[:20000]
f = open('cheater list.txt','r')
cheaterlist = [s.replace('\n','') for s in f.readlines()]
dpmlist = []
acclist = []
#def primary(character):
#    if scout
def tothree(id):
    a = int(id) - 76561197960265728
    return f"[U:1:{a}]"
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs//' + str(log) + '.json', 'r') as f:
        try:
            logtext = json.load(f)
        except:
            continue
        names = logtext.get("names")
        namesid = list(names.keys())
        if any(name in namesid for name in [tothree(s) for s in cheaterlist]):
            common = list(set([tothree(s) for s in cheaterlist]).intersection(set(namesid)))
            for cheater in common:
                players = logtext["players"]
                stats = players[cheater]["class_stats"]
                for h in stats:
                    if char in h["type"]:
                        damage = h["dmg"]
                        dpm = round(int(damage) / h["total_time"] * 60, 2)
                        dpmlist.append(dpm)
                        print(f'Adding {dpm} to list from cheater {cheater}')
                        if "scattergun" in h["weapon"]:
                            if not h["weapon"]["scattergun"]["shots"] == 0:
                                acc = h["weapon"]["scattergun"]["hits"]/h["weapon"]["scattergun"]["shots"]
                                acclist.append(acc)
                                print(f'Adding accuracy of {acc*100}% from cheater {cheater}')
print(f'Average {char} DPM: {round(sum(dpmlist) / len(dpmlist), 2)}')
print(f"Average {char} accuracy: {round(sum(acclist) / len(acclist) ,2) * 100}%")
print(len(dpmlist))
print(len(acclist))
                    