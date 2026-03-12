import os
import json
import sys
log = sys.argv[1]
f = open(f'/srv/http/tf2/Tf2LogSearcher/logsraw/log_{log}.log','r')
rawlines = [s.replace('\n','') for s in f.readlines()]
hitshots = []
goodlines = []
for ind,line in enumerate(rawlines):
    if not "[U:1:107081628]" in line and not "quake_rl" in line:
        rawlines.pop(ind)
    elif "[U:1:107081628]" in line and "quake_rl" in line:
        goodlines.append(line)
for ind,line in enumerate(goodlines):
    if "[U:1:107081628]" in line and "quake_rl" in line:
        if "shot_fired" in line:
            if "shot_hit" in rawlines[ind+1]:
                hitshots.append(1)
            else:
                hitshots.append(0)
accuracy = sum(hitshots) / len(hitshots) * 100
print(accuracy)