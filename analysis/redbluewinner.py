import os
import json
import csv
import sys
map = sys.argv[1]
logslist = os.listdir(r'/srv/http/tf2/Tf2LogSearcher/logs/')
logslist = [log.replace('.json','') for log in logslist]
logslist = [int(i) for i in logslist]
logslist.sort()
games = 0
bluwins = 0
redwins = 0
draws = 0
scorelist = []
for log in logslist:
    with open(r'/srv/http/tf2/Tf2LogSearcher/logs//'+str(log)+'.json','r') as f:
        try:
            logtext = json.loads(f.read())
        except json.JSONDecodeError:
            print(f'{log} is not a JSON')
            continue
        if logtext["success"]:
            if map in logtext["info"]["map"]:
                print(f'Matching game ({log}) found.')
                games += 1
                redscore = logtext["teams"]["Red"]["score"]
                bluscore = logtext["teams"]["Blue"]["score"]
                scores = (redscore, bluscore)
                scorelist.append(scores)
                if redscore < bluscore:
                    bluwins += 1
                    print('Adding win for Blu')
                elif redscore > bluscore:
                    redwins += 1
                    print('Adding win for Red')
                else:
                    draws += 1
                    print('Adding draw.')
print(f'Blu Wins = {bluwins}. Red Wins = {redwins}. Draws = {draws}. Total games = {games}.')
print(f'Blu win % = {round((bluwins / games)*100,2)}%. Red win % = {round((redwins / games)*100,2)}%. Draw % = {round((draws / games)*100,2)}%.')
with open(f'{map} winrates.csv', 'w', newline='') as csvfile:
    file = csv.writer(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
    for score in scorelist:
        file.writerow(score)