import os
import json
logs = [int(s.replace('.json','')) for s in os.listdir('/srv/http/tf2/Tf2LogSearcher/logs')]
logs.sort()
print(f"Start:{logs[0]} \nEnd:{logs[-1]}")
badlogs = []
for log in logs:
    with open(f'/srv/http/tf2/Tf2LogSearcher/logs/{log}.json','r') as f:
        try:
            logjson = json.load(f)
        except json.JSONDecodeError:
            print(f'{log} is not a JSON!')
            badlogs += [log]
            continue
        if not logjson.get('success'):
            error = logjson.get('error')
            print(f'Bad Log ({log}) found. Error: {error}')
print(badlogs)
