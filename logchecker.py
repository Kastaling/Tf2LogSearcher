import os
logslist = os.listdir('/srv/http/tf2/Tf2LogSearcher/logs/')
logslist = [s.replace('.json','') for s in logslist]
logslist = [int(log) for log in logslist]
logslist.sort()
def missing(list):
    start = int(list[0])
    end = int(list[-1])
    return sorted(set(range(start, end + 1)).difference(list))
print(missing(logslist))
print(f'Min: {logslist[0]} | Max: {logslist[-1]}')