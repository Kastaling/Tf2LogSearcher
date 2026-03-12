import os
logsraw = os.listdir("/srv/http/tf2/Tf2LogSearcher/logsraw")
for file in logsraw:
    if "zip" in file:
        print(f'Removing {file}')
        os.remove("/srv/http/tf2/Tf2LogSearcher/logsraw/"+file)