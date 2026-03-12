import zipfile
import requests
import os
url = "https://drops.tf/raw_logs/2000000/"
for zip in range(2000000,3000000,1000):
    if not os.path.exists(f"{zip}.zip"):
        print(f"Downloading {zip}.zip")
        logdata = requests.get(url+str(zip)+'.zip')
        if logdata.status_code == 200:
            print(f"Saving {zip}.zip")
            open(rf'{zip}.zip', 'wb').write(logdata.content)