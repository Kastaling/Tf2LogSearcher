import zipfile
import os
zips = [file for file in os.listdir('./') if 'zip' in file and not "py" in file]
for zip in zips:
    print(zip)
    zip_ref = zipfile.ZipFile(zip, 'r')
    zip_ref.extractall()
    files = os.listdir('./'+zip.replace('.zip',''))
    logsraw = os.listdir("/srv/http/tf2/Tf2LogSearcher/logsraw/")
    if set(logsraw).intersection(set(files)) != 1:
        for file in files:
            print(f'Moving {file} to logsraw')
            os.replace('./'+zip.replace('.zip','')+'/'+file, r"/srv/http/tf2/Tf2LogSearcher/logsraw/"+file)
        os.replace('./'+zip, r"./logsarchive/"+zip)
    else:
        print(f"Already unpacked {zip}")
    os.rmdir('./'+zip.replace('.zip',''))