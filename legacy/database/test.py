import sqlite3
import sys
import os
word = sys.argv[1].lower()
steamid = sys.argv[2]
dir = os.listdir('/srv/http/tf2/Tf2LogSearcher/database/')
i = 0
for file in dir:
    if file.endswith('.sqlite3'):
        conn = sqlite3.connect(file)
        db = conn.cursor()
        cum = db.execute(f"select * from chat where steam_id='{steamid}' and message like '%{word}%'")
        shit = cum.fetchall()
        for index in shit:
            print(f'<a href="https://logs.tf/{index[0]}" target="_blank">{index[0]}</a> {index[3]}: {index[4]}</br>')
            i += 1
        db.close()
print(f'<br>Total {i} occurrences found of "{word}" for <a href="https://logs.tf/profile/{steamid}" target="_blank">{steamid}</a>')