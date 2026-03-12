from requests_html import HTMLSession
import os
session = HTMLSession()
resp = session.get('https://rgl.gg/Public/PlayerBanList.aspx?r=24')
for x in range(170):
    if not os.path.exists(f'./bans/{x}.html'):
        print(f'Writing {x}.html to files')
        f = open(f"./bans/{x}.html","w")
        f.write(resp.html.html)
        f.close()
        resp.html.render(sleep=5,script="javascript:__doPostBack('ctl00$ContentPlaceHolder1$btnNext','')")
    else:
        print(f'{x}.html already exists')