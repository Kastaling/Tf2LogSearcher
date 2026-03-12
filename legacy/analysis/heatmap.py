import os
import json
import sys
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib.image as mpimg 
import sqlite3
import seaborn as sns
map = sys.argv[1]
file = f"./{map}.csv"
df = pd.read_csv(file, header=None, usecols=[0,1])
map_img = mpimg.imread(f'{map}.png') 
hmax = sns.kdeplot(df[0], df[1], cmap="Reds", shade=True, bw=.15)
hmax.collections[0].set_alpha(0)
if 'metalworks' in map:
    xmin = -3034
    xmax = 3374 
    ymin = -6699
    ymax = 4939
elif 'product' in map:
    xmin = -2859
    xmax = -171
    ymin = -3668
    ymax = 3776
elif 'process' in map:
    xmin = -5222
    xmax = 5216 
    ymin = -3146
    ymax = 3128
plt.imshow(map_img, zorder=0, extent=[xmin, xmax, ymin, ymax],resample=False)
plt.savefig(f'{map} heatmap.png', dpi=1200)
plt.show()
#1617.79	-914.43	200

#Overview: scale 12.00, pos_x -6196, pos_y 5236
