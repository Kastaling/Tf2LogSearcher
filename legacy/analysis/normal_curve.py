import numpy as np
import scipy.stats as stats
import pylab as pl
import pandas as pd
from matplotlib import pyplot as plt
from scipy.stats import norm
file = f"./hl uber averages.csv"
df = pd.read_csv(file, header=None, usecols=[0])
df[0].sort_values()
fit = stats.norm.pdf(df[0], np.mean(df[0]), np.std(df[0]))  #this is a fitting indeed
std = np.std(df[0])
mean = np.mean(df[0])
domain = np.linspace(np.min(df[0]),np.max(df[0]))
plt.plot(domain, norm.pdf(domain,mean,std))
plt.hist(df[0],density=True)
plt.show()
#pl.plot(df[0],fit,'-o')

#pl.hist(df[0])      #use this to draw histogram of your data

#pl.show()    