from scipy import stats
import pandas as pd
import sys
map = sys.argv[1]
file = f"./{map} winrates.csv"
df = pd.read_csv(file, header=None, usecols=[0,1])
print(stats.ttest_rel(df[0],df[1]))