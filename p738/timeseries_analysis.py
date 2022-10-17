from TimeUtils.utils import str2stamp

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import numpy as np


TIMESERIES = 'out/_738_twitter/locations.txt'


w, ratio = 10, 0.4
plt.rcParams["figure.figsize"] = [w, w*ratio]
plt.rcParams["figure.autolayout"] = True


FROM = str2stamp('2021-08-01 00:00:00')
TO = str2stamp('2021-09-01 00:00:00')

out = {'usa':{},'india':{}}
with open(TIMESERIES, 'r') as f:
	for line in f:
		start, end, loc = line.split('\t')
		
		if int(start) >= FROM and int(end) < TO:		
			idx = int((int(start) + int(end)) / 2)
			try:
				out[loc.strip()][idx] += 1
			except KeyError:
				out[loc.strip()][idx] = 1

X_usa = [k for k,v in out['usa'].items()]
Y_usa = [v for k,v in out['usa'].items()]

X_india = [k for k,v in out['india'].items()]
Y_india = [v for k,v in out['india'].items()]
		

ax1 = plt.subplot()
l1, = ax1.plot(X_usa, np.log2(Y_usa), color=(0,0,0,.33), linewidth=1, linestyle='--')
ax2 = ax1.twinx()
l2, = ax2.plot(X_india, np.log2(Y_india), color='blue', linewidth=.75)

plt.show()


	
   # REF = 'data/india_merged_JoeBiden'
    # TIME = 'data/timeseries/timeseries_JoeBiden'
    
    # df1 = pd.read_csv(REF, sep=' ', header=None, names=['time','a','b','c'], index_col='time')
    # df1.index = pd.to_datetime(df1.index, unit='s')
    # df1 = df1.sort_index()
    # df1 = df1.resample('24H').sum()

    # df2 = pd.read_csv(TIME, sep=' ', header=None, names=['time', 'value'], index_col='time')
    # df2.index = pd.to_datetime(df2.index, unit='s')
    # df2 = df2.sort_index()
    # df2 = df2.resample('24H').sum()
    # df2[df2 == 0] = 1

    # df3 = pd.DataFrame({'ratio': df1.a / df2.value})
    
    # start = dt.datetime.strptime('2020-01-01', '%Y-%m-%d')
    # i, d = 0, 365*24
    # fr = start + dt.timedelta(hours=i * d)
    # to = start + dt.timedelta(hours=(i+1) * d)
    
    # df2 = df2[fr:to]
    # df3 = df3[fr:to]
    

    
    # dates = [
    #     '2020-02-29 05:00:00', '2020-11-03 05:00:00',
    #     '2020-05-25 05:00:00', '2020-08-11 05:00:00'
    # ]
    # dates = [dt.datetime.utcfromtimestamp(int(str2stamp(x))) for x in dates]
    
    # plt.axvline(dates[0], color=(0,0,0,.2), linewidth=.8)
    # plt.text(dates[0], 6.25, "South Carolina\nDem. Primary", verticalalignment='top')
    
    # plt.axvline(dates[1], color=(0,0,0,.2), linewidth=.8)
    # plt.text(dates[1], 6.25, "Election day", verticalalignment='top')
    
    # plt.axvline(dates[2], color=(0,0,0,.2), linewidth=.8)
    # plt.text(dates[2], 6.25, "Memorial day", verticalalignment='top')
    
    # plt.axvline(dates[3], color=(0,0,0,.2), linewidth=.8)
    # plt.text(dates[3], 6.25, "Kamala Harris\nAnnouncement", verticalalignment='top')
    
    # ax1 = plt.subplot()
    # l1, = ax1.plot(df2.index, np.log10(df2.value), color=(0,0,0,.33), linewidth=1, linestyle='--')
    # ax2 = ax1.twinx()
    # l2, = ax2.plot(df3.index, df3.ratio, color='blue', linewidth=.75)
    
    # ax1.set_yticks([x for x in range(1,7)])
    # ax1.set_yticklabels(['10', '100', '1K', '10K', '100K', '1M'])
    # ax2.set_yticklabels([f'{round(x*100, 1)}%' for x in ax2.get_yticks().tolist()])
    
    # ax1.tick_params(axis='both', which='major', labelsize=13)
    # ax2.tick_params(axis='both', which='major', labelsize=13)
    
    # locator = mdates.MonthLocator()  # every month
    # fmt = mdates.DateFormatter('%b')
    # X = plt.gca().xaxis
    # X.set_major_locator(locator)
    # X.set_major_formatter(fmt)

    # ax1.set_ylabel('Followers trend (flws/hour)\n', fontweight='bold', fontsize=15)
    # ax2.set_ylabel('\nIndian profiles (%)', fontweight='bold', fontsize=15)
    # ax1.set_xlabel('\nUTC timestamps (date-hour)', fontweight='bold', fontsize=15)
    # plt.legend([l1, l2], ['Followers trend', 'Indian profiles'], loc='lower right')
    # plt.grid(visible=True, linewidth=0.75, linestyle='--', c=(.9,.9,.9))
    
    # plt.savefig('biden_indian.pdf', dpi=300, bbox_inches='tight')
    # #plt.show()
