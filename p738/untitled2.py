from StorageUtils.SQLite import SQLite
from TimeUtils.utils import str2stamp

import matplotlib.pyplot as plt


OUT = 'out/p738'
CFG = 'config/p738'
START = int(str2stamp('2020-06-01 00:00:00'))
END = int(str2stamp('2021-09-01 00:00:00'))

db = SQLite(f'{OUT}/JoeBiden.db', config=f'{CFG}/p738.yaml')

q = (
	  'SELECT (a.b1 + a.b2) / 2, b.class FROM Fws a '
	  'INNER JOIN BotClass b ON a.id = b.id;'
	  )

OUT = {}
for i,row in enumerate(db.yields(query=q)):
	if not i % 100000: print(i)
	
	day = int(row[0] / 86400) * 86400
	
	if day < START or day > END:
		continue
	
	try:
		OUT[day][row[1]] += 1
	except KeyError:
		OUT[day] = [0,0,0]
		OUT[day][row[1]] += 1

X = sorted(OUT.keys())
Y = [
	  [OUT[x][0] / sum(OUT[x]) for x in X],
	  [OUT[x][1] / sum(OUT[x]) for x in X],
	  [OUT[x][2] / sum(OUT[x]) for x in X]
	  ]

plt.plot(X,Y[0], c='blue')
plt.plot(X,Y[1], c='orange')
plt.plot(X,Y[2], c='red')

plt.show()
#print(db.size('BotClass'))