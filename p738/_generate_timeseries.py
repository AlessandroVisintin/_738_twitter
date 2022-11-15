from StorageUtils.SQLite import SQLite


OUT = 'out/p738'
CFG = 'config/p738'

username, userid = ('JoeBiden', 939091)

IsFollowedBy = SQLite(f'{OUT}/IsFollowedBy.db', config=f'{CFG}/IsFollowedBy.yaml')
IsBot = SQLite(f'{OUT}/IsBot.db', config=f'{CFG}/IsBot.yaml')

count = 0
cache = {}
out = {}
query = f'SELECT * FROM IsFollowedBy WHERE Users_id1 = {userid};'
for row in IsFollowedBy.yields(query=query):
	count += 1
	
	cache[row[1]] = (row[2],row[3])
	if len(cache) > 10000:
		done = 0
		ids = ','.join([str(x) for x in cache.keys()])
		for detail in IsBot.yields(name='select_idlist', format={'joined_ids':ids}):
			done += 1
			timeframe = cache[detail[0]]
			bot = detail[1]
			try:
				out[timeframe][bot] += 1
				out[timeframe][2] += 1
			except KeyError:
				out[timeframe] = [0, 0, 1]
				out[timeframe][bot] += 1
		cache = {}
		print(count, done)

ids = ','.join([str(x) for x in cache.keys()])
for detail in IsBot.yields(name='select_idlist', format={'joined_ids':ids}):
	timeframe = cache[detail[0]]
	bot = detail[1]
	try:
		out[timeframe][bot] += 1
		out[timeframe][2] += 1
	except KeyError:
		out[timeframe] = [0, 0, 1]
		out[timeframe][bot] += 1

with open(f'{OUT}/bot_timeseries.txt', 'w') as f:
	for k in sorted(out.keys(), key=lambda t:t[0]):
		v = out[k]
		f.write(f'{k[0]}\t{k[1]}\t{v[0]}\t{v[1]}\t{v[2]}\n')
