from _738_twitter.utils import twt2stamp
from _738_twitter.utils import stamp2crs
from _738_twitter.utils import crs2stamp
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import get_followers_ids
from WebUtils.threaded_twitter import get_followers
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import time
from queue import Queue
from threading import Thread, Event


###
#
# PARAMS
#
###

# uncertainty tolerance in seconds
RESOLUTION = 2 * 3600 
# starting point
#FROM = time.time()
FROM = 1587009600
# ending point (-1 for full lists)
UNTIL = 1577836800 # Jan 1st 2020

# path to resources
CRED = 'config/WebUtils/twitterapi_cred.yaml'
USERS = 'config/_738_twitter/Users.yaml'
USERSINFO = 'config/_738_twitter/UsersInfo.yaml'
ISFOLLOWEDBY = 'config/_738_twitter/IsFollowedBy.yaml'
OUTPATH = 'out/_738_twitter'

# list of accounts to collect
COLLECT = [
	('JoeBiden', 939091),
	('KamalaHarris', 30354991),
#	('HillaryClinton', 1339835893),
#	('BarackObama', 813286),
	]


###
#
# CODE
#
###

# load authentication	
credentials = load(CRED)
# open databases
db = {
		'Users':SQLite(USERS),
		'UsersInfo':SQLite(USERSINFO),
		'IsFollowedBy':SQLite(ISFOLLOWEDBY)
		}
# create communication for threads
end = Event()
queues = [Queue(),Queue(),Queue(),Queue()]
# create threads
apis = {
	'users': [
		Thread(
			target=get_followers,
			args=(v, 'user', end, queues[0], queues[1])
			) for k,v in credentials.items()
		],
	'ids' : [
		Thread(
			target=get_followers_ids,
			args=(v, 'user', end, queues[0], queues[1])
			) for k,v in credentials.items()
		],
	'lookup' : [
		Thread(
			target=lookup_users,
			args=(v, 'user', end, queues[2], queues[3])
			) for k,v in credentials.items()	
		]
	}
# start threads
for k,v in apis.items():
	for t in v:
		t.start()
# cache
cache = [[],[],[]]
# begin collection
for username,userid in COLLECT:

	# get info on collected user
	queues[2].put([userid])
	data = queues[3].get()[0]
	creation = twt2stamp(data['created_at'])
	cache[0].append(
		(userid,data['screen_name'],creation))
	print(data['screen_name'])

	# prepare boundaries of collection	
	fr = int(FROM / RESOLUTION) * RESOLUTION
	to = int((UNTIL if UNTIL > 0 else creation) / RESOLUTION) * RESOLUTION

	while fr > to:
		print(f'Timestamp {fr}')
		
		# collect info
		prv = fr
		intervals = {}
		while True:
			print('\t5000')
			queues[0].put((username, stamp2crs(prv), 5000))
			data = queues[1].get()
			nxt = crs2stamp(data['next_cursor'])
			intervals.update({k:[fr-RESOLUTION,fr] for k in data['ids']})
			print('\tinfo')
			for i in range(0, len(data['ids']), 100):
				queues[2].put(data['ids'][i:i+100])
			while not queues[2].empty():
				data = queues[3].get()
				cache[0].extend([
					(u['id'], u['screen_name'], twt2stamp(u['created_at']))
					for u in data])
				cache[1].extend([
					(
						u['id'], int(time.time()), u['name'], u['location'],
						u['description'], u['protected'],
						u['followers_count'], u['friends_count'],
						u['statuses_count'], u['default_profile_image']
						)
					for u in data])
			if nxt <= fr - RESOLUTION:
				break
			prv = nxt
		
		# chunk data RESOLUTION wide
		found = True
		while found:
			print('\t200')
			found = False
			queues[0].put((username, stamp2crs(fr-RESOLUTION), 200))
			data = queues[1].get()
			for u in data['users']:
				if u['id'] in intervals:
					found = True
					fr -= RESOLUTION
					intervals[u['id']] = [fr-RESOLUTION,fr]
					break
		
		# adjust intervals and store
		prev = float('inf')
		for k in intervals:
			if intervals[k][1] < prev:
				prev = intervals[k][1]
			intervals[k] = [prev-RESOLUTION,prev]
		cache[2].extend([(userid, k, v[0], v[1])for k,v in intervals.items()])

		# store when threshold
		if len(cache[0]) > 25000:
			print(f'Storing {len(cache[0])} users')
			db['Users'].insert('Users',cache[0])
			cache[0] = []	
		if len(cache[1]) > 25000:
			print(f'Storing {len(cache[1])} info')
			db['UsersInfo'].insert('UsersInfo',cache[1])
			cache[1] = []
		if len(cache[2]) > 25000:
			print(f'Storing {len(cache[2])} followers')
			db['IsFollowedBy'].insert('IsFollowedBy',cache[2])
			cache[2] = []

	# store remaining
	db['Users'].insert('Users',cache[0])
	db['UsersInfo'].insert('UsersInfo',cache[1])
	db['IsFollowedBy'].insert('IsFollowedBy',cache[2])

# close threads
end.set()
print('closing threads..')
for t in apis['users']:
	queues[0].put(None)
for t in apis['ids']:
	queues[0].put(None)
for t in apis['lookup']:
	queues[2].put(None)
for k,v in apis.items():
	for t in v:
		t.join()

# close dbs
print('closing dbs...')
for k,v in db.items():
	del v
