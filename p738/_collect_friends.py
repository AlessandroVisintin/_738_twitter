from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import get_friend_ids
from JSONWrap.utils import load

import time
from queue import Queue, PriorityQueue
from threading import Thread, Event


OUT = 'out/p738'
CFG = 'config/p738'

USERNAME, USERID = ('JoeBiden', 939091)
	
credentials = load('config/WebUtils/twitterapi_cred.yaml')


db = {
		'Users' : SQLite(f'{OUT}/Users.db', config=f'{CFG}/Users.yaml'),
		'IsFollowedBy' : SQLite(f'{OUT}/IsFollowedBy.db', config=f'{CFG}/IsFollowedBy.yaml')
		}

end = Event()
queues = [Queue(),Queue()]
apis = [
		Thread(
			target=get_friend_ids,
			args=(v, 'user', end, queues[0], queues[1])
			) for k,v in credentials.items()
		]

for t in apis:
	t.start()

print('Loading priority queue...')
pset = set()
pqueue = PriorityQueue()
query = f'SELECT * FROM IsFollowedBy WHERE Users_id1 = {USERID};'
for row in db['IsFollowedBy'].yields(query=query):
	pqueue.put((2,row[1]))

count = 0
while pqueue.qsize() > 0:
	p, uid = pqueue.get()
	
	print(count, p)
	
	queues[0].put((uid,-1,5000))
	data = queues[1].get()
	if data is None:
		continue
	
	tmp = [(i,None,None) for i in data['ids']]
	db['Users'].fetch(name='insert_ignore', params=tmp)
	tmp = [(i,uid,None,None) for i in data['ids']]
	db['IsFollowedBy'].fetch(name='insert_replace', params=tmp)
	
	uids = '","'.join([str(x) for x in data['ids']])
	query = f'SELECT * FROM IsFollowedBy WHERE Users_id1 = "{uid}" AND Users_id2 IN ("{uids}");'
	intersection = db['IsFollowedBy'].fetch(query=query) 
	for row in intersection:
		pqueue.put((1 / len(intersection), uid))
	
	count += 1

end.set()
print('closing threads..')
for t in apis:
	queues[0].put(None)
for t in apis:
	t.join()

# close dbs
print('closing dbs...')
for k,v in db.items():
	del v
