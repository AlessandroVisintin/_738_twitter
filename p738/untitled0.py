from p660.utils import parse_user_object
from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load

import sys
import time
from queue import Queue
from threading import Thread


OUT = 'out/p738'
CFG = 'config/p738'

qs = [Queue(), Queue()]
ts = [Thread(target=lookup_users, args=(k, 'user', qs[0], qs[1]))
		for name, k in load('config/WebUtils/twitterapi_cred.yaml').items()]
for t in ts:
	t.start() 


old = SQLite(f'{OUT}/JoeBiden.db')
new = SQLite(f'{OUT}/_JoeBiden.db', config=f'{CFG}/p738.yaml')

for i,rows in enumerate(old.fetchmany(batch=100, query='SELECT * FROM Users;')):
	print(i)
	
	ids = {x[0]:x for x in rows}
	
	qs[0].put(list(ids.keys()))
	ids = [parse_user_object(x) for x in qs[1].get()]
	
	new.fetch(name='insert_Users', params=ids)
	
	time.sleep(0.05)

del new
del old
