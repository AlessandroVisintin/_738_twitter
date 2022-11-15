from p738.utils import twt2stamp

from StorageUtils.SQLite import SQLite
from WebUtils.threaded_twitter import lookup_users
from JSONWrap.utils import load
from TimeUtils.utils import str2stamp

import time
from queue import Queue
from threading import Thread, Event


PATH = 'out/p738'
db = SQLite(f'{PATH}/Twibot.db', config='config/p738/Twibot.yaml')
credentials = load('config/WebUtils/twitterapi_cred.yaml')

end = Event()
in_queue = Queue()
out_queue = Queue()

apis = [
		Thread(
			target=lookup_users,
			args=(v, 'user', end, in_queue, out_queue)
			) for k,v in credentials.items()
		]

for t in apis:
	t.start()

cache = {} 
with open(f'{PATH}/TwiBot-22/label.csv', 'r') as f:
	next(f)
	for i,line in enumerate(f):
		userid, typ = line.split(',')
		cache[int(userid[1:])] = typ.strip()
	
		if len(cache) == 100:
			print(i)
			in_queue.put(list(cache.keys()))
			data = out_queue.get()
			
			insert = []
			for u in data:
				insert.append((
					u['id'],
					u['name'],
					u['screen_name'],
					u['statuses_count'],
					u['followers_count'],
					u['friends_count'],
					u['favourites_count'],
					u['listed_count'],
					twt2stamp(u['created_at']),
					None if not 'url' in u else u['url'],
					None if not 'location' in u else u['location'],
					u['default_profile'],
					u['default_profile_image'],
					None if not 'profile_banner_url' in u else u['profile_banner_url'],
					u['profile_background_image_url_https'],
					u['profile_image_url_https'],
					u['protected'],
					u['verified'],
					None if not 'description' in u else u['description'],
					int(time.time()),
					cache[u['id']],
					))
				
			db.fetch(name='insert_all', params=insert)
			cache = {}
			time.sleep(2)

in_queue.put(list(cache.keys()))
data = out_queue.get()
			
insert = []
for u in data:
	insert.append((
		u['id'],
		u['name'],
		u['screen_name'],
		u['statuses_count'],
		u['followers_count'],
		u['friends_count'],
		u['favourites_count'],
		u['listed_count'],
		twt2stamp(u['created_at']),
		None if not 'url' in u else u['url'],
		None if not 'location' in u else u['location'],
		u['default_profile'],
		u['default_profile_image'],
		None if not 'profile_banner_url' in u else u['profile_banner_url'],
		u['profile_background_image_url_https'],
		u['profile_image_url_https'],
		u['protected'],
		u['verified'],
		None if not 'description' in u else u['description'],
		int(time.time()),
		cache[u['id']],
		))
	
db.fetch(name='insert_all', params=insert)

end.set()
for t in apis:
	in_queue.put(None)
for t in apis:
	t.join()
	