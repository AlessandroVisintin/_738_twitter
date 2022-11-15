from StorageUtils.SQLite import SQLite

import joblib
import numpy as np
import pandas as pd


OUT = 'out/p738'
CFG = 'config/p738'

Users = SQLite(f'{OUT}/Users.db', config=f'{CFG}/Users.yaml')
UsersInfo = SQLite(f'{OUT}/UsersInfo.db', config=f'{CFG}/UsersInfo.yaml')
IsBot = SQLite(f'{OUT}/IsBot.db', config=f'{CFG}/IsBot.yaml')

model = joblib.load(f'{OUT}/random_forest_model.joblib')
count = 0
cache = {}
data = {
	'id':[], 'created_at':[], 'updated':[],
	'location':[], 'description':[], 'followers_count':[],
	'friends_count':[], 'statuses_count':[], 'default_profile_image':[]
	
	}
for row in Users.yields(name='select_all'):
	count += 1
	
	cache[row[0]] = row[2]
	if len(cache) > 5000:
		done = 0
		ids = ','.join([str(x) for x in cache.keys()])
		for detail in UsersInfo.yields(name='select_idlist', format={'joined_ids':ids}):
			if detail[0] in cache:
				done += 1
				data['id'].append(detail[0])
				data['created_at'].append(cache[detail[0]])
				data['updated'].append(detail[1])
				data['location'].append(detail[2])
				data['description'].append(detail[3])
				data['followers_count'].append(detail[4])
				data['friends_count'].append(detail[5])
				data['statuses_count'].append(detail[6])
				data['default_profile_image'].append(False if detail[7] == 0 else True)
		
		DATA = pd.DataFrame(data)
		FEATURES = pd.DataFrame()
		FEATURES = FEATURES.assign(
			a = DATA['friends_count'] / DATA['followers_count']**2,
			b = DATA['updated'] - DATA['created_at'],
			c = DATA['statuses_count'],
			e = DATA['friends_count'],
			g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
			h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
			j = DATA['default_profile_image'],
			k = DATA['friends_count'] / DATA['followers_count'] >= 50,
			l = 'bot' in DATA['description'].str.lower(),
			n = 2 * DATA['followers_count'] < DATA['friends_count'],
			o = ~(DATA['location'].notnull() & DATA['location'] != ''),
			p = ~((DATA['description'].notnull() & DATA['description'] != '') | (DATA['location'].notnull() & DATA['location'] != '') | DATA['friends_count'] < 100),
			q = ~(DATA['description'].notnull() & DATA['description'] != ''),
			r = DATA['followers_count'],
			)
 	
		np.nan_to_num(FEATURES['a'], copy=False, posinf=np.float32(2**127))
		pred = np.where(model.predict(FEATURES) > 0.5, True, False)
		params = [(int(i),int(j)) for i,j in zip(data['id'],pred)]
		IsBot.fetch(name='insert_all', params=params)
		cache = {}
		data = {
			'id':[], 'created_at':[], 'updated':[],
			'location':[], 'description':[], 'followers_count':[],
			'friends_count':[], 'statuses_count':[], 'default_profile_image':[]
			}
		
		print(count, done)

ids = ','.join([str(x) for x in cache.keys()])
for detail in UsersInfo.yields(name='select_idlist', format={'joined_ids':ids}):
	if detail[0] in cache:
		data['id'].append(detail[0])
		data['created_at'].append(cache[detail[0]])
		data['updated'].append(detail[1])
		data['location'].append(detail[2])
		data['description'].append(detail[3])
		data['followers_count'].append(detail[4])
		data['friends_count'].append(detail[5])
		data['statuses_count'].append(detail[6])
		data['default_profile_image'].append(False if detail[7] == 0 else True)

DATA = pd.DataFrame(data)
FEATURES = pd.DataFrame()
FEATURES = FEATURES.assign(
	a = DATA['friends_count'] / DATA['followers_count']**2,
	b = DATA['updated'] - DATA['created_at'],
	c = DATA['statuses_count'],
	e = DATA['friends_count'],
	g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
	h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
	j = DATA['default_profile_image'],
	k = DATA['friends_count'] / DATA['followers_count'] >= 50,
	l = 'bot' in DATA['description'].str.lower(),
	n = 2 * DATA['followers_count'] < DATA['friends_count'],
	o = ~(DATA['location'].notnull() & DATA['location'] != ''),
	p = ~((DATA['description'].notnull() & DATA['description'] != '') | (DATA['location'].notnull() & DATA['location'] != '') | DATA['friends_count'] < 100),
	q = ~(DATA['description'].notnull() & DATA['description'] != ''),
	r = DATA['followers_count'],
	)
 	
np.nan_to_num(FEATURES['a'], copy=False, posinf=np.float32(2**127))
pred = np.where(model.predict(FEATURES) > 0.5, True, False)
params = [(int(i),int(j)) for i,j in zip(data['id'],pred)]
IsBot.fetch(name='insert_all', params=params)
cache = {}
data = {
	'id':[], 'created_at':[], 'updated':[],
	'location':[], 'description':[], 'followers_count':[],
	'friends_count':[], 'statuses_count':[], 'default_profile_image':[]
	}

del Users
del UsersInfo
del IsBot
