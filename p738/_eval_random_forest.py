from p738.utils import twt2stamp

from TimeUtils.utils import str2stamp

import numpy as np
import joblib
import pandas as pd


def features(DATA):
	
	return pd.DataFrame().assign(
		a = DATA['friends_count'] / DATA['followers_count']**2,
		b = DATA['updated'] - DATA['created_at'],
		c = DATA['statuses_count'],
		d = ~(DATA['name'].notnull() & DATA['name'] != ''),
		e = DATA['friends_count'],
		f = ~DATA['url'].notnull(),
		g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
		h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
		i = ~(DATA['listed_count'] > 0),
		j = DATA['default_profile_image'],
		k = DATA['friends_count'] / DATA['followers_count'] >= 50,
		l = 'bot' in DATA['description'].str.lower(),
		n = 2 * DATA['followers_count'] < DATA['friends_count'],
		o = ~(DATA['location'].notnull() & DATA['location'] != ''),
		p = ~((DATA['description'].notnull() & DATA['description'] != '') | (DATA['location'].notnull() & DATA['location'] != '') | DATA['friends_count'] < 100),
		q = ~(DATA['description'].notnull() & DATA['description'] != ''),
		r = DATA['followers_count']
	)

# 	return pd.DataFrame().assign(
# 	 	a = DATA['friends_count'] / DATA['followers_count']**2,
# 	 	b = DATA['updated'] - DATA['created_at'],
# 	 	c = DATA['statuses_count'],
# 	 	e = DATA['friends_count'],
# 	 	g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
# 	 	h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
# 	 	j = DATA['default_profile_image'],
# 	 	k = DATA['friends_count'] / DATA['followers_count'] >= 50,
# 	 	l = 'bot' in DATA['description'].str.lower(),
# 	 	n = 2 * DATA['followers_count'] < DATA['friends_count'],
# 	 	o = ~(DATA['location'].notnull() & DATA['location'] != ''),
# 	 	p = ~((DATA['description'].notnull() & DATA['description'] != '') | (DATA['location'].notnull() & DATA['location'] != '') | DATA['friends_count'] < 100),
# 	 	q = ~(DATA['description'].notnull() & DATA['description'] != ''),
# 	 	r = DATA['followers_count'],
# 	 	)


PATH = 'out/p738'

DATA = {
	0 : f'{PATH}/FameForSale/TEST_DATASET/hum.csv',
	1 : f'{PATH}/FameForSale/TEST_DATASET/fak.csv'
	}

model = joblib.load(f'{PATH}/rf_twibot.joblib')

CONVERTERS = {
	'id' : lambda x: -1 if x == 'nan' else int(x),
	'statuses_count' : lambda x: -1 if x == 'nan' else int(x),
	'followers_count' : lambda x: -1 if x == 'nan' else int(x),
	'friends_count' : lambda x: -1 if x == 'nan' else int(x),
	'favourites_count' : lambda x: -1 if x == 'nan' else int(x),
	'listed_count' : lambda x: -1 if x == 'nan' else int(x),
	'utc_offset' : lambda x: -1 if x == 'nan' or x == '' else int(x),
	'default_profile' : lambda x: False if x == 'nan' else bool(x),
	'is_translator' : lambda x: False if x == 'nan' else bool(x),
	'follow_request_sent' : lambda x: False if x == 'nan' else bool(x),
	'following' : lambda x: False if x == 'nan' else bool(x),
	'notifications' : lambda x: False if x == 'nan' else bool(x),
	'contributors_enabled' : lambda x: False if x == 'nan' else bool(x),
	'default_profile_image' : lambda x: False if x == 'nan' else bool(x),
	'geo_enabled' : lambda x: False if x == 'nan' else bool(x),
	'profile_use_background_image' : lambda x: False if x == 'nan' else bool(x),
	'profile_background_tile' : lambda x: False if x == 'nan' else bool(x),
	'protected' : lambda x: False if x == 'nan' else bool(x),
	'verified' : lambda x: False if x == 'nan' else bool(x),
	'name' : lambda x: str(x),
	'screen_name': lambda x: str(x),
	'url' : lambda x: None if x == 'nan' else str(x),
	'time_zone' : lambda x: None if x == 'nan' else str(x),
	'location' : lambda x: None if x == 'nan' else str(x),
	'profile_image_url' : lambda x: None if x == 'nan' else str(x),
	'profile_banner_url' : lambda x: None if x == 'nan' else str(x),
	'profile_background_image_url_https' : lambda x: None if x == 'nan' else str(x),
	'profile_text_color' : lambda x: None if x == 'nan' else str(x),
	'profile_image_url_https' : lambda x: None if x == 'nan' else str(x),
	'profile_sidebar_border_color' : lambda x: None if x == 'nan' else str(x),
	'profile_sidebar_fill_color' : lambda x: None if x == 'nan' else str(x),
	'profile_background_image_url' : lambda x: None if x == 'nan' else str(x),
	'profile_background_color' : lambda x: None if x == 'nan' else str(x),
	'profile_link_color' : lambda x: None if x == 'nan' else str(x),
	'description' : lambda x: None if x == 'nan' else str(x),
	'updated' : str2stamp,
	'dataset' : lambda x: None if x == 'nan' else str(x),
	'created_at' : twt2stamp,
	'crawled_at' : str2stamp
	}

PRED = {}
for k,v in DATA.items():
	data = pd.read_csv(v, converters=CONVERTERS)
	feat = features(data)
	np.nan_to_num(feat['a'], copy=False, posinf=np.float32(2**127))
	pred = np.where(model.predict(feat) > 0.5, 1, 0)
	
	true = len(pred[pred == k])
	false = len(pred) - true

	PRED[(k,k)] = true
	PRED[(k,(k+1)%2)] = false

STATS = {
	'TP rate' : PRED[(1,1)] / (PRED[(1,1)] + PRED[(1,0)]),
	'FP rate' : PRED[(0,1)] / (PRED[(0,1)] + PRED[(1,1)]),
	'TN rate' : PRED[(0,0)] / (PRED[(0,0)] + PRED[(0,1)]),
	'FN rate' : PRED[(1,0)] / (PRED[(1,0)] + PRED[(1,1)]),
	'PP val' : PRED[(1,1)] / (PRED[(1,1)] + PRED[(0,1)]),
	'NP val' : PRED[(0,0)] / (PRED[(0,0)] + PRED[(0,1)]),
	'ACC' : (PRED[(1,1)] + PRED[(0,0)]) / (PRED[(1,1)]+PRED[(0,1)]+PRED[(0,0)]+PRED[(1,0)])
	}
