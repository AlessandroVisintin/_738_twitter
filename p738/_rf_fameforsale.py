from p738.utils import twt2stamp

from TimeUtils.utils import str2stamp

import numpy as np
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor


PATH = 'out/p738'

DATA = {
	
	'e13' : f'{PATH}/TRAIN_DATASET/E13.csv/users.csv',
	'fsf' : f'{PATH}/TRAIN_DATASET/FSF.csv/users.csv',
	'int' : f'{PATH}/TRAIN_DATASET/INT.csv/users.csv',
	'tfp' : f'{PATH}/TRAIN_DATASET/TFP.csv/users.csv',
	'twt' : f'{PATH}/TRAIN_DATASET/TWT.csv/users.csv'
	
	}

CONVERTERS = {
	'id' : lambda x: -1 if x == 'nan' else int(x),
	'statuses_count' : lambda x: -1 if x == 'nan' else int(x),
	'followers_count' : lambda x: -1 if x == 'nan' else int(x),
	'friends_count' : lambda x: -1 if x == 'nan' else int(x),
	'favourites_count' : lambda x: -1 if x == 'nan' else int(x),
	'listed_count' : lambda x: -1 if x == 'nan' else int(x),
	'utc_offset' : lambda x: -1 if x == 'nan' or x == '' else int(x),
	'default_profile' : lambda x: False if x == 'nan' else bool(x),
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
	'created_at' : twt2stamp
	}


DATA = {k : pd.read_csv(v, converters=CONVERTERS) for k,v in DATA.items()}

# hum should have 1950 rows
DATA['hum'] = DATA['e13'].merge(DATA['tfp'], how='outer')
del DATA['e13']
del DATA['tfp']

# fak should have 3351 rows
DATA['fak'] = DATA['fsf'].merge(DATA['int'], how='outer').merge(DATA['twt'], how='outer')
del DATA['fsf']
del DATA['int']
del DATA['twt']

DATA['hum']['dataset'] = 'hum'
DATA['fak']['dataset'] = 'fak'
DATA = DATA['fak'].merge(DATA['hum'], how='outer')

# class-A features
FEATURES = pd.DataFrame()
FEATURES = FEATURES.assign(
	#friends/(followers^2) ratio
	a = DATA['friends_count'] / DATA['followers_count']**2,
	#age
	b = DATA['updated'] - DATA['created_at'],
	#number of tweets
	c = DATA['statuses_count'],
	#profile has name (inverted)
	#d check 12
	#number of friends
	e = DATA['friends_count'],
	#has URL in profile (inverted)
	#f = ~DATA['url'].notnull(),
	#following rate
	g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
	#default image after 2 months
	h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
	# belongs to a list (inverted)
	#i = ~DATA['listed_count'] > 0,
	# profile has image (inverted)
	j = DATA['default_profile_image'],
	#friends/followers ≥ 50
	k = DATA['friends_count'] / DATA['followers_count'] >= 50,
	#bot in biography
	l = 'bot' in DATA['description'].str.lower(),
	# duplicate profile pictures
	#m  check 11
	# 2 × followers ≥ friends (inverted)
	n = 2 * DATA['followers_count'] < DATA['friends_count'],
	# has address (inverted)
	o = ~(DATA['location'].notnull() & DATA['location'] != ''),
	# no bio, no location, friends ≥100
	p = ~((DATA['description'].notnull() & DATA['description'] != '') | (DATA['location'].notnull() & DATA['location'] != '') | DATA['friends_count'] < 100),
	# has biography (inverted)
	q = ~(DATA['description'].notnull() & DATA['description'] != ''),
	# number of followers
	r = DATA['followers_count'],
	dataset = DATA['dataset']
	)

# change inf to max
np.nan_to_num(FEATURES['a'], copy=False, posinf=np.float32(2**127))

# random undersample to obtain balanced dataset
test_size = FEATURES.shape[0] - 2*1950
TEST = FEATURES[FEATURES['dataset'] == 'fak'].sample(n=test_size)
FEATURES = FEATURES.merge(TEST, how='left', indicator=True)
FEATURES = FEATURES[FEATURES['_merge'] == 'left_only']
FEATURES = FEATURES.drop('_merge', axis=1)

# prepare numeric labels (hum=0, fak=1)
LABELS = np.where(FEATURES['dataset'] == 'hum', False, True)
FEATURES = FEATURES.drop('dataset', axis=1)
TEST = TEST.drop('dataset', axis=1)

# regress random forest
rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(FEATURES, LABELS)

pred = np.where(rf.predict(TEST) > 0.5, 1, 0)

print('TEST True Positive: ', sum(pred) / len(pred))

joblib.dump(rf, f'{PATH}/random_forest_model.joblib')
