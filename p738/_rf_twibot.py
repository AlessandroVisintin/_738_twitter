from p738.utils import twt2stamp

from StorageUtils.SQLite import SQLite
from TimeUtils.utils import str2stamp

import numpy as np
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor


PATH = 'out/p738'

db = SQLite(f'{PATH}/Twibot.db', config='config/p738/Twibot.yaml')

df = pd.read_sql_query('SELECT * FROM Twibot', db.db)
df.astype({
    'id':'int64',
    'name':'object',
    'screen_name':'object',
    'statuses_count':'int64',    
    'followers_count':'int64',
    'friends_count':'int64',
    'favourites_count':'int64',
    'listed_count':'int64',
    'created_at':'int64',
    'url':'object',
    'location':'object',
    'default_profile':'int64',
    'default_profile_image':'int64',
    'profile_banner_url':'object',
    'profile_background_image_url_https':'object',
    'profile_image_url_https':'object',
    'protected':'int64',
    'verified':'int64',
    'description':'object',
    'updated':'int64',
    'dataset':'object'
	}, copy=False)

human = df[df['dataset'] == 'human']
bot = df[df['dataset'] == 'bot']

if len(human) < len(bot):
	bot = bot.sample(n=len(human))
else:
	human = human.sample(n=len(bot))	

human.loc[:,'dataset'] = 'hum'
bot.loc[:,'dataset'] = 'fak'

DATA = human.merge(bot, how='outer')

out = DATA['friends_count'] / DATA['followers_count']**2

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
	d = ~(DATA['name'].notnull() & DATA['name'] != ''),
	#number of friends
	e = DATA['friends_count'],
	#has URL in profile (inverted)
	f = ~DATA['url'].notnull(),
	#following rate
	g = DATA['friends_count'] / (DATA['updated'] - DATA['created_at']),
	#default image after 2 months
	h = DATA['default_profile_image'] & (DATA['updated'] - DATA['created_at'] > 60*60*24*30*2),
	# belongs to a list (inverted)
	i = ~(DATA['listed_count'] > 0),
	# profile has image (inverted)
	j = DATA['default_profile_image'],
	#friends/followers ≥ 50
	k = DATA['friends_count'] / DATA['followers_count'] >= 50,
	#bot in biography
	l = 'bot' in DATA['description'].str.lower(),
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

# prepare numeric labels (hum=0, fak=1)
LABELS = np.where(FEATURES['dataset'] == 'hum', False, True)
FEATURES = FEATURES.drop('dataset', axis=1)

# regress random forest
rf = RandomForestRegressor(n_estimators=100, random_state=42, verbose=1)
rf.fit(FEATURES, LABELS)

joblib.dump(rf, f'{PATH}/rf_twibot.joblib')
