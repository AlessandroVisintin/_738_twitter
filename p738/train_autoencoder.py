from StorageUtils.SQLite import SQLite
from TimeUtils.utils import str2stamp
from TimeUtils.utils import stamp2str

import keras
import numpy as np
import pandas as pd
from keras import layers
from sklearn.preprocessing import QuantileTransformer


TYPES = [
	('id','int64'), ('created_at','int64'), ('default_profile','bool'),
	('default_profile_image','bool'), ('description','object'),
	('favourites_count','Int64'), ('followers_count','int64'),
	('friends_count','int64'), ('listed_count','Int64'),
	('location','object'), ('name','object'),
	('profile_banner_url','object'), ('profile_image_url_https','object'),
	('protected','bool'), ('screen_name','object'),
	('statuses_count','int64'), ('url','object'),
	('verified','bool'), ('updated','int64'),
	('id1','int64'), ('id2','int64'),
	('b1','int64'), ('b2','int64')
	]


def df2feat(df):
	df.astype({x[0]:x[1] for x in TYPES}, copy=False)

	# class-A features
	FEATURES = pd.DataFrame()
	FEATURES = FEATURES.assign(
	 	a = df['friends_count'] / df['followers_count']**2, #friends/(followers^2) ratio
	 	b = df['b2'] - df['created_at'], #age
	 	c = df['statuses_count'], #number of tweets
 	 	d = ~(df['name'].notnull() & df['name'] != ''), #profile has name (inverted)
 	 	e = df['friends_count'], #number of friends
 	 	f = ~df['url'].notnull(), #has URL in profile (inverted)
 	 	g = df['friends_count'] / (df['updated'] - df['created_at']), #following rate
 	 	h = df['default_profile_image'] & (df['updated'] - df['created_at'] > 60*60*24*30*2), #default image after 2 months
 	 	i = ~(df['listed_count'] > 0), # belongs to a list (inverted)
 	 	j = df['default_profile_image'], # profile has image (inverted)
 	 	k = df['friends_count'] / df['followers_count'] >= 50, #friends/followers ≥ 50
 	 	l = 'bot' in df['description'].str.lower(), #bot in biography
 	 	n = 2 * df['followers_count'] < df['friends_count'], # 2 × followers ≥ friends (inverted)
 	 	o = ~(df['location'].notnull() & df['location'] != ''), # has address (inverted)
 	 	p = ~((df['description'].notnull() & df['description'] != '') | (df['location'].notnull() & df['location'] != '') | df['friends_count'] < 100), # no bio, no location, friends ≥100
 	 	q = ~(df['description'].notnull() & df['description'] != ''), # has biography (inverted)
 	 	r = df['followers_count'], # number of followers
		s = df['b2']
 	)
	
	np.nan_to_num(FEATURES['a'], copy=False, posinf=np.float32(2**127))
	FEATURES = pd.DataFrame(QuantileTransformer().fit_transform(FEATURES))
	return FEATURES
	


USERNAME, USERID = 'JoeBiden', 939091
START = int(str2stamp('2019-01-01 00:00:00'))
END = int(str2stamp('2022-10-01 00:00:00'))

OUT = 'out/p738'
CFG = 'config/p738'

print('Prepare db', end='')
p738 = SQLite(f'{OUT}/p738.db', config=f'{CFG}/p738.yaml')
print('.', end='')
p738.fetch(name='create_isbot', format={'t' : USERID})
print('.', end='')
p738.add_index(f'u{USERID}_fws_index1', f'u{USERID}_fws', 'b1', if_not_exists=True)
print('.', end='')
p738.add_index(f'u{USERID}_fws_index2', f'u{USERID}_fws', 'b2', if_not_exists=True)

print('\n\nStart process')
delta = 60*60*24*28
for i in range(START, END, delta):
	print(stamp2str(i))
		
	q = (
		f'SELECT COUNT(*) FROM u{USERID}_fws '
	  	f'WHERE b1 >= {i} AND b2 < {i + delta * 6};'
		)
	tot = p738.fetch(query=q)[0][0]
	
	if tot == 0:
		continue
	
	l = 5000 if tot * 0.05 > 5000 else int(tot * 0.05)
	q = (
		f'SELECT * FROM Users a '
		f'INNER JOIN ('
		f'SELECT * FROM u{USERID}_fws '
		f'WHERE b1 >= {i} AND b2 < {i + delta * 6} '
		f'ORDER BY RANDOM() LIMIT {l}'
		f') b ON a.id = b.id2;'
		)

	df = pd.read_sql_query(q, p738.db)
	X = df2feat(df).values

	inp = keras.Input(shape=(X.shape[1],))
	encoded = layers.Dense(X.shape[1], activation='sigmoid')(inp)
	decoded = layers.Dense(X.shape[1], activation='sigmoid')(encoded)
	autoencoder = keras.Model(inp, decoded)
	
	encoder = keras.Model(inp, encoded)
	encoded_inp = keras.Input(shape=(X.shape[1],))
	decoder_layer = autoencoder.layers[-1]
	decoder = keras.Model(encoded_inp, decoder_layer(encoded_inp))
	
	optimizer = keras.optimizers.Adam(learning_rate=0.01)
	autoencoder.compile(optimizer=optimizer, loss='mean_squared_error')
	
	print('train..')
	train = int(l*0.8)
	history = autoencoder.fit(X[:train], X[:train],
					  epochs=5000,
					  batch_size=1024,
					  shuffle=True,
					  validation_data=(X[train:], X[train:]),
					  callbacks=[
						  keras.callbacks.EarlyStopping(monitor='loss', patience=10, restore_best_weights=True),
						  keras.callbacks.EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
						  ],
					  verbose=0
					  )
	val_loss = min(history.history['val_loss'])

	q = (
		f'SELECT * FROM Users a '
		f'INNER JOIN ('
		f'SELECT * FROM u{USERID}_fws '
		f'WHERE b1 >= {i} AND b2 < {i + delta * 6}'
		f') b ON a.id = b.id2;'
		)
	
	num = 0
	for batch in p738.fetchmany(batch=5000, query=q):
		print(num, end=' ')
		df = pd.DataFrame(batch, columns=[x[0] for x in TYPES])
		
		X = df2feat(df).values
		encoded_vals = encoder.predict(X, verbose=0)
		decoded_vals = decoder.predict(encoded_vals, verbose=0)
		
		MSE = ((decoded_vals - X)**2).mean(1)
		
		rows = [(row[0], i, val_loss, mse) for row,mse in zip(batch,MSE)]
		p738.fetch(name='insert_isbot', format={'t' : USERID}, params=rows)
		num += len(rows)
	print('\n')

del p738
