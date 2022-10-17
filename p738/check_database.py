from StorageUtils.SQLite import SQLite

USERS = 'config/_738_twitter/Users.yaml'
USERSINFO = 'config/_738_twitter/UsersInfo.yaml'
ISFOLLOWEDBY = 'config/_738_twitter/IsFollowedBy.yaml'
OUTPATH = 'out/_738_twitter'

db = {
		'Users':SQLite(USERS),
		'UsersInfo':SQLite(USERSINFO),
		'IsFollowedBy':SQLite(ISFOLLOWEDBY)
		}

users = [r for r in db['Users'].select('all_Users')]
usersinfo = [r for r in db['UsersInfo'].select('all_UsersInfo')]
isfollowedby = [r for r in db['IsFollowedBy'].select('all_IsFollowedBy')]

out = 0
for d in usersinfo:
	if not d[3] is None and not d[3] == '':
		out += 1