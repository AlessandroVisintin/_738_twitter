from StorageUtils.SQLite import SQLite

import datetime
import re


ISFOLLOWEDBY = 'config/_738_twitter/IsFollowedBy.yaml'
USERSINFO = 'config/_738_twitter/UsersInfo.yaml'
OUTPATH = 'out/_738_twitter'


def stamp2str(stamp:str, form:str='%Y-%m-%d %H:%M:%S'):
	"""
	
	Convert timestamp to formatted UTC string.
	
	"""
	
	return datetime.datetime.utcfromtimestamp(stamp).strftime(form)


db_UsersInfo = SQLite(USERSINFO)
count, locations = 0, {}
for row in db_UsersInfo.select('all'):
	if not count % 10000: print(count)
	count += 1

	loc = row[3].strip().lower()
	if not loc == '':
		if re.match(r'\s*india\s*', loc):
			locations[row[0]] = 'india'
		elif re.match(r'\s*united states\s*', loc) or re.match(r'\s*usa\s*', loc):
			locations[row[0]] = 'usa'

del db_UsersInfo


db_IsFollowedBy = SQLite(ISFOLLOWEDBY)
with open(f'{OUTPATH}/locations.txt', 'w') as f:
	for row in db_IsFollowedBy.select('all'):
		if row[1] in locations:
			print(stamp2str(row[2]))
			f.write(f'{row[2]}\t{row[3]}\t{locations[row[1]]}\n')

del db_IsFollowedBy



# countries = {}
# for i,loc in enumerate(locations):
# 	if not i % 100: print(i)
# 	for iso in ['en','es','fr','hi','ar','ru','ja','pt']:
# 		for code, country in dict(countries_for_language(iso)).items():
# 			if country.lower() in loc:
# 				try:
# 					countries[loc].append(code)
# 				except KeyError:
# 					countries[loc] = [code]



# def append_geolocation(contains:str, coords:list):
# 	
# 	locations = set()
# 	with open('locations.txt', 'r', encoding='utf-8') as f:
# 		for line in f:
# 			locations.add(line.strip())
# 	
# 	out = set()
# 	with open('geolocation.txt', 'a+', encoding='utf-8') as g:
# 		while locations:
# 			loc = locations.pop()
# 			if contains in loc:
# 				g.write(f'{loc}\t{coords[0]}\t{coords[1]}\n')
# 			else:
# 				out.add(loc)

# 	with open('locations.txt', 'w', encoding='utf-8') as f:
# 		for e in out:
# 			f.write(f'{e}\n')


# GEO = {
# 	'switzerland' : ["N 47° 0' 0'","E 8° 0' 51''"],
# 	'india' : ["N 22° 0' 0''","E 79° 0' 0''"],
# 	'nigeria' : ["N 10° 0' 0''","E 8° 0' 0''"],
# 	'bangladesh' : ["N 24° 0' 0''","E 90° 0' 0''"]
# }

# append_geolocation('bangladesh', GEO['bangladesh'])


# def tmp():	
# 	count = 0
# 	with open('geolocation.txt', 'w', encoding='utf-8') as f:
# 		
# 		with open('locations.txt', encoding='utf-8') as g:
# 			for line in g:
# 				if not count % 10: print(count)
# 		
# 				count += 1
# 				line = line.strip()
# 				for (lat,lon), names in locations.items():
# 					for name in names:
# 						if name in line:
# 							print(line, name)
# 							f.write(f'{line}\t{lat}\t{lon}\n')
# 							break
				
				
