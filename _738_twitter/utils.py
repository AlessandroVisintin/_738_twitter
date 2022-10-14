from datetime import datetime as dt


def twt2stamp(twitter_date:str) -> int:
	"""
	
	Convert Twitter time string to timestamp.
	
	Args:
		twitter_date : twitter date to convert.
	
	Returns:
		timestamp value.
	
	"""
	
	t = dt.strptime(twitter_date, '%a %b %d %H:%M:%S %z %Y')
	return int(round(t.timestamp()))


def crs2stamp(cursor:int) -> int:
	"""
	
	Convert Twitter cursor to timestamp.
	
	Args:
		cursor : Twitter cursor to convert.
	
	Returns:
		timestamp value.
	
	"""

	return round((cursor >> 22) / 250, 3)
    

def stamp2crs(stamp:int) -> int:
	"""
	
	Convert timestamp to Twitter cursor.
	
	Args:
		stamp : timestamp to convert.
	
	Returns:
		Twitter cursor value.
	
	"""
	
	return int(round((stamp * 250))) << 22
