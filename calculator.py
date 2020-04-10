from math import sin, cos, sqrt, atan2, radians

def countDistanceFromLatLon(lat1,lon1,lat2,lon2):
	# https://www.ridgesolutions.ie/index.php/2013/11/14/algorithm-to-calculate-speed-from-two-gps-latitude-and-longitude-points-and-time-difference/
	# stop if lat or lon is zero
	if lat1 == 0.0 or lon1 == 0.0 or lat2 == 0.0 or lon2 == 0.0 :
		return 0.0
	# Radius of the earth in km
	R = 6371
	dLat = radians(lat2-lat1)
	dLon = radians(lon2-lon1)
	rLat1 = radians(lat1)
	rLat2 = radians(lat2)
	a = sin(dLat/2) * sin(dLat/2) + cos(rLat1) * cos(rLat2) * sin(dLon/2) * sin(dLon/2) 
	c = 2 * atan2(sqrt(a), sqrt(1-a))
	d = R * c # waktu x kecepatan, dalam kilometer
	return d

def calcVelocity(dist_km, time_start, time_end):
	# https://stackoverflow.com/questions/45819066/average-speed-based-on-time-and-distance-python
	result = 0
	try :
		# Kecepatan = jarak / waktu tempuh
		result = dist_km / (time_end - time_start).seconds if time_end > time_start else 0
	except Exception as e :
		"""Return 0 if time_start == time_end, avoid dividing by 0"""
		print(e)

	return result