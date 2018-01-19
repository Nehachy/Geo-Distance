import csv
import traceback
from collections import OrderedDict
from math import radians, cos, sin, asin, sqrt

def geodistance(long1,lat1):
	f = open ("longlat.csv")
	longlat_f = csv.reader(f,delimiter=',')
		
	calc1 = {}
		
	for row in longlat_f:
		station = row[0]
  		radlat = radians(float(row[1])) - radians(float(lat1))
  		radlong = radians(float(row[2])) - radians(float(long1))
  		a = (sin(radlat/2)**2 + cos(radians(float(lat1)))*cos(radians(float(row[1])))*sin(radlong/2)**2)
  		#computing the straight line distance using Haversine formula

  		calc1[station] = 6371*2*asin(sqrt(a))

 		minn = min(calc1.values())
 		keys = [x for x,y in calc1.items() if y ==minn]
 		print keys
 		print minn
 			
		f.close()
		return keys

#geodistance(-93.144848,45.056743)
