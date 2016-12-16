import pycurl
import traceback
from io import BytesIO
from xml.dom.minidom import parseString
import xml.dom.minidom
from collections import defaultdict
from datetime import datetime
import dba.data_save as ds
from dba import read_data as rd
from dba import db_util
import sys
from datetime import timedelta
URL = "http://forecast.weather.gov/MapClick.php?lat=%s&lon=%s&FcstType=digitalDWML"
mapper = {'hourly':'tmpf','dew point':'dwpf','relative':'relh'}


def pullLongLat():
     dinp = rd.Input('192.168.198.6', 27017)
     d_init = {
        'db': 'sbat',
        'schema': 'storeattr_data',
        'query': {},
        'sort_key': '_id',
        'sort_dir': 1
     }
     dinp.find(**d_init)
     lng = dinp.data.get('storeattr_data.lngtd_i')
     lat = dinp.data.get('storeattr_data.lattd_i')
     store_id = dinp.data.get('storeattr_data.loc_i')
     return lng, lat, store_id


def getPredictedWeather(latitude,longitude):
  try:
     url = URL%(latitude,longitude)
     print "URL: %s"%url
     buffer_ = BytesIO()
     c = pycurl.Curl()
     c.setopt(c.URL,url)
     c.setopt(c.WRITEDATA,buffer_)
     c.perform()

     body = buffer_.getvalue()
     print "Curl Request Completed.."
     return body

  except:
     traceback.print_exc()

def getTagData(tagObject):
  try:
     tag_map = defaultdict(list)
     for _obj in tagObject:
         if _obj.hasAttribute('type'):
            type_ = _obj.getAttribute('type')

         values = _obj.getElementsByTagName('value')
         for val in values:
            #print val.childNodes[0].data
            try:
               tag_map[type_].append(val.childNodes[0].data)
            except:
               #tag_map[type_].append('NA')
               pass

     return tag_map
  except:
     traceback.print_exc()

def parseXML(data):
  try:
     timelist = []

     domTree = parseString(data)
     data = domTree.documentElement

     weatherinfo = data.getElementsByTagName("time-layout")
     for elem in weatherinfo:
        start_time = elem.getElementsByTagName("start-valid-time")
        end_time = elem.getElementsByTagName("end-valid-time")

        for stime in start_time:
           timelist.append(stime.childNodes[0].data)

     temperatures = data.getElementsByTagName("temperature")
     temp_map = getTagData(temperatures)

     humidity = data.getElementsByTagName("humidity")
     hum_map = getTagData(humidity)

     return timelist,temp_map,hum_map

  except:
     traceback.print_exc()

#2016-05-11T17:00:00-05:00 to 05/11/2016 17:00
def dateformatter(date):
   try:
      date = '-'.join(date.split("-")[:-1])
      date_obj = datetime.strptime(date,"%Y-%m-%dT%H:%M:%S")
      date_new = datetime.strftime(date_obj,"%m/%d/%Y %H:%M")
      return date_new,date_obj
   except:
      traceback.print_exc()






def databaseInsertion(*args):
   try:
      timelist = args[0]
      temp_map = args[1]
      hum_map  = args[2]
      store_id = args[3]
      dbs      = args[4]

      counter = min(len(timelist),min([len(v) for k,v in temp_map.items() if k in mapper]),min([len(v) for k,v in hum_map.items()]))

      print "Total dataset is %d store = %d "%(counter,store_id)

      data_map = defaultdict(list)

      for i in range(counter):
         #time fetch
         time = timelist[i]
         new_time,time_obj = dateformatter(time)
         data_map['timestamp'].append(new_time)
         data_map['timestamp_obj'].append(time_obj)
    	 data_map['store'].append(store_id)

         #dew point , hourly temperature fetch
         for k,v in temp_map.items():
            converted_name = mapper.get(k)
            if converted_name:
               value = float(v[i])
               data_map[converted_name].append(value)

         #relative humidity fetch
         for k,v in hum_map.items():
            converted_name = mapper.get(k)
            if converted_name:
               value = float(v[i])
               data_map[converted_name].append(value)

         #print "Processing for %s"%new_time

      sch = db_util.Schema()
      sch._name = 'weather_data'
      sch._id = 'weather'
      sch._result = data_map
      sch._data_len = counter

      #dbs = ds.DBSave()
      ##dbs.save([sch])
      dbs.update([sch],['timestamp_obj','store'])
      #dbs.close()

   except:
      traceback.print_exc()

if __name__=='__main__':

   dbs = ds.DBSave()
   lng, lat, store_id = pullLongLat()
   for index in range(len(lng)):
        try:
           print "fetching for store = ",store_id[index]
   	   data = getPredictedWeather(lat[index],lng[index])
   	   a,b,c = parseXML(data)
   	   databaseInsertion(a,b,c,store_id[index],dbs)
           db_util.updateStatus2('weather','completed')
           db_util.enterLastProcessDate2('weather')
        except:
           print "Problem in ",store_id[index]
   dbs.close()
