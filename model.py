#!/usr/bin/python3

import numpy as np
import random
import math
import pymongo
import sys
import pycurl
import pickle
from datetime import datetime, timedelta
from collections import Counter

#setting
weather_data = 'wx.dat'
lookup_table_file = 'lookup_table.pickle'
begin_year = 2012
begin_month = 1
begin_day = 1
end_year = 2013
end_month = 12
end_day = 31
range_forecast = 6 #in hour
min_data = 4

def definearahangin(arah):
	if arah < 45:
		arahangin = 'Northeast'
	elif arah < 90:
		arahangin = 'East'
	elif arah < 135:
		arahangin = 'Southeast'
	elif arah < 180:
		arahangin = 'South'
	elif arah < 225:
		arahangin = 'Southwest'
	elif arah < 270:
		arahangin = 'East'
	elif arah < 315:
		arahangin = 'Northwest'
	else:
		arahangin = 'North'
	return arahangin

def most_frequent(List): 
	dict = {} 
	count, itm = 0, '' 
	for item in reversed(List): 
		dict[item] = dict.get(item, 0) + 1
		if dict[item] >= count : 
			count, itm = dict[item], item 
	return(itm)

#load table
lookup_table = open('lookup_table.pickle', 'rb')
lookup_table = pickle.load(lookup_table)

print(lookup_table)

if len(sys.argv) < 8:
	print('format: ./build_table.py icao begin_year begin_month begin_day end_year end_month end_day')
	exit()

icao = sys.argv[1]
try:
	begin_year = int(sys.argv[2])
	begin_month = int(sys.argv[3])
	begin_day = int(sys.argv[4])
	end_year = int(sys.argv[5])
	end_month = int(sys.argv[6])
	end_day = int(sys.argv[7])
except ValueError:
	print('format: ./build_table.py icao begin_year begin_month begin_day end_year end_month end_day')
	exit()

weather_open = open(weather_data)
weather = weather_open.read().split('\n')
if weather[-1] == '':
	weather = weather[:-1]

client = pymongo.MongoClient()
metardb = client['bayes']['metar']

begin_time = datetime(begin_year, begin_month, begin_day, 5, 0)
end_time = datetime(end_year, end_month, end_day, 23, 00)

waktu_jalan = begin_time
begin_encode = ''
end_encode = ''
test_data = []
#cnt_coba = 0
while True:
	select_awal = waktu_jalan
	select_akhir = waktu_jalan + timedelta(hours=range_forecast)
	selection = list(metardb.find({'waktu': {'$lt': select_akhir, '$gte': select_awal}, 'icao' : icao}))
	
	if len(selection) < min_data:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	#get kec_angin
	cnt_kecepatan_angin = []
	cnt_total_data_kec_angin = 0
	for i in range(len(selection)):
		if math.isnan(selection[i]['kecepatan_angin']):
			continue
		cnt_kecepatan_angin.append(selection[i]['kecepatan_angin'])
		
	
	#buang data jika...
	if len(cnt_kecepatan_angin) == 0:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	#get rata kec angin
	rata_kec_angin = np.mean(cnt_kecepatan_angin)
	rata_kec_angin = int(rata_kec_angin)
	
	if rata_kec_angin < 5:
		kecepatan_angin = '<5KT'
	elif rata_kec_angin < 10:
		kecepatan_angin = '5-10KT'
	elif rata_kec_angin < 15:
		kecepatan_angin = '10-15KT'
	elif rata_kec_angin < 20:
		kecepatan_angin = '15-20KT'
	elif rata_kec_angin < 25:
		kecepatan_angin = '20-25KT'
	else:
		kecepatan_angin = '>25KT'
	
	#get arah_angin
	cnt_arah_angin = []
	for i in range(len(selection)):
		if math.isnan(selection[i]['kecepatan_angin']):
			continue
		if selection[i]['arah_angin'] != 'VRB':
			cnt_arah_angin.append(definearahangin(selection[i]['arah_angin']))
		else:
			cnt_arah_angin.append('VRB')
	
	#buang data jika...
	if len(cnt_arah_angin) == 0:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	#get arah angin dominan
	arah_angin_dominan = most_frequent(cnt_arah_angin)
	
	#get visibility
	cnt_vis = []
	for i in range(len(selection)):
		if math.isnan(selection[i]['visibility']):
			continue

		if selection[i]['visibility'] >= 1000 and selection[i]['visibility'] != 9999:
			cnt_vis.append(round(selection[i]['visibility'], -3))
		elif selection[i]['visibility'] == 9999:
			cnt_vis.append(9999)
		elif selection[i]['visibility'] < 1000 and selection[i]['visibility'] > 500:
			cnt_vis.append(500)
		else:
			cnt_vis.append(0)
	
	#buang data jika...
	if len(cnt_vis) == 0:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	visibility_min = min(cnt_vis)
	visibility_max = max(cnt_vis)
	
	#get suhu
	cnt_suhu = []
	for i in range(len(selection)):
		if math.isnan(selection[i]['suhu']):
			continue
		cnt_suhu.append(selection[i]['suhu'])
	
	#buang data jika...
	if len(cnt_suhu) == 0:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	suhu_min = min(cnt_suhu)
	suhu_max = max(cnt_suhu)
	suhu_rata = int(round(np.mean(cnt_suhu)))
	
	#get RH dari dew point
	cnt_rh = []
	for i in range(len(selection)):
		if math.isnan(selection[i]['dew_point']) or math.isnan(selection[i]['suhu']):
			continue
		suhu_temp = selection[i]['suhu']
		dewpoint_temp = selection[i]['dew_point']
		rh_temp = 100*(np.exp((17.625*dewpoint_temp)/(243.04+dewpoint_temp))/np.exp((17.625*suhu_temp)/(243.04+suhu_temp)))
		cnt_rh.append(rh_temp)
	
	#buang data jika...
	if len(cnt_rh) == 0:
		waktu_jalan += timedelta(hours=range_forecast)
		begin_encode = ''
		end_encode = ''
		continue
	
	rata_rh = int(round(np.mean(cnt_rh), -1))
	
	#get cuaca
	cnt_cuaca = 0
	for i in range(len(selection)):
		if selection[i]['dew_point'] == 'NOSIG':
			continue

		try:
			cnt_cuaca_temp = weather.index(selection[i]['cuaca'])+1
			if cnt_cuaca_temp > cnt_cuaca:
				cnt_cuaca = cnt_cuaca_temp
		except ValueError:
			continue
	
	if cnt_cuaca == 0:
		cuaca = 'NOSIG'
	else:
		cuaca = weather[cnt_cuaca-1]
	
	
	#encode parameter cuaca
	param_encode = '%s|%s|%s|%s|%s|%s|%s'%(kecepatan_angin, arah_angin_dominan, visibility_min, visibility_max, suhu_rata, rata_rh, cuaca)
	if begin_encode == '' and end_encode == '':
		begin_encode = param_encode
	elif begin_encode != '' and end_encode == '':
		end_encode = param_encode
	elif begin_encode != '' and end_encode != '':
		begin_encode = end_encode
		end_encode = param_encode
	
	#jika begin dan end ada, daftarkan ke lookup table
	if begin_encode != '' and end_encode != '':
		test_data.append([begin_encode, end_encode])
	
	print(waktu_jalan, param_encode)
	waktu_jalan += timedelta(hours=range_forecast)
	if waktu_jalan > end_time:
		break

#test performance
result = open('result.dat', 'w')
result.write('prior,obs,prediction\n')
for i in range(len(test_data)):
	try:
		prediction_list = lookup_table[test_data[i][0]]
		dominant_prediction = most_frequent(prediction_list)
		val_dominant = prediction_list.count(dominant_prediction)
		
		dominant_list = []
		prediction_keylist = list(Counter(prediction_list).keys())
		#check there are more than 1 dominant
		for j in range(len(prediction_keylist)):
			if prediction_list.count(prediction_keylist[j]) == val_dominant:
				dominant_list.append(prediction_keylist[j])
		prediction = random.choice(dominant_list)
	except KeyError:
		prediction = 'UNKNOWN'

	result_str = '%s,%s,%s\n'%(test_data[i][0],test_data[i][1],prediction)
	print(result_str)
	result.write(result_str)
