import psycopg2
from time import time
from datetime import datetime
import numpy as np
import matplotlib
matplotlib.use('Agg')
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import math
from geopy.distance import geodesic



trans_type_dict = {
		#0:'none',
		0:'vehicle',
		1:'bicycle',
		2:'foot',
		3:'still',
		4:'tilting',
		5:'unknown'
		}

trans_color_dict = {
		#0:'none',
		0:'#18bef0',
		1:'#f1524e',
		2:'#197f00',
		3:'#eccf47',
		4:'#ae01c7',
		5:'#101e45'
		}

def query_all(in_sql, cur):
	cur.execute()
	return cur.fetchall()

def lookup(in_conn):
	cur_trips = in_conn.cursor()
	cur_gpmode = in_conn.cursor()
	cur_loc = in_conn.cursor()
	cur_trips.itersize = 10000
	cur_gpmode.itersize = 10000
	cur_loc.itersize = 10000

	trips_sql = 'SELECT * FROM trips;'
	cur_trips.execute(trips_sql)
	for row_trips in cur_trips:
		trips_key_val = row_trips[9] #row_trips[9]: userid
		trip_dep_time = str(row_trips[3]) #row_trips[3]: departtime
		trip_arr_time = str(row_trips[4]) #row_trips[4]: arrivetime

		gpmode_sql = '''SELECT *
						FROM traces_gpmode as gp, traces_location as loc
						WHERE gp.deviceid = \'{}\'
						AND gp.modetime>=\'{}\'
						AND gp.modetime<=\'{}\'
						AND gp.deviceid = loc.deviceid
						AND gp.epochtime >= loc.epochtime - 25
						AND gp.epochtime <= loc.epochtime + 25
						ORDER BY gp.modetime, loc.pointtime;
				 	 '''.format(trips_key_val,
				 				trip_dep_time,
				 				trip_arr_time)
		cur_gpmode.execute(gpmode_sql)
		lookup_result = list(cur_gpmode)	

		if len(lookup_result) > 0:
			yield lookup_result
		else:
			continue

def simple_test_query(db_conn):
	cur_ = db_conn.cursor()
	cur_.itersize = 10000
	test_sql_ = "SELECT * FROM traces_location WHERE deviceid=\'616\';"
	cur_.execute(test_sql_)
	for item in list(cur_)[:10]:
		print(item)

def plot_sf():
	sf_map = Basemap(projection='merc',
					 resolution='h',
					 area_thresh=.01,
					 llcrnrlon=-122.5194,
					 llcrnrlat=37.6749,
					 urcrnrlon=-122.3119,
					 urcrnrlat=37.8749)

	sf_map.drawcoastlines()
	sf_map.drawcountries()
	sf_map.fillcontinents(color = 'coral')
	sf_map.drawmapboundary()

def get_type(trace):
	type_ = np.array([int(x) for x in trace[2:8]]).argmax()
	return type_

def plot_trace(in_traces):

	lon_0 = -122.5194
	lon_1 = -122.3119
	lat_0 = 37.6749
	lat_1 = 37.8749

	test_lat = in_traces[0][15]
	test_lon = in_traces[0][16]
	if test_lat >= lat_0 and test_lat <= lat_1 and test_lon >= lon_0 and test_lon <= lon_1:

		print('plot')
		plt.figure(figsize=(15, 15), dpi=100)

		sf_map = Basemap(projection='merc',
					 resolution='h',
					 area_thresh=.01,
					 llcrnrlon=float(test_lon)-.008,
					 llcrnrlat=float(test_lat)-.008,
					 urcrnrlon=float(test_lon)+.008,
					 urcrnrlat=float(test_lat)+.008)
					 #llcrnrlon=-125.5194,
					 #llcrnrlat=33.6749,
					 #urcrnrlon=-117.3119,
					 #urcrnrlat=40.8749)

		sf_map.readshapefile('sf_shape/geo_export_sf',
	    					 'SF')
	    					 #color='none',
	    				     #zorder=2)

		sf_map.drawcoastlines()
		sf_map.drawcountries()
		#sf_map.fillcontinents(color = 'coral')
		#sf_map.drawmapboundary()
			
		ver_pos = 2200
		trace_i = 0
		for trace in in_traces:
			lat = float(trace[15])
			lon = float(trace[16])
			x,y = sf_map(lon, lat)

			trace_type_num = get_type(trace)
			trace_type = trans_type_dict[trace_type_num]
			mark_color = trans_color_dict[trace_type_num]
			#sf_map.plot(x, y, 'x', markersize=10, color=mark_color)
			plt.text(x, y, str(trace_i), fontsize=20, color=mark_color)

			plt.text(1800, ver_pos, '{}: {}'.format(trace_i, trace_type), fontsize=20, color=mark_color)
			ver_pos -= 50
			trace_i+=1

		#text positions
		#plt.text(1800, 2200,'aaaaa')
		#plt.text(1800, 2160,'aaaaa')
			

		#lon = [x for x in in_traces[16]]
		#lat = [x for x in in_traces[15]]

		
		#plt.show()
		fname = 'plots_/{}_{}.png'.format(in_traces[0][14], in_traces[0][0])
		plt.savefig(fname)
	


def get_trip_type_max_val(traces_of_trip, trans_type_dict=trans_type_dict):

	trace_types = []
	for trace in traces_of_trip:
		curr_type = np.array([int(x) for x in trace[2:8]]).argmax()
		#print(trans_type_dict[curr_type])
		trace_types.append(trans_type_dict[curr_type])

	print(trace_types)
	print('')

def show_type_prob(in_traces, trans_type_dict=trans_type_dict):
	print(['vehicle','bicycle','foot','still','tilting','unknown'])
	#print('step')
	prev_time = in_traces[0][1]

	for trace in in_traces:
		#print(float(trace[15]), float(trace[16]))
		#print(trace)
		max_type = trans_type_dict[np.array([int(x) for x in trace[2:8]]).argmax()]
		interval = trace[1] - prev_time
		prev_time = trace[1]
		print([int(x) for x in trace[2:8]], max_type, interval)
	print('')

def get_speeds(in_traces):
	prev_time = 0
	prev_loc = (0.0, 0.0)
	speeds = []

	for trace in in_traces:
		time_delta = float(trace[10] - prev_time)
		prev_time = trace[10]
		print(time_delta)

		curr_loc = (trace[15], trace[16])
		distance = geodesic(prev_loc, curr_loc).miles
		prev_loc = curr_loc

		if time_delta != 0:
			speeds.append(distance/time_delta)

	return speeds


#consider mode inaccurate if speed doesn't correspond to trans mode
def speed_filter(in_traces):
	speeds_ = get_speeds(in_traces)
	print(speeds_)
	print(len(speeds_))
	print(len(in_traces))



	

		


def get_trip_type_test(traces_of_trip, trans_type_dict=trans_type_dict):

	trace_types = []
		

def look_up_trips(db_conn, size=1):

	lookup_gen = lookup(db_conn)
	n = 0

	
	for item in lookup_gen:
		#for row in item:
			#print(row)

		#test get_trip_type
		#get_trip_type_max_val(item)
		show_type_prob(item)
		speed_filter(item)
		#plot_sf()
		#plot_trace(item)
		

		n+=1
		if n>=size:
			break
	

	


def main():
	conn = psycopg2.connect("dbname='sf_data' user='larry' host='localhost' password='randomdbpass'")
	t0 = time()

	#simple_test_query(conn)
	look_up_trips(conn)


	t1 = time()
	print('time:', t1-t0)
	conn.close()





if __name__ == '__main__':
	main()













































