import psycopg2
from time import time
from datetime import datetime
import numpy as np
import matplotlib
from mpl_toolkits.basemap import Basemap
matplotlib.use('agg')
import matplotlib.pyplot as plt

trans_type_dict = {
		#0:'none',
		0:'vehicle',
		1:'bicycle',
		2:'foot',
		3:'still',
		4:'tilting',
		5:'unknown'
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

def plot_trace(in_traces):

	sf_map = Basemap(projection='merc',
					 resolution='h',
					 area_thresh=.01,
					 llcrnrlon=-122.5194,
					 llcrnrlat=37.6749,
					 urcrnrlon=-122.3119,
					 urcrnrlat=37.8749)

	#sf_map.drawcoastlines()
	#sf_map.drawcountries()
	#sf_map.fillcontinents(color = 'coral')
	#sf_map.drawmapboundary()

	for trace in in_traces:
		lat = float(trace[15])
		lon = float(trace[16])
		x,y = sf_map(lon, lat)
		sf_map.plot(x, y, 'bo', markersize=2)
		


	#lon = [x for x in in_traces[16]]
	#lat = [x for x in in_traces[15]]
	plt.show()



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
	prev_time = in_traces[0][1]

	for trace in in_traces:
		print(trace[1], trace[13])
		max_type = trans_type_dict[np.array([int(x) for x in trace[2:8]]).argmax()]
		interval = trace[1] - prev_time
		prev_time = trace[1]
		#print([int(x) for x in trace[2:8]], max_type, interval)
	print('')


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
		#plot_sf()
		plot_trace(item)
		

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













































