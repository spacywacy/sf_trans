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

trans_type_rules = {
		0:(0, 80),
		1:(0, 30),
		2:(0, 10),
		3:(0, 3),
		4:(0, 80),
		5:(0, 80)
}

def query_all(in_sql, cur):
	cur.execute()
	return cur.fetchall()	

def lookup_gpmode(in_conn, trips_key_val, trip_dep_time, trip_arr_time):
	cur_gpmode = in_conn.cursor()
	cur_gpmode.itersize = 10000

	gpmode_sql = '''SELECT *
					FROM traces_gpmode as gp
					WHERE gp.deviceid = \'{}\'
					AND gp.modetime>=\'{}\'
					AND gp.modetime<=\'{}\'
					ORDER BY gp.modetime;
				 '''.format(trips_key_val,
							trip_dep_time,
							trip_arr_time)
	cur_gpmode.execute(gpmode_sql)
	lookup_result = list(cur_gpmode)

	if len(lookup_result) > 0:
		return lookup_result
	else:
		return []

def lookup_loc(in_conn, trips_key_val, trip_dep_time, trip_arr_time):
	cur_loc = in_conn.cursor()
	cur_loc.itersize = 10000

	loc_sql = '''
				SELECT *
				FROM traces_location as loc
				WHERE loc.deviceid = \'{}\'
				AND loc.pointtime>=\'{}\'
				AND loc.pointtime<=\'{}\'
				ORDER BY loc.pointtime;
			  '''.format(trips_key_val,
						 trip_dep_time,
						 trip_arr_time)

	cur_loc.execute(loc_sql)
	lookup_result = list(cur_loc)

	if len(lookup_result) > 0:
		return lookup_result
	else:
		return []

def lookup(in_conn):
	#get trips
	cur_trips = in_conn.cursor()
	cur_trips.itersize = 10000
	trips_sql = 'SELECT * FROM trips;'
	cur_trips.execute(trips_sql)

	#loop trips
	for row_trips in cur_trips:
		trips_key_val = row_trips[9] #row_trips[9]: userid
		trip_dep_time = str(row_trips[3]) #row_trips[3]: departtime
		trip_arr_time = str(row_trips[4]) #row_trips[4]: arrivetime
		print('gpmode:', trip_dep_time, trip_arr_time)
		gpmode_traces = lookup_gpmode(in_conn, trips_key_val, trip_dep_time, trip_arr_time)
		loc_traces = lookup_loc(in_conn, trips_key_val, trip_dep_time, trip_arr_time)

		yield gpmode_traces, loc_traces



def simple_test_query(db_conn):
	cur_ = db_conn.cursor()
	cur_.itersize = 10000
	test_sql_ = "SELECT * FROM traces_location WHERE deviceid=\'616\';"
	cur_.execute(test_sql_)
	for item in list(cur_)[:10]:
		print(item)

def get_type(trace):
	type_ = np.array([int(x) for x in trace[2:8]]).argmax()
	return type_

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
		max_type = trans_type_dict[np.array([int(x) for x in trace[2:8]]).argmax()]
		interval = trace[1] - prev_time
		prev_time = trace[1]
		print([int(x) for x in trace[2:8]], max_type, interval)
	print('')

def print_trace(in_traces):
	for trace in in_traces:
		print(trace)

def get_speed(p0, p1, t0, t1):
	distance = geodesic(p0, p1).miles
	return distance * 3600 / float(t1 - t0)

def get_avg_speed(in_traces):
	speeds = []
	print('len:', len(in_traces))
	for i in range(len(in_traces)-1):
		p0 = (in_traces[i][6], in_traces[i][7])
		p1 = (in_traces[i+1][6], in_traces[i+1][7])
		t0 = in_traces[i][1]
		t1 = in_traces[i+1][1]

		speed = get_speed(p0, p1, t0, t1)
		#print('speed', speed)
		speeds.append(speed)

	avg_speed = np.array(speeds).mean()
	return avg_speed

def drop_by_index(in_list, drop_indecies):
	out_list = []
	print(drop_indecies)
	for item in in_list:
		if in_list.index(item) not in drop_indecies:
			out_list.append(item)

	return out_list

def speed_filter(gp, loc):
	loc_pos = 0
	drop = []

	for i in range(len(gp)-1):
		gp_trace_0 = gp[i]
		gp_trace_1 = gp[i+1]
		type0 = get_type(gp_trace_0)
		type1 = get_type(gp_trace_1)
		t0 = gp_trace_0[0]
		t1 = gp_trace_1[0]
		loc_traces = []

		if type0 == type1:
			for j in range(loc_pos, len(loc)):
				loc_trace_ = loc[j]
				loc_t = loc_trace_[1]

				#print('t0', t0)
				#print('t1', t1)
				#print('loc_t', loc_t)

				if loc_t >= t0 and loc_t <= t1:
					loc_traces.append(loc_trace_)
				if loc_t > t1:
					break

			avg_speed = get_avg_speed(loc_traces)
			limits = trans_type_rules[type0]
			#print('avg speed:', avg_speed)
			#print('limits:', limits[0], limits[1])
			if avg_speed < limits[0] or avg_speed > limits[1]:
				print('type:', trans_type_dict[type0])
				#print('avg speed:', avg_speed)
				#print('')
				drop.append(gp.index(gp_trace_0))
				print('dropped')

			print('')

		#loc_pos = i

	return drop_by_index(gp, drop)




def get_trip_type_test(traces_of_trip, trans_type_dict=trans_type_dict):

	trace_types = []
		

def look_up_trips(db_conn, size=3):

	lookup_gen = lookup(db_conn)
	n = 0
	
	for gpmode_traces, loc_traces in lookup_gen:

		#print_trace(gpmode_traces)
		#print('')
		#print_trace(loc_traces)
		#print('')
		#print(len(gpmode_traces))
		#print('')

		if n == 2:
			filtered = speed_filter(gpmode_traces, loc_traces)
		
		#print_trace(filtered)
		#print(len(filtered))

		print('-----------------------------------------\n\n')


		

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













































