import datetime as dt
import pandas as pd
import numpy as np
from tqdm import tqdm
import pickle
import time

def create_dict_and_pickling():

    stops_data=pd.read_csv("stops.txt")
    df_stops=stops_data.copy()
    labels_dict={stop_id : ['2021-06-10 23:59:59']*6 for stop_id in tqdm(df_stops.stop_id)}
    with open("labels_dict_pickle.pkl","wb") as labels_dict_pickle_obj:
        pickle.dump(labels_dict,labels_dict_pickle_obj)


    stops_data=pd.read_csv("stops.txt")
    df_stops=stops_data.copy()
    labels_star={stop_id : '2021-06-10 23:59:59' for stop_id in tqdm(df_stops.stop_id)}
    with open("labels_star_pickle.pkl","wb") as labels_star_pickle_obj:
        pickle.dump(labels_star,labels_star_pickle_obj)


    stops_data=pd.read_csv("stops.txt")
    df_stops=stops_data.copy()
    mark_dict={stop_id : 0 for stop_id in tqdm(df_stops.stop_id)}
    with open("mark_dict_pickle.pkl","wb") as mark_dict_pickle_obj:
        pickle.dump(mark_dict,mark_dict_pickle_obj)


    stop_times_merged_data=pd.read_csv("stop_times_merged.txt")
    df_stop_times_merged=stop_times_merged_data.copy()
    routes_groupby_stops=df_stop_times_merged.groupby("stop_id")["route_id"]
    routes_serving_stops={stop_id:list(np.unique(np.array(list(route)))) for stop_id,route in tqdm(routes_groupby_stops)}
    with open("routes_serving_stops_pickle.pkl","wb") as routes_serving_stops_pickle_obj:
        pickle.dump(routes_serving_stops,routes_serving_stops_pickle_obj)

    ''' stop in route dict old'''
    stop_times_merged_data=pd.read_csv("stop_times_merged.txt")
    df_stop_times_merged=stop_times_merged_data.copy()
    stops_groupby_routes=df_stop_times_merged.groupby("route_id")[["stop_sequence","stop_id"]]
    stops_in_route_dict={route_id:list(route.sort_values("stop_sequence").stop_id.unique()) for route_id,route in tqdm(stops_groupby_routes)}
    with open("stops_in_route_dict_pickle.pkl","wb") as stops_in_route_dict_pickle_obj:
        pickle.dump(stops_in_route_dict,stops_in_route_dict_pickle_obj)



    stop_times_merged_data=pd.read_csv("stop_times_merged.txt")
    df_stop_times_merged=stop_times_merged_data.copy()
    df_stop_times_merged["arrival_time_converted"]=pd.to_datetime(df_stop_times_merged.arrival_time)
    trips_groupby_routes = df_stop_times_merged[df_stop_times_merged.stop_sequence==0].groupby("route_id")[["arrival_time_converted","trip_id"]]
    trips_in_route_dict={route_id: list(route.sort_values("arrival_time_converted").trip_id) for route_id,route in tqdm(trips_groupby_routes)}
    with open("trips_in_route_dict_pickle.pkl","wb") as trips_in_route_dict_pickle_obj:
        pickle.dump(trips_in_route_dict,trips_in_route_dict_pickle_obj)



    stop_times_merged_data=pd.read_csv("stop_times_merged.txt")
    df_stop_times_merged=stop_times_merged_data.copy()
    df_stop_times_merged["arrival_time_converted"]=pd.to_datetime(df_stop_times_merged.arrival_time)
    stops_groupby_trips = df_stop_times_merged.groupby("trip_id")[["stop_id","arrival_time_converted"]]
    stops_in_trip_dict={}
    for trip_id,content in tqdm(stops_groupby_trips):
        # stops_in_trip_dict[trip_id]={}
        content.sort_values("arrival_time_converted")
        content["arrival_time_converted"]=content["arrival_time_converted"].astype(str)
        temp={}
        for i in range(content.shape[0]):
            temp[content.stop_id.iloc[i]]=content.arrival_time_converted.iloc[i]
        stops_in_trip_dict[trip_id] = temp
    with open("stops_in_trip_dict_pickle.pkl","wb") as stops_in_trip_dict_pickle_obj:
        pickle.dump(stops_in_trip_dict,stops_in_trip_dict_pickle_obj)



    transfers_data=pd.read_csv("transfers.txt")
    df_transfers=transfers_data.copy()
    df_transfers["time"]=pd.to_datetime(df_transfers['min_transfer_time'],unit="s",origin="2019-06-10").dt.time
    df_transfers["time"]=df_transfers["time"].astype(str)
    foothpath_stops_groupby_stops=df_transfers.groupby("from_stop_id")[["to_stop_id","time"]]
    footpath_dict={}
    for stop_id,content in tqdm(foothpath_stops_groupby_stops):
        temp=[]
        for i in range(content.shape[0]):
            temp1=(content.to_stop_id.iloc[i],content.time.iloc[i])
            temp.append(temp1)
        footpath_dict[stop_id]=temp
    with open("footpath_dict_pickle.pkl","wb") as footpath_dict_pickle_obj:
        pickle.dump(footpath_dict,footpath_dict_pickle_obj)

# labels_dict={201:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              202:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              203:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              204:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              205:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              206:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              207:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              208:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59'],
#              209:['2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59','2021-06-10 23:59:59']}
#
# labels_star={201:'2021-06-10 23:59:59',
#              202:'2021-06-10 23:59:59',
#              203:'2021-06-10 23:59:59',
#              204:'2021-06-10 23:59:59',
#              205:'2021-06-10 23:59:59',
#              206:'2021-06-10 23:59:59',
#              207:'2021-06-10 23:59:59',
#              208:'2021-06-10 23:59:59',
#              209:'2021-06-10 23:59:59'}
#
# mark_dict={201:0,
#              202:0,
#              203:0,
#              204:0,
#              205:0,
#              206:0,
#              207:0,
#              208:0,
#              209:0}
# routes_serving_stops={201:[1001,1002],
#                       202:[1001,1003],
#                       203:[1001],
#                       204:[1005],
#                       205:[1002],
#                       206:[1003],
#                       207:[1005],
#                       208:[1002,1004],
#                       209:[1003,1004,1005]}
#
# stops_in_route_dict={1001:[201,202,203],  # stops should be in order the of there occurence in route
#                 1002:[201,205,208],
#                 1003:[202,206,209],
#                 1004:[208,209],
#                 1005:[209,207,204]}
#
# footpath_dict={203:[(204,'00:40:00')]} # here in this demo network only one foothpath connection from 203  to 204 but in actual there can be many footpath connection from 203 to many stops , so change data structure of foothpath!
#
# trips_in_route_dict={1001:[1,6,11,16,21,26,31,36],
#                 1002:[2,7,12,17,22,27,32,37],
#                 1003:[3,8,13,18,23,28,33,38],
#                 1004:[4,9,14,19,24,29,34,39],
#                 1005:[5,10,15,20,25,30,35,40]}
#
# stops_in_trip_dict={1:{201:"2021-06-10 08:00:00",202:"2021-06-10 08:10:00",203:"2021-06-10 08:20:00"},
#                6:{201: "2021-06-10 08:10:00",202: "2021-06-10 08:20:00",203: "2021-06-10 08:30:00"},
#                11:{201:"2021-06-10 08:20:00",202:"2021-06-10 08:30:00",203:"2021-06-10 08:40:00"},
#                16:{201:"2021-06-10 08:30:00",202:"2021-06-10 08:40:00",203:"2021-06-10 08:50:00"},
#                21:{201:"2021-06-10 08:40:00",202:"2021-06-10 08:50:00",203:"2021-06-10 09:00:00"},
#                26:{201:"2021-06-10 08:50:00",202:"2021-06-10 09:00:00",203:"2021-06-10 09:10:00"},
#                31:{201:"2021-06-10 09:00:00",202:"2021-06-10 09:10:00",203:"2021-06-10 09:20:00"},
#                36:{201:"2021-06-10 09:10:00",202:"2021-06-10 09:20:00",203:"2021-06-10 09:30:00"},
#
#                2:{201:"2021-06-10 08:00:00",205:"2021-06-10 08:10:00",208:"2021-06-10 08:20:00"},
#                7:{201: "2021-06-10 08:10:00",205: "2021-06-10 08:20:00",208: "2021-06-10 08:30:00"},
#                12:{201:"2021-06-10 08:20:00",205:"2021-06-10 08:30:00",208:"2021-06-10 08:40:00"},
#                17:{201:"2021-06-10 08:30:00",205:"2021-06-10 08:40:00",208:"2021-06-10 08:50:00"},
#                22:{201:"2021-06-10 08:40:00",205:"2021-06-10 08:50:00",208:"2021-06-10 09:00:00"},
#                27:{201:"2021-06-10 08:50:00",205:"2021-06-10 09:00:00",208:"2021-06-10 09:10:00"},
#                32:{201:"2021-06-10 09:00:00",205:"2021-06-10 09:10:00",208:"2021-06-10 09:20:00"},
#                37:{201:"2021-06-10 09:10:00",205:"2021-06-10 09:20:00",208:"2021-06-10 09:30:00"},
#
#                3:{202:"2021-06-10 08:00:00",206:"2021-06-10 08:10:00",209:"2021-06-10 08:20:00"},
#                8:{202: "2021-06-10 08:10:00",206: "2021-06-10 08:20:00",209: "2021-06-10 08:30:00"},
#                13:{202:"2021-06-10 08:20:00",206:"2021-06-10 08:30:00",209:"2021-06-10 08:40:00"},
#                18:{202:"2021-06-10 08:30:00",206:"2021-06-10 08:40:00",209:"2021-06-10 08:50:00"},
#                23:{202:"2021-06-10 08:40:00",206:"2021-06-10 08:50:00",209:"2021-06-10 09:00:00"},
#                28:{202:"2021-06-10 08:50:00",206:"2021-06-10 09:00:00",209:"2021-06-10 09:10:00"},
#                33:{202:"2021-06-10 09:00:00",206:"2021-06-10 09:10:00",209:"2021-06-10 09:20:00"},
#                38:{202:"2021-06-10 09:10:00",206:"2021-06-10 09:20:00",209:"2021-06-10 09:30:00"},
#
#                4:{208:"2021-06-10 08:00:00",209:"2021-06-10 08:10:00"},
#                9:{208:"2021-06-10 08:10:00",209:"2021-06-10 08:20:00"},
#                14:{208:"2021-06-10 08:20:00",209:"2021-06-10 08:30:00"},
#                19:{208:"2021-06-10 08:30:00",209:"2021-06-10 08:40:00"},
#                24:{208:"2021-06-10 08:40:00",209:"2021-06-10 08:50:00"},
#                29:{208:"2021-06-10 08:50:00",209:"2021-06-10 09:00:00"},
#                34:{208:"2021-06-10 09:00:00",209:"2021-06-10 09:10:00"},
#                39:{208:"2021-06-10 09:10:00",209:"2021-06-10 09:20:00"},
#
#                5:{209:"2021-06-10 08:00:00",207:"2021-06-10 08:10:00",204:"2021-06-10 08:20:00"},
#                10:{209:"2021-06-10 08:10:00",207:"2021-06-10 08:20:00",204:"2021-06-10 08:30:00"},
#                15:{209:"2021-06-10 08:20:00",207:"2021-06-10 08:30:00",204:"2021-06-10 08:40:00"},
#                20:{209:"2021-06-10 08:30:00",207:"2021-06-10 08:40:00",204:"2021-06-10 08:50:00"},
#                25:{209:"2021-06-10 08:40:00",207:"2021-06-10 08:50:00",204:"2021-06-10 09:00:00"},
#                30:{209:"2021-06-10 08:50:00",207:"2021-06-10 09:00:00",204:"2021-06-10 09:10:00"},
#                35:{209:"2021-06-10 09:00:00",207:"2021-06-10 09:10:00",204:"2021-06-10 09:20:00"},
#                40:{209:"2021-06-10 09:10:00",207:"2021-06-10 09:20:00",204:"2021-06-10 09:30:00"},
#                }



# backtrack_label={201:{},
#                  202:{},
#                  203:{},
#                  204:{},
#                  205:{},
#                  206:{},
#                  207:{},
#                  208:{},
#                  209:{}}
#

# print(stops_in_trip_dict[1][201])

def et(route, stop_in_route, time_in_earlier, trips_in_route_dict, stops_in_trip_dict):

    for trip_id in trips_in_route_dict[route]:
        # print(trip_id)
        if dt.datetime.strptime(stops_in_trip_dict[trip_id][stop_in_route],'%Y-%m-%d %H:%M:%S')>=dt.datetime.strptime(time_in_earlier,'%Y-%m-%d %H:%M:%S'):
            return trip_id
    return -1

def RAPTOR(source_stop_id, destination_stop_id, dep_time, labels_dict,trips_in_route_dict,stops_in_trip_dict,k=6):
    '''

    :param source_stop_id:
    :param destination_stop_id:
    :param dep_time:
    :param labels_dict:
    :param trips_in_route_dict:
    :param stops_in_trip_dict:
    :param k:
    :return:
    '''
    labels_dict[source_stop_id][0]=dep_time # time store in string format
    mark_dict[source_stop_id]=1
    i=1 # because zero trip do not signify anything!
    while(i<k):
        print("Round", i)
        Q={} # key=route, value=stop_id
        for marked_stop in mark_dict:
            if mark_dict[marked_stop]==1:
                for route in routes_serving_stops[marked_stop]:
                    if route in Q.keys():
                        if stops_in_route_dict[route].index(marked_stop) < stops_in_route_dict[route].index(Q[route]):
                            Q[route]=marked_stop
                    else:
                        Q[route]=marked_stop
            mark_dict[marked_stop]=0


        for route in Q:
            t = -1
            for stop_in_route in stops_in_route_dict[route][stops_in_route_dict[route].index(Q[route]):]:
                if ((t!=-1) and (dt.datetime.strptime(stops_in_trip_dict[t][stop_in_route],'%Y-%m-%d %H:%M:%S') < min(dt.datetime.strptime(labels_star[stop_in_route],'%Y-%m-%d %H:%M:%S'),dt.datetime.strptime(labels_star[destination_stop_id],'%Y-%m-%d %H:%M:%S')))):
                    # print(t)
                    labels_dict[stop_in_route][i] = dt.datetime.strptime(stops_in_trip_dict[t][stop_in_route],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    labels_star[stop_in_route] = dt.datetime.strptime(stops_in_trip_dict[t][stop_in_route],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    mark_dict[stop_in_route]=1
                    ''' THIS IS THE PLACE TO ADD BACKTRACKING LABELS because here time of next stop in the route is assigned! '''
                    # backtrack_label[stop_in_route].update({i:[stops_in_route_dict[route][(stops_in_route_dict[route].index(stop_in_route))-1], indicator]})

                if (t==-1) or (dt.datetime.strptime(labels_dict[stop_in_route][i-1],'%Y-%m-%d %H:%M:%S')<=dt.datetime.strptime(stops_in_trip_dict[t][stop_in_route],'%Y-%m-%d %H:%M:%S')):
                    t = et(route,stop_in_route,labels_dict[stop_in_route][i-1],trips_in_route_dict,stops_in_trip_dict)

        temp_mark = []
        for marked_stop in mark_dict:
            if mark_dict[marked_stop]==1:
                if marked_stop in footpath_dict:
                    for tup in footpath_dict[marked_stop]:
                        temp_mark.append(tup[0])
                        if (dt.datetime.strptime(labels_dict[tup[0]][i],'%Y-%m-%d %H:%M:%S')) < (dt.datetime.strptime(labels_dict[marked_stop][i], '%Y-%m-%d %H:%M:%S')-dt.datetime.strptime('00:00:00', '%H:%M:%S')+dt.datetime.strptime(tup[1], '%H:%M:%S')):
                            labels_dict[tup[0]][i]=dt.datetime.strptime(labels_dict[tup[0]][i],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                            labels_star[tup[0]]=dt.datetime.strptime(labels_dict[tup[0]][i],'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            time=(dt.datetime.strptime(labels_dict[marked_stop][i], '%Y-%m-%d %H:%M:%S')-dt.datetime.strptime('00:00:00', '%H:%M:%S')+dt.datetime.strptime(tup[1], '%H:%M:%S'))
                            labels_dict[tup[0]][i]=str(time)
                            labels_star[tup[0]]=str(time)
        for stop in temp_mark:
            mark_dict[stop]=1
        ''' THIS IS THE PLACE TO ADD BACKTRACKING LABELS because here time of next stop due to footpath in the route is assigned! '''
                        # backtrack_label[footpath_dict[marked_stop][0]][i]=[marked_stop,indicator]

        for marked_stop in mark_dict:
            if mark_dict[marked_stop]==1:
                i+=1
                break
        else:
            i=k



    return labels_dict

''' ALL THE PICKLED FILES'''
with open("labels_dict_pickle.pkl","rb") as labels_dict_pickle_obj:
    labels_dict=pickle.load(labels_dict_pickle_obj)
with open("labels_star_pickle.pkl","rb") as labels_star_pickle_obj:
    labels_star = pickle.load(labels_star_pickle_obj)
with open("mark_dict_pickle.pkl","rb") as mark_dict_pickle_obj:
    mark_dict=pickle.load(mark_dict_pickle_obj)
with open("routes_serving_stops_pickle.pkl","rb") as routes_serving_stops_pickle_obj:
    routes_serving_stops=pickle.load(routes_serving_stops_pickle_obj)
with open("stops_in_route_dict_pickle.pkl","rb") as stops_in_route_dict_pickle_obj:
    stops_in_route_dict=pickle.load(stops_in_route_dict_pickle_obj)
with open("trips_in_route_dict_pickle.pkl","rb") as trips_in_route_dict_pickle_obj:
    trips_in_route_dict=pickle.load(trips_in_route_dict_pickle_obj)
with open("stops_in_trip_dict_pickle.pkl","rb") as stops_in_trip_dict_pickle_obj:
    stops_in_trip_dict=pickle.load(stops_in_trip_dict_pickle_obj)
with open("footpath_dict_pickle.pkl","rb") as footpath_dict_pickle_obj:
    footpath_dict=pickle.load(footpath_dict_pickle_obj)



''' CALLING THE RAPTOR FUNCTION'''
source = 2077
destination = 1482
departure_time="2019-06-10 00:00:00"
start_time=time.time()
final_label=RAPTOR(source, destination, departure_time, labels_dict, trips_in_route_dict, stops_in_trip_dict)
# print(final_label[destination])
print()
print("Pareto Optimal Journeys")
for i in range(len(final_label[destination])):
    if final_label[destination][i]!="2021-06-10 23:59:59":
        print("time=",final_label[destination][i],"with number of trip=",i)
for i in range(len(final_label[destination])):
    if final_label[destination][i]!="2021-06-10 23:59:59":
        break
else:
    print("NO JOURNEY IS AVAILABLE WITH MAXIMUM 5 TRANSFERS")
last_time=time.time()
print("time taken by algorithm in seconds =",last_time-start_time)
