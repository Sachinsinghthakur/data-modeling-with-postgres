import os
import glob
import pandas as pd
from sql_queries import *
import psycopg2

import numpy
from psycopg2.extensions import register_adapter, AsIs


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)



def get_all_json_files_path(path):
    json_files =[]
    for root,folders,files in os.walk(path):
        temp_paths = glob.glob(os.path.join(root,"*.json"))
        for f in temp_paths:
            json_files.append(os.path.abspath(f))
    return json_files
        

def insert_from_song_data(cur,path):
    
    song_files_path=get_all_json_files_path(os.path.abspath(path))
        
    dfs=pd.concat(pd.read_json(f,lines=True) for f in song_files_path)
    
    song_data_df=dfs[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    
    cur.executemany(insert_into_songs,song_data_df)
    
    artist_data_df=dfs[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()
    
    cur.executemany(insert_into_artists,artist_data_df)


def insert_from_load_data(cur,path):
    
    log_files_path=get_all_json_files_path(os.path.abspath(path))
    
    log_df=pd.concat(pd.read_json(f,lines=True) for f in log_files_path)
    
    log_df.reset_index(drop=True,inplace=True)
    
    
    user_data = log_df[["userId","firstName","lastName","gender","level"]].values.tolist()
    
    cur.executemany(insert_into_users,user_data)
    
    
    
    
    
    temp_df=log_df[log_df["page"]=="NextSong"].copy()
    
    temp_df["ts"]=pd.to_datetime(temp_df["ts"],unit="ms")
    
    time_df_values = (temp_df.ts, temp_df.ts.dt.hour , temp_df.ts.dt.day , temp_df.ts.dt.dayofweek , temp_df.ts.dt.month , temp_df.ts.dt.year , temp_df.ts.dt.weekday)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_df_values)))
    
    time_df.reset_index(drop=True,inplace=True)
    
    time_data=time_df.values.tolist()
    
    cur.executemany(insert_into_time,time_data)
    

    

    
    log_df["ts"] = pd.to_datetime(log_df["ts"],unit="ms")
    
    log_df['ts'] = log_df['ts'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    
    log_df = log_df[log_df['length'].notna()]
    
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(select_song_artist_id, (row.song, row.artist, row.length))
        result = cur.fetchone()
        
        if result:
            song_id,artist_id =result
        else:
            song_id,artist_id=None,None
    
        songplay_data=[index+1,song_id,artist_id,row.level,row.ts,row.userId,row.sessionId,row.location,row.userAgent]
        cur.execute(insert_into_songplays,songplay_data)
    
    
    

def main():
    try:
        conn=psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=Bas617448")
    except psycopg2.Error() as e:
        print("error while connecting with new database \n")
        print(e)
        
    try:
        cur=conn.cursor()
    except psycopg2.Error() as e:
        print("error while creating the cursor for the new database \n")
        print(e)
        
    path_for_song_data = "C:\\Users\\Canopus-57\\postgresDM\\data\\song_data"
    
    path_for_log_data = "C:\\Users\\Canopus-57\\postgresDM\\data\\log_data"
    
    insert_from_song_data(cur, path_for_song_data)
    insert_from_load_data(cur, path_for_log_data)
    
    conn.commit()
    
    conn.close()
    
if __name__=="__main__":
    
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_int32)

    main()
    
    

    
    
    