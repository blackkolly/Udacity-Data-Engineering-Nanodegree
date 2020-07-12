import os
import glob
import psycopg2
import pandas as pd
from sql_queries2 import *


def process_song_file(cur, filepath):
    """
    Update the song table and artist record from song file
    -open the song file with pandas
    -insert the song record
    -insert artist record
    """
    # open the song file with pandas
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = \
        df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """
    Update the songplay,user and time table from log file
    -open the log file
    -filter by Nextsong action
    -convert timestamp column to datetime
    -Insert time data record
    -insert user records
    -insert songplay record
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = df.copy()
    t['ts'] = pd.to_datetime(t['ts'], unit='ms')

    # insert time data records
    time_data = (
        [(x, x.hour, x.day, x.week, x.month, x.year, x.dayofweek) for x in
         [pd.to_datetime(row, unit="ms") for row in t]])
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        starttime = pd.to_datetime(row.ts, unit='ms')

        # insert songplay record
        songplay_data = (index, starttime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        process the song and log data
        - get total number of files found
        -iterate over files and process

        """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    return None


def main():
    conn = psycopg2.connect(user="postgres",
                                  password="postgres.",
                                  host="127.0.0.1",
                                  port="5433",
                                  database="sparkifydb")
    cur = conn.cursor()

    process_data(cur, conn, filepath='C:/Users/admin/Desktop/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='C:/Users/admin/Desktop/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()