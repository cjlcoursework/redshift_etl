# This is a sample Python script.

import time

from create_tables import init_database, redshift_cluster_down
from queries import perform_queries
from sql_statements import *

"""
    Load log and song tables from a public s3 folders directly into two STAGING tables
    - configuration data is in the dwh.cfg file in this project
    - The song_data in s3 is found in configuration(S3.SONG_DATA)
    - The log_data in s3 is found in configuration(S3.LOG_DATA)
    
    We want to ETL the 
    - COPY data from the song_data into a stage.songs staging table
    - COPY data from the log_data into a stage.logs staging table
    - INSERT into a Star schema containing the following tables:
        - fact_songplays --  events where users have 'listened' to a song
        - dim_song -- information about each song, such as title, duration, etc.
        - dim_user -- a user is the persona that listened to each song
        - dim_artist  --- an artist is the person who created each song
        - dim_time -- the time dimension just breaks out the timestamp of each event into data parts
        
    The overall flow is:
        * Initialize the Redshift database:
            - create a Redshift cluster
            - create a STAGE schema and a DATA schema
        * create the songs and logs table in the STAGE schema
        * create the star schema in the DATA schema
        * insert data from the stage tables into the Star schema

"""


def load_staging_tables(conn, configs):
    """
    Load log and song tables from a public s3 folders directly into two
    stage tables.

    Also, upload jsonpaths file if we are using a local version, or use the Udacity provided s3 file

    :param configs: configurations primarily pulled from the dwh.cfg file
    :param conn: the redshift_connection
    """
    credentials=configs.get("IAM", "ARN")
    song_data = configs.get("S3", "SONG_DATA")
    log_data = configs.get("S3", "LOG_DATA")
    # use_local_jsonpaths = False

    json_paths_log_file = configs.get("S3", "LOG_JSONPATH")

    # load from s3
    load_one_staging_table(conn=conn, table=STAGING_LOGS_TABLE, prefix=log_data,
                           credentials=credentials,
                           json=json_paths_log_file)

    load_one_staging_table(conn=conn, table=STAGING_SONG_TABLE, prefix=song_data,
                           credentials=credentials,
                           json="auto")


def load_one_staging_table(conn, table, prefix, credentials, json="auto"):
    """
    This is a worker utility to load one file into the staging schema
    TODO - this does not really check for completion

    :param conn: the redshift_connection
    :param table: the table to load into
    :param prefix: the file prefix (song_data or log_data)
    :param credentials: aws credentials
    :param json: this is "auto" when the data maps directly to the table names, or a jsonpaths file
    :return:
    """
    cursor = conn.cursor()

    sql_copy = f"""
    copy {table} from '{prefix}' 
    credentials 'aws_iam_role={credentials}'
    region 'us-west-2' 
    format as json '{json}'
    """

    print(f"Copying {table} from s3 ....  this could take several minutes ...")
    ts1 = time.time()

    cursor.execute(sql_copy)
    ts2 = time.time() - ts1
    print(f"Completed {table} took {ts2} milliseconds ...\n-------------------\n")
    conn.commit()


def insert_tables(conn):
    """
    Insert data from the staging tables (songs, logs) into the Star schema.
    The SQL statements are all constants in the sql_statements.py file

    :param conn: the Redshift connector

    """
    cursor = conn.cursor()

    print(f"Load {DIM_SONG_TABLE} ....")
    cursor.execute(insert_dim_song)

    print(f"Load {DIM_ARTIST_TABLE} ....")
    cursor.execute(insert_dim_artist)

    print(f"Load {DIM_USER_TABLE} ....")
    cursor.execute(insert_dim_user)

    print(f"Load {FACT_SONGPLAY_TABLE} ....")
    cursor.execute(insert_fact_songplay)

    print(f"Load {DIM_TIME_TABLE} ....")
    cursor.execute(insert_dim_time)

    conn.commit()


def main(drop_cluster=False):
    """
    Main flow:
        1. create Redshift cluster and database
        2. load staging data into a STAG schema
        3. insert from STAG tables into the Star schema in the DATA schema

    :param drop_cluster: whether to drop the Redshift cluster after we are done
    :return:
    """

    # get configs
    conn, configs = init_database()

    try:
        # load staging data from s3
        load_staging_tables(conn=conn, configs=configs)

        # load star schema from staging
        insert_tables(conn=conn)
        print("Done")

        # perform some queries
        perform_queries(conn)

        # drop the cluster
        if drop_cluster:
            redshift_cluster_down(configs=configs)

    finally:
        if conn:
            conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main(drop_cluster=True)


