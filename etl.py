# This is a sample Python script.

import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

from create_tables import init_database, redshift_cluster_down
from queries import perform_queries
from sql_statements import *


def load_staging_tables(conn, credentials):
    json_meta_bucket = "redshift-meta"
    json_paths_log_file = "logs-jsonpath.json"

    # upload json path files
    upload_file(json_paths_log_file, json_meta_bucket)

    # load from s3
    load_one_staging_table(conn=conn, table=STAGING_LOGS_TABLE, prefix="s3://udacity-dend/log_data",
                           credentials=credentials,
                           json=f"s3://{json_meta_bucket}/{json_paths_log_file}")

    load_one_staging_table(conn=conn, table=STAGING_SONG_TABLE, prefix="s3://udacity-dend/song_data",
                           credentials=credentials,
                           json="auto")

    # load from s3
    # load_one_staging_table(conn=conn, table=STAGING_LOGS_TABLE, prefix="s3://udacity-dend/log_data",
    #                        credentials=credentials,
    #                        json=f"s3://{json_meta_bucket}/{json_paths_log_file}")
    #
    # load_one_staging_table(conn=conn, table=STAGING_SONG_TABLE, prefix="s3://udacity-dend/song_data",
    #                        credentials=credentials,
    #                        json="auto")


def load_one_staging_table(conn, table, prefix, credentials, json="auto"):
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


def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True


def insert_tables(conn):
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

    # get configs
    conn, configs = init_database()

    try:
        # load staging data from s3
        load_staging_tables(conn=conn, credentials=configs.get("IAM", "ARN"))

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

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
