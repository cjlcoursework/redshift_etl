from redshift_connector import Connection

from sql_statements import *

QUERIES = [
    f"select count(*) from {FACT_SONGPLAY_TABLE} limit 5",
    f"select count(*)  from {DIM_USER_TABLE} limit 5",
    f"select count(*)  from {DIM_SONG_TABLE} limit 5",
    f"select count(*)  from {DIM_ARTIST_TABLE} limit 5",
    f"select count(*)  from {DIM_TIME_TABLE} limit 5",

    f"select * from {FACT_SONGPLAY_TABLE} limit 5",
    f"select * from {DIM_USER_TABLE} limit 5",
    f"select * from {DIM_SONG_TABLE} limit 5",
    f"select * from {DIM_ARTIST_TABLE} limit 5",
    f"select * from {DIM_TIME_TABLE} limit 5",
    f"""select distinct  F.songplay_id, U.user_id, S.song_id, A.artist_id, T.time_id
     from {FACT_SONGPLAY_TABLE} F 
    join {DIM_USER_TABLE} U on U.user_id = F.user_id
    join {DIM_SONG_TABLE} S on S.song_id = F.song_id
    join {DIM_ARTIST_TABLE} A on A.artist_id = F.artist_id
    join {DIM_TIME_TABLE} T on T.time_id = F.time_id
    limit 5
    """
]


def perform_queries(conn: Connection):
    """
    Perform all queries in the global List "QUERIES"

    :param conn: A live Redshift connection
    :return:
    """
    cur = conn.cursor()
    print("\nquery tables ...")
    for query in QUERIES:
        cur.execute(query)
        print("\n----------------")
        print(query)
        print("----------------")
        results = cur.fetchall()
        for row in results:
            print(row)

