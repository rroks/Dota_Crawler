#!/usr/bin/env python
# -*- coding: utf-8 -*-
import multiprocessing
import time
from multiprocessing import Pool

import dota2api
import mysql.connector
from DBUtils.PooledDB import PooledDB
from dota2api.src.exceptions import APIError

from api_caller import get_match_details, get_latest_match
from db_operations import insert_match, get_latest_match_id, get_earliest_match_id

PROCESS_NUMBER = multiprocessing.cpu_count()
DB_CONNECTIONS_NUMBER = PROCESS_NUMBER + 2
DATABASE_POOL = PooledDB(mysql.connector, DB_CONNECTIONS_NUMBER, db='dota', user='fen', autocommit=True)
API_KEY = '352CBD8EBE2C282BB58AF974C9DE1FD2'
DOTA_API = dota2api.Initialise(api_key=API_KEY)


def getting_match(args):
    try:
        raw_match = get_match_details(args[0], args[1])
        return raw_match
    except APIError as e:
        print(e.msg)
    return None


def store_match(match):
    print(match)
    if match is not None and 0 != len(match):
        db_connection = DATABASE_POOL.connection()
        try:
            insert_match(db_connection, match)
            return True
        except mysql.connector.IntegrityError as e:
            print(e.msg)
        finally:
            db_connection.close()
        return False
    else:
        return False


def update_previous_matches(number_of_matches=10000):
    pool = Pool(4)
    connection = DATABASE_POOL.connection()
    earliest_local_match_id = get_earliest_match_id(connection)
    connection.close()

    match_list = ([(DOTA_API, earliest_local_match_id - i) for i in range(number_of_matches)])
    for match in pool.map(getting_match, match_list):
        if match is not None:
            store_match(match)
    pool.terminate()


def update_recent_matches(terminating_match_id):
    pool = Pool(4)
    connection = DATABASE_POOL.connection()
    latest_local_match_id = get_latest_match_id(connection)
    connection.close()

    if latest_local_match_id is not None:
        number_of_matches = 10000
        if terminating_match_id - latest_local_match_id > number_of_matches:
            upper_bound = latest_local_match_id + number_of_matches
        else:
            upper_bound = terminating_match_id + 1
        match_list = ([(DOTA_API, i + 1,) for i in range(latest_local_match_id, upper_bound)])
        for match in pool.map(getting_match, match_list):
            if match is not None:
                store_match(match)
        pool.terminate()
    else:
        flag = False
        while not flag:
            match = getting_match([DOTA_API, terminating_match_id])
            flag = store_match(match)
            terminating_match_id -= 1


if __name__ == '__main__':
    start = time.time()
    update_previous_matches()
    latest_match_id = get_latest_match(DOTA_API)
    # latest_match_id = 4048727696
    update_recent_matches(latest_match_id)
    print(time.time() - start)
