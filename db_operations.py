import mysql.connector


def insert_match(connection, match_details):
    """
    Persist match info.
    :param connection: Database connection object.
    :param match_details: A dict() of match info.
    :return: SQL execution result.
    """
    columns = get_match_columns(connection)
    column_str = ','.join(columns)
    # placeholder = ','.join(('%s',) * len(columns))
    values = list()
    for key in columns:
        if type(match_details[key]) == unicode:
            values.append(match_details[key].encode('utf-8'))
        else:
            values.append(match_details[key])
    print(columns)
    value_str = ','.join(str(match_details[key]) for key in columns)

    # sql = 'INSERT INTO match_details ({}) VALUES ({})'.format(column_str, placeholder)
    # sql = "INSERT INTO match_details VALUES (%s)" % ",".join(['?'] * len(match_details))
    sql = 'INSERT INTO match_details ({}) VALUES ({})'.format(column_str, value_str)

    cursor = connection.cursor()
    try:
        cursor.execute(sql)
    except mysql.connector.IntegrityError as e:
        # print (e.msg)
        raise e
    except Exception as e:
        raise e
    finally:
        cursor.close()


def get_latest_match_id(connection):
    """
    Query id number of the latest played match id in database.
    :param connection: Database latest object.
    :return: id number of the earliest played match.
    """
    cursor = connection.cursor()
    sql = "SELECT max(match_id) FROM match_details"
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result[0][0]


def get_earliest_match_id(connection):
    """
    Find id number of the earliest played match in database.
    :param connection: Database connection object.
    :return: Id number of the earliest played match.
    """
    cursor = connection.cursor()
    sql = "SELECT min(match_id) FROM match_details"
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result[0][0]


def get_match_columns(connection):
    """
    Get match column names of target database.
    :param connection: Database connection object.
    :return: A list() of match column names.
    """
    sql = "select column_name from information_schema.columns where table_schema='dota' and table_name='match_details';"
    cursor = connection.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    match_columns = list()
    for i in result:
        match_columns.append(i[0].encode('utf-8'))
    cursor.close()
    return match_columns
