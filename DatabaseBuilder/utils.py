import sys

import mariadb


def connectdb(user, password, host, port, database):
    try:
        conn = mariadb.connect(user=user,
                               password=password,
                               host=host,
                               port=port,
                               database=database)
    except mariadb.Error as e:
        print(f'Error connecting to MariaDB: {e}')
        sys.exit(1)
    cur = conn.cursor()
    
    return conn, cur
