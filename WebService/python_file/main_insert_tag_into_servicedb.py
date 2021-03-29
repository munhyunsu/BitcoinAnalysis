import os
import pandas as pd
import sqlite3
import time

FLAGS = _ = None
DEBUG = False
STIME = None

def prepare_conn(dbpath, indexpath, corepath):
	conn = sqlite3.connect(dbpath)
	cur = conn.cursor()
	cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
	cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
	conn.commit()
    
	return conn, cur

def insert_db(conn, cur, data):
	global DEBUG
	global STIME

	try:
		# Get AddrID
		cur.execute('''SELECT id FROM DBINDEX.AddrID
			   WHERE DBINDEX.AddrID.addr = ?;''', (data['address'],))
		addrid = cur.fetchone()[0]
	
		# Get TagID
		cur.execute('''INSERT OR IGNORE INTO TagID (tag)
			   VALUES (?);''', (data['tag'],))
		conn.commit()
		cur.execute('''SELECT id FROM TagID
			   WHERE TagID.tag = ?;''', (data['tag'],))
		tagid = cur.fetchone()[0]
	
		# Insert Addr-Tag
		cur.execute('''INSERT OR IGNORE INTO Tag (addr, tag)
			   VALUES (?, ?);''', (addrid, tagid))
		conn.commit()
	except:
		print("Detect NonType so not insert this type")

def main():
	if DEBUG:
		print(f'Parsed arguments {FLAGS}')
		print(f'Unparsed arguments {_}')
	print(f'[{int(time.time()-STIME)}] Start tag insert')

	conn, cur = prepare_conn(FLAGS.service, FLAGS.index, FLAGS.core)

	reader = pd.read_excel(FLAGS.tag, engine='openpyxl')
	reader = reader.iloc[:1001, :]
	for row in reader.values:
		insert_db(conn, cur,
				{'tag': row[0], # clustername
				'address': row[2]}) # rootaddress
		print(f'[{int(time.time()-STIME)}] Inserted {row}')
    
	print(f'[{int(time.time()-STIME)}] Terminate tag insert')

if __name__ == '__main__':
	root_path = os.path.abspath(__file__)
	root_dir = os.path.dirname(root_path)
	os.chdir(root_dir)

	import argparse

	parser = argparse.ArgumentParser()

	parser.add_argument('--debug', action='store_true', help='The present debug message')
	parser.add_argument('--index', type=str, default='/home/juseong/Tracking_Bitcoin/Database/dbv3-index.db', help='The path for index database')
	parser.add_argument('--core', type=str, default='/home/juseong/Tracking_Bitcoin/Database/dbv3-core.db', help='The path for core database')
	parser.add_argument('--service', type=str, default='./db_file/db3-service.db', help='The path for util database')
	parser.add_argument('--tag', type=str, default='./csv_file/ChainAnalysisNamedClusters.xlsx', help='The path for tag csv')

	FLAGS, _ = parser.parse_known_args()

	FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
	FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
	FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
	FLAGS.tag = os.path.abspath(os.path.expanduser(FLAGS.tag))

	DEBUG = FLAGS.debug

	STIME = time.time()
	main()
