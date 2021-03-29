import os
import pandas as pd
import sqlite3
import time
import pickle
import joblib
import collections
import multiprocessing
from datetime import datetime

from feature_operation import feature_extract

FLAGS = _ = None
DEBUG = False
STIME = None

def prepare_conn(dbpath, indexpath, corepath, predictpath):
	conn = sqlite3.connect(dbpath)
	cur = conn.cursor()
	cur.execute(f'''ATTACH DATABASE '{indexpath}' AS DBINDEX;''')
	cur.execute(f'''ATTACH DATABASE '{corepath}' AS DBCORE;''')
	cur.execute(f'''ATTACH DATABASE '{predictpath}' AS DBPREDICT;''')
	conn.commit()

	return conn, cur

def get_predict_db(conn, cur, str_addr):
	cur.execute('''SELECT id FROM DBINDEX.AddrID
					WHERE DBINDEX.AddrID.addr = ?;''', (str_addr,))
	addrid = cur.fetchone()[0]

	cur.execute('''SELECT label FROM DBPREDICT.Predict
					WHERE DBPREDICT.Predict.addr = ?;''', (addrid,))
	return cur.fetchone()[0]

def check_predict_db(conn, cur, str_addr):
	cur.execute('''SELECT id FROM DBINDEX.AddrID
					WHERE DBINDEX.AddrID.addr = ?;''', (str_addr,))
	addrid = cur.fetchone()[0]

	cur.execute('''SELECT EXISTS (
					SELECT * FROM DBPREDICT.Predict
					WHERE DBPREDICT.Predict.addr = ?)''', (addrid,))
	return cur.fetchone()[0]

# insert to clustering list
def insert_clustering_addr(conn, cur, addrid):
	global DEBUG
	global STIME

	if type(addrid) == int:
		cur.execute('''SELECT addr FROM DBINDEX.AddrID WHERE DBINDEX.AddrID.id = ?;''', (addrid,))
		str_addr = cur.fetchone()[0]
	else:
		str_addr = addrid
	
	cur.execute('''INSERT OR IGNORE INTO DBPREDICT.Cluster (addr) 
					VALUES (?);''', (str_addr,))
	conn.commit()

def insert_predict_db(conn, cur, str_addr, data):
	global DEBUG
	global STIME

	cur.execute('''SELECT id FROM DBINDEX.AddrID
					WHERE DBINDEX.AddrID.addr = ?;''', (str_addr,))
	addrid = cur.fetchone()[0]

	cur.execute('''INSERT OR IGNORE INTO DBPREDICT.Predict (addr, label)
					VALUES (?, ?);''', (addrid, data,))
	conn.commit()

def get_entity(conn, cur, addr):
	global DEBUG
	global STIME

	if type(addr) == str:
		cur.execute('''SELECT id FROM DBINDEX.AddrID
						WHERE DBINDEX.AddrID.addr = ?;''', (addr,))
		addrid = cur.fetchone()[0]
	else:
		addrid = addr

	cur.execute('''SELECT cluster FROM Cluster 
					WHERE addr = ?;''', (addrid,))
	clusterid = cur.fetchone()[0]

	cur.execute('''SELECT TagID.tag
					FROM Tag
					INNER JOIN TagID ON TagID.id = Tag.tag
					WHERE Tag.addr IN (SELECT Cluster.addr
										FROM Cluster
										WHERE Cluster.cluster = ?);''', (clusterid,))
	tags = set()
	for row in cur.fetchall():
		tags.add(row[0])

	return tags

# for get transaction classification result function
def get_tx(conn, cur, tx):
	global DEBUG
	global STIME
	
	cur.execute('''SELECT id FROM DBINDEX.TxID
					WHERE DBINDEX.TxID.txid = ?;''', (tx,))
	txid = cur.fetchone()[0]

	iaddr = set()
	cur.execute('''SELECT DBCORE.TxOut.addr
					FROM DBCORE.TxIn
					INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
												DBCORE.TxIn.pn = DBCORE.TxOut.n
					WHERE DBCORE.TxIn.tx = ?;''', (txid,))
	for row in cur.fetchall():
		iaddr.add(row[0])

	oaddr = set()
	cur.execute('''SELECT DBCORE.TxOut.addr
					FROM DBCORE.TxOut
					WHERE DBCORE.TxOut.tx = ?;''', (txid,))
	for row in cur.fetchall():
		oaddr.add(row[0])

	itag = set()
	for iad in iaddr:
		itag.update(get_entity(conn, cur, iad))

	otag = set()
	for oad in oaddr:
		otag.update(get_entity(conn, cur, oad))
	
	return iaddr, oaddr, itag, otag

# for get transaction classification result function
def classify_category_for_tx(clf, addrid):
	global NOT_ONE_ENTITY_GROUP

	clustering_list = classification.multi_input_clustering(addrid)
	if len(clustering_list) != 0:
		entity_txcount = get_tx_count_grouping(clustering_list)
		NOT_ONE_ENTITY_GROUP = classification.check_one_group_del(entity_txcount)
		metadata = get_manage_feature(NOT_ONE_ENTITY_GROUP)
		category = classification.operation_feature(clf, addrid, metadata)

		return list(collections.Counter(category).keys())
	else:
		return "X"

# for get transaction classification result function
def get_tx_cat(conn, cur, clf, tx, model):
	global DEBUG
	global STIME

	cur.execute('''SELECT id FROM DBINDEX.TxID
					WHERE DBINDEX.TxID.txid = ?;''', (tx,))
	txid = cur.fetchone()[0]

	cur.execute('''SELECT DBCORE.TxOut.addr, COUNT(DBCORE.TxOut.addr)
					FROM DBCORE.TxIn
					INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
												DBCORE.TxIn.pn = DBCORE.TxOut.n
					WHERE DBCORE.TxIn.tx = ?;''', (txid,))
	iaddr, iaddr_count = cur.fetchone()
	# below for part2 model
	#icat = classify_category_for_tx(clf, iaddr)[0]
	icat = classification.get_category(clf, iaddr)[0]

	oaddr = set()
	cur.execute('''SELECT DBCORE.TxOut.addr
					FROM DBCORE.TxOut
					WHERE DBCORE.TxOut.tx = ?;''', (txid,))
	for row in cur.fetchall():
		oaddr.add(row[0])

	ocat = set()
	for oad in oaddr:
		# below for part2 model
		#ocat.add(classify_category_for_tx(clf, oad)[0])
		ocat.add(classification.get_category(clf, oad)[0])

	return icat, iaddr_count, ocat

# for get transaction classification result function
def get_tx_basic_info(conn, cur, tx):
	cur.execute('''SELECT id FROM DBINDEX.TxID
					WHERE DBINDEX.TxID.txid = ?;''', (tx,))
	tx_id = cur.fetchone()[0]

	# input/output btc
	inaddr_btc = 0
	cur.execute('''SELECT DBCORE.TxOut.btc 
					FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut 
						ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
					WHERE DBCORE.TxIn.tx = ?;''', (tx_id,))
	for row in cur:
		inaddr_btc += row[0]

	outaddr_btc = 0
	cur.execute('''SELECT btc
					FROM DBCORE.TxOut
					WHERE DBCORE.TxOut.tx = ?;''', (tx_id,))
	for row in cur:
		outaddr_btc += row[0]

	# included block
	includ_block = 0
	cur.execute('''SELECT blk
					FROM DBCORE.BlkTx
					WHERE DBCORE.BlkTx.tx = ?;''', (tx_id,))
	includ_block = cur.fetchone()[0]

	return str(inaddr_btc), str(outaddr_btc), str(includ_block)

def prepare_clf(model_path):
	clf = None
	with open(model_path, 'rb') as f:
		clf = pickle.load(f)
	return clf

def get_tx_count(addr):
	if type(addr) == str:
		addr_id = classification.change_str_to_int_addr(addr)
	else:
		addr_id = addr

	cur.execute('''SELECT TXOUT_TX.txout_count+TXIN_TX.txin_count
						FROM
						(SELECT COUNT(tx) AS txout_count FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=?) AS TXOUT_TX,
						(SELECT COUNT(TxIn.tx) AS txin_count FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut
							ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=?) AS TXIN_TX''',
						(addr_id, addr_id,))
	return cur.fetchone()[0], addr_id

def get_tx_count_grouping(multiinput_clustering):
	entity_txcount = dict()
	pool_num = multiprocessing.cpu_count() // 2
	with multiprocessing.Pool(pool_num) as p:
		for idx, addr in p.imap(get_tx_count, multiinput_clustering):
			#if idx == addr_tx_count:
				#entity_txcount.add(addr)
			if idx not in entity_txcount:
				entity_txcount[idx] = [addr]
			else:
				temp_list = entity_txcount[idx]
				temp_list.append(addr)
				entity_txcount[idx] = temp_list

	return entity_txcount

def manage_process(extx):
	global NOT_ONE_ENTITY_GROUP
	save_list = classification.control_addr_feature(NOT_ONE_ENTITY_GROUP[extx])
	if save_list == []:
		return extx, []
	return extx, save_list

def get_manage_feature(not_one_entity_group):
	total_entity_addr_feature = dict()

	pool_num = multiprocessing.cpu_count() // 2
	with multiprocessing.Pool(pool_num) as p:
		for et, slist in p.imap(manage_process, list(not_one_entity_group.keys())):
			total_entity_addr_feature[str(et)] = slist
	return total_entity_addr_feature

# address information page -> necessary information
def input_addr_info_feature():
	cur.execute('''SELECT id FROM DBINDEX.AddrID WHERE addr = ?''', (FLAGS.target,))
	addr_id = cur.fetchone()[0]

	# start transcation, BTC
	cur.execute('''SELECT TXIN_TX.TX, SUM(TXIN_TX.INBTC+TXOUT_TX.OUTBTC)
					FROM
					(SELECT (TxIn.tx) AS TX, SUM(TxOut.btc) AS INBTC FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=? GROUP BY DBCORE.TxIn.tx) AS TXIN_TX,
					(SELECT SUM(btc) AS OUTBTC FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=?) AS TXOUT_TX''', (addr_id, addr_id,))
	start_tx, btc = cur.fetchone()

	# transaction count
	addr_tx_count, addrid = get_tx_count(addr_id)

	# balance
	cur.execute('''SELECT Income.value-Outcome.value AS Balance
					FROM
					(SELECT SUM(btc) AS value FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=?) AS Income,
					(SELECT SUM(btc) AS value FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut
						ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=?) AS Outcome''', (addr_id, addr_id,))
	balance = cur.fetchone()[0]

	return start_tx, btc, addr_tx_count, balance

def	change_txid_to_tx(str_tx):
	cur.execute('''SELECT txid FROM DBINDEX.TxID WHERE DBINDEX.TxID.id=?''', (str_tx,))
	return cur.fetchone()[0]

### --- --- --- --- --- --- --- ###

def insert_clustering_addr_db(clustering_list):
	cur.execute('''SELECT EXISTS (SELECT * FROM DBPREDICT.Cluster WHERE id=1);''')
	if cur.fetchone()[0] == 1:
		cur.execute('''SELECT id
						FROM DBINDEX.AddrID
						WHERE DBINDEX.AddrID.addr = (SELECT addr FROM DBPREDICT.Cluster ORDER BY ROWID LIMIT 1);''')
		if int(cur.fetchone()[0]) != int(clustering_list[0]):
			cur.execute('''DELETE FROM DBPREDICT.Cluster;''')
			for cl in clustering_list:
				insert_clustering_addr(conn, cur, cl)
		else:
			for cl in clustering_list:
				insert_clustering_addr(conn, cur, cl)

def main():
	global NOT_ONE_ENTITY_GROUP

	clf = prepare_clf(FLAGS.model)

	if len(FLAGS.target) <= 45:
		tags = get_entity(conn, cur, FLAGS.target)
		print(f'{tags}|')
	
		cluster_size = 0
		check_ = check_predict_db(conn, cur, FLAGS.target)
		if check_ == 0:
			if 'part2' in FLAGS.model or 'part3' in FLAGS.model:
				stime = time.time()
				clustering_list = classification.multi_input_clustering(FLAGS.target)
				cluster_size = len(clustering_list)

				# insert cluster addr
				insert_clustering_addr_db(clustering_list)

				stime = time.time()
				entity_txcount = get_tx_count_grouping(clustering_list)
				NOT_ONE_ENTITY_GROUP = classification.check_one_group_del(entity_txcount)
				metadata = get_manage_feature(NOT_ONE_ENTITY_GROUP)
				category = classification.operation_feature(clf, FLAGS.target, metadata)
				get_lab = list(collections.Counter(category).keys())[0]
				print(get_lab + '|')
			elif 'part1' in FLAGS.model:
				clustering_list = classification.multi_input_clustering(FLAGS.target)
				cluster_size = len(clustering_list)
				insert_clustering_addr_db(clustering_list)
				category = classification.get_category(clf, FLAGS.target)
				get_lab = category[0]
				print(get_lab + '|')
			
			# if need to result db below add...
			#insert_predict_db(conn, cur, FLAGS.target, get_lab)
		else:
			get_label = get_predict_db(conn, cur, FLAGS.target)
			print(get_label + '|')
		st, bc, atc, bal = input_addr_info_feature()
		print(f'{change_txid_to_tx(st)}|{str(bc)}|{str(atc)}|{str(cluster_size-1)}|{str(bal)}')
	elif FLAGS.target[:6] == "000000": # block
		# block height
		cur.execute('''SELECT id FROM BlkID WHERE blkhash = ?''', (FLAGS.target,))
		block_height = cur.fetchone()[0]
		# block timestamp
		cur.execute('''SELECT unixtime FROM DBCORE.BlkTime WHERE DBCORE.BlkTime.blk = ?''', (block_height,))
		unix2date = datetime.utcfromtimestamp(int(cur.fetchone()[0])).strftime('%Y-%m-%d %H:%M:%S')

		print(f'{str(block_height)}|{str(unix2date)}')
	else: # input target : trascation
		if 'part1' in FLAGS.model:
			in_addr, out_addr, in_tags, out_tags = get_tx(conn, cur, FLAGS.target)
			in_tags_str = ",".join(in_tags)
			print(f'{in_tags_str}|')
			out_tags_str = ",".join(out_tags)
			print(f'{out_tags_str}|')

			in_cats, in_cats_count, out_cats = get_tx_cat(conn, cur, clf, FLAGS.target, FLAGS.model)
			print(f'{in_cats}|{in_cats_count}|')
			out_cats_str = ",".join(out_cats)
			print(f'{out_cats_str}')

			# total input, output btc & included block
			tinbtc, toutbtc, included_block = get_tx_basic_info(conn, cur, FLAGS.target)
			print(f'|{tinbtc}|{toutbtc}|{included_block}')

if __name__ == '__main__':
	root_path = os.path.abspath(__file__)
	root_dir = os.path.dirname(root_path)
	os.chdir(root_dir)

	import argparse

	parser = argparse.ArgumentParser()

	parser.add_argument('--debug', action='store_true', help='The present debug message')
	parser.add_argument('--index', type=str, default='/home/juseong/Tracking_Bitcoin/Database/dbv3-index.db', help='The path for index database')
	parser.add_argument('--core', type=str, default='/home/juseong/Tracking_Bitcoin/Database/dbv3-core.db', help='The path for core database')
	parser.add_argument('--service', type=str, default='/home/juseong/Tracking_Bitcoin/Database/dbv3-service.db', help='The path for util database')
	parser.add_argument('--predict', type=str, default='/home/juseong/Bitcoin_Web/210306_bitcoin_service/db_file/db3-predict.db', help='The path for predict database')
	parser.add_argument('--model', type=str, required=True, help='The path for category model')
	parser.add_argument('--target', type=str, required=True, help='Target address')

	FLAGS, _ = parser.parse_known_args()

	FLAGS.index = os.path.abspath(os.path.expanduser(FLAGS.index))
	FLAGS.core = os.path.abspath(os.path.expanduser(FLAGS.core))
	FLAGS.service = os.path.abspath(os.path.expanduser(FLAGS.service))
	FLAGS.predict = os.path.abspath(os.path.expanduser(FLAGS.predict))
	FLAGS.model = os.path.abspath(os.path.expanduser(FLAGS.model))

	DEBUG = FLAGS.debug

	conn, cur = prepare_conn(FLAGS.service, FLAGS.index, FLAGS.core, FLAGS.predict)
	classification = feature_extract(conn, cur)
	STIME = time.time()
	NOT_ONE_ENTITY_GROUP = None
	main()
