import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import statistics
import multiprocessing
import time
from sklearn.preprocessing import MinMaxScaler


class feature_extract(object):
	def __init__(self, conn, cur):
		self.conn_cluster = sqlite3.connect('/home/juseong/Tracking_Bitcoin/Database/dbv3cluster.db')
		self.cur_cluster = self.conn_cluster.cursor()

		self.sec_conn_core = sqlite3.connect("/home/juseong/Tracking_Bitcoin/Database/dbv3-core.db")
		self.sec_cur_core = self.sec_conn_core.cursor()
		self.thr_conn_core = sqlite3.connect("/home/juseong/Tracking_Bitcoin/Database/dbv3-core.db")
		self.thr_cur_core = self.thr_conn_core.cursor()

		self.conn = conn
		self.cur = cur
		self.preprocess_csv = pd.DataFrame()

	def change_str_to_int_addr(self, str_addr):
		self.cur.execute('''SELECT id FROM DBINDEX.AddrID WHERE DBINDEX.AddrID.addr = ?;''', (str_addr,))
		return self.cur.fetchone()[0]

	def multi_input_clustering(self, str_addr):
		if type(str_addr) == str:
			addr_id = self.change_str_to_int_addr(str_addr)
		else:
			addr_id = str_addr

		self.cur_cluster.execute('''SELECT address 
									FROM Cluster
									WHERE number = (SELECT number FROM Cluster
														WHERE address = ?);''', (addr_id,))
		clu_addr_list = list()
		for clu_addr in self.cur_cluster:
			clu_addr_list.append(int(clu_addr[0]))

		return clu_addr_list

	def get_tx_count(self, addr):
		if type(addr) == str:
			addr_id = self.change_str_to_int_addr(addr)
		else:
			addr_id = addr

		self.cur.execute('''SELECT tx FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr = ?;''', (addr_id,))
		txout_ = set()
		for row in self.cur:
			txout_.add(row[0])
		self.cur.execute('''SELECT TxIn.tx FROM DBCORE.TxIn
							INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
							AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr = ?;''', (addr_id,))
		txin_ = set()
		for row in self.cur:
			txin_.add(row[0])

		return (len(txout_.union(txin_)), addr_id)

	def check_one_group_del(self, check_dict):
		not_one_group = dict()
		for cd in check_dict.keys():
			if len(check_dict[cd]) != 1:
				not_one_group[cd] = check_dict[cd]

		return not_one_group
		
	# https://kr.investing.com/crypto/bitcoin/historical-data
	def convert_btc_to_usd(self):
		bitcoin_past_csv = pd.read_csv('/home/dnlabblocksci/BitcoinAnalysis/BitcoinBlockSampler/bitcoin_past_value.csv')
		
		def change_date_format(date):
			year = date.split('년 ')
			month = year[1].split('월 ')
			day = month[1].split('일')[0]

			return year[0] + '-' + month[0] + '-' + day

		bitcoin_past_csv = bitcoin_past_csv[['날짜', '종가']]
		bitcoin_past_csv['date'] = bitcoin_past_csv.apply(lambda x:change_date_format(x['날짜']), axis=1)

		self.preprocess_csv = bitcoin_past_csv.copy()
		self.preprocess_csv = self.preprocess_csv.drop(['날짜'], axis=1)
		self.preprocess_csv = self.preprocess_csv.rename(columns = {'종가' : 'price'})
		self.preprocess_csv = self.preprocess_csv[['date', 'price']]

	def med_avg_std(self, flist):
		return (np.median(flist), np.mean(flist), statistics.stdev(flist))

	def m1_calculate(self, total_bc, blk_d):
		m1_cal = 0
		for bd in blk_d:
			m1_cal += bd * (blk_d[bd] / total_bc)

		m1_cal -= min(list(blk_d.keys()))

		return m1_cal

	def control_addr_feature(self, address_list):
		# preprocess_csv
		self.convert_btc_to_usd()
		
		## -- metadata operation -- ##
		## ------------------------ ##
		def get_addr_btc_usd_spent(addr_id, ftd, tbc, bd):
			total_spent_value = 0.0; total_spent_usd = 0.0; convert_susd = 0.0
			spent_n_input = 0; spent_n_output = 0
			self.cur.execute('''SELECT tx, SUM(btc) FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=? GROUP BY DBCORE.TxOut.tx''', (addr_id,))
			for tx, btc in self.cur.fetchall():
				# btc cal
				total_spent_value += btc

				# usd cal
				self.sec_cur_core.execute('''SELECT BlkTx.blk, BlkTime.unixtime FROM BlkTime JOIN BlkTx ON BlkTime.blk = BlkTx.blk WHERE BlkTx.tx=?''', (tx,))
				blk_, get_time = self.sec_cur_core.fetchone()

				# frequency tx dict
				if get_time not in ftd:
					ftd[get_time] = 1
				else:
					ftd[get_time] += 1

				itime = datetime.fromtimestamp(get_time).strftime('%Y-%m-%d')
				try:
					convert_susd = float(list(self.preprocess_csv[self.preprocess_csv['date'] == itime]['price'])[0].replace(",","")) # search usd
				except IndexError:
					convert_susd = float(list(self.preprocess_csv[self.preprocess_csv['date'] == '2010-07-18']['price'])[0].replace(",","")) # search usd
				total_spent_usd += btc * convert_susd

				# m1
				if blk_ not in bd:
					bd[blk_] = 1
				else:
					bd[blk_] += 1
				tbc += 1

				# n_input / n_output
				self.sec_cur_core.execute('''SELECT COUNT(addr) FROM TxOut WHERE tx=?''', (tx,))
				spent_n_output += self.sec_cur_core.fetchone()[0]
				self.sec_cur_core.execute('''SELECT COUNT(n) FROM TxIn WHERE tx=?''', (tx,))
				spent_n_input += self.sec_cur_core.fetchone()[0]

			total_n = spent_n_output + spent_n_input

			return total_spent_value, total_spent_usd, spent_n_input/total_n, spent_n_output/total_n, bd, tbc

		def get_addr_btc_usd_received(addr_id, ftd, tbc, bd):
			total_received_value = 0.0 # btc
			total_received_usd = 0.0; convert_susd = 0.0 # usd
			utxo_count = 0

			self.cur.execute('''SELECT TxIn.tx, SUM(TxOut.btc) FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=? GROUP BY DBCORE.TxIn.tx''', (addr_id,))
			for tx, btc in self.cur.fetchall():
				# btc cal
				total_received_value += btc

				# usd cal
				self.sec_cur_core.execute('''SELECT BlkTx.blk, BlkTime.unixtime FROM BlkTime JOIN BlkTx ON BlkTime.blk = BlkTx.blk WHERE BlkTx.tx = ?''', (tx,))
				blk_, get_time = self.sec_cur_core.fetchone()

				# frequency tx dict
				if get_time not in ftd:
					ftd[get_time] = 1
				else:
					ftd[get_time] += 1

				itime = datetime.fromtimestamp(get_time).strftime('%Y-%m-%d')
				try:
					convert_susd = float(list(self.preprocess_csv[self.preprocess_csv['date'] == itime]['price'])[0].replace(",","")) # search usd
				except IndexError:
					convert_susd = float(list(self.preprocess_csv[self.preprocess_csv['date'] == '2010-07-18']['price'])[0].replace(",","")) # search usd

				total_received_usd += btc * convert_susd

				# m1
				if blk_ not in bd:
					bd[blk_] = 1
				else:
					bd[blk_] += 1
				tbc += 1

				# utxo sum cal
				self.sec_cur_core.execute('''SELECT addr FROM TxOut WHERE tx=? GROUP BY addr''', (tx,))
				for addr in self.sec_cur_core.fetchall():
					try:
						self.thr_cur_core.execute('''SELECT EXISTS ( SELECT * FROM TxIn INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n WHERE TxOut.addr = ? )''', (addr[0],))
						if self.thr_cur_core.fetchone()[0] == 0:
							utxo_count += 1
					except KeyError:
						continue

			return total_received_value, total_received_usd, utxo_count, bd, tbc

		def get_age(addr_id):
			txhash_list = list()

			self.cur.execute('''SELECT MAX(InTime.max_time, OutTime.max_time)-OutTime.min_time
									FROM
									(SELECT MAX(BlkTime.unixtime) AS max_time, MIN(BlkTime.unixtime) AS min_time
										FROM DBCORE.BlkTime
										JOIN DBCORE.BlkTx ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
										JOIN DBCORE.TxOut ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
										WHERE DBCORE.TxOut.addr = ?) AS OutTime,
									(SELECT MAX(BlkTime.unixtime) AS max_time
										FROM (SELECT TxIn.tx
												FROM DBCORE.TxIn INNER
												JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
												WHERE DBCORE.TxOut.addr = ?) AS T
										INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
										INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk) AS InTime''', (addr_id, addr_id,))

			return self.cur.fetchone()[0]

		def get_balance(addr_id):
			self.cur.execute('''SELECT Income.value-Outcome.value AS Balance
										FROM 
										(SELECT SUM(btc) AS value FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=?) AS Income,
										(SELECT SUM(btc) AS value FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut
											ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=?) AS Outcome''', (addr_id, addr_id,))

			return self.cur.fetchone()[0]

		def get_number_spent_received(addr_id):
			self.cur.execute('''SELECT SpentCount.spent_ctx, ReceivedCount.received_ctx
										FROM 
										(SELECT COUNT(tx) AS spent_ctx FROM DBCORE.TxOut WHERE DBCORE.TxOut.addr=?) AS SpentCount,
										(SELECT COUNT(TxIn.tx) AS received_ctx FROM DBCORE.TxIn INNER JOIN DBCORE.TxOut
											ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n WHERE DBCORE.TxOut.addr=?) AS ReceivedCount''', (addr_id, addr_id,))
			count_feature = self.cur.fetchone()
			return count_feature[0], count_feature[1]
		## -- metadata operation -- ##
		## ------------------------ ##

		# extract feature!!
		if len(address_list) == 1:
			return [] # lack of information or new machine learning model (one address feature)

		btc_spent_list = list(); btc_received_list = list()
		usd_spent_list = list(); usd_received_list = list()
		utxo_count_list = list(); age_list = list()
		balance_list = list(); number_spent_list = list(); number_received_list = list()
		frequency_tx_list = list(); m1_cal_list = list()

		for address in address_list:
			if type(address) == str:
				self.cur.execute('''SELECT id FROM DBINDEX.AddrID WHERE DBINDEX.AddrID.addr=?''', (address,))
				index_addr = self.cur.fetchone()[0]
			else:
				index_addr = address

			frequency_tx_dict = dict(); total_blk_count = 0; blk_dict = dict()

			spent_btc, spent_usd, msn_input, msn_output, blk_dict, total_blk_count = get_addr_btc_usd_spent(index_addr, frequency_tx_dict, total_blk_count, blk_dict)
			if spent_btc == None or spent_usd == None:
				return []

			received_btc, received_usd, cutxo, blk_dict, total_blk_count = get_addr_btc_usd_received(index_addr, frequency_tx_dict, total_blk_count, blk_dict)
			if received_btc == None or received_usd == None or cutxo == None:
				return []

			age = get_age(index_addr)
			if age == None:
				return []

			balance = get_balance(index_addr)
			if balance == None:
				return []

			cspent, creceived = get_number_spent_received(index_addr)
			if cspent == None or creceived == None:
				return []
			try:
				max_freq = max(frequency_tx_dict.values())
				m1cal = self.m1_calculate(total_blk_count, blk_dict)
			except TypeError as e:
				return []

			frequency_tx_list.append(max_freq)
			m1_cal_list.append(m1cal)
			btc_spent_list.append(spent_btc)
			usd_spent_list.append(spent_usd)
			btc_received_list.append(received_btc)
			usd_received_list.append(received_usd)
			utxo_count_list.append(cutxo)
			age_list.append(age)
			balance_list.append(balance)
			number_spent_list.append(cspent)
			number_received_list.append(creceived)
		
		try:
			return [self.med_avg_std(btc_spent_list), self.med_avg_std(btc_received_list), self.med_avg_std(usd_spent_list), self.med_avg_std(usd_received_list), self.med_avg_std(utxo_count_list), self.med_avg_std(age_list), self.med_avg_std(balance_list), self.med_avg_std(number_spent_list), self.med_avg_std(number_received_list), self.med_avg_std(frequency_tx_list), self.med_avg_std(m1_cal_list)]
		except Exception as e:
			print(e)
			return []

	def manage_process(self, extx):
		save_list = self.control_addr_feature(not_one_entity_group[extx])
		if save_list == []:
			return extx, []
		return extx, save_list

	def change_dict(self, feature_dict):
		fixed_feature_dict = dict()
		feature_name = ['btc_spent', 'btc_received', 'usd_spent', 'usd_received', 'utxo_count', 'age', 'balance', 'count_spent', 'count_received', 'freq_tx', 'm1_value']
		for fn in feature_name:
			fixed_feature_dict[fn + '_mid'] = list()
			fixed_feature_dict[fn + '_avg'] = list()
			fixed_feature_dict[fn + '_std'] = list()
		
		for fl in feature_dict.keys():
			_body = feature_dict[str(fl)]
			for i, b in enumerate(_body):
				fixed_feature_dict[feature_name[i] + '_mid'] += [b[0]]
				fixed_feature_dict[feature_name[i] + '_avg'] += [b[1]]
				fixed_feature_dict[feature_name[i] + '_std'] += [b[2]]

		return list(fixed_feature_dict.keys()), np.array(list(fixed_feature_dict.values()))

	def operation_feature(self, clf, str_addr, metadata):
		feature_label, array_metadata = self.change_dict(metadata)
		minmaxsclaer_data = MinMaxScaler().fit_transform(array_metadata.T)

		return clf.predict(minmaxsclaer_data)

	# category classification part 2 (up)
	# category classification part 1 (below)
	def get_category(self, clf, str_addr):
		if type(str_addr) == str:
			addrid = self.change_str_to_int_addr(str_addr)
		else:
			addrid = str_addr

		self.cur.execute('''SELECT cluster FROM Cluster WHERE addr = ?;''', (addrid,))
		clusterid = self.cur.fetchone()[0]

		addresses = set()
		self.cur.execute('''SELECT addr FROM Cluster WHERE cluster = ?;''', (clusterid,))
		for row in self.cur.fetchall():
			addresses.add(row[0])

		inds = list(); inbs = list(); outds = list(); outbs = list()
		for address in addresses:
			for ind, inb in self.cur.execute('''SELECT COUNT(*) AS Outdegree, SUM(btc) AS Outcome
												FROM DBCORE.TxIn
												INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
												AND DBCORE.TxIn.pn = DBCORE.TxOut.n
												WHERE DBCORE.TxOut.addr = ?;''', (address,)):
				if ind is None:
					ind = 0
				if inb is None:
					inb = 0
				inds.append(ind)
				inbs.append(inb)
			for outd, outb in self.cur.execute('''SELECT COUNT(*) AS Indegree, SUM(btc) AS Income
													FROM DBCORE.TxOut
													WHERE DBCORE.TxOut.addr = ?;''', (address,)):
				if outd is None:
					outd = 0
				if outb is None:
					outb = 0
				outds.append(outd)
				outbs.append(outb)
		feature = [
			min(inds),
			statistics.mean(inds),
			statistics.median(inds),
			max(inds),
			min(inbs),
			statistics.mean(inbs),
			statistics.median(inbs),
			max(inbs),
			min(outds),
			statistics.mean(outds),
			statistics.median(outds),
			max(outds),
			min(outbs),
			statistics.mean(outbs),
			statistics.median(outbs),
			max(outbs)
		]

		return clf.predict([feature])
