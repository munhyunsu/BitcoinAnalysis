var express = require('express');
var main = express();

var fs = require('fs');
var spawn = require('child_process').spawn;

var bodyParser = require('body-parser');
main.use(bodyParser.urlencoded({ extended: false }));

main.locals.pretty = true;
main.set('views', './views');
main.set('view engine', 'jade');

var path = require('path');
main.use(express.static(path.join(__dirname, '/')));

var sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('/home/juseong/Tracking_Bitcoin/Database/dbv3-index.db', sqlite3.OPEN_READWRITE, (err) => {
	if (err) {
		console.log(err);
	} else {
		console.log('sqlite3 dbv3-index database connected success');
	}
});
const db_predict = new sqlite3.Database('/home/juseong/Bitcoin_Web/210306_bitcoin_service/db_file/db3-predict.db', sqlite3.OPEN_READWRITE, (err) => {
	if (err) {
		console.log(err);
	} else {
		console.log('sqlite3 db3-predict database connected success');
	}
});

// query -> sqlite3 database -> but get time... very slow
main.get('/', (req, res) => {
	const query = `SELECT ADDR_COUNT.AID, TX_COUNT.TID, BLK_COUNT.BID
					FROM 
					(SELECT (id) AS AID FROM AddrID ORDER BY ROWID DESC LIMIT 1) AS ADDR_COUNT,
					(SELECT (id) AS TID FROM TxID ORDER BY ROWID DESC LIMIT 1) AS TX_COUNT,
					(SELECT (id) AS BID FROM BlkID ORDER BY ROWID DESC LIMIT 1) AS BLK_COUNT`;
	db.serialize();
	db.all(query, (err, row) => {
		const acount = row[0]["AID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		const tcount = row[0]["TID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		const bcount = row[0]["BID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		res.render('main_page', {addr:acount, tx:tcount, blk:bcount});
	});
});

var saddr = ""; var smodel = ""; var py_result = "";
var ps = require('python-shell');
main.post('/page', (req, res) => {
	var str_addr = req.body.addr;
	saddr = str_addr;
	var select_ml = req.body.sel_ml;
	smodel = select_ml;

	console.log('Select machine learning model : ' + select_ml);
	console.log('Start ' + str_addr + ' classifiaction with python file');

	if(select_ml == 'model_one') {
		args_list = ['--debug', '--model', '/home/juseong/Bitcoin_Web/210306_bitcoin_service/model_file/part1_category_clf.pkl', '--target', str_addr]
	} else if(select_ml == 'part2-slow') {
		args_list = ['--debug', '--model', '/home/juseong/Bitcoin_Web/210306_bitcoin_service/model_file/part2_1_category_classifiaction.pkl', '--target', str_addr]
	} else if(select_ml == 'part3-txoutgroup') {
		args_list = ['--debug', '--model', '/home/juseong/Bitcoin_Web/210306_bitcoin_service/model_file/part3_category_classification_TxOutgroup.pkl', '--target', str_addr]
	}

	var options = {
		mode: 'text',
		pythonPath: '',
		pythonOptions: ['-u'],
		scriptPath: '',
		args: args_list
	};

	ps.PythonShell.run('./python_file/classification_manage.py', options, (err, result) => {
		if(err) throw err;
		py_result = result;
		res.redirect('/pages/info');
	});
});

main.get('/pages/info', (req, res) => {
	console.log(py_result);
	var array_Predict = py_result.join(',').split('|');

	if(saddr.length <= 45) {
		// cluster addr list
		const cquery = `SELECT addr FROM Cluster`;
		db_predict.serialize();
		db_predict.all(cquery, (err, rows) => {
			var addr_list = []; var addr_type = {'P2PKH':0, 'P2SH':0, 'Bech32':0};
			for(var i = 0; i < rows.length; i++) {
				var adict = {'addr':rows[i]['addr']};
				addr_list.push(adict);
	
				if(rows[i]['addr'].slice(0, 1) == '1') {
					addr_type['P2PKH'] += 1;
				} else if(rows[i]['addr'].slice(0, 1) == '3') {
					addr_type['P2SH'] += 1;
				} else {
					addr_type['Bech32'] += 1;
				}
			}
			
			res.render('classify_category', {tag_list:array_Predict[0], category:array_Predict[1].slice(1), addr:saddr, model:smodel, tx:array_Predict[2].slice(1), btc:array_Predict[3], txcount:array_Predict[4], cluster_size:array_Predict[5], balance:array_Predict[6], cluster_addrlist:addr_list, cluster_p2pkh:addr_type['P2PKH'].toString(), cluster_p2sh:addr_type['P2SH'].toString(), cluster_bech32:addr_type['Bech32'].toString()});
		});
	} else if(saddr.slice(0,6).indexOf('000000') > -1) { // block
		res.render('block_information', {blockhash:saddr, blockheight:array_Predict[0], blocktimestamp: array_Predict[1]});
	} else {
		res.render('tx_classify', {tx:saddr, intags:array_Predict[0].split(','), outtags:array_Predict[1].split(','), incats:array_Predict[2].slice(1), incat_count:array_Predict[3], outcats:array_Predict[4].split(','), inbtc:array_Predict[5], outbtc:array_Predict[6], incblock:array_Predict[7]});
	}
});

main.get('/f4', (req, res) => {
	const query = `SELECT ADDR_COUNT.AID, TX_COUNT.TID, BLK_COUNT.BID
					FROM 
					(SELECT (id) AS AID FROM AddrID ORDER BY ROWID DESC LIMIT 1) AS ADDR_COUNT,
					(SELECT (id) AS TID FROM TxID ORDER BY ROWID DESC LIMIT 1) AS TX_COUNT,
					(SELECT (id) AS BID FROM BlkID ORDER BY ROWID DESC LIMIT 1) AS BLK_COUNT`;
	db.serialize();
	db.all(query, (err, row) => {
		const acount = row[0]["AID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		const tcount = row[0]["TID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		const bcount = row[0]["BID"].toString().replace(/\B(?<!\.\d*)(?=(\d{3})+(?!\d))/g, ",");
		res.render('main_page_f4', {addr:acount, tx:tcount, blk:bcount});
	});
});

main.listen(3306, '127.0.0.1', () => {
	console.log('Connected, 3306 port!');
});
