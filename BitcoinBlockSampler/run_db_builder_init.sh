#!/bin/bash

python3 main_db_builder.py --debug --type index --pagesize 65536 --cachesize -6553600
python3 main_db_builder.py --debug --type core --index dbv3-index.db --pagesize 65536 --cachesize -6553600

echo "Update Edge Database"
CMD="
PRAGMA synchronous = OFF;
PRAGMA journal_mode = OFF;

PRAGMA cache_size = -6553600;
PRAGMA page_size = 65536;
VACUUM;

ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;

CREATE TABLE IF NOT EXISTS Edge (
    tx INTEGER NOT NULL,
    src INTEGER NOT NULL,
    dst INTEGER NOT NULL,
    btc REAL NOT NULL,
    UNIQUE (tx, src, dst, btc));

INSERT OR IGNORE INTO Edge (tx, src, dst, btc)
SELECT TXI.tx, TXI.addr, TXO.addr, TXO.btc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxIn.n AS n, 
             DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND 
                                 DBCORE.TxOut.n = DBCORE.TxIn.pn) AS TXI
INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n, 
                   DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
            FROM DBCORE.TxOut
           ) AS TXO ON TXO.tx = TXI.tx;
           
CREATE INDEX idx_Edge_1 ON Edge(tx);
CREATE INDEX idx_Edge_2 ON Edge(src);
CREATE INDEX idx_Edge_3 ON Edge(dst);

PRAGMA cache_size = -2000;
PRAGMA page_size = 4096;
VACUUM;

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;
"
sqlite3 dbv3-util.db "${CMD}"
echo "Building complete"
