#!/bin/bash

python3 main_db_builder.py --debug --type index --pagesize 65536 --cachesize 6553600
python3 main_db_builder.py --debug --type core --index dbv3-index.db --pagesize 65536 --cachesize 6553600

echo "Update Edge Database"
CMD="ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;

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
           ) AS TXO ON TXO.tx = TXI.tx;"
sqlite3 dbv3-util.db "${CMD}"
echo "Resume complete"
