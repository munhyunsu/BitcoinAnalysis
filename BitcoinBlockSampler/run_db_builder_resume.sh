#!/bin/bash

CMD='SELECT MAX(id) FROM BlkID;'
BHEIGHT=`sqlite3 dbv3-index.db "${CMD}"`
echo "Resume from ${BHEIGHT}"

python3 main_db_builder.py --debug --type index --cachesize -6553600 --resume
python3 main_db_builder.py --debug --type core --index dbv3-index.db --cachesize -6553600 --resume

echo "Update Edge Database"
CMD="ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;

INSERT OR IGNORE INTO Edge (tx, src, dst, btc)
SELECT TXI.tx, TXI.addr, TXO.addr, TXO.btc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxIn.n AS n, 
             DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND 
                                 DBCORE.TxOut.n = DBCORE.TxIn.pn
      WHERE DBCORE.TxIn.tx IN (SELECT DBCORE.BlkTx.tx
                               FROM DBCORE.BlkTx
                               WHERE DBCORE.BlkTx.blk >= ${BHEIGHT})) AS TXI
INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n, 
                   DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
            FROM DBCORE.TxOut
            WHERE DBCORE.TxOut.tx IN (SELECT DBCORE.BlkTx.tx
                                      FROM DBCORE.BlkTx
                                      WHERE DBCORE.BlkTx.blk >= ${BHEIGHT})
           ) AS TXO ON TXO.tx = TXI.tx;"
sqlite3 dbv3-util.db "${CMD}"

python3 main_clusterer_bottomup.py --debug --index dbv3-index.db --core dbv3-core.db --service dbv3-service.db
echo "Resume complete"
