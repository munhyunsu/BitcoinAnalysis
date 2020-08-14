### How to use SQL query for getting transactions between addresses

1. Get the addresses of input and output

    ```sql
    .header on
    SELECT id FROM AddrID WHERE addr='FROM';
    SELECT id FROM AddrID WHERE addr='TO';
    ```

2. Prepare the query using INNER JOIN table

    ```sql
    .header on
    SELECT * FROM TxIn INNER JOIN TxOut ON TxIn.tx = TxOut.tx AND TxIn.n = TxOut.n LIMIT 10;
    ```

3. Query to find transactions from JOINED table

    ```sql
    .header on
    SELECT * FROM TxIn INNER JOIN TxOut ON TxIn.tx = TxOut.tx AND TxIn.n = TxOut.n
        WHERE TxIn.addr=FromAddrID AND TxOut.addr=ToAddrID;
    ```

    - Example: [Reference](https://www.blockchain.com/btc/tx/677b67a894d2587c423976ed65131d5ea730d9bd164e7692beffc0441f40eebf)

        ```sql
        .header on
        SELECT * FROM TxIn INNER JOIN TxOut ON TxIn.tx = TxOut.tx AND TxIn.n = TxOut.n
            WHERE TxIn.addr=339583522 AND TxOut.addr=349873947;
        ```

### Limitation for making SQL in Python3

1. Prepare the query using parameters

    ##### ATTACH
    ```sql
    ATTACH DATABASE 'dbv3-index.db' AS DBINDEX;
    ```

    ##### TxIn
    ```sql
    EXPLAIN QUERY PLAN SELECT TxIn.tx AS tx, TxIn.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
    FROM TxIn
    INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n;
    ```

    ##### TxOut
    ```sql
    EXPLAIN QUERY PLAN SELECT TxOut.tx AS tx, TxOut.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
    FROM TxOut;
    ```

    ##### Edges
    ```sql
    EXPLAIN QUERY PLAN SELECT TXI.addr, TXO.addr, TXO.btc
    FROM (SELECT TxIn.tx AS tx, TxIn.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
          FROM TxIn
          INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n) AS TXI
    INNER JOIN (SELECT TxOut.tx AS tx, TxOut.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                FROM TxOut) AS TXO ON TXI.tx = TXO.tx;
    ```

    ##### Edges and group by
    ```sql
    EXPLAIN QUERY PLAN SELECT TXI.addr, TXO.addr, COUNT(*), SUM(TXO.btc)
    FROM (SELECT TxIn.tx AS tx, TxIn.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
          FROM TxIn
          INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n) AS TXI
    INNER JOIN (SELECT TxOut.tx AS tx, TxOut.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                FROM TxOut) AS TXO ON TXI.tx = TXO.tx
    GROUP BY TXI.addr, TXO.addr;
    ```

    ##### Edge Based on blocktime and group by
    ```sql
    EXPLAIN QUERY PLAN SELECT TX.txi, TX.txo, TX.btc
    FROM (SELECT BlkTx.tx AS tx FROM BlkTime
            INNER JOIN BlkTx ON BlkTime.blk = BlkTx.blk
            WHERE 1577804400 <= BlkTime.unixtime AND
                  BlkTime.unixtime <= 1580482799) AS BT
    INNER JOIN (SELECT TXI.tx AS tx, TXI.addr AS txi, TXO.addr AS txo, TXO.btc AS btc
                FROM (SELECT TxIn.tx AS tx, TxIn.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                      FROM TxIn
                      INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n) AS TXI
                INNER JOIN (SELECT TxOut.tx AS tx, TxOut.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                            FROM TxOut) AS TXO ON TXI.tx = TXO.tx) AS TX ON BT.tx = TX.tx;
    ```

    ##### Edge Based on blocktime and group by
    ```sql
    EXPLAIN QUERY PLAN SELECT TX.txi AS src, TX.txo AS dst, COUNT(*) AS cnt, SUM(TX.btc) AS btc
    FROM (SELECT BlkTx.tx AS tx FROM BlkTime
            INNER JOIN BlkTx ON BlkTime.blk = BlkTx.blk
            WHERE 1577804400 <= BlkTime.unixtime AND
                  BlkTime.unixtime <= 1580482799) AS BT
    INNER JOIN (SELECT TXI.tx AS tx, TXI.addr AS txi, TXO.addr AS txo, TXO.btc AS btc
                FROM (SELECT TxIn.tx AS tx, TxIn.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                      FROM TxIn
                      INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n) AS TXI
                INNER JOIN (SELECT TxOut.tx AS tx, TxOut.n AS n, TxOut.addr AS addr, TxOut.btc AS btc
                            FROM TxOut) AS TXO ON TXI.tx = TXO.tx) AS TX ON BT.tx = TX.tx
    GROUP BY TX.txi, TX.txo;
    ```

2. Calculate the maximum number of parameters

    - [Maximum Length Of An SQL Statement](https://www.sqlite.org/limits.html)

    1. 1000000 - 122 = 999888

    2. 999888 / 2 = 499944 by (?,)

    3. sqrt(499944) = 707.0672

    - So, we can set about 700 parameters

3. Conclusions

    - If we need to get transactions below 700 addresses, then we just use one SQL query

    - But, we need to get transactions above 700 addresses, then we split 1 to 499943 addresses by addresses
