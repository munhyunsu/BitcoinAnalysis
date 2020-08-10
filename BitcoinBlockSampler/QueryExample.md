# 데이터베이스 쿼리 예제

## 준비물
1. Version 3 Index, Core 데이터베이스 구축

### 사전 작업
1. Core 데이터베이스 연결
```bash
sqlite3 ./dbv3-core.db
```

2. Index 데이터베이스 연결
```sql
ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;
```

3. 결과 헤더 On
```
.header on
```

##### 입출금 정보 조회

```sql
-- 입금 횟수 및 총 금액
SELECT COUNT(*) AS Indegree, SUM(btc) AS Income
FROM TxOut
WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                    FROM DBINDEX.AddrID
                    WHERE DBINDEX.AddrID.addr = 'ADDR');
```

```sql
-- 출금 횟수 및 총 금액
SELECT COUNT(*) AS Outdegree, SUM(btc) AS Outcome
FROM TxIn
INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                    FROM DBINDEX.AddrID
                    WHERE DBINDEX.AddrID.addr = 'ADDR');
```

```sql
-- 현 잔액
SELECT Income.value-Outcome.value AS Balance
FROM
(SELECT SUM(btc) AS value
 FROM TxOut
 WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                     FROM DBINDEX.AddrID
                     WHERE DBINDEX.AddrID.addr = 'ADDR')) AS Income,
(SELECT SUM(btc) AS value
 FROM TxIn
 INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
 WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                     FROM DBINDEX.AddrID
                     WHERE DBINDEX.AddrID.addr = 'ADDR')) AS Outcome;
```

```sql
-- 특정 시점 손익
SELECT Income.value-Outcome.value AS Balance
FROM
(SELECT SUM(btc) AS value
 FROM TxOut
 WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                     FROM DBINDEX.AddrID
                     WHERE DBINDEX.AddrID.addr = 'ADDR') AND
       TxOut.tx IN (SELECT BlkTx.tx
                    FROM BlkTx
                    INNER JOIN BlkTime ON BlkTime.blk = BlkTx.blk
                    WHERE (SELECT STRFTIME('%s', 'DATETIME1')) <= BlkTime.unixtime AND
                           BlkTime.unixtime <= (SELECT STRFTIME('%s', 'DATETIME2')))) AS Income,
(SELECT SUM(btc) AS value
 FROM TxIn
 INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
 WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                     FROM DBINDEX.AddrID
                     WHERE DBINDEX.AddrID.addr = 'ADDR') AND
       TxIn.tx IN (SELECT BlkTx.tx
                   FROM BlkTx
                   INNER JOIN BlkTime ON BlkTime.blk = BlkTx.blk
                   WHERE (SELECT STRFTIME('%s', 'DATETIME1')) <= BlkTime.unixtime AND
                          BlkTime.unixtime <= (SELECT STRFTIME('%s', 'DATETIME2')))) AS Outcome;
```

##### 유용한 데이터베이스

```sql
-- UTXO 주소
SELECT DBINDEX.TxID.txid AS txid, DBCORE.TxOut.n AS n, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxOut
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBCORE.TxOut.tx
WHERE NOT EXISTS (SELECT *
                  FROM DBCORE.TxIn
                  WHERE DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
                        DBCORE.TxIn.pn = DBCORE.TxOut.n);
GROUP BY txid, n;
```

```sql
-- UTXO 트랜잭션 번호, 인덱스
SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n
FROM DBCORE.TxOut
WHERE NOT EXISTS (SELECT *
                  FROM DBCORE.TxIn
                  WHERE DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
                        DBCORE.TxIn.pn = DBCORE.TxOut.n);
GROUP BY tx, n;
```

##### 클러스터 데이터베이스
```sql
SELECT DBINDEX.AddrID.addr
FROM Cluster
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = Cluster.addr
WHERE Cluster.cluster = (SELECT Cluster.cluster
                         FROM Cluster
                         WHERE Cluster.addr = (SELECT DBINDEX.Addr.id 
                                               FROM DBINDEX.Addr
                                               WHERE DBINDEX.Addr.addr = 'ADDR'));
```