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

ATTACH DATABASE './dbv3-util.db' AS DBEDGE;
ATTACH DATABASE './cluster.db' AS DBCLUSTER;
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

-- 클러스터 입금 횟수 정렬
CREATE TABLE IF NOT EXISTS Node(
    addr INTEGER PRIMARY KEY)

.import csv

SELECT Node.addr AS addr, DBINDEX.AddrID.addr AS addr_hash,
       COUNT(DBCORE.TxOut.tx) AS indegree, SUM(DBCORE.TxOut.btc) AS income
FROM Node
INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.addr = Node.addr
INNER JOIN DBINDEX.AddrID ON Node.addr = DBINDEX.AddrID.id
ORDER BY indegree DESC
```

```sql
-- 출금 횟수 및 총 금액
SELECT COUNT(*) AS Outdegree, SUM(btc) AS Outcome
FROM TxIn
INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
WHERE TxOut.addr = (SELECT DBINDEX.AddrID.id
                    FROM DBINDEX.AddrID
                    WHERE DBINDEX.AddrID.addr = 'ADDR');

-- 클러스터 출금 횟수 정렬
CREATE TABLE IF NOT EXISTS Node(
    addr INTEGER PRIMARY KEY)

.import csv

SELECT Node.addr AS addr, DBINDEX.AddrID.addr AS addr_hash,
       COUNT(TxIn.tx) AS outdegree, SUM(DBCORE.TxOut.btc) AS outcome
FROM Node
INNER JOIN (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
            FROM DBCORE.TxIn 
            INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                                       DBCORE.TxOut.n = DBCORE.TxIn.pn
            ) AS TxIn
ON TxIn.addr = Node.addr ON DBCORE.TxOut.addr = Node.addr
INNER JOIN DBINDEX.AddrID ON Node.addr = DBINDEX.AddrID.id
ORDER BY outdegree DESC
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
-- 같은 클러스터 주소 리스트
SELECT DBINDEX.AddrID.addr
FROM Cluster
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = Cluster.addr
WHERE Cluster.cluster = (SELECT Cluster.cluster
                         FROM Cluster
                         WHERE Cluster.addr = (SELECT DBINDEX.AddrID.id 
                                               FROM DBINDEX.AddrID
                                               WHERE DBINDEX.AddrID.addr = 'ADDR'));
```

```sql
-- 서비스1: 주소 ==> 태그
SELECT TagID.tag
FROM Tag
INNER JOIN TagID ON TagID.id = Tag.tag
WHERE Tag.addr = (SELECT DBINDEX.AddrID.id
                  FROM DBINDEX.AddrID
                  WHERE DBINDEX.AddrID.addr = 'ADDR');

-- 서비스2: 태그 ==> 주소 리스트
SELECT DBINDEX.AddrID.addr
FROM Cluster
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = Cluster.addr
WHERE Cluster.cluster IN (SELECT Cluster.cluster
                          FROM Cluster
                          WHERE Cluster.addr IN (SELECT Tag.addr 
                                                 FROM Tag
                                                 INNER JOIN TagID ON TagID.id = Tag.tag
                                                 WHERE TagID.tag = 'TAG'));
```

```sql
-- Find Edge
SELECT DBINDEX.TxID.txid
FROM DBEDGE.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBEDGE.Edge.tx
WHERE DBEDGE.Edge.src in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster = (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr = (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'upbit.com2'
            )
        )
    )
)
AND   DBEDGE.Edge.dst in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster = (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr = (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'upbit.com'
            )
        )
    )
);

-- Find Node

SELECT SRC.tx AS tx, DBINDEX.TxID.txid AS tx_hash, SRC.addr AS saddr, SRC.addr_hash AS saddr_hash,
       DST.addr AS daddr, DST.addr_hash AS daddr_hash, DST.btc AS btc
FROM
(SELECT TX.tx, EDGE.addr AS addr, DBINDEX.AddrID.addr AS addr_hash, TX.btc AS btc
 FROM 
 (SELECT DBCLUSTER.Cluster.addr AS addr
  FROM DBCLUSTER.Cluster
  WHERE DBCLUSTER.Cluster.cluster = (
      SELECT DBCLUSTER.Cluster.cluster
      FROM DBCLUSTER.Cluster
      WHERE DBCLUSTER.Cluster.addr = (
          SELECT DBCLUSTER.Tag.addr
          FROM DBCLUSTER.Tag
          WHERE DBCLUSTER.Tag.tag = (
              SELECT DBCLUSTER.TagID.id
              FROM DBCLUSTER.TagID
              WHERE DBCLUSTER.TagID.tag = 'upbit.com'
          )
      )
  )
 ) AS EDGE
 INNER JOIN (
     SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
     FROM DBCORE.TxIn
     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
 ) AS TX ON EDGE.addr = TX.addr
 INNER JOIN DBINDEX.AddrID ON TX.addr = DBINDEX.AddrID.id
) AS SRC
INNER JOIN
(SELECT TX.tx, EDGE.addr AS addr, DBINDEX.AddrID.addr AS addr_hash, TX.btc AS btc
 FROM 
 (SELECT DBCLUSTER.Cluster.addr AS addr
  FROM DBCLUSTER.Cluster
  WHERE DBCLUSTER.Cluster.cluster = (
      SELECT DBCLUSTER.Cluster.cluster
      FROM DBCLUSTER.Cluster
      WHERE DBCLUSTER.Cluster.addr = (
          SELECT DBCLUSTER.Tag.addr
          FROM DBCLUSTER.Tag
          WHERE DBCLUSTER.Tag.tag = (
              SELECT DBCLUSTER.TagID.id
              FROM DBCLUSTER.TagID
              WHERE DBCLUSTER.TagID.tag = 'upbit.com2'
          )
      )
  )
 ) AS EDGE
 INNER JOIN (
     SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
     FROM DBCORE.TxOut
 ) AS TX ON EDGE.addr = TX.addr
 INNER JOIN DBINDEX.AddrID ON TX.addr = DBINDEX.AddrID.id
) AS DST
ON SRC.tx = DST.tx
INNER JOIN DBINDEX.TxID ON SRC.tx = DBINDEX.TxID.txid;
```

##### 비트코인 휴리스틱
```sql
-- Multi input
SELECT DBCORE.TxOut.addr AS addr
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE txIn.tx IN (SELECT TxIn.tx
                  FROM TxIn
                  INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
                  WHERE addr = ?)
GROUP BY addr;

-- One output
SELECT DBINDEX.AddrID.addr
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND DBCORE.TxOut.n = DBCORE.TxIn.pn
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
WHERE DBCORE.TxIn.tx IN (
    SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.tx
    WHERE DBCORE.TxIn.tx IN (
        SELECT DBCORE.TxOut.tx
        FROM DBCORE.TxOut
        WHERE DBCORE.TxOut.addr = (
            SELECT DBINDEX.AddrID.id
            FROM DBINDEX.AddrID
            WHERE DBINDEX.AddrID.addr = 'ADDRID'
        )
    )
    GROUP BY DBCORE.TxIn.tx
    HAVING COUNT(DISTINCT DBCORE.TxIn.n) > 1 AND COUNT(DISTINCT DBCORE.TxOut.n) = 1
)
GROUP BY DBCORE.TxOut.addr;
```

##### 그래프 데이터베이스
```sql
SELECT TXI.tx AS tx, TXI.addr AS src, TXO.addr AS dst, TXO.btc AS btc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxIn.n AS n, 
             DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND 
                                 DBCORE.TxOut.n = DBCORE.TxIn.pn) AS TXI
INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n, 
                   DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
            FROM DBCORE.TxOut) AS TXO ON TXO.tx = TXI.tx;
```