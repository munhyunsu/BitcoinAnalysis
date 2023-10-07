# 데이터베이스 쿼리 예제

## 준비물
1. Version 3 Index, Core 데이터베이스 구축

### 사전 작업
1. Core 데이터베이스 연결
```bash
SQLITE_TMPDIR=./ sqlite3
```

2. Index 데이터베이스 연결
```sql
ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;
ATTACH DATABASE './dbv3-util.db' AS DBUTIL;
ATTACH DATABASE './dbv3-service.db' AS DBSERVICE;
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

-- 입금 횟수 및 총 금액 리스트
SELECT DBINDEX.AddrID.addr AS addr, COUNT(*) AS Indegree, SUM(DBCORE.TxOut.btc) AS Income
FROM DBCORE.TxOut
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
WHERE DBCORE.TxOut.addr IN (SELECT DBINDEX.AddrID.id
                            FROM DBINDEX.AddrID
                            WHERE DBINDEX.AddrID.addr IN ('ADDR',
                                                          'ADDR'))
GROUP BY DBCORE.TxOut.addr;
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

SELECT DBINDEX.AddrID.addr AS addr, COUNT(*) AS Outdegree, SUM(btc) AS Outcome
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
WHERE DBCORE.TxOut.addr IN (SELECT DBINDEX.AddrID.id
                            FROM DBINDEX.AddrID
                            WHERE DBINDEX.AddrID.addr IN ('ADDR',
                                                          'ADDR'))
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

```sql
-- 클러스터로 입금된 트랜잭션 리스트
SELECT DBINDEX.TxID.txid, SRC.addr, DST.addr, DBEDGE.Edge.btc
     , DBEDGE.Edge.tx, DBEDGE.Edge.src, DBEDGE.Edge.dst, DBCORE.BlkTime.unixtime
FROM DBEDGE.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBEDGE.Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBEDGE.Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBEDGE.Edge.dst
INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBEDGE.Edge.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBEDGE.Edge.src NOT IN (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster = (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
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
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
);
```

```sql
SELECT DBINDEX.TxID.txid, SRC.addr, DST.addr, DBUTIL.Edge.btc
     , DBUTIL.Edge.tx, DBUTIL.Edge.src, DBUTIL.Edge.dst, DBCORE.BlkTime.unixtime
FROM DBUTIL.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBUTIL.Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBUTIL.Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBUTIL.Edge.dst
INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBUTIL.Edge.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBUTIL.Edge.src IN (
    SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = (
        SELECT DBSERVICE.Cluster.cluster
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.addr IN (
            SELECT DBSERVICE.Tag.addr
            FROM DBSERVICE.Tag
            WHERE DBSERVICE.Tag.tag = (
                SELECT DBSERVICE.TagID.id
                FROM DBSERVICE.TagID
                WHERE DBSERVICE.TagID.tag = 'UPbit.com'
            )
        )
    )
)
AND   DBUTIL.Edge.dst in (
    SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = (
        SELECT DBSERVICE.Cluster.cluster
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.addr IN (
            SELECT DBSERVICE.Tag.addr
            FROM DBSERVICE.Tag
            WHERE DBSERVICE.Tag.tag = (
                SELECT DBSERVICE.TagID.id
                FROM DBSERVICE.TagID
                WHERE DBSERVICE.TagID.tag = 'UPbit.com'
            )
        )
    )
);
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
-- Find Edge with Addr
SELECT DBINDEX.TxID.txid AS txid, SRC.addr AS saddr, DST.addr AS daddr , DBEDGE.Edge.btc AS btc
     , DBEDGE.Edge.tx AS tx, DBEDGE.Edge.src AS saddr_id, DBEDGE.Edge.dst AS daddr_id
FROM DBEDGE.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBEDGE.Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBEDGE.Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBEDGE.Edge.dst
WHERE DBEDGE.Edge.src in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
AND   DBEDGE.Edge.dst in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
);

-- Find Node (without Edge, but slow)
SELECT DBINDEX.TxID.txid, SRC.addr, DST.addr, Edge.btc
     , Edge.tx, Edge.src, Edge.dst
FROM (SELECT TXI.tx AS tx, TXI.addr AS src, TXO.addr AS dst, TXO.btc AS btc
      FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxIn.n AS n, 
                   DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
            FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND 
                                 DBCORE.TxOut.n = DBCORE.TxIn.pn) AS TXI
      INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n, 
                         DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
                  FROM DBCORE.TxOut) AS TXO ON TXO.tx = TXI.tx
) AS Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = Edge.dst
WHERE Edge.src in (
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
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
AND   Edge.dst in (
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
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
);

-- Edge by date
EXPLAIN QUERY PLAN


ATTACH DATABASE './dbv3-index.db' AS DBINDEX;
ATTACH DATABASE './dbv3-core.db' AS DBCORE;
ATTACH DATABASE './dbv3-util.db' AS DBUTIL;
.header on
.mode csv
.once '2020-10-03.csv'
SELECT DBUTIL.Edge.src AS src, DBUTIL.Edge.dst AS dst, 
       COUNT(DBUTIL.Edge.tx) AS cnt, DBUTIL.Edge.btc AS btc
FROM DBUTIL.Edge
INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBUTIL.Edge.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBUTIL.Edge.tx IN (
    SELECT DBCORE.BlkTx.tx
    FROM DBCORE.BlkTx
    INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
    WHERE (
        SELECT (STRFTIME('%s', '2020-10-03T00:00:00+09:00')) <= DBCORE.BlkTime.unixtime AND
                DBCORE.BlkTime.unixtime <= (SELECT STRFTIME('%s', '2020-10-03T23:59:59+09:00'))))
GROUP BY src, dst;




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


SELECT DBINDEX.TxID.txid AS txid, SRC.addr AS saddr, DST.addr AS daddr , DBEDGE.Edge.btc AS btc
     , DBEDGE.Edge.tx AS tx, DBEDGE.Edge.src AS saddr_id, DBEDGE.Edge.dst AS daddr_id
FROM DBEDGE.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBEDGE.Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBEDGE.Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBEDGE.Edge.dst
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk
WHERE DBEDGE.Edge.src in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
AND   DBEDGE.Edge.dst in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
GROUP BY saddr_id, daddr_id;
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

-- First appeared block and unixtime
SELECT DBCORE.BlkTx.blk AS blk, MIN(DBCORE.BlkTime.unixtime) AS unixtime
FROM DBCORE.BlkTx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBCORE.BlkTx.tx IN (
    SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr = 'ADDR'
);
```

##### 휴리스틱 with 업데이트
```sql
UPDATE Cluster SET cluster = (SELECT (CASE WHEN cluster != -1
                                      THEN cluster
                                      ELSE (SELECT MAX(cluster)+1 FROM Cluster)
                                      END) as num
                              FROM Cluster 
                              WHERE addr = (SELECT DBINDEX.AddrID.id 
                                            FROM DBINDEX.AddrID 
                                            WHERE DBINDEX.AddrID.addr = 'ADDR'))
WHERE addr in (SELECT DBCORE.TxOut.addr AS addr
               FROM DBCORE.TxIn
               INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
               WHERE txIn.tx IN (SELECT TxIn.tx
                                 FROM TxIn
                                 INNER JOIN TxOut ON TxIn.ptx = TxOut.tx AND TxIn.pn = TxOut.n
                                 WHERE addr = (SELECT DBINDEX.AddrID.id 
                                               FROM DBINDEX.AddrID 
                                               WHERE DBINDEX.AddrID.addr = 'ADDR'))
               GROUP BY addr);
SELECT COUNT(*) 
FROM Cluster 
WHERE cluster = (SELECT cluster
                 FROM Cluster 
                 WHERE addr = (SELECT DBINDEX.AddrID.id 
                               FROM DBINDEX.AddrID 
                               WHERE DBINDEX.AddrID.addr = 'ADDR'));
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
            
-- Export intra-cluster edge
SELECT SRC.addr AS saddr, DST.addr AS daddr, SUM(DBEDGE.Edge.btc) AS btc,
       DBEDGE.Edge.src AS saddr_id, DBEDGE.Edge.dst AS daddr_id, COUNT(DBEDGE.Edge.tx) AS cnt
FROM DBEDGE.Edge
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBEDGE.Edge.tx
INNER JOIN DBINDEX.AddrID AS SRC ON SRC.id = DBEDGE.Edge.src
INNER JOIN DBINDEX.AddrID AS DST ON DST.id = DBEDGE.Edge.dst
WHERE DBEDGE.Edge.src in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
AND   DBEDGE.Edge.dst in (
    SELECT DBCLUSTER.Cluster.addr
    FROM DBCLUSTER.Cluster
    WHERE DBCLUSTER.Cluster.cluster IN (
        SELECT DBCLUSTER.Cluster.cluster
        FROM DBCLUSTER.Cluster
        WHERE DBCLUSTER.Cluster.addr IN (
            SELECT DBCLUSTER.Tag.addr
            FROM DBCLUSTER.Tag
            WHERE DBCLUSTER.Tag.tag = (
                SELECT DBCLUSTER.TagID.id
                FROM DBCLUSTER.TagID
                WHERE DBCLUSTER.TagID.tag = 'TAG'
            )
        )
    )
)
GROUP BY DBEDGE.Edge.src, DBEDGE.Edge.dst;
```

##### 잔액 
```sql
SELECT DBCORE.TxOut.addr AS addr_id, DBINDEX.AddrID.addr AS addr, 
       SUM(DBCORE.TxOut.btc) AS btc, COUNT(DBCORE.TxOut.btc) AS cnt
FROM DBUTXO.UTXO
INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBUTXO.UTXO.tx AND DBCORE.TxOut.n = DBUTXO.UTXO.n
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
GROUP BY DBCORE.TxOut.addr;
```

##### 클러스터 Intra-analysis
```sql
-- Indegree outdegree on cluster (too slow)
SELECT IND.addr, IND.addr_id, IND.indegree, IND.income, OUTD.outdegree, OUTD.outcome
FROM (SELECT DBINDEX.AddrID.addr AS addr, DBCLUSTER.Cluster.addr AS addr_id,
             COUNT(DBCORE.TxOut.tx) AS indegree, SUM(DBCORE.TxOut.btc) AS income
      FROM DBCLUSTER.Cluster
      INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.addr = DBCLUSTER.Cluster.addr
      INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCLUSTER.Cluster.addr
      WHERE DBCLUSTER.Cluster.cluster IN (
          SELECT DBCLUSTER.Cluster.cluster
          FROM DBCLUSTER.Cluster
          WHERE DBCLUSTER.Cluster.addr IN (
              SELECT DBCLUSTER.Tag.addr
              FROM DBCLUSTER.Tag
              WHERE DBCLUSTER.Tag.tag = (
                  SELECT DBCLUSTER.TagID.id
                  FROM DBCLUSTER.TagID
                  WHERE DBCLUSTER.TagID.tag = 'TAG')))
      GROUP BY DBCLUSTER.Cluster.addr) AS IND
INNER JOIN (SELECT DBINDEX.AddrID.addr AS addr, DBCLUSTER.Cluster.addr AS addr_id,
                   COUNT(TxIn.tx) AS outdegree, SUM(TxIn.btc) AS outcome
            FROM DBCLUSTER.Cluster
            INNER JOIN (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBCORE.TxOut.btc AS btc
                        FROM DBCORE.TxIn 
                        INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.ptx AND
                                                   DBCORE.TxOut.n = DBCORE.TxIn.pn
                       ) AS TxIn ON TxIn.addr = DBCLUSTER.Cluster.addr
            INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCLUSTER.Cluster.addr
            WHERE DBCLUSTER.Cluster.cluster IN (
                SELECT DBCLUSTER.Cluster.cluster
                FROM DBCLUSTER.Cluster
                WHERE DBCLUSTER.Cluster.addr IN (
                    SELECT DBCLUSTER.Tag.addr
                    FROM DBCLUSTER.Tag
                    WHERE DBCLUSTER.Tag.tag = (
                        SELECT DBCLUSTER.TagID.id
                        FROM DBCLUSTER.TagID
                        WHERE DBCLUSTER.TagID.tag = 'TAG')))
            GROUP BY DBCLUSTER.Cluster.addr) AS OUTD ON OUTD.addr_id = IND.addr_id;
```

#### Graph

```sql
SELECT DBEDGE.Edge.src AS src, DBEDGE.Edge.dst AS dst, SUM(DBEDGE.Edge.btc) AS btc, COUNT(DBEDGE.Edge.tx) AS cnt
FROM DBEDGE.Edge
WHERE DBEDGE.Edge.src IN (SELECT DBCLUSTER.Cluster.addr
                          FROM DBCLUSTER.Cluster
                          WHERE DBCLUSTER.Cluster.cluster IN (
                              SELECT DBCLUSTER.Cluster.cluster
                              FROM DBCLUSTER.Cluster
                              WHERE DBCLUSTER.Cluster.addr IN (
                                  SELECT DBCLUSTER.Tag.addr
                                  FROM DBCLUSTER.Tag
                                  WHERE DBCLUSTER.Tag.tag = (
                                      SELECT DBCLUSTER.TagID.id
                                      FROM DBCLUSTER.TagID
                                      WHERE DBCLUSTER.TagID.tag = 'TAG'))))
OR    DBEDGE.Edge.dst IN (SELECT DBCLUSTER.Cluster.addr
                          FROM DBCLUSTER.Cluster
                          WHERE DBCLUSTER.Cluster.cluster IN (
                              SELECT DBCLUSTER.Cluster.cluster
                              FROM DBCLUSTER.Cluster
                              WHERE DBCLUSTER.Cluster.addr IN (
                                  SELECT DBCLUSTER.Tag.addr
                                  FROM DBCLUSTER.Tag
                                  WHERE DBCLUSTER.Tag.tag = (
                                      SELECT DBCLUSTER.TagID.id
                                      FROM DBCLUSTER.TagID
                                      WHERE DBCLUSTER.TagID.tag = 'TAG'))))
GROUP BY DBEDGE.Edge.src, DBEDGE.Edge.dst;

SELECT SRC.srcid AS srcid, DST.dstid AS dstid, SRC.src AS src, DST.dst AS dst, SUM(DST.btc) AS btc, COUNT(SRC.tx) AS cnt
FROM (
    SELECT DBEDGE.Edge.tx, DBEDGE.Edge.src AS srcid, DBINDEX.AddrID.addr AS src
    FROM DBEDGE.Edge
    INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBEDGE.Edge.src
    ) AS SRC
INNER JOIN (
    SELECT DBEDGE.Edge.tx, DBEDGE.Edge.dst AS dstid, DBINDEX.AddrID.addr AS dst, DBEDGE.Edge.btc AS btc
    FROM DBEDGE.Edge
    INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBEDGE.Edge.dst) AS DST ON DST.tx = SRC.tx
WHERE SRC.src IN (SELECT DBCLUSTER.Cluster.addr
                          FROM DBCLUSTER.Cluster
                          WHERE DBCLUSTER.Cluster.cluster IN (
                              SELECT DBCLUSTER.Cluster.cluster
                              FROM DBCLUSTER.Cluster
                              WHERE DBCLUSTER.Cluster.addr IN (
                                  SELECT DBCLUSTER.Tag.addr
                                  FROM DBCLUSTER.Tag
                                  WHERE DBCLUSTER.Tag.tag = (
                                      SELECT DBCLUSTER.TagID.id
                                      FROM DBCLUSTER.TagID
                                      WHERE DBCLUSTER.TagID.tag = 'TAG'))))
OR    DST.dst IN (SELECT DBCLUSTER.Cluster.addr
                          FROM DBCLUSTER.Cluster
                          WHERE DBCLUSTER.Cluster.cluster IN (
                              SELECT DBCLUSTER.Cluster.cluster
                              FROM DBCLUSTER.Cluster
                              WHERE DBCLUSTER.Cluster.addr IN (
                                  SELECT DBCLUSTER.Tag.addr
                                  FROM DBCLUSTER.Tag
                                  WHERE DBCLUSTER.Tag.tag = (
                                      SELECT DBCLUSTER.TagID.id
                                      FROM DBCLUSTER.TagID
                                      WHERE DBCLUSTER.TagID.tag = 'TAG'))))
GROUP BY SRC.src, DST.dst;

-- values
SELECT DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS tx, DBCORE.TxOut.btc AS value
FROM DBCORE.TxOut
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBCORE.TxOut.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBCORE.TxOut.addr IN (SELECT DBINDEX.AddrID.id
                            FROM DBINDEX.AddrID
                            WHERE DBINDEX.AddrID.addr IN ('ADDR'))
UNION
SELECT DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS tx, -1*DBCORE.TxOut.btc AS value
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
INNER JOIN DBINDEX.TxID ON DBINDEX.TxID.id = DBCORE.TxIn.tx
INNER JOIN DBCORE.BlkTx ON DBCORE.BlkTx.tx = DBCORE.TxIn.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTime.blk = DBCORE.BlkTx.blk
WHERE DBCORE.TxOut.addr IN (SELECT DBINDEX.AddrID.id
                            FROM DBINDEX.AddrID
                            WHERE DBINDEX.AddrID.addr IN ('ADDR'));

```


- ICBC2022

```sql
.header on
.mode csv
.once tx_by_blk.csv
SELECT DBCORE.BlkTx.blk, COUNT(DBCORE.BlkTx.tx)
FROM DBCORE.BlkTx
GROUP BY DBCORE.BlkTx.blk
ORDER BY DBCORE.BlkTx.blk ASC;

.once txoutaddr_by_blk.csv
SELECT DBCORE.BlkTx.blk, COUNT(DBCORE.TxOut.addr)
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
GROUP BY DBCORE.BlkTx.blk
ORDER BY DBCORE.BlkTx.blk ASC;

.once io_by_blktx.csv
SELECT DBCORE.BlkTx.blk, TXIC.t, TXIC.c, TXOC.c
FROM (SELECT DBCORE.TxIn.tx AS t, COUNT(DBCORE.TxIn.n) AS c
      FROM DBCORE.TxIn
      GROUP BY DBCORE.TxIn.tx
      ORDER BY DBCORE.TxIn.tx ASC) AS TXIC
INNER JOIN (SELECT DBCORE.TxOut.tx AS t, COUNT(DBCORE.TxOut.n) AS c
            FROM DBCORE.TxOut
            GROUP BY DBCORE.TxOut.tx
            ORDER BY DBCORE.TxOut.tx ASC) AS TXOC
        ON TXIC.t = TXOC.t
INNER JOIN DBCORE.BlkTx ON TXIC.t = DBCORE.BlkTx.tx
ORDER BY DBCORE.BlkTx.blk ASC, TXIC.t ASC;

.once daddr_by_blk.csv
SELECT TC.blk, TC.tc, MAX(DBCORE.TxOut.addr)
FROM (SELECT DBCORE.BlkTx.blk AS blk, MAX(DBCORE.BlkTx.tx) AS tc
      FROM DBCORE.BlkTx
      GROUP BY DBCORE.BlkTx.blk) AS TC
INNER JOIN DBCORE.BlkTx ON TC.tc = DBCORE.BlkTx.tx
INNER JOIN DBCORE.TxOut ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
GROUP BY DBCORE.BlkTx.tx
ORDER BY DBCORE.BlkTx.tx ASC;

-- Original
-- 669565116,b675c78f0a2fc4d7c06d9351d5bb91323e1c198d05dad1b721ad8c6c62e7ffea
SELECT DBINDEX.BlkID.blkhash AS blockhash, DBINDEX.TxID.txid AS txid, TI.address AS iaddress, TI.btc AS ibtc, DBINDEX.AddrID.addr AS oaddress, DBCORE.TxOut.btc AS obtc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBINDEX.AddrID.addr AS address, DBCORE.TxOut.btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                             AND DBCORE.TxIn.pn = DBCORE.TxOut.n
      INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id) AS TI
INNER JOIN DBCORE.TxOut ON TI.tx = DBCORE.TxOut.tx
INNER JOIN DBCORE.BlkTx ON TI.tx = DBCORE.BlkTx.tx
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTx.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON TI.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE TI.tx = 669565116;

SELECT DBINDEX.BlkID.id AS blockid, DBINDEX.BlkID.blkhash AS blockhash, DBINDEX.TxID.id AS tx, DBINDEX.TxID.txid AS txid, TI.addr AS iaddr, TI.address AS iaddress, TI.btc AS ibtc, DBINDEX.AddrID.id AS oaddr, DBINDEX.AddrID.addr AS oaddress, DBCORE.TxOut.btc AS obtc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBINDEX.AddrID.addr AS address, DBCORE.TxOut.btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                             AND DBCORE.TxIn.pn = DBCORE.TxOut.n
      INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id) AS TI
INNER JOIN DBCORE.TxOut ON TI.tx = DBCORE.TxOut.tx
INNER JOIN DBCORE.BlkTx ON TI.tx = DBCORE.BlkTx.tx
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTx.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON TI.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE TI.tx = 669565116;

SELECT DBINDEX.BlkID.id AS blockid, DBINDEX.TxID.id AS tx, TI.addr AS iaddr, TI.btc AS ibtc, DBINDEX.AddrID.id AS oaddr, DBCORE.TxOut.btc AS obtc
FROM (SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.addr AS addr, DBINDEX.AddrID.addr AS address, DBCORE.TxOut.btc
      FROM DBCORE.TxIn
      INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                             AND DBCORE.TxIn.pn = DBCORE.TxOut.n
      INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id) AS TI
INNER JOIN DBCORE.TxOut ON TI.tx = DBCORE.TxOut.tx
INNER JOIN DBCORE.BlkTx ON TI.tx = DBCORE.BlkTx.tx
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTx.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON TI.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE TI.tx = 669565116;


SELECT *
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.tx = 669565116;


 
SELECT DBCORE.BlkTx.blk, COUNT(DBCORE.TxIn.n), COUNT(DBCORE.TxOut.n)
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
GROUP BY DBCORE.BlkTx.blk
ORDER BY DBCORE.BlkTx.blk ASC;

```


```sql
SELECT COUNT(DISTINCT DBCORE.TxOut.addr)
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
WHERE DBCORE.BlkTx.blk <= 710000;


.once daddr_by_blk.csv
SELECT DBCORE.BlkTx.blk, COUNT(DISTINCT DBCORE.TxOut.addr)
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON (DBCORE.BlkTx.blk >= 0 AND DBCORE.BlkTx.blk <= 712895);

.once daddr_by_blk.csv
SELECT TC.blk, TC.tc, MAX(DBCORE.TxOut.addr)
FROM (SELECT DBCORE.BlkTx.blk AS blk, MAX(DBCORE.BlkTx.tx) AS tc
      FROM DBCORE.BlkTx
      GROUP BY DBCORE.BlkTx.blk) AS TC
INNER JOIN DBCORE.BlkTx ON TC.tc = DBCORE.BlkTx.tx
INNER JOIN DBCORE.TxOut ON DBCORE.BlkTx.tx = DBCORE.TxOut.tx
GROUP BY DBCORE.BlkTx.tx
ORDER BY DBCORE.BlkTx.tx ASC;
```

- 2021. 12. 10.

```sql
--- Single output from hot walllet (DISTINCT address)
--- Input: Hot wallet, Output: User wallet
SELECT DISTINCT DBINDEX.AddrID.addr
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
            WHERE DBINDEX.AddrID.addr = '3Ld9ZzTLJ7iX6smhJY6AmqeZnr843pBw3c'
        )
    )
    GROUP BY DBCORE.TxIn.tx
    HAVING COUNT(DBCORE.TxIn.n) > 1 AND COUNT(DISTINCT DBCORE.TxOut.addr) = 1
)
LIMIT 10;

--- Single output from user wallet (DISTINCT address)
--- Input: User wallet/Hot wallet, Output: Hot wallet (estimated)
SELECT DISTINCT DBINDEX.AddrID.addr
FROM DBCORE.TxOut
INNER JOIN DBINDEX.AddrID ON DBINDEX.AddrID.id = DBCORE.TxOut.addr
WHERE DBCORE.TxOut.tx IN (
    SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxOut.tx = DBCORE.TxIn.tx
    WHERE DBCORE.TxIn.tx IN (
        SELECT DBCORE.TxIn.tx
        FROM DBCORE.TxIn
        INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                               AND DBCORE.TxIn.pn = DBCORE.TxOut.n
        WHERE DBCORE.TxOut.addr = (
            SELECT DBINDEX.AddrID.id
            FROM DBINDEX.AddrID
            WHERE DBINDEX.AddrID.addr = '3KAoW72jkHrh7mk2iekhKBRWyBuqYky1Mo'
        )
    )
    GROUP BY DBCORE.TxIn.tx
    HAVING COUNT(DBCORE.TxIn.n) > 1 AND COUNT(DISTINCT DBCORE.TxOut.addr) = 1
);

--- Peeling chain
--- Input: Address, Output: Peeling chain transaction
SELECT DBCORE.TxIn.tx, DBINDEX.TxID.txid
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                       AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
WHERE DBCORE.TxIn.tx IN (
      SELECT DBCORE.TxOut.tx
      FROM DBCORE.TxOut
      WHERE DBCORE.TxOut.addr = (
          SELECT DBINDEX.AddrID.id
          FROM DBINDEX.AddrID
          WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg'))
  AND DBCORE.TxIn.tx IN (
      SELECT DBCORE.TxOut.tx
      FROM DBCORE.TxOut
      INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
      WHERE DBCORE.TxOut.tx IN (
          SELECT DBCORE.TxOut.tx
          FROM DBCORE.TxOut
          WHERE DBCORE.TxOut.addr = (
              SELECT DBINDEX.AddrID.id
              FROM DBINDEX.AddrID
              WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg'))
      GROUP BY DBCORE.TxOut.tx
      HAVING COUNT(DBCORE.TxOut.n) = 2)
GROUP BY DBCORE.TxIn.tx
HAVING COUNT(DBCORE.TxIn.n) = 1;


---
SELECT DBCORE.TxIn.tx, DBINDEX.TxID.txid
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                       AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
WHERE DBCORE.TxIn.tx IN (
      SELECT DBCORE.TxOut.tx
      FROM DBCORE.TxOut
      WHERE DBCORE.TxOut.addr = (
          SELECT DBINDEX.AddrID.id
          FROM DBINDEX.AddrID
          WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg'))
  AND DBCORE.TxIn.tx IN (
      SELECT DBCORE.TxOut.tx
      FROM DBCORE.TxOut
      INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
      WHERE DBCORE.TxOut.tx IN (
          SELECT DBCORE.TxOut.tx
          FROM DBCORE.TxOut
          WHERE DBCORE.TxOut.addr = (
              SELECT DBINDEX.AddrID.id
              FROM DBINDEX.AddrID
              WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg'))
      GROUP BY DBCORE.TxOut.tx
      HAVING COUNT(DBCORE.TxOut.n) = 2)
GROUP BY DBCORE.TxIn.tx
HAVING COUNT(DBCORE.TxIn.n) = 1;






SELECT DBCORE.TxOut.tx, DBINDEX.TxID.txid, COUNT(DBCORE.TxOut.n)
FROM DBCORE.TxOut
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
WHERE DBCORE.TxOut.tx IN (
      SELECT DBCORE.TxOut.tx
      FROM DBCORE.TxOut
      WHERE DBCORE.TxOut.addr = (
          SELECT DBINDEX.AddrID.id
          FROM DBINDEX.AddrID
          WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg')
  )
GROUP BY DBCORE.TxOut.tx
HAVING COUNT(DBCORE.TxOut.n) = 2;
```

```sql
SELECT DBCORE.TxOut.tx
FROM DBCORE.TxOut
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
WHERE DBCORE.TxOut.addr = (
    SELECT DBINDEX.AddrID.id
    FROM DBINDEX.AddrID
    WHERE DBINDEX.AddrID.addr = '3B1sd9LfVGbLq6664mk5k98dRNWQ2au3Yg')
```


--- TNSM 2022.09.

```sql
-- Clustering distribution
.header on
.mode csv
.once cluster_groupby.csv
SELECT DBSERVICE.Cluster.addr, COUNT(DBSERVICE.Cluster.addr)
FROM DBSERVICE.Cluster
GROUP BY DBSERVICE.Cluster.addr
ORDER BY DBSERVICE.Cluster.addr;

```


### clusterRelations
```sql
-- 861860936
SELECT DBINDEX.AddrID.id
FROM DBINDEX.AddrID
WHERE DBINDEX.AddrID.addr = 'bc1q7e9valp3uq3nx7kvye9tlptqq5adyrsa4s4ca8';

-- 850577526
SELECT DBINDEX.AddrID.id
FROM DBINDEX.AddrID
WHERE DBINDEX.AddrID.addr = 'bc1q2lgzm0mh6qgydmc5vvauv2uh6e745yn797wwqs';

```

```sql
-- 860103337: clusterId
SELECT MIN(DBSERVICE.Cluster.addr)
FROM DBSERVICE.Cluster
WHERE DBSERVICE.Cluster.cluster = (
    SELECT DBSERVICE.Cluster.cluster
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.addr = 861860936);

-- 519495737:
SELECT MIN(DBSERVICE.Cluster.addr)
FROM DBSERVICE.Cluster
WHERE DBSERVICE.Cluster.cluster = (
    SELECT DBSERVICE.Cluster.cluster
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.addr = 850577526);
```

```sql
SELECT DBSERVICE.TagID.tag
FROM DBSERVICE.Tag
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Tag.addr = 860103337;
```

```sql
-- 
SELECT DBSERVICE.TagID.tag
FROM DBSERVICE.Tag
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Tag.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337);
```

```sql
SELECT Income.value-Outcome.value AS Balance
FROM
(SELECT SUM(DBCORE.TxOut.btc) AS value
 FROM DBCORE.TxOut
 WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
     FROM DBSERVICE.Cluster
     WHERE DBSERVICE.Cluster.cluster = 860103337)) AS Income,
(SELECT SUM(DBCORE.TxOut.btc) AS value
 FROM DBCORE.TxIn
 INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
 WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
     FROM DBSERVICE.Cluster
     WHERE DBSERVICE.Cluster.cluster = 860103337)) AS Outcome
```

```sql
SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
```

```sql
SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
```

```sql
-- 86 에서 얼마나 받았는가?
SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.btc AS btc, DBCORE.BlkTime.unixtime AS unixtime
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 519495737)
AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337));
-- ORDER BY DBCORE.TxOut.tx DESC;
-- 669566365|0.02405446
```

```sql
-- 86 에서 얼마나 보냈는가?
SELECT DBCORE.TxIn.tx AS tx, DBCORE.TxOut.btc AS btc, DBCORE.BlkTime.unixtime AS unixtime
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 519495737));
-- ORDER BY DBCORE.TxIn.tx DESC;
-- 669566365|0.00420332
-- 669566365|0.00414299
-- 669566365|0.00036684
-- 669566365|0.00051551
-- 669566365|0.00042912
-- 669566365|0.00112194
-- 669566365|0.0013316
-- 669566365|0.00051551
-- 669566365|0.00066925
-- 669566365|0.00547489
-- 669566365|8.836e-05
-- 669566365|0.00045303
-- 669566365|0.00067309
-- 669566365|0.00051284
-- 669566365|0.00078292
-- 669566365|0.0005528
-- 669566365|0.00090788
-- 669566365|0.00030405
-- 669566365|0.00116407
-- 669566365|0.00083253
-- 669566365|0.00035089
```


```sql
SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 519495737));
```

```sql
-- 1123.9360892-1121.73190763

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
SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc, DBCORE.BlkTime.unixtime AS unixtime
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxIn.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DISTINCT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
       WHERE DBSERVICE.Cluster.cluster = 633156))
AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
   FROM DBCORE.TxOut
   WHERE DBCORE.TxOut.addr IN (SELECT DISTINCT DBSERVICE.Cluster.addr
       FROM DBSERVICE.Cluster
       WHERE DBSERVICE.Cluster.cluster = 1))
GROUP BY DBCORE.TxIn.tx
ORDER BY DBCORE.TxIn.tx ASC;


SELECT DBCORE.TxOut.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc, DBCORE.BlkTime.unixtime AS unixtime
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxOut.tx IN (SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DISTINCT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 1))
AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DISTINCT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 633156))
GROUP BY DBCORE.TxOut.tx
ORDER BY DBCORE.TxOut.tx ASC;
```




### edgeSelect

```sql
-- 861860936
SELECT DBINDEX.AddrID.id
FROM DBINDEX.AddrID
WHERE DBINDEX.AddrID.addr = 'bc1q95dyrax0udgejrh3crqdtu8m6dzahlu4t4yxls';

-- 850577526
SELECT DBINDEX.AddrID.id
FROM DBINDEX.AddrID
WHERE DBINDEX.AddrID.addr = 'bc1q2lgzm0mh6qgydmc5vvauv2uh6e745yn797wwqs';
```

```sql
-- 860103337: clusterId
SELECT MIN(DBSERVICE.Cluster.addr)
FROM DBSERVICE.Cluster
WHERE DBSERVICE.Cluster.cluster = (
    SELECT DBSERVICE.Cluster.cluster
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.addr = 861860936);

-- 519495737:
SELECT MIN(DBSERVICE.Cluster.addr)
FROM DBSERVICE.Cluster
WHERE DBSERVICE.Cluster.cluster = (
    SELECT DBSERVICE.Cluster.cluster
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.addr = 850577526);
```

```sql
SELECT DBSERVICE.TagID.tag
FROM DBSERVICE.Tag
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Tag.addr = 860103337;

-- 
SELECT DBSERVICE.TagID.tag
FROM DBSERVICE.Tag
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Tag.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337);
```

```sql
-- 86에서 나가는 tx들
SELECT DISTINCT DBCORE.TxOut.tx AS tx
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 519495737)
AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337));
-- ORDER BY DBCORE.TxOut.tx DESC;
-- 669566365|0.02405446

-- 86 으로 들어오는 tx들
SELECT DISTINCT DBCORE.TxIn.tx AS tx
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 519495737));
        
-- 아래 2개 동일 쿼리임
-- 86에서 출발한 tx
SELECT DISTINCT DBCORE.TxOut.tx AS tx
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 519495737)
AND DBCORE.TxOut.tx IN (SELECT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337));

SELECT DISTINCT DBCORE.TxIn.tx AS tx
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
AND DBCORE.TxIn.tx IN (SELECT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 519495737));
```

```sql
-- tx의 inputs
SELECT DBCORE.BlkTime.blk AS blk, DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS txid, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTime.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxIn.tx = 669566365
ORDER BY DBCORE.TxIn.n ASC;

-- tx의 outputs
SELECT DBCORE.BlkTime.blk AS blk, DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS txid, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTime.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxOut.tx = 669566365
ORDER BY DBCORE.TxOut.n ASC;
```

### ClusterInfo

```sql
-- 860103337|bc1qhga64g9j5r5tsjgtctqrjar6lpj93a5rd9gfe7
SELECT DBINDEX.AddrID.id, DBINDEX.AddrID.addr
FROM DBINDEX.AddrID
WHERE DBINDEX.AddrID.id = (SELECT MIN(DBSERVICE.Cluster.addr)
    FROM DBSERVICE.Cluster
    INNER JOIN DBINDEX.AddrID ON DBSERVICE.Cluster.addr = DBINDEX.AddrID.id
    WHERE DBSERVICE.Cluster.cluster = (
        SELECT DBSERVICE.Cluster.cluster
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.addr = 861860936));
```

```sql

SELECT DBCORE.BlkTime.blk AS blk, DBCORE.BlkTime.unixtime AS unixtime, DBINDEX.TxID.txid AS txid, DBINDEX.AddrID.addr AS addr, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.BlkID ON DBCORE.BlkTime.blk = DBINDEX.BlkID.id
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxIn.tx = 669566365
ORDER BY DBCORE.TxIn.n ASC;
```


```sql
-- 클러스터 입력 합
SELECT DBCORE.TxIn.tx, SUM(DBCORE.TxOut.btc), MIN(DBCORE.BlkTime.unixtime)
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
GROUP BY DBCORE.TxIn.tx
ORDER BY DBCORE.TxIn.tx ASC;



-- 클러스터 출력 합
SELECT DBCORE.TxOut.tx, SUM(DBCORE.TxOut.btc), MIN(DBCORE.BlkTime.unixtime)
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
GROUP BY DBCORE.TxOut.tx
ORDER BY DBCORE.TxOut.tx ASC;
```

```sql
SELECT COUNT(DBSERVICE.Cluster.cluster)
FROM DBSERVICE.Cluster
WHERE DBSERVICE.Cluster.cluster = 860103337
GROUP BY DBSERVICE.Cluster.cluster;
```

```sql
-- Fee
-- Inputs
SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
-- AND DBCORE.TxIn.tx = 669566365
GROUP BY DBCORE.TxIn.tx;

-- Outputs
SELECT DBCORE.TxOut.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
-- AND DBCORE.TxOut.tx = 669566365
GROUP BY DBCORE.TxOut.tx;
```

```sql
-- SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc) AS ibtc, T.btc AS obtc, SUM(DBCORE.TxOut.btc)-T.btc AS fee
SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc)-T.btc AS fee
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
        FROM DBCORE.TxIn
        INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
        WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
            FROM DBSERVICE.Cluster
            WHERE DBSERVICE.Cluster.cluster = 860103337)
        UNION
        SELECT DISTINCT DBCORE.TxOut.tx
        FROM DBCORE.TxOut
        WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
            FROM DBSERVICE.Cluster
            WHERE DBSERVICE.Cluster.cluster = 860103337))
    GROUP BY DBCORE.TxOut.tx) AS T ON DBCORE.TxIn.tx = T.tx
WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
-- AND DBCORE.TxIn.tx = 669566365
GROUP BY DBCORE.TxIn.tx;


SELECT COUNT(DISTINCT DBCORE.TxIn.tx)
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
-- AND DBCORE.TxIn.tx = 669566365
SELECT COUNT(DISTINCT DBCORE.TxOut.tx)
FROM DBCORE.TxOut
WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
```


```sql
-- Outcomes
SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, -SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
GROUP BY DBCORE.TxIn.tx

-- Incomes
SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
GROUP BY DBCORE.TxOut.tx

-- UNION!
SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, -SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS clusterid
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
GROUP BY DBCORE.TxIn.tx
UNION
SELECT DBINDEX.TxID.txid AS tx, DBCORE.BlkTime.unixtime as unixtime, SUM(DBCORE.TxOut.btc) AS btc, DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS clusterid
FROM DBCORE.TxOut
INNER JOIN DBCORE.BlkTx ON DBCORE.TxOut.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = 860103337)
GROUP BY DBCORE.TxOut.tx
ORDER BY unixtime ASC;
```

### detailTransfer
```sql
SELECT DBCORE.BlkTx.blk as height, DBINDEX.TxID.txid as txid, DBCORE.BlkTime.unixtime AS unixtime
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBCORE.BlkTx ON DBCORE.TxIn.tx = DBCORE.BlkTx.tx
INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
WHERE DBINDEX.TxID.txid = '0aacf25d562d9078cfd79118bda4acb84aae83d634c941a933e782719a9be749'
GROUP BY DBCORE.TxIn.tx;
-- AND DBCORE.TxIn.tx = 669566365

-- Sents
SELECT DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS cluster, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN DBINDEX.TxID ON DBCORE.TxIn.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
WHERE DBCORE.TxIn.tx = 669566365
ORDER BY DBCORE.TxIn.tx, DBCORE.TxIn.n;

-- Received
SELECT DBINDEX.AddrID.addr AS addr, DBSERVICE.Cluster.cluster AS cluster, DBCORE.TxOut.btc AS btc
FROM DBCORE.TxOut
INNER JOIN DBINDEX.TxID ON DBCORE.TxOut.tx = DBINDEX.TxID.id
INNER JOIN DBINDEX.AddrID ON DBCORE.TxOut.addr = DBINDEX.AddrID.id
INNER JOIN DBSERVICE.Cluster ON DBCORE.TxOut.addr = DBSERVICE.Cluster.addr
WHERE DBCORE.TxOut.tx = 669566365
ORDER BY DBCORE.TxOut.tx, DBCORE.TxOut.n;
```

```sql

SELECT DBSERVICE.Cluster.addr, DBSERVICE.Cluster.cluster, DBSERVICE.TagId.tag
FROM DBSERVICE.Cluster
LEFT OUTER JOIN DBSERVICE.Tag ON DBSERVICE.Cluster.addr = DBSERVICE.Tag.addr
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Cluster.addr = 861860936


SELECT DBSERVICE.Cluster.addr, DBSERVICE.Cluster.cluster
FROM DBSERVICE.Cluster
LEFT OUTER JOIN DBSERVICE.Tag ON DBSERVICE.Cluster.addr = DBSERVICE.Tag.addr
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
WHERE DBSERVICE.Cluster.addr = 861860936

SELECT DBSERVICE.Cluster.cluster, DBSERVICE.Tag.tag
FROM DBSERVICE.Tag
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
INNER JOIN DBSERVICE.Cluster ON DBSERVICE.Tag.addr = DBSERVICE.Cluster.addr
WHERE DBSERVICE.Cluster.addr IN (SELECT DBSERVICE.Cluster.addr
    FROM DBSERVICE.Cluster
    WHERE DBSERVICE.Cluster.cluster = (SELECT DBSERVICE.Cluster.cluster AS cluster
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.addr = 861860936))
```




```sql


-- SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc) AS ibtc, T.btc AS obtc, SUM(DBCORE.TxOut.btc)-T.btc AS fee
SELECT DBCORE.TxIn.tx AS tx, SUM(DBCORE.TxOut.btc)-T.btc AS fee
FROM DBCORE.TxIn
INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
INNER JOIN (SELECT DBCORE.TxOut.tx AS tx, SUM(DBCORE.TxOut.btc) AS btc
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
        FROM DBCORE.TxIn
        INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
        WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
            FROM DBSERVICE.Cluster
            WHERE DBSERVICE.Cluster.cluster = 860103337)
        UNION
        SELECT DISTINCT DBCORE.TxOut.tx
        FROM DBCORE.TxOut
        WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
            FROM DBSERVICE.Cluster
            WHERE DBSERVICE.Cluster.cluster = 860103337))
    GROUP BY DBCORE.TxOut.tx) AS T ON DBCORE.TxIn.tx = T.tx
WHERE DBCORE.TxIn.tx IN (SELECT DISTINCT DBCORE.TxIn.tx
    FROM DBCORE.TxIn
    INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND DBCORE.TxIn.pn = DBCORE.TxOut.n
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337)
    UNION
    SELECT DISTINCT DBCORE.TxOut.tx
    FROM DBCORE.TxOut
    WHERE DBCORE.TxOut.addr IN (SELECT DBSERVICE.Cluster.addr
        FROM DBSERVICE.Cluster
        WHERE DBSERVICE.Cluster.cluster = 860103337))
-- AND DBCORE.TxIn.tx = 669566365
GROUP BY DBCORE.TxIn.tx;
```

### Export cluster id
```sql
SELECT DBSERVICE.TagID.tag AS clusterName, DBSERVICE.Cluster.cluster AS clusterId, DBINDEX.AddrID.addr AS address
FROM DBSERVICE.Cluster
INNER JOIN DBSERVICE.Tag ON DBSERVICE.Cluster.addr = DBSERVICE.Tag.addr
INNER JOIN DBINDEX.AddrID ON DBSERVICE.Cluster.addr = DBINDEX.AddrID.id
INNER JOIN DBSERVICE.TagID ON DBSERVICE.Tag.tag = DBSERVICE.TagID.id
ORDER BY DBSERVICE.Cluster.addr ASC
LIMIT 10;
```

### Export Address id
```sql
SELECT DBINDEX.AddrID.id AS ClusterID, DBINDEX.AddrID.addr AS Address
FROM DBINDEX.AddrID
ORDER BY DBINDEX.AddrID.id ASC
LIMIT 10;
