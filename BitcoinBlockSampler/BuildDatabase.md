### Database schema design
- Disk usage prediction criteria
    - Block height: 620000
    - Transactions: 508923998
    - Addresses: 628875885
    - TxIn: 1242812467
    - TxOut: 1351292737

---
#### Native design (a.k.a. Version 1)
- Schema

```sql
CREATE TABLE IF NOT EXISTS Blk (
    height INTEGER PRIMARY KEY,
    blkhash TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS Tx (
    txid TEXT PRIMARY KEY,
    height INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS TxIn (
    txid TEXT NOT NULL,
    n INTEGER NOT NULL,
    addr TEXT NOT NULL,
    UNIQUE (txid, n, addr));

CREATE TABLE IF NOT EXISTS TxOut (
    txid TEXT NOT NULL,
    n INTEGER NOT NULL,
    addr TEXT NOT NULL,
    UNIQUE (txid, n, addr));
```

- Disk usage prediction
    - Blk: (8 + 64) * 620000 = 44640000
    - Tx: (64 + 8) * 508923998 = 36642527856
    - TxIn: (64 + 8 + 35) * 1242812467 = 132980933969
    - TxOut: (64 + 8 + 35) * 1351292737 = 144588322859
    - Total: 44640000 + 36642527856 + 132980933969 + 144588322859 = 314256424684 (314 GBytes)
        - With indexes: 44640000*2 + 36642527856*2 + 132980933969*3 + 144588322859*3 = 906082106196 (906 GBytes)

---
#### Index design (a.k.a. Version 2)
- Schema

```sql
CREATE TABLE IF NOT EXISTS BlkID (
    id INTEGER PRIMARY KEY,
    blkhash TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS TxID (
    id INTEGER PRIMARY KEY,
    txid TEXT NOT NULL UNIQUE);

CREATE TAblE IF NOT EXISTS AddrID (
    id INTEGER PRIMARY KEY,
    addr TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS TxIn (
    tx INTEGER,
    n INTEGER,
    addr INTEGER,
    UNIQUE (tx, n, addr));

CREATE TABLE IF NOT EXISTS TxOut (
    tx INTEGER,
    n INTEGER,
    addr INTEGER,
    UNIQUE (tx, n, addr));
```

- Disk usage prediction
    - BlkID: (8 + 64) * 620000 = 44640000
    - TxID: (8 + 64) * 508923998 = 36642527856
    - AddrID: (8 + 35) * 628875885 = 27041663055
    - TxIn (8 + 8 + 8) * 1242812467 = 29827499208
    - TxOut: (8 + 8 + 8) * 1351292737 = 32431025688
    - Total: 44640000 + 36642527856 + 27041663055 + 29827499208 + 32431025688 = 125987355807 (125 GBytes)
        - Total without height field in TxID: 44640000 + 36642527856 + 27041663055 + 29827499208 + 32431025688 = 125987355807 (125 GBytes)
        - With indexes: 44640000*2 + 36642527856*2 + 27041663055*2 + 29827499208*3 + 32431025688*3 = 314233236510 (314 GBytes)

##### Child database tables

- Edge table
```sql
    CREATE TABLE Edge AS
        SELECT other.TxIn.addr AS src, other.TxOut.addr AS dst, COUNT(other.TxIn.tx) AS weight
        FROM other.TxIn
        INNER JOIN other.TxOut ON other.TxIn.tx = other.TxOut.tx
        WHERE other.TxIn.addr != 0 AND other.TxOut.addr != 0
        GROUP BY other.TxIn.addr, other.TxOut.addr;
```

---
#### Hierarchical database design (a.k.a. Version 3)

- Level 1: Index Tables (file: index.db)

```sql
CREATE TABLE IF NOT EXISTS BlkID (
    id INTEGER PRIMARY KEY, -- block height
    blkhash TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS TxID (
    id INTEGER PRIMARY KEY,
    txid TEXT NOT NULL UNIQUE);

CREATE TAblE IF NOT EXISTS AddrID (
    id INTEGER PRIMARY KEY,
    addr TEXT NOT NULL UNIQUE);
```

- Level 2: Core Tables (file: core.db)

```sql
CREATE TABLE IF NOT EXISTS BlkTime (
    blk INTEGER PRIMARY KEY,
    unixtime INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS BlkTx (
    blk INTEGER NOT NULL,
    tx INTEGER NOT NULL,
    UNIQUE (blk, tx));

CREATE TABLE IF NOT EXISTS TxIn (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    addr INTEGER NOT NULL,
    btc REAL NOT NULL, 
    UNIQUE (tx, n));

CREATE TABLE IF NOT EXISTS TxOut (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    addr INTEGER NOT NULL,
    btc REAL NOT NULL,
    UNIQUE (tx, n));
```

- Level 3: Util Tables (file: util.db)

```sql
CREATE TABLE IF NOT EXISTS FirstBlk (
    addr INTEGER PRIMARY KEY,
    blk INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS UTXO (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    UNIQUE (tx, n));
```

---
#### Hierarchical database design (a.k.a. Version 4)

- Level 1: Index Tables (file: index.db)

```sql
CREATE TABLE IF NOT EXISTS BlkID (
    id INTEGER PRIMARY KEY, -- block height
    blkhash TEXT NOT NULL UNIQUE);

CREATE TABLE IF NOT EXISTS TxID (
    id INTEGER PRIMARY KEY,
    txid TEXT NOT NULL UNIQUE);

CREATE TAblE IF NOT EXISTS AddrID (
    id INTEGER PRIMARY KEY,
    addr TEXT NOT NULL UNIQUE);
```

- Level 2: Core Tables (file: core.db)

```sql
CREATE TABLE IF NOT EXISTS BlkTime (
    blk INTEGER PRIMARY KEY,
    unixtime INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS BlkTx (
    blk INTEGER NOT NULL,
    tx INTEGER NOT NULL,
    UNIQUE (blk, tx));

CREATE TABLE IF NOT EXISTS TxIn (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    ptx INTEGER NOT NULL,
    pn INTEGER NOT NULL,
    UNIQUE (tx, n));

CREATE TABLE IF NOT EXISTS TxOut (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    addr INTEGER NOT NULL,
    btc REAL NOT NULL,
    UNIQUE (tx, n, addr));
    
CREATE INDEX idx_BlkTime_2 ON BlkTime(unixtime);
CREATE INDEX idx_BlkTx_2 ON BlkTx(tx);
CREATE INDEX idx_TxIn_3_4 ON TxIn(ptx, pn);
CREATE INDEX idx_TxOut_3 ON TxOut(addr);
```

- Level 3: Util Tables (file: util.db)

```sql
PRAGMA synchronous = OFF;
PRAGMA journal_mode = OFF;

ATTACH DATABASE './dbv3-core.db' AS DBCORE;
ATTACH DATABASE './dbv3-index.db' AS DBINDEX;

-- Graph Edge 
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
            FROM DBCORE.TxOut) AS TXO ON TXO.tx = TXI.tx;

CREATE INDEX idx_Edge_1 ON Edge(tx);
CREATE INDEX idx_Edge_2 ON Edge(src);
CREATE INDEX idx_Edge_3 ON Edge(dst);

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;
```

- Level 4: Service Tables (file: service.db)

```sql
CREATE TABLE IF NOT EXISTS Cluster (
    addr INTEGER PRIMARY KEY,
    cluster NOT NULL);
    
CREATE TABLE IF NOT EXISTS TagID (
    id INTEGER PRIMARY KEY,
    tag TEXT UNIQUE);
    
CREATE TABLE IF NOT EXISTS Tag (
    addr INTEGER NOT NULL,
    tag INTEGER NOT NULL,
    UNIQUE (addr, tag));

CREATE INDEX idx_Cluster_2 ON Cluster(cluster);
```

- Level 5: Temp Tables (file: temp.db)

```sql
PRAGMA synchronous = OFF;
PRAGMA journal_mode = OFF;

ATTACH DATABASE './dbv3-core.db' AS DBCORE;
ATTACH DATABASE './dbv3-index.db' AS DBINDEX;

-- UTXO
DROP TABLE IF EXISTS UTXO;

CREATE TABLE IF NOT EXISTS UTXO (
    tx INTEGER NOT NULL,
    n INTEGER NOT NULL,
    UNIQUE (tx, n));

INSERT OR IGNORE INTO UTXO (tx, n)
SELECT DBCORE.TxOut.tx AS tx, DBCORE.TxOut.n AS n
FROM DBCORE.TxOut
WHERE NOT EXISTS (SELECT *
                  FROM DBCORE.TxIn
                  WHERE DBCORE.TxIn.ptx = DBCORE.TxOut.tx AND
                        DBCORE.TxIn.pn = DBCORE.TxOut.n);
GROUP BY tx, n;

PRAGMA synchronous = NORMAL;
PRAGMA journal_mode = WAL;

```