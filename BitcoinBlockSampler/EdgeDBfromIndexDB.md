# Create Edge Database from Index and Tx Database

## Prerequirements
- Build index and tx database

### Build edge table
```sql
ATTACH DATABASE "index.db" AS other
CREATE TABLE DEdge AS
    SELECT other.TxIn.addr AS src, other.TxOut.addr AS dst, COUNT(other.TxIn.tx) AS weight
    FROM other.TxIn
    INNER JOIN other.TxOut ON other.TxIn.tx = other.TxOut.tx
    GROUP BY other.TxIn.addr, other.TxOut.addr;
CREATE TABLE UDEdge AS
    SELECT MIN(src, dst) AS src,
       MAX(dst, src) AS dst,
       SUM(weight) AS weight
    FROM DEdge
    GROUP BY MIN(src, dst), MAX(dst, src);
```

- Backup: If we want to filter coinbase
```sql
WHERE other.TxIn.addr != 0 AND other.TxOut.addr != 0
```

### Export csv
```sql
.mode list
.separator ' '
.once edge.csv
SELECT src, dst FROM UDEdge;
```

#### Practice
```sql
CREATE TABLE IF NOT EXISTS Edge(
  src INTEGER NOT NULL,
  dst INTEGER NOT NULL,
  weight INTEGER NOT NULL,
    UNIQUE (src, dst)
  );
INSERT OR IGNORE INTO Edge VALUES (1, 2, 1);
INSERT OR IGNORE INTO Edge VALUES (2, 3, 1);
INSERT OR IGNORE INTO Edge VALUES (1, 3, 1);
INSERT OR IGNORE INTO Edge VALUES (3, 2, 1);
INSERT OR IGNORE INTO Edge VALUES (2, 1, 1);
INSERT OR IGNORE INTO Edge VALUES (1, 1, 1);
INSERT OR IGNORE INTO Edge VALUES (2, 2, 1);

SELECT MIN(src, dst) AS src,
       MAX(dst, src) AS dst,
       SUM(weight) AS weight
FROM Edge
GROUP BY MIN(src, dst), MAX(dst, src);
```