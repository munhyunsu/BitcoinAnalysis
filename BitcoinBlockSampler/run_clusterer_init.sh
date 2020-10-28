#!/bin/bash

sqlite3 init_cluster.db '
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
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
ATTACH DATABASE "dbv3-index.db" as DBINDEX;
INSERT OR IGNORE INTO Cluster (addr, cluster)
    SELECT id, -1 
    FROM DBINDEX.TxID 
    ORDER BY DBINDEX.TxID.id ASC;
CREATE INDEX idx_Cluster_2 ON Cluster(cluster);
PRAGMA journal_mode = NORMAL;
PRAGMA synchronous = WAL;
'