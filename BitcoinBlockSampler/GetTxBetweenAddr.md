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