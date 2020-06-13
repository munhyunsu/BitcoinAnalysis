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

  ```sql
  SELECT * FROM TxIn INNER JOIN TxOut ON TxIn.tx = TxOut.tx AND TxIn.n = TxOut.n WHERE TxIn.addr IN () AND TxOut.addr IN ();
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
