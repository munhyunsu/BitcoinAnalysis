### Usage

  1. Download (bitcoin core)[https://bitcoin.org/en/download] and Block data
    
    - Run bitcoind or bitcoin-qt
    
      - CLI version

      ```bash
      ./bitcoind
      ```

      - GUI version

      ```bash
      ./bitcoin-qt
      ```

  2. configuration edit
  
    - Download sample [bitcoin.conf](https://github.com/bitcoin/bitcoin/blob/master/share/examples/bitcoin.conf)

    - Edit file
    
      - add at line 75

      ```bash
        server=1
      ```
    
      - add at line 102: result from _BitcoinCoreRPCAuth.ipynb_

      ```bash
        rpcauth=...
      ```

      - add at line 153

      ```bash
        txindex=1
      ```

  3. Reindex and Rescan (JUST ONCE!!)

    - CLI version
    ```bash
      bitcoind -reindex -rescan
    ```

    - GUI version

    ```bash
      bitcoin-qt -reindex -rescan
    ```

    - After performing once, run without parameters

  4. Check bitcoin-cli COMMAND

  ```bash
    ./bitcoin-cli getblockheight 1
  ```

#### Build database

```bash
./run_db_builder_init.sh
```

#### Resume database

```bash
./run_db_builder_resume.sh
```

#### BlockClustering
  - Crate Bitcoin Address - Transaction Hash Table

