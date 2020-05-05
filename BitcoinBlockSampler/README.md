### Usage


  - configuration edit
  
    - Download configuration file
    
      - [bitcoin.conf](https://github.com/bitcoin/bitcoin/blob/master/share/examples/bitcoin.conf)

    - line 72

      ```bash
        server=1
      ```
    
    - line 99

      ```bash
        rpcauth=...
      ```

    - line 151

      ```bash
        txindex=1
      ```

  - Reindex and Rescan

    ```bash
      bitcoind-cli -reindex -rescan
    ```


#### BlockHeightFinder
  - Bitcoin block height finder by datatime

#### BlockSampleRegtest
  - Bitcoin block sampling and creating Regtest database

### TODO
  1. Script to address improvements
    - More coverage
