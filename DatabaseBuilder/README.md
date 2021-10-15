# The manual for building BitSQL database with MariaDB

## Requirements

1. Download (bitcoin core)[https://bitcoin.org/en/download] and Block data

```bash
./bitcoind
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

```bash
bitcoind -reindex -rescan
```

  - After performing once, run without parameters

4. Check bitcoin-cli COMMAND

```bash
./bitcoin-cli getblockheight 1
```

1. create virtual environments and install essential library

- Install docker and mariadb

  - [Install on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

  - (Recommended) Running docker on a non-root user

  - [Guide: Post-installation steps for Linux](https://docs.docker.com/engine/install/linux-postinstall/)

  ```bash
  sudo usermod -aG docker $USER
  ```

  - [MariaDB Docker Hub Reference](https://hub.docker.com/_/mariadb)$

- Install python3 library

```
python3 -m venv venv
pip3 install --upgrade -r requirements.txt
```

## Create MariaDB account

1. Run database container

```bash
docker run --name some-mariadb -v /my/own/datadir:/var/lib/mysql -e MARIADB_ROOT_PASSWORD=my-secret-pw -d mariadb:tag
```

2. Create remote user

```sql
CREATE DATABASE DATABASENAME;
GRANT ALL PRIVILEGES ON DATABASENAME.* TO 'USERNAME'@'%' IDENTIFIED BY 'PASSWORD';
FLUSH PRIVILEGES;
```

3. Create `secret.py`

```python
# Contents of secret.py
db_username = USERNAME
db_password = PASSWORD
db_address = ADDRESS
db_port = PORT"
```
