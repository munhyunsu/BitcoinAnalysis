# The manual for building BitSQL database with MariaDB

## Requirements

1. Download (bitcoin core)[https://bitcoin.org/en/download] and Block data

```bash
./bitcoind
```

2. configuration edit

- Download sample [bitcoin.conf](https://github.com/bitcoin/bitcoin/blob/master/share/examples/bitcoin.conf)

- Edit file

```
# add at line 75
server=1
    
# add at line 102: result from _BitcoinCoreRPCAuth.ipynb_
rpcauth=...

# add at line 153
txindex=1
```

3. Reindex and Rescan (JUST ONCE!!)

- After performing once, run without parameters

```bash
bitcoind -reindex -rescan
```

4. Check bitcoin-cli COMMAND

```bash
./bitcoin-cli getblockheight 1
```

5. Create MariaDB container on docker

- Install docker and mariadb

- [Install on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

- (Recommended) Running docker on a non-root user

- [Guide: Post-installation steps for Linux](https://docs.docker.com/engine/install/linux-postinstall/)

```bash
sudo usermod -aG docker $USER
```

- [MariaDB Docker Hub Reference](https://hub.docker.com/_/mariadb)

```bash
docker pull mariadb
docker run --name bitsqldb -p 3306:3306 -v /my/own/datadir/var/lib/mysql:/var/lib/mysql -e MARIADB_ROOT_PASSWORD=my-secret-pw -d mariadb:latest
```

- Create user and database

```bash
docker exec -it bitsqldb bash
mariadb -uroot -p
```

```sql
CREATE DATABASE bitsqldb;
CREATE USER IF NOT EXISTS user@bitsqldb IDENTIFIED BY 'user-secret-pw';
SHOW WARNINGS;
GRANT ALL PRIVILEGES ON bitsqldb.* TO 'user'@'%' IDENTIFIED BY 'user-secret-pw';
FLUSH PRIVILEGES;
```

- Create `secret.py`

```python
# Contents of secret.py
rpc_username = USERNAME
rpc_password = PASSWORD
db_username = USERNAME
db_password = PASSWORD
db_address = ADDRESS
db_port = PORT
```

6. Install python3 library and install library

```
python3 -m venv venv
source venv/bin/activate
pip3 install --upgrade -r requirements.txt
```
