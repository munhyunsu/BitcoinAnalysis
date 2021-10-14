# The manual for building BitSQL database with MariaDB

## Requirements

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

```bash
docker run --name some-mariadb -v /my/own/datadir:/var/lib/mysql -e MARIADB_ROOT_PASSWORD=my-secret-pw -d mariadb:tag
```
