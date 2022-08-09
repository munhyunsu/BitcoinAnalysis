# Simple run (development)

```bash
uvicorn main:app --reload --port 8888
```

## Production mode (gunicorn)

```bash
gunicorn -w 4 -k uvicorn.workers.UvicornWorker --chdir [PATH] main:app --bind 0.0.0.0:8888
```

# BitSQL Service using `systemd`

```bash
sudo cp $HOME/.../run_bitsql /opt/bitsql/etc/systemd/run_bitsql
sudo chmod +x /opt/bitsql/etc/systemd/run_bitsql
```

```bash
sudo ln -s /opt/bitsql/etc/systemd/bitsql.service /etc/systemd/system/bitsql.service
sudo systemctl daemon-reload
sudo systemctl enable bitsql.service
sudo systemctl start bitsql.service
sudo systemctl status bitsql.service
```

