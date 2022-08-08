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

