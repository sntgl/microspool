# Installing microspool on FlashForge AD5M / AD5M Pro with Z‑Mod

Tested on AD5M, stock firmware 3.2.7, Z‑Mod 1.7.3, Moonraker 0.10.0 (Z‑Mod), HelixScreen 1.0.

> Do everything while the printer is **idle**. Restarting Moonraker during a print will interrupt it,
> and on a 128 MB printer extra SSH/disk load during a print can trigger `Timer too close`.

1. **Copy the binary** (Z‑Mod's dropbear has no SFTP, so stream it over SSH; Z‑Mod root password is `root`):

   ```sh
   ssh root@PRINTER 'mkdir -p /opt/config/mod_data/microspool'
   ssh root@PRINTER 'cat > /opt/config/mod_data/microspool/microspool && chmod +x /opt/config/mod_data/microspool/microspool' \
       < microspool-armv7
   ```

2. **Start it now and at every boot** — append to `/opt/config/mod_data/power_on.sh` (Z‑Mod runs it on power‑on
   and keeps `mod_data` across updates):

   ```sh
   # microspool
   pidof microspool >/dev/null || nohup nice -n 10 /opt/config/mod_data/microspool/microspool \
       -l 127.0.0.1 -p 7912 -d /opt/config/mod_data/microspool/data.json >/dev/null 2>&1 &
   ```

   Run the same line once by hand, then `wget -qO- http://127.0.0.1:7912/api/v1/health` → `{"status":"healthy"}`.

3. **Connect Moonraker** — append to `/opt/config/mod_data/user.moonraker.conf`:

   ```ini
   [spoolman]
   server: http://127.0.0.1:7912
   ```

   Restart Moonraker (`POST /server/restart`) and check `GET /server/spoolman/status` →
   `"spoolman_connected": true`.

4. **Add spools** (HelixScreen → Filament → Spoolman → Add, or HTTP — see the main README) and set the
   active one: `POST /server/spoolman/spool_id {"spool_id": N}`.

The data file lives in `/opt/config/mod_data/microspool/data.json`, next to the rest of Z‑Mod's user data
(included in `TAR_CONFIG` backups of `mod_data`).

## Uninstall

Remove the lines from `power_on.sh` and the `[spoolman]` section, restart Moonraker,
`killall microspool`, delete `/opt/config/mod_data/microspool`.
