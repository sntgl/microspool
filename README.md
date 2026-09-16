# microspool

Tiny [Spoolman](https://github.com/Donkie/Spoolman)-compatible spool server for low-memory Klipper hosts.
Single static C binary: **~105–115 KB**, **~0.2 MB RAM**. Built for the FlashForge AD5M
([Z-Mod](https://github.com/ghzserg/zmod), 128 MB RAM), works with any Moonraker.

Implements only what Moonraker and HelixScreen use: `vendor` / `filament` / `spool` CRUD,
`spool/{id}/use`, `info`, `health`, and the `/api/v1/spool` WebSocket. Data is one JSON file.
No auth, no SpoolmanDB. Not affiliated with Spoolman.

Includes a tiny built-in web UI at `http://<host>:7912/` (run with `-l 0.0.0.0` to reach it
from other devices — there is no auth, so only do this on a trusted network); pass `-U` to
disable it entirely.

## Status

- Moonraker `[spoolman]` — tested on a real AD5M (connection, proxy, usage reports).
- HelixScreen — should work (same API calls), on-device check pending.
- Fluidd/Mainsail spool panels — not tested.

## Build

```sh
make release   # build/microspool-{armv7,aarch64,x86_64}, static musl (needs zig)
make test      # needs uv
```

## Use

```sh
microspool -l 127.0.0.1 -p 7912 -d microspool.json
```

```ini
# moonraker.conf
[spoolman]
server: http://127.0.0.1:7912
```

AD5M / Z-Mod install: [contrib/zmod/INSTALL.md](contrib/zmod/INSTALL.md).

## License

MIT. Bundles [cJSON](https://github.com/DaveGamble/cJSON) (MIT).
