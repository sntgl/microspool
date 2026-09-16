# microspool

Track your 3D printing filament spools — material, colour and grams left — **on the printer itself**.
microspool speaks the [Spoolman](https://github.com/Donkie/Spoolman) REST API, so Moonraker subtracts
filament usage while you print and Fluidd, Mainsail and HelixScreen show your spools as usual.

A single static C binary: **~106 KB on disk, ~0.2 MB RAM**. No Python, no database, no second host.
That is the point: a printer like the FlashForge Adventurer 5M running
[Z-Mod](https://github.com/ghzserg/zmod) has 128 MB of RAM for Klipper, Moonraker and the touchscreen —
nothing left over for a filament inventory service, so this one had to be tiny.

It implements the part of the Spoolman API that Moonraker and the interfaces actually call:
`vendor` / `filament` / `spool` CRUD, `spool/{id}/use`, `info`, `health`, materials / locations /
lot numbers and the `/api/v1/spool` WebSocket. The inventory lives in one JSON file. A built-in web UI
(~8 KB gzipped) is served at `http://<host>:7912/` when started with `-l 0.0.0.0` — there is no auth, so
keep it on a trusted network; `-U` disables it. No SpoolmanDB mirror, no multi-printer inventory: if your
host can run Spoolman, run Spoolman. Independent project, not affiliated with it.

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
