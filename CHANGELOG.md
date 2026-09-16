# Changelog

## 0.2.0 — 2026-09-15

- Built-in web UI (single `ui/index.html`, gzip-embedded in the binary): spool cards with
  search/archived filter, quick actions (set/use weight, location/comment, archive, delete,
  `extra` editor), add/edit spool with inline filament+brand creation, filament/brand list
  management, optional Moonraker active-spool badge. Served at `GET /`/`/index.html`
  (`Content-Encoding: gzip`; `406` if the client doesn't advertise `Accept-Encoding: gzip`);
  `-U` disables it (`404`). Adds armv7 ~10.8 KB.
- `GET /api/v1/setting/currency` (for Fluidd); any other `/api/v1/setting/*` → 404.
- `GET /api/v1/material`, `/api/v1/location`, `/api/v1/lot-number` — sorted unique values
  in use, for Fluidd/Mainsail/HelixScreen pickers.

## 0.1.0 — 2026-09-15

First public version.

- Spoolman API subset: vendor / filament / spool CRUD, `spool/{id}/use`, info, health, empty external catalog.
- WebSocket `/api/v1/spool` with ping/pong and spool events (required by Moonraker `[spoolman]`).
- Atomic JSON persistence with deferred writes for usage reports.
- Static musl builds for armv7, aarch64, x86_64 (~95–105 KB).
- Verified on FlashForge AD5M (Z‑Mod 1.7.3): Moonraker connects, proxy and usage reporting work, RSS 124–224 KB.
