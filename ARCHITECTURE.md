# PilotLog 9 — architecture

Operational information exists once:

`activities[id] -> { type, date, status, roster, operational fields, source, revision, ... }`

Views do not own duplicate operational records:
- Roster: planned/roster-visible activities
- Logbook: completed FLIGHT and SIM activities
- Payroll: month-specific operational source derived from the authoritative activities
- Trips: grouping/calculation over the same operational source
- Totals/FTL: aggregation over completed authoritative activities

## Roster identity during v8 migration
Strong identity is preferred in this order: v8 `completedEntryId`, `rosterItemId`, AeroLINE key/leg identity.

When those links are absent, RC4 reproduces the v8.9 fallback used by `savedEntryForRosterSector()`: an existing Flight can be associated with a Roster sector by exact date + normalized flight number + departure + arrival, provided the match is unique. This is necessary for historical LogTen flights that existed before the AeroLINE Roster was imported.

Once linked, there is still only one authoritative operational Flight. Roster/AeroLINE information is metadata/default context only; final values already saved in that Flight remain authoritative, including Schedule OUT / Schedule IN used for Credit H.

For months with Roster data, Payroll uses these Roster-linked authoritative flights plus applicable non-flight activities and does not mix duplicate LogTen rows into the same month. Months without Roster data continue to use LogTen history when available.

Non-operational entities have dedicated stores:
- `expiry`
- `trips`
- `settings`
- `ledger`
- `backups`
- `archives`

Local unsaved drafts remain device-local and are excluded from Payroll, Totals and Cloud until saved.

PilotLog 9 remains isolated from PilotLog 8.9:
- local DB: `pilotlog9-core`
- cloud protocol: `db9`
- local keys: `pilotlog9_*`
