# PilotLog 9 — architecture

Operational data is stored once:

`activities[id] -> { type, date, status, roster, operational fields, source, revision, ... }`

Views do not own copies of operational activities:
- Roster: `activity.roster.planned === true`
- Logbook: completed `FLIGHT` and `SIM`
- Payroll: calculations over activities
- Trips: grouping/calculations over activities
- Totals/FTL: aggregation over activities

Non-operational entities have dedicated stores:
- `expiry`
- `trips`
- `settings`
- `ledger`
- `backups`
- `archives`

These are not duplicate operational databases. The authoritative flight/duty/activity record exists only in `activities`.

PilotLog 9 is isolated from PilotLog 8.9:
- local DB: `pilotlog9-core`
- cloud protocol: `db9`
- local keys: `pilotlog9_*`
