# PilotLog 9.0 RC4 — actual backup regression test

Test input: `pilotlog_backup_2026-09-01.json` exported by PilotLog 8.9.

## Automated assertions passed
- JavaScript syntax: PASS
- Main navigation targets present: Roster / Logbook / Payroll / Expiry / Trips / Totals / Settings: PASS
- v8 legacy rows retained: 7,924 / 7,924
- Roster sectors linked to existing authoritative Flights with v8.9 identity: 29
- Genuinely unmatched Roster sectors added as planned: 19
- Unified activities: 7,960
- Unified activity IDs duplicated: 0
- Trips: 168 / 168
- Expiry input: 36
- Expiry semantic duplicates merged: 6
- Expiry output: 30
- Active local draft retained: 1
- Historical Flight total: 939,782 minutes before / 939,782 after

## Payroll regression
- August 2026 operational source: 36 entries
- August 2026 Credit H: 6,420 min = **107:00**
- 31 August sectors: 2
- MAC111 Credit H: 3:30
- MAC112 Credit H: 3:30
- August total before final MAC112: 103:30
- September 2026 Credit H: 6,135 min = 102:15

Result: ALL ASSERTIONS PASS.

The migration/payroll assertions execute the actual RC4 `Rules` and `Importers` modules extracted from the release `index.html` against the supplied real v8.9 backup.
