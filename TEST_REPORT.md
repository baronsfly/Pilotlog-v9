# PilotLog v9.1 — Test Report

## Credit Hours regression
Test source: the real PilotLog v8.9 Full Backup exported 2026-09-01.

- August 2026 Roster sectors: 30
- Existing v8.9 Flight matches: 29
- Planned Roster-only sector: 1 (3O481, 20 Aug)
- Non-flight activities included by the v8.9 roster-primary payroll source: 6
- Exact v8.9 Scheduled Block payroll reconstruction: 6,420 min = 107:00 — PASS
- 31 Aug MAC111: 3:30 Credit H
- 31 Aug MAC112: 3:30 Credit H
- August before MAC112: 103:30; after MAC112: 107:00

Flight Credit H invariant:
1. source = Schedule OUT -> Schedule IN only;
2. round each Flight scheduled block up to the next 30 minutes;
3. apply Morocco scheduled-departure premium using the exact v8.9 same-date immediate-return rule;
4. never use Actual OUT/IN, Actual Block, OFF/ON or imported Flight credit as the Flight Credit H source.

## Build checks
- Inline application JavaScript parsed with `node --check`: PASS
- Mobile navigation CSS keeps the 7-tab bar at the bottom for widths <= 900 px: PASS
- Development service-worker application caching disabled to avoid stale GitHub Pages builds: PASS
- v8 migration has a second Credit H verification after records are written and reloaded: PASS
