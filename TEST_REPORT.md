# PilotLog 9.0 RC6 — regression report

Test input: supplied real `pilotlog_backup_2026-09-01.json` (PilotLog Backup v8.9).

## Migration / Payroll
- Legacy entries: 7,924 retained.
- Roster sectors: 29 linked to existing authoritative Flight entries, 19 retained as planned sectors.
- Trips: 168.
- Expiry: 36 source records → 30 after semantic duplicate merge.
- August 2026 Credit H: **107:00**.
- September 2026 Credit H: **102:15**.
- Migration-level Credit H reference checks: PASS for 2026-08 and 2026-09.

## Rigid Credit H source invariant
A completed Flight was cloned and its Actual OUT/IN, OFF/ON, Actual Block, airborne time and stored credit were deliberately changed while Schedule OUT/IN were left unchanged.
- Credit before mutation: 2:30.
- Credit after actual-data mutation: 2:30.
- Result: PASS — actual operational times cannot affect Flight Credit H.

A synthetic Flight was then tested with only Schedule IN changed.
- Credit changed from 2:30 to 3:00.
- Result: PASS — Schedule OUT/IN are the driving source.

## Navigation / syntax
- JavaScript syntax check: PASS.
- Mobile 390×844 structural navigation test: Roster, Logbook, Payroll, Expiry, Trips, Totals, Settings — PASS; exactly one view active after every click.
- Visible version badge: v9.0 RC6.


## RC6 verified regression tests

### Responsive UI parity against v8.9
- Source reference inspected: PilotLog v8.9 CSS uses a bottom 7-tab navigation bar at viewport widths <= 900 px.
- iPhone viewport 390x844: navigation box = x 0, y 778, width 390, height 66; fixed to bottom; body left padding 0. PASS.
- Portrait tablet viewport 820x1180: navigation box = x 0, y 1114, width 820, height 66; fixed to bottom. PASS.
- Desktop viewport 1180x820: left navigation rail retained. PASS.
- Roster, Logbook, Payroll, Expiry, Trips, Totals, Settings: every tab clicked and corresponding view active/visible. PASS.
- JavaScript page errors during the responsive/navigation test: none.

### Real v8.9 backup payroll regression
Test source: pilotlog_backup_2026-09-01.json (real v8.9 Full Backup).
- Legacy entries retained: 7,924.
- Unified activities: 7,960.
- Roster links to existing flights: 29; planned sectors added: 19.
- August 2026 Credit H: 107:00. PASS.
- September 2026 Credit H: 102:15. PASS.
- Mutation test: every August flight Actual OUT/IN, stored Actual Block, and imported credit were intentionally replaced with extreme values; August Credit H remained 107:00. PASS.
- Therefore Flight Credit H is independent of actual times/actual block/imported flight credit and is driven by Scheduled OUT -> Scheduled IN.
