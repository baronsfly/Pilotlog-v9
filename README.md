# PilotLog 9.0 — clean rebuild (RC4)

PilotLog 9 is a new implementation. PilotLog 8.9 is used only as a read-only functional, calculation and visual reference.

## Deployment
Upload these release files directly to the root of the separate `Pilotlog-v9` GitHub repository:
- `index.html`
- `manifest.webmanifest`
- `sw.js`

The app shell is self-contained in `index.html`; there is no local `src/` dependency that can break navigation after a partial GitHub upload.

## Core architecture
- One authoritative operational `activities` store.
- Stable unique activity ID and type: FLIGHT, SIM, DHD, DHP, STBY, GROUND or OFF.
- Roster is a planned-activity view of the same store.
- Logbook is the completed FLIGHT + SIM view of the same store.
- Payroll, Trips and Totals calculate from the same activity records.
- An existing completed flight is linked to its Roster sector instead of creating a duplicate Roster flight.
- Imported AeroLINE data does not overwrite final manually saved operational values.
- Separate stores exist only for non-operational entities such as Expiry, Trips, settings, ledger, backups and archives.
- PilotLog 9 uses its own local database/cloud namespace and does not modify PilotLog 8.9/db8.

## RC4 — v8.9 Roster/Logbook identity parity
RC4 was validated directly against the supplied `pilotlog_backup_2026-09-01.json`.

The important correction is the v8.9 Roster fallback identity rule. A Roster sector can already correspond to a saved historical Flight even when its Roster row still says `planned`. PilotLog 8.9 identifies that Flight by date + flight number + route when the strong IDs are absent. This is required for historical LogTen entries imported before the AeroLINE Roster.

On the supplied backup RC4 now:
- retains all 7,924 v8 activity/entry rows 1:1;
- links 29 Roster sectors to existing authoritative flights;
- adds only the 19 genuinely unmatched Roster sectors as planned activities;
- retains 17 Day OFF duties;
- preserves 168 Trips;
- restores the active local draft;
- produces 7,960 unique authoritative activities with zero duplicate IDs;
- resolves 36 Expiry rows to 30 authoritative records after six semantic duplicate merges.

## Payroll regression
Roster-first Payroll now uses the same saved-flight identity that v8.9 used. On the supplied backup:
- August 2026: **107:00 Credit H** (6,420 minutes)
- 31 August MAC111: 3:30 Credit H
- 31 August MAC112: 3:30 Credit H
- August total before the final 31 August sector: 103:30
- September 2026: 102:15 Credit H

The Flight Credit H formula itself was not changed in RC4. The correction is the Roster→existing-Flight association used as the Payroll source.

Historical flight totals remain unchanged: 939,782 flight minutes before and after conversion.

## Safety during migration
A v8 import is a clean local replacement of PilotLog 9 test data. It clears the v9 sync ledger, keeps Auto Sync OFF before and after the operation, creates a local v9 backup after verification, and never writes to PilotLog 8.9.
