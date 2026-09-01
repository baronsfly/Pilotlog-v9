# PilotLog 9.0 — clean rebuild (RC1)

This build is a new PilotLog 9 implementation. PilotLog 8.9 was used only as a read-only functional and visual reference.

## Deployment
Upload the files in this folder directly to the root of the separate `Pilotlog-v9` GitHub repository:
- `index.html`
- `manifest.webmanifest`
- `sw.js`

There are no `src/` folders or external local JavaScript/CSS dependencies. The complete app shell is self-contained in `index.html`, specifically to avoid the missing-folder/navigation failure seen in the earlier prototype.

## Core architecture
- One authoritative operational `activities` store.
- Each activity has one stable unique ID and one type: FLIGHT, SIM, DHD, DHP, STBY, GROUND or OFF.
- Roster is a view of planned activities.
- Logbook is a filtered view of the same records: completed FLIGHT + SIM.
- Payroll, Trips and Totals calculate from the same activity records.
- Completing a Roster flight updates the same activity record; it does not create a duplicate Logbook flight.
- Manual saved values are authoritative over imported defaults.
- Change ledger/tombstone architecture is kept for sync/delete history.
- PilotLog 9 uses its own local database and db9 cloud protocol; it does not touch PilotLog 8.9/db8.

## Functional coverage included
- AeroLINE monthly JSON import/re-import with source-backed IDs.
- Training/line-training mapping.
- Ground, SIM, DHD, DHP, STBY and OFF activities.
- LogTen Tab import and complete LogTen package support.
- Expiry import/deduplication, lock and attachment support.
- Full flight editor, Return Flight, crew fields, instruction, PF/IFR, approaches, Day OFF flag.
- Central credit/payroll calculations, Morocco night premium and training-sector allowance.
- Trips and layover calculations.
- Totals and EASA FTL rolling limits.
- Search and Lock all Logbook entries.
- Backup/restore, separate db9 sync, screenshot review/import.
- Pre-flight WX framework with ETA -2h / +1h TAF window.

## Important
Keep PilotLog 8.9 as the operational version while this RC is being tested. This build uses a separate database namespace.
