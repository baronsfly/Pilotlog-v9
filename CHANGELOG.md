# PilotLog 9 Changelog

## v9.0.0 — new-authoritative-database test build

- Reimplemented PilotLog as a new project; v8.9 is specification/reference only.
- Added one authoritative IndexedDB activity store with stable ID + activity type.
- Roster, Logbook, Payroll, Trips, Totals and FTL read the same activity records.
- Logbook is a filter for completed Flight + Simulator only.
- Added v8 Full Backup copy-import with Roster/Logbook unification.
- Preserved complete LogTen migration/archive workflow with original SQLite kept local only.
- Added structured AeroLINE roster import, crew/training mapping, validations and direct-sync fallback path.
- Preserved manual-override authority for operational/calculation fields.
- Centralized Flight Credit H, Morocco premium, DHD/DHP, Simulator, Ground and Training Sector rules.
- Added source-backed deterministic identities, revisions and ledger/tombstones.
- Added isolated verified-generation `db9` Cloud Sync, manual Sync now, optional Auto Sync and db9-only deep reset.
- Added local drafts for new and existing unlocked entries.
- Added Return Flight route reversal, next flight number, PF inversion and cross-midnight date handling.
- Added automated night taxi/airborne calculation, PF day/night takeoff/landing allocation, Delay Reason, approach counts and seat totals.
- Added Expiry categories, semantic dedupe, AeroLINE re-import handling, photo attachment, lock and LPC English defaults.
- Added Trip cash suppression of layover pay, Trip-Start historical FX and global currency path.
- Added Payroll arrears and per-Day-OFF payroll remarks.
- Added Today/Next Duty plus Maximum Daily FDP and rolling FTL views.
- Added local self-contained A4 EASA-style PDF export with no PDF CDN dependency.
- Added Pre-flight WX framework, Report−40 configuration and TAF ETA−2/+1 filtering with relevance guidance.
- Isolated PilotLog 9 service-worker cache prefix from PilotLog 8.x caches and excluded external APIs from service-worker interception.
- Added bounded IndexedDB/airport startup paths so data/network faults cannot disable navigation.
