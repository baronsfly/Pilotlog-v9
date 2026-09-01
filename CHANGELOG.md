# PilotLog Changelog

## v9.0 RC1
- Clean PilotLog 9 implementation kept separate from PilotLog 8.9.
- Restored v8.9 visual direction: left navigation rail, professional light interface and familiar section ordering.
- Replaced multi-file module dependency with a self-contained deployment shell to prevent navigation failure when GitHub uploads omit folders.
- Single authoritative operational activity database.
- Roster and Logbook use the same activity ID; Logbook is a completed FLIGHT/SIM filter.
- Central Payroll, Trips, Totals and FTL calculations.
- AeroLINE / LogTen import paths, Expiry, backups and db9 sync architecture.
- Added Today Duty / Next Duties to Totals.
- Browser navigation regression test added before release.
