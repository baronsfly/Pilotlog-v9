# PilotLog Changelog

## v9.1
- Restored the exact v8.9 Flight Credit H calculation path.
- Flight Credit H uses Schedule OUT → Schedule IN only, rounded up to 30 minutes.
- Morocco night +50% and immediate-return inheritance now follow the exact v8.9 same-date rule.
- Payroll roster-primary source is constrained to one authoritative Flight per Roster sector.
- v8 import now verifies Credit H again after the migrated data have actually been written and reloaded from PilotLog 9 storage.
- Development application caching/service-worker persistence disabled to prevent an older build from remaining active after GitHub Pages updates.
- Version naming simplified: 9.1, 9.2, 9.3… (no RC suffixes).
