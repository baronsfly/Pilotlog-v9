# PilotLog 9.0 RC6

Clean v9 codebase. PilotLog 8.9 is used only as the behavioural/data reference and is not modified.

RC6 hardens the non-negotiable Flight Credit H rule:

**Flight Credit H = Schedule OUT → Schedule IN only.**

Actual OUT/IN, Actual Block, airborne time and stored/imported Flight credit are never valid Credit H sources or fallbacks. The existing 30-minute upward rounding and Morocco +50% premium logic remain unchanged.

The v8 Full Backup importer also performs an independent scheduled-source Credit H reconstruction for every roster month. If the migrated database does not match it exactly, the import fails rather than accepting a wrong Payroll result.

Validated on the supplied real v8.9 backup:
- August 2026: 107:00 Credit H
- September 2026: 102:15 Credit H
- 7,924 legacy entries retained
- 29 Roster sectors linked to existing Flight entries
- 19 future/planned sectors retained
- 168 Trips retained

Auto Sync remains OFF after v8 import. Review locally before enabling cloud sync.
