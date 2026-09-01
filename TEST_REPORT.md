# PilotLog 9.0 test report

## Automated checks passed

- JavaScript syntax check for all application modules and service worker.
- DOM integrity: no duplicate IDs; all simple `getElementById` references resolved; all navigation targets exist.
- Service-worker asset list verified and cache namespace confirmed isolated to `pilotlog9-*`.
- Central Flight Credit H test: Scheduled 3:15 -> 3:30; actual times do not change credit.
- Morocco night premium test including immediate return after UTC midnight.
- DHD=0, DHP editable, Ground GI default, Ground TNE=0.
- Future Simulator exclusion from Logbook.
- Simulator duty-window test (−90/+30).
- Trip charge test: DHD excluded, Ground fixed 5h, DHP uses saved credit.
- Training Sector pay test: only final saved Flight Instruction counts.
- Real AeroLINE September 2026 JSON parsing: 18 Flight, 5 Simulator, 3 Ground, 1 DHD, 2 DHP, 1 STBY, 8 OFF, 17 Expiry.
- AeroLINE re-import idempotency: no duplicate source activities.
- Manual-override re-import protection test.
- Synthetic v8 Roster + Logbook same-flight migration unifies to one activity.
- Complete LogTen package parser and original SQLite archive format test.

## Environment limitation

A full automated headless-Chromium interaction test could not be completed in the build container because Chromium did not complete navigation in that environment. Static hosting was reachable and all module/static tests above passed. The first iPhone/iPad/browser run remains the required UI smoke test before replacing PilotLog 8.9.
