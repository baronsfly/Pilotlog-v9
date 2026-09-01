# PilotLog 9.0 RC1 — test report

## Static
- Bundled JavaScript: Node syntax check passed.
- All seven primary views exist in the deployment HTML.
- Default view: Totals.
- No PilotLog 8.9 JS/CSS source file is included in the deployment build.

## Browser navigation
Tested in Chromium by injecting the self-contained deployment HTML into a clean page:
- Totals -> Roster: PASS
- Roster -> Logbook: PASS
- Logbook -> Payroll: PASS
- Payroll -> Expiry: PASS
- Expiry -> Trips: PASS
- Trips -> Totals: PASS
- Totals -> Settings: PASS
- Exactly one active view after every navigation: PASS
- Navigation active-state follows the selected view: PASS
- JavaScript exception events during navigation: 0

## UI
- 390 x 844 mobile viewport rendered successfully.
- Left-side navigation rail and professional light PilotLog layout restored to the v8.9 visual direction.
- Today Duty, Next Duties and Total Statistics visible on Totals.

## Logic
Existing PilotLog 9 logic test suite:
- AeroLINE September 2026: 38 activities parsed
  - FLIGHT 18
  - SIM 5
  - DHD 1
  - DHP 2
  - STBY 1
  - GROUND 3
  - OFF 8
- Complete LogTen package test: PASS
- Credit calculation test: PASS

## Deployment hardening
The earlier prototype depended on a `src/` folder. This RC does not:
- application CSS is inline
- application JavaScript is bundled inline
- no local module import is required for page navigation
