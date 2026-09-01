# PilotLog 9.0 RC6

## RC6 mobile UI parity fix
- Restored PilotLog 8.9 responsive navigation behavior: primary navigation is a fixed bottom tab bar at widths <= 900 px (including iPhone/iPad portrait), not a left rail.
- Desktop/wide-tablet left rail remains unchanged.
- Flight Credit H invariant from RC5 is unchanged: Scheduled OUT -> Scheduled IN only; actual times are never a credit source.


## Credit Hours hard lock
- Flight Credit Hours are derived only from Schedule OUT → Schedule IN.
- Actual OUT / IN, Actual Block, airborne time and imported/stored Flight credit can never be used as a fallback for Flight Credit H.
- Existing 30-minute upward rounding and Morocco +50% logic are preserved.
- Roster ↔ Logbook matching uses trailing flight-number digits (e.g. 3O337 = MAC337) plus date and route, matching the v8.9 workflow.
- v8 backup migration now runs a Credit H regression check for every roster month and aborts if the migrated result differs from a direct scheduled-source reconstruction.
- Import confirmation/completion displays the verified monthly Credit H values.

## Cache hardening
- Visible build badge bumped to v9.0 RC6.
- Service worker cache bumped to `pilotlog9-rc6-v1`.
- Service worker registration requests updates without HTTP cache, reducing the chance of an older RC build remaining active after GitHub Pages deployment.