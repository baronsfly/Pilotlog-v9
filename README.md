# PilotLog 9.0 — New Architecture

PilotLog 9.0 is a new implementation. PilotLog 8.9 was used only as a behavioural/specification reference; its production JavaScript is not part of this build.

## Core architecture

PilotLog 9 has one authoritative operational IndexedDB database: `pilotlog9-core`.

Each operational activity exists once with a stable `id` and a `type`:

- `FLIGHT`
- `SIM`
- `DHD`
- `DHP`
- `STBY`
- `GROUND`
- `OFF`

Roster, Logbook, Payroll, Trips and Totals are views/calculations over the same activity records. Logbook shows only completed Flight and Simulator records. Completing a Roster flight updates that same activity; it does not create a second Logbook copy.

## Safety / coexistence with 8.9

- PilotLog 9 local DB: `pilotlog9-core`
- PilotLog 9 cloud namespace: `db9`
- PilotLog 9 localStorage keys: `pilotlog9_*`
- PilotLog 9 cache prefix: `pilotlog9-*`

PilotLog 9 reset controls intentionally target only PilotLog 9/db9. They do not target PilotLog 8.9/db8.

For the first test, deploy PilotLog 9 to a **separate folder, branch, subdomain or repository**. Do not overwrite the live 8.9 GitHub Pages files until you have completed your own data comparison.

## Migration / import

The Settings page supports:

- Import v8 Full Backup — imports a copy and unifies legacy Roster + Logbook activity representations.
- Complete LogTen migration — retains the original SQLite database byte-for-byte in a local archive and imports normalized records with stable LogTen identities.
- LogTen Tab compatibility import.
- AeroLINE monthly JSON import and browser-session Direct Sync fallback path.
- Calendar / roster CSV and reviewed screenshot import.

Large/bulk imports remain local first and disable Auto Sync until the user deliberately reviews the result and presses **Sync now**.

## Important consolidated rules

- Flight Credit H = Scheduled OUT → Scheduled IN only, rounded upward to 30 minutes.
- Actual OUT/IN never determines Flight Credit H.
- Existing Morocco scheduled-departure +50% rule is centralized; the immediate return sector inherits it, including across midnight.
- DHD = zero credit.
- DHP credit remains editable and is chargeable in Trip duty according to its saved credit.
- Simulator time remains separate from Flight/PIC/SIC totals; simulator duty window is simulator start −90 min to simulator end +30 min.
- Ground Course: GI defaults to configured Ground Credit; TNE defaults to 0; final manual credit remains editable/authoritative.
- Training Sector pay is generated only by the final saved `Flight Instruction` value, not an unsaved AeroLINE flag.
- Imported/derived values are prefills. A final explicit user save is authoritative for downstream calculations.
- Logbook lock protects Flight/Simulator editing. Non-flight Roster activities remain directly editable from Roster.
- Deleted source-backed rows write ledger/tombstone information so normal re-import/sync does not silently resurrect them.

## WX

PilotLog 9 includes the agreed pre-flight WX framework:

- default trigger: Report −40 min while the PWA is active;
- destination METAR + TAF;
- TAF operational window: ETA −2 h to ETA +1 h;
- FM/BECMG/TEMPO/PROB groups are filtered and labelled as `RELEVANT AT ETA` or `MONITOR / CONTINGENCY`;
- BECMG transition wording explains whether the change may occur at ETA or has become prevailing.

Exact background notification delivery while the web app is closed still requires a push backend. CTOT remains external until authorised CTOT Flight API/webhook credentials are available.

## First test sequence

1. Open the new PilotLog 9 deployment without importing anything and verify every bottom tab navigates immediately.
2. Set Profile name/role/home base.
3. Import a PilotLog 8.9 Full Backup **as a copy**.
4. Compare Logbook totals and several individual flights against 8.9.
5. Import the current AeroLINE JSON and verify Roster activities and Expiry.
6. Check Payroll Credit H, Training Sector, Simulator, DHD/DHP and Ground Course examples.
7. Keep Auto Sync OFF during the first review.
8. Only after the local database is verified, sign in and press **Sync now** to create/use db9.

