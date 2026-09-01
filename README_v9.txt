PILOTLOG v9.2 — SINGLE AUTHORITATIVE DATABASE
================================================

BASE
----
Built directly from PilotLog v8.9. The existing interface, CSS, fields, navigation, imports, exports and established business rules are preserved.

ARCHITECTURE
------------
1. PilotLog now has one authoritative operational IndexedDB database: pilotlog9-authoritative-data.
2. Every operational activity is stored once with one stable ID, its activity type and visibility/state flags.
3. A planned Roster flight and its completed Logbook flight are the same record. Completing it changes the state of that record; it does not create a linked copy.
4. Roster, Logbook, Duty, Payroll, Trips, Dashboard and Totals consume filtered projections from the same activity collection.
5. Trips, Expiry, settings, payroll configuration, FX data, drafts and the sync ledger are stored in the same authoritative database state.
6. Device/session credentials and safety backups remain separate from operational data and are not alternative authoritative sources.

CALCULATION ENGINE
------------------
- PilotLogEngine is the only calculation service used by the UI.
- Entry metrics, Credit Hours, duty sessions, operational monthly views, trip duty/layover, Payroll, Totals, Logbook statistics and Dashboard duty projections are produced centrally.
- Pages format and display engine projections; they do not own separate business-rule formulas.
- No v8.9 Credit Hours, Payroll, Training Sector, Simulator, Day OFF, layover, Trip, Expiry, currency or Totals formula was intentionally changed.

MIGRATION / COMPATIBILITY
-------------------------
- Existing v8.9 Full Backup JSON files are accepted and migrated in one operation.
- Legacy flights, roster sectors and duties are merged into unique activity records without losing their original IDs/source references.
- A v9.1 Full Backup contains the single authoritative database rather than separate module datasets.
- Existing cloud snapshots with the prior section layout are accepted and migrated before merge; new snapshots transport the one database in verified chunks.
- Weekly backup and recovery functions are retained as non-authoritative safety copies.

VERIFICATION WITH THE PROVIDED BACKUP
-------------------------------------
- 7,924 Logbook/activity entries preserved.
- 48 Roster sectors preserved.
- 17 duty records preserved.
- 168 Trips preserved.
- 36 Expiry records preserved.
- 7,960 unique operational activity records after consolidation.
- Zero duplicate activity IDs.
- Payroll compared with v8.9 across 270 populated months: zero differences.
- Totals compared with v8.9: zero differences.

VISUAL PRESERVATION
-------------------
- pilotlog-8.3.0.css is byte-for-byte unchanged from v8.9.
- index.html keeps the same page structure, controls and navigation; only the displayed version and JavaScript filename changed.
- A non-UI v8.9 service-worker bridge replaces the old offline cache on devices already running v8.9.

FILES
-----
- index.html
- pilotlog-9.2.js
- pilotlog-8.3.0.css
- sw-9.2.js
- sw-8.9.js (cache upgrade bridge only)
- manifest.webmanifest
- README_v9.txt
- CHANGELOG.md


v9.1 RESTORE PERFORMANCE FIX
----------------------------
- Fixes a severe Trips rendering slowdown after restoring a large v8.9 backup.
- Trip operational entries are built once and reused while rendering the Trips list instead of rebuilding the full multi-year operational history for every saved trip.
- Added indexed trip-entry lookup for saved trip contents.
- No UI, payroll, credit-hour, roster, logbook, trip, expiry, sync, or business-rule formula was changed.


v9.2 UI PERFORMANCE FIX
-----------------------
- Logbook Credit display no longer rebuilds the complete 7,000+ entry archive for every visible row.
- Logbook statistics now group entries by date once, preserving the same Credit Hours rules while avoiding repeated full-database scans.
- Trips DOM is rendered only when the Trips page is opened, instead of being built invisibly during startup.
- Logbook anchor lookup no longer scans every rendered row.
- Logbook initially renders 400 entries at a time, with an explicit Load older entries control; search still runs against the full authoritative database and statistics still cover all matches.
- No data schema, business rule, Payroll formula, Credit Hours rule, Roster logic, Trips formula, Expiry logic or visual layout was changed.
