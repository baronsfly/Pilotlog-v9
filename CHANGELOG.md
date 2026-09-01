# PilotLog 9.0 RC4

- Corrected the v8.9 Full Backup migration Roster-link rule.
- Historical LogTen/manual Flight records are now linked to their matching Roster sector by the same v8.9 fallback identity: date + flight number + From + To, when a strong completedEntryId/rosterItemId/AeroLINE key is unavailable.
- The fallback is accepted only when the matching authoritative Flight is unique.
- A Roster row does not need to already say `done` if an authoritative saved Flight exists; this matches v8.9 `savedEntryForRosterSector()` behavior.
- No Credit H formula change was made. The fix corrects which already-saved Flight supplies Schedule OUT / Schedule IN to Roster-first Payroll.
- Supplied real backup: Roster links increased from the incorrect RC3 value 5 to the correct 29; genuinely unmatched planned sectors reduced from 43 to 19.
- Supplied real backup August 2026 Credit H corrected from RC3 test value 106:30 to **107:00**.
- Verified final 31 August sector contributes 3:30, taking August from 103:30 to 107:00.
- September 2026 Credit H remains 102:15.
- Preserves all 7,924 legacy v8 entries, 168 Trips, 30 post-dedup Expiry records and 939,782 historical Flight minutes.
- Unified activity IDs remain duplicate-free: 7,960 / 7,960 unique.
- No UI/graphics/navigation change from RC3.
- Service-worker cache bumped to `pilotlog9-rc4-v1`.
