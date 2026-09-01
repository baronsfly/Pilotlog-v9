# PilotLog v9.1

PilotLog 9 new-codebase development build. PilotLog 8.9 is reference-only and remains untouched.

Critical payroll invariant: Flight Credit H is calculated only from Schedule OUT → Schedule IN. Actual OUT/IN, Actual Block, OFF/ON and imported Flight credit values never drive Flight Credit H. The Morocco night premium / return-sector behavior is reproduced from v8.9.

For testing: replace the files in the Pilotlog-v9 repository, reopen the page, verify v9.1 is shown, import the v8.9 Full Backup again, and keep Sync disabled until data checks pass.
