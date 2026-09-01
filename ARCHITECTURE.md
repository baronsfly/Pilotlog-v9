# PilotLog 9.0 RC6 architecture note

PilotLog 9 keeps one authoritative operational activity store. Roster, Logbook, Payroll, Trips and Totals are views/calculations over those records.

## Flight Credit H invariant
The calculation boundary is explicit:
`scheduledBlockMinutesForCredit(activity)` accepts only `schedOut` and `schedIn` for Flight Credit H. There is deliberately no fallback to `out`, `in`, `block`, `off`, `on`, `flight` or stored Flight `credit`.

## v8 migration guard
Roster sectors are associated with existing v8 Flight records using strong identities first and the v8.9 fallback of date + trailing flight-number digits + departure + arrival. After migration, each roster month is recalculated twice: once from the migrated v9 activity store and once directly from the legacy v8 source model. A mismatch aborts the migration.
