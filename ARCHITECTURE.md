# PilotLog 9 architecture

## One authoritative activity record

```text
pilotlog9-core / activities
        |
        +-- Roster   -> planned/visible filter
        +-- Logbook  -> completed FLIGHT + SIM filter
        +-- Payroll  -> centralized calculations
        +-- Trips    -> time-range grouping/calculation
        +-- Totals   -> aggregation
        +-- FTL      -> operational duty/flight aggregation
```

A real activity is not copied between modules. Its activity ID is the identity used by the application and synchronization layer.

## Storage separation

Operational data and other domain data are separate object stores inside one IndexedDB database, rather than separate duplicate module databases:

- `activities` — authoritative operational records
- `expiry` — licences/medical/courses/endorsements
- `trips` — trip group records
- `settings` — settings/rates/FX
- `ledger` — create/edit/delete/lock/source tombstones
- `backups` — rotating local recovery slot
- `archives` — large local-only material such as original LogTen SQLite and airport cache

The original LogTen SQLite archive is intentionally excluded from Cloud Sync.

## Startup isolation

Navigation event handlers are bound before any database operation. IndexedDB open has a bounded timeout and a non-blocking fallback path so a storage initialization fault cannot disable tab navigation.

## Sync isolation

PilotLog 9 uses a separate `db9` snapshot protocol. Uploads are written as a new generation, chunked, fetched back/verified, and only then promoted through active metadata. The old active generation is not deleted before replacement verification.

The local change ledger participates in conflict/deletion handling. Manual **Sync now** remains available regardless of Auto Sync.
