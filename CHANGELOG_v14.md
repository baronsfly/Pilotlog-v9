# PilotLog v14 — modifiche rispetto alla v13.11

- Import completo dei tre file LogTen `.sql`, `.sql-wal`, `.sql-shm`, con verifica WAL e conservazione degli originali nell'archivio locale.
- Riconciliazione per UID e dati operativi: recupero dei record mancanti, merge con AeroLINE/manuali senza duplicare voli o simulatori, consolidamento degli UID già duplicati.
- Conservazione distinta di valori LogTen, zero e NULL; protezione delle modifiche manuali, con segnalazione dei conflitti. Nessuna sostituzione automatica del Night LogTen con il ricalcolo astronomico.
- Correzione dei campi concordati: Night, PIC Night, Multi Pilot, Dual Received, P1US, Simulator, SFI/SFE e Total Instrument. Multi Pilot conservato anche nelle sessioni SIM; Total Instrument riportato in Totals.
- MAC478 del 04/09/2026: Dual Received e P1US ripristinati a 0 tramite il dato LogTen.
- Aircraft Breakdown: A319 e A320 aggregati in A320 Family da tutti i record Core; simulatori esclusi dal breakdown di volo.
- Versione e riferimenti della cache aggiornati a 14. Layout, navigazione, CSS, icone e altre regole dell'app invariati.

Verifica sui file forniti: 7.951 UID LogTen unici, inclusi i 22 record del WAL; tutti i 13 totali concordati coincidono al minuto. A320 Family: Total **15.467:04**, PIC **11.675:47**, SIC **3.784:50**. Reimportazione: nessun nuovo record. Preservati 222 campi manuali e i contatori di decolli/atterraggi. Quattro tipi aeromobile manuali discordanti conservati e segnalati.

Per applicare le correzioni ai dati già presenti, dopo l'aggiornamento aprire **Import complete LogTen migration** e selezionare insieme i tre file LogTen originali con lo stesso nome base. Il solo aggiornamento dell'app non reimporta il database.
