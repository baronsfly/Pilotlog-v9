/*
 PilotLog source map
  01 Core/storage/settings
  02 Airports/night/flight-entry rules
  03 Duty + EASA FTL
  04 Roster + trips
  05 Imports
  06 Exports
  07 Payroll
  08 Expiry / validity tracking
  09 Cloud sync
  10 UI/render/events
*/
(() => {
'use strict';
const VERSION='16.3';
const TRIPS_KEY='pilotlog7_trips_v1', PAY_SETTINGS_KEY='pilotlog7_pay_settings_v1', PAY_MONTH_KEY='pilotlog7_pay_month_v1', FX_KEY='pilotlog7_fx_v1', APP_SETTINGS_KEY='pilotlog7_app_settings_v1', LAST_EMAIL_KEY='pilotlog7_last_email_v1', ENTRY_DRAFT_KEY='pilotlog7_entry_draft_v1', ENTRY_DRAFTS_KEY='pilotlog8_entry_drafts_v1', CLOUD_TOMBSTONES_KEY='pilotlog7_local_delete_queue_v1', SYNC_LEDGER_KEY='pilotlog8_sync_ledger_v1', EXPIRY_KEY='pilotlog7_expiry_v1', RECONCILIATION_REVIEW_KEY='pilotlog9_reconciliation_review_v1', LOGTEN_ARCHIVE_META_KEY='pilotlog7_logten_archive_meta_v1', AEROLINE_CONFIG_KEY='pilotlog7_aeroline_config_v1', SYNC_DEVICE_KEY='pilotlog7_device_v1';
const SYNC_PROTOCOL='db8', SYNC_READY_KEY='pilotlog7_database_initialized', CLOUD_DIRTY_KEY='pilotlog7_cloud_dirty_v1', CLOUD_BASE_REV_KEY='pilotlog7_cloud_base_revision_v1', CLOUD_PENDING_IMPORT_KEY='pilotlog7_cloud_pending_import_v1';
const $=id=>document.getElementById(id);
const LEGACY_FLIGHT_BACKUP_KEY='pilotlog7_flights_backup_v1';
// Read-only migration inputs. Runtime code never reads or writes separate Roster/Logbook stores.
const LEGACY_FLIGHTS_STORAGE_KEY='pilotlog7_flights_v1',LEGACY_ROSTER_STORAGE_KEY='pilotlog7_roster_v1',LEGACY_DUTIES_STORAGE_KEY='pilotlog7_duties_v1';
const LEGACY_FLIGHT_DB_NAME='pilotlog7-core-data',LEGACY_FLIGHT_DB_STORE='records',LEGACY_FLIGHT_DB_RECORD='flights-v1';
const FLIGHT_DB_NAME='pilotlog9-authoritative-data',FLIGHT_DB_STORE='records',FLIGHT_DB_RECORD='pilotlog-database-v1',FLIGHT_DB_META_KEY='pilotlog9_database_meta_v1',FALLBACK_DB_KEY='pilotlog9_database_fallback_v1';
const BUSINESS_ARRAY_KEYS=new Set([TRIPS_KEY,EXPIRY_KEY,SYNC_LEDGER_KEY,ENTRY_DRAFTS_KEY,CLOUD_TOMBSTONES_KEY,RECONCILIATION_REVIEW_KEY]);
const BUSINESS_OBJECT_KEYS=new Set([PAY_SETTINGS_KEY,PAY_MONTH_KEY,FX_KEY,APP_SETTINGS_KEY,ENTRY_DRAFT_KEY,LOGTEN_ARCHIVE_META_KEY,AEROLINE_CONFIG_KEY]);
const BUSINESS_KEYS=new Set([...BUSINESS_ARRAY_KEYS,...BUSINESS_OBJECT_KEYS]);
let operationalDatabase=null,flightStoreReady=false,flightPersistActive=false,flightPersistDirty=false,flightPersistTimer=null,flightPersistPromise=Promise.resolve();

function emptyOperationalDatabase(){
  return{schema:'PilotLog Operational Database',schemaVersion:1,records:[],values:{},updatedAt:new Date().toISOString()};
}
function isOperationalDatabase(value){return !!(value&&value.schema==='PilotLog Operational Database'&&Number(value.schemaVersion)===1&&Array.isArray(value.records)&&value.values&&typeof value.values==='object')}
function normalizeOperationalDatabase(value){
  const db=isOperationalDatabase(value)?value:emptyOperationalDatabase();
  return{...db,records:normalizeCoreActivityRecords(Array.isArray(db.records)?db.records:[]),values:db.values&&typeof db.values==='object'?db.values:{},updatedAt:db.updatedAt||new Date().toISOString()};
}
function legacyLocalJson(key,def){try{const v=JSON.parse(localStorage.getItem(key)||'null');return v==null?def:v}catch{return def}}
function legacyLocalFlights(){const v=legacyLocalJson(LEGACY_FLIGHTS_STORAGE_KEY,[]);return Array.isArray(v)?copyJson(v):[]}
function openFlightStoreDb(){
  return new Promise((resolve,reject)=>{
    const req=indexedDB.open(FLIGHT_DB_NAME,1);
    req.onupgradeneeded=()=>{const db=req.result;if(!db.objectStoreNames.contains(FLIGHT_DB_STORE))db.createObjectStore(FLIGHT_DB_STORE)};
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error||new Error('PilotLog data database unavailable'));
  });
}
async function readFlightStore(){
  if(window.PilotLogNative?.isNative)return window.PilotLogNative.readDatabase();
  const db=await openFlightStoreDb();
  try{return await new Promise((resolve,reject)=>{const tx=db.transaction(FLIGHT_DB_STORE,'readonly'),req=tx.objectStore(FLIGHT_DB_STORE).get(FLIGHT_DB_RECORD);req.onsuccess=()=>resolve(req.result??null);req.onerror=()=>reject(req.error)})}
  finally{db.close()}
}
async function writeFlightStore(database,{verifiedSnapshot=false}={}){
  const data=verifiedSnapshot?database:normalizeOperationalDatabase(database);
  if(!verifiedSnapshot)data.updatedAt=new Date().toISOString();
  if(window.PilotLogNative?.isNative){await window.PilotLogNative.writeDatabase(data);return true}
  const db=await openFlightStoreDb();
  try{await new Promise((resolve,reject)=>{const tx=db.transaction(FLIGHT_DB_STORE,'readwrite');tx.objectStore(FLIGHT_DB_STORE).put(data,FLIGHT_DB_RECORD);tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error||new Error('PilotLog data write aborted'))})}
  finally{db.close()}
  try{localStorage.setItem(FLIGHT_DB_META_KEY,JSON.stringify({records:data.records.length,updatedAt:data.updatedAt,schemaVersion:data.schemaVersion}))}catch{}
  return true;
}
function readLegacyFlightStore(){
  return new Promise(resolve=>{
    try{
      const req=indexedDB.open(LEGACY_FLIGHT_DB_NAME,1);
      req.onupgradeneeded=()=>{};
      req.onerror=()=>resolve([]);
      req.onsuccess=()=>{
        const db=req.result;
        if(!db.objectStoreNames.contains(LEGACY_FLIGHT_DB_STORE)){db.close();resolve([]);return}
        const tx=db.transaction(LEGACY_FLIGHT_DB_STORE,'readonly'),q=tx.objectStore(LEGACY_FLIGHT_DB_STORE).get(LEGACY_FLIGHT_DB_RECORD);
        q.onsuccess=()=>{const rows=Array.isArray(q.result)?q.result:[];db.close();resolve(rows)};
        q.onerror=()=>{db.close();resolve([])};
      };
    }catch{resolve([])}
  });
}
function rawFlightDigits(value){const m=String(value||'').toUpperCase().match(/(\d{2,4})$/);return m?m[1]:''}
function rawCanonicalType(value,fallback='Flight'){
  const key=String(value||'').trim().toUpperCase().replace(/\s+/g,' ');
  const aliases={'FLIGHT':'Flight','SIM':'Simulator','SIMULATOR':'Simulator','RT':'Simulator','GROUND COURSE':'Ground Course','GROUND TRAINING':'Ground Course','GROUND TRAINING/COURSE':'Ground Course','GRT':'Ground Course','DHD':'DHD','DHP':'DHP','STBY':'STBY','STANDBY':'STBY','HSBY':'STBY','DAY OFF':'Day OFF','OFF':'Day OFF','FLIGHT DUTY':'Flight Duty','DUTY':'Duty','SICK LEAVE':'Sick Leave','ANNUAL LEAVE':'Annual Leave'};
  return aliases[key]||String(value||'').trim()||fallback;
}
const ACTIVITY_STATUS=Object.freeze({DRAFT:'DRAFT',PLANNED:'PLANNED',OPEN:'OPEN',COMPLETED:'COMPLETED'});
const LEGACY_ACTIVITY_FIELDS=Object.freeze({entry:['entry','Visible'].join(''),roster:['roster','Visible'].join(''),duty:['duty','Visible'].join(''),completed:['completed','EntryId'].join(''),rosterItem:['roster','ItemId'].join(''),legacyRoster:['legacy','RosterId'].join(''),rosterAeroline:['roster','AerolineKey'].join('')});
function legacyActivityField(row,key){return row?.[LEGACY_ACTIVITY_FIELDS[key]]}
function activityStatusRank(value){return value===ACTIVITY_STATUS.COMPLETED?3:value===ACTIVITY_STATUS.OPEN?2:1}
function canonicalActivityStatus(source={},origin=''){
  const explicit=String(source.activityStatus||source.status||'').trim().toUpperCase();
  if(explicit==='DRAFT'&&!source.locked)return ACTIVITY_STATUS.DRAFT;
  const sourceName=String(source.source||'').toLowerCase();
  /* LogTen status comes from LogTen itself: closed/locked rows are completed; unclosed rows remain OPEN.
     Source values remain authoritative in either state. */
  if(sourceName==='logten')return source.locked?ACTIVITY_STATUS.COMPLETED:ACTIVITY_STATUS.OPEN;
  /* A saved/manual non-LogTen Logbook entry is completed according to the existing PilotLog workflow. */
  if(source.manualOverride||source.completedAt||source.locked)return ACTIVITY_STATUS.COMPLETED;
  /* Anything coming from the legacy Entries collection was already a saved operational
     entry, even when its original source was AeroLINE (DHD/DHP/SIM/Ground included). */
  if(origin==='entries'&&legacyActivityField(source,'entry'))return ACTIVITY_STATUS.COMPLETED;
  if(explicit==='COMPLETED'||explicit==='DONE')return ACTIVITY_STATUS.COMPLETED;
  if(explicit==='OPEN')return ACTIVITY_STATUS.OPEN;
  if(explicit==='PLANNED'||explicit==='PLAN')return ACTIVITY_STATUS.PLANNED;
  return ACTIVITY_STATUS.PLANNED;
}
function stripLegacyActivityFields(row){
  const rest={...(row||{})};
  for(const key of Object.values(LEGACY_ACTIVITY_FIELDS))delete rest[key];
  for(const key of ['status','std','sta','report','end','minutes','notes','flight','scheduleTotal','instructionType','trainingSector','instructorFlightTime','instructorExaminer','multiCrew'])delete rest[key];
  return rest;
}
function normalizeCoreActivity(source={},origin=''){
  const dutyType=rawCanonicalType(source.dutyType||(origin==='duties'?source.type:'')||'Flight',origin==='duties'?'Duty':'Flight');
  const category='OPERATIONAL';
  const clean=stripLegacyActivityFields(source);
  if((clean.schedBlock===undefined||clean.schedBlock===null||clean.schedBlock==='')&&source.scheduleTotal!==undefined&&source.scheduleTotal!==null&&source.scheduleTotal!=='')clean.schedBlock=source.scheduleTotal;
  delete clean.scheduleTotal;
  const hasTotalDuty=source.totalDuty!==undefined&&source.totalDuty!==null&&source.totalDuty!=='';
  const legacyTotalDuty=source.dutyMinutes!==undefined&&source.dutyMinutes!==null?source.dutyMinutes:source.minutes;
  const totalDuty=hasTotalDuty?source.totalDuty:(legacyTotalDuty!==undefined&&legacyTotalDuty!==null?Number(legacyTotalDuty):undefined);
  delete clean.dutyMinutes;
  return{
    ...clean,
    id:source.id||makeActivityId(),recordKind:'activity',activityCategory:category,dutyType,
    schedOut:source.schedOut||source.std||'',schedIn:source.schedIn||source.sta||'',
    onDuty:source.onDuty||source.report||'',offDuty:source.offDuty||source.end||'',
    remarks:source.remarks||source.notes||'',
    ...(totalDuty!==undefined?{totalDuty}:{}),
    activityStatus:canonicalActivityStatus(source,origin)
  };
}
function directUnifiedFlightMatch(a,b){
  if(!a||!b||rawCanonicalType(a.dutyType)!=='Flight')return false;
  if(a.id&&b.id&&a.id===b.id)return true;
  if(a.aerolineKey&&b.aerolineKey&&a.aerolineKey===b.aerolineKey)return true;
  if(a.aerolineFlightLegId&&b.aerolineFlightLegId&&String(a.aerolineFlightLegId)===String(b.aerolineFlightLegId))return true;
  const ad=rawFlightDigits(a.flightNo),bd=rawFlightDigits(b.flightNo);
  return !!(a.date&&a.date===b.date&&ad&&ad===bd&&String(a.dep||'').toUpperCase()===String(b.dep||'').toUpperCase()&&String(a.arr||'').toUpperCase()===String(b.arr||'').toUpperCase());
}
function scheduledFlightIdentityMatch(a,b){
  if(!a||!b||rawCanonicalType(a.dutyType)!=='Flight'||rawCanonicalType(b.dutyType)!=='Flight')return false;
  const aOut=a.schedOut||a.std||'',aIn=a.schedIn||a.sta||'',bOut=b.schedOut||b.std||'',bIn=b.schedIn||b.sta||'';
  const timeDistance=(left,right)=>{const parse=value=>{const m=String(value||'').match(/^(\d{1,2}):(\d{2})$/);return m?Number(m[1])*60+Number(m[2]):null},x=parse(left),y=parse(right);if(x==null||y==null)return 9999;const d=Math.abs(x-y);return Math.min(d,1440-d)};
  return !!(a.date&&a.date===b.date&&String(a.dep||'').toUpperCase()===String(b.dep||'').toUpperCase()&&String(a.arr||'').toUpperCase()===String(b.arr||'').toUpperCase()&&aOut&&aIn&&timeDistance(aOut,bOut)<=15&&timeDistance(aIn,bIn)<=15);
}
function uniqueUnifiedFlightMatch(records,row){
  const direct=(records||[]).filter(candidate=>directUnifiedFlightMatch(candidate,row));
  if(direct.length===1)return direct[0];
  if(direct.length>1)return null;
  const scheduled=(records||[]).filter(candidate=>scheduledFlightIdentityMatch(candidate,row));
  return scheduled.length===1?scheduled[0]:null;
}
function unifiedFlightMatch(a,b){return directUnifiedFlightMatch(a,b)||scheduledFlightIdentityMatch(a,b)}
function blankOperationalValue(value){return value===undefined||value===null||value===''}
function activityAuthorityScore(row){
  return (row?.locked?500:0)+activityStatusRank(row?.activityStatus)*100+Object.values(row||{}).filter(v=>!blankOperationalValue(v)).length;
}
function mergeCoreActivities(a,b){
  let primary;
  const ta=String(a?._updatedAt||''),tb=String(b?._updatedAt||'');
  if(ta!==tb)primary=ta>tb?a:b;
  else if(recordRevision(a)!==recordRevision(b))primary=recordRevision(a)>recordRevision(b)?a:b;
  else primary=activityAuthorityScore(a)>=activityAuthorityScore(b)?a:b;
  const secondary=primary===a?b:a,merged={...secondary,...primary};
  const primaryAll=!!primary?.locked,secondaryAll=!!secondary?.locked;
  const primaryLogTen=String(primary?.source||'').toLowerCase()==='logten',secondaryLogTen=String(secondary?.source||'').toLowerCase()==='logten';
  const chosenSecondary=new Set(),keys=new Set([...Object.keys(primary||{}),...Object.keys(secondary||{})]);
  for(const key of keys){
    if(['id','manualFields','timeOverrides','fieldSources','_updatedAt','_syncRev'].includes(key))continue;
    const pManual=primaryAll||entryManualField(primary,key),sManual=secondaryAll||entryManualField(secondary,key);
    if(sManual&&!pManual&&Object.prototype.hasOwnProperty.call(secondary,key)){merged[key]=secondary[key];chosenSecondary.add(key)}
    else if(pManual&&!sManual)continue;
    else if(secondaryLogTen&&!primaryLogTen&&Object.prototype.hasOwnProperty.call(secondary,key)){merged[key]=secondary[key];chosenSecondary.add(key)}
    else if(primaryLogTen&&!secondaryLogTen)continue;
    else if(blankOperationalValue(merged[key])&&!blankOperationalValue(secondary?.[key]))merged[key]=secondary[key];
  }
  merged.manualFields={...(secondary?.manualFields||{}),...(primary?.manualFields||{})};
  merged.timeOverrides={...(secondary?.timeOverrides||{}),...(primary?.timeOverrides||{})};
  merged.fieldSources={...(secondary?.fieldSources||{}),...(primary?.fieldSources||{})};
  for(const key of chosenSecondary){
    if(secondary?.fieldSources&&Object.prototype.hasOwnProperty.call(secondary.fieldSources,key))merged.fieldSources[key]=secondary.fieldSources[key];
    else if(secondaryLogTen)merged.fieldSources[key]='logten';
  }
  for(const key of Object.keys(merged.manualFields))if(merged.manualFields[key]===true)merged.fieldSources[key]='manual';
  for(const key of Object.keys(merged.timeOverrides))if(merged.timeOverrides[key]===true)merged.fieldSources[key]='manual';
  merged.id=primary.id||secondary.id||makeActivityId();merged.recordKind='activity';
  if(primaryLogTen||secondaryLogTen)merged.source='logten';
  if(merged.source==='logten')merged.activityStatus=merged.locked?ACTIVITY_STATUS.COMPLETED:ACTIVITY_STATUS.OPEN;
  else merged.activityStatus=activityStatusRank(a.activityStatus)>=activityStatusRank(b.activityStatus)?a.activityStatus:b.activityStatus;
  return normalizeCoreActivity(merged,merged.activityCategory==='DUTY'?'duties':'entries');
}
function normalizeCoreActivityRecords(rows){
  // SAFETY INVARIANT: normalization must never collapse two distinct Core records.
  // Only repeated representations of the exact same Core ID may be merged.
  const out=[],byId=new Map();
  for(const raw of (rows||[])){
    if(!raw)continue;
    const row=normalizeCoreActivity(raw,raw.activityCategory==='DUTY'?'duties':'');
    const id=String(row.id||'');
    if(id&&byId.has(id)){
      const index=byId.get(id);
      out[index]=mergeCoreActivities(out[index],row);
    }else{
      const index=out.length;out.push(row);if(id)byId.set(id,index);
    }
  }
  return out;
}
function migrateSnapshotToOperationalDatabase(snapshot={}){
  if(isOperationalDatabase(snapshot?.database))return normalizeOperationalDatabase(snapshot.database);
  if(isOperationalDatabase(snapshot))return normalizeOperationalDatabase(snapshot);
  if(Array.isArray(snapshot?.databaseRecords)){
    const values=Array.isArray(snapshot.databaseValues)?Object.fromEntries(snapshot.databaseValues.filter(x=>x&&typeof x.key==='string').map(x=>[x.key,x.value])):(snapshot.databaseValues||{});
    return normalizeOperationalDatabase({schema:snapshot.databaseMeta?.schema||'PilotLog Operational Database',schemaVersion:Number(snapshot.databaseMeta?.schemaVersion)||1,records:snapshot.databaseRecords,values,updatedAt:snapshot.databaseMeta?.updatedAt||new Date().toISOString()});
  }
  const db=emptyOperationalDatabase(),records=[];
  for(const source of (Array.isArray(snapshot.flights)?snapshot.flights:[])){
    records.push(normalizeCoreActivity(source,'entries'));
  }
  for(const source of (Array.isArray(snapshot.roster)?snapshot.roster:[])){
    const roster=normalizeCoreActivity({...source,dutyType:'Flight',activityStatus:source.status||ACTIVITY_STATUS.PLANNED},'roster');
    records.push(roster);
  }
  for(const source of (Array.isArray(snapshot.duties)?snapshot.duties:[])){
    const duty=normalizeCoreActivity({...source,activityCategory:'OPERATIONAL'},'duties');
    const i=records.findIndex(x=>x.id===duty.id||(x.date===duty.date&&rawCanonicalType(x.dutyType,'Duty')===duty.dutyType&&(x.onDuty||'')===(duty.onDuty||'')&&(x.offDuty||'')===(duty.offDuty||'')));
    if(i>=0)records[i]=mergeCoreActivities(records[i],duty);else records.push(duty);
  }
  db.records=normalizeCoreActivityRecords(records);
  db.values={
    [TRIPS_KEY]:Array.isArray(snapshot.trips)?snapshot.trips:[],
    [EXPIRY_KEY]:Array.isArray(snapshot.expiry)?snapshot.expiry:[],
    [SYNC_LEDGER_KEY]:Array.isArray(snapshot.syncLedger)?snapshot.syncLedger:[],
    [APP_SETTINGS_KEY]:snapshot.appSettings||{},
    [PAY_SETTINGS_KEY]:snapshot.paySettings||{},
    [PAY_MONTH_KEY]:snapshot.payrollExtras||snapshot.payMonth||{},
    [FX_KEY]:snapshot.fx||{},
    [LOGTEN_ARCHIVE_META_KEY]:snapshot.logTenArchiveMeta||{},
    [AEROLINE_CONFIG_KEY]:snapshot.aerolineConfig||{},
    [ENTRY_DRAFTS_KEY]:Array.isArray(snapshot.drafts)?snapshot.drafts:[],
    [ENTRY_DRAFT_KEY]:snapshot.activeDraft||null
  };
  return normalizeOperationalDatabase(db);
}
function legacySnapshot(flights){
  return{flights:Array.isArray(flights)&&flights.length?flights:legacyLocalFlights(),roster:legacyLocalJson(LEGACY_ROSTER_STORAGE_KEY,[]),duties:legacyLocalJson(LEGACY_DUTIES_STORAGE_KEY,[]),trips:legacyLocalJson(TRIPS_KEY,[]),expiry:legacyLocalJson(EXPIRY_KEY,[]),syncLedger:legacyLocalJson(SYNC_LEDGER_KEY,[]),appSettings:legacyLocalJson(APP_SETTINGS_KEY,{}),paySettings:legacyLocalJson(PAY_SETTINGS_KEY,{}),payMonth:legacyLocalJson(PAY_MONTH_KEY,{}),fx:legacyLocalJson(FX_KEY,{}),logTenArchiveMeta:legacyLocalJson(LOGTEN_ARCHIVE_META_KEY,{}),aerolineConfig:legacyLocalJson(AEROLINE_CONFIG_KEY,{}),drafts:legacyLocalJson(ENTRY_DRAFTS_KEY,[]),activeDraft:legacyLocalJson(ENTRY_DRAFT_KEY,null)};
}
async function initFlightStore(){
  try{
    const stored=await readFlightStore(),fallback=legacyLocalJson(FALLBACK_DB_KEY,null);
    if(stored!=null&&!isOperationalDatabase(stored))throw Object.assign(Error('Incompatible local database format'),{incompatible:true});
    if(isOperationalDatabase(stored)){
      operationalDatabase=stored;
      flightStoreReady=true;
      return operationalDatabase;
    }
    else if(isOperationalDatabase(fallback))operationalDatabase=normalizeOperationalDatabase(fallback);
    else operationalDatabase=migrateSnapshotToOperationalDatabase(legacySnapshot(await readLegacyFlightStore()));
    reconcileOperationalFlightIdentities();
    await writeFlightStore(operationalDatabase);
    flightStoreReady=true;
    try{localStorage.removeItem(FALLBACK_DB_KEY)}catch{}
    try{if(navigator.storage?.persist)navigator.storage.persist().catch(()=>{})}catch{}
  }catch(e){
    if(e.incompatible||window.PilotLogNative?.isNative||operationalDatabase)throw e;
    console.warn('IndexedDB operational database unavailable; using one local fallback database',e);
    const savedFallback=legacyLocalJson(FALLBACK_DB_KEY,null);
    if(!isOperationalDatabase(savedFallback))throw e;
    operationalDatabase=savedFallback;flightStoreReady=false;
  }
  return operationalDatabase;
}
function queueFlightPersist(){
  flightPersistDirty=true;
  if(!flightStoreReady)return Promise.resolve(false);
  if(flightPersistTimer){clearTimeout(flightPersistTimer);flightPersistTimer=null}
  if(flightPersistActive)return flightPersistPromise;
  flightPersistActive=true;
  flightPersistPromise=(async()=>{
    try{while(flightPersistDirty){flightPersistDirty=false;try{await writeFlightStore(operationalDatabase,{verifiedSnapshot:true})}catch(error){flightPersistDirty=true;throw error}}}
    finally{flightPersistActive=false}
    return true;
  })();
  return flightPersistPromise;
}
function scheduleFlightPersist(delay=0){
  flightPersistDirty=true;
  if(!flightStoreReady){if(operationalDatabase)localStorage.setItem(FALLBACK_DB_KEY,JSON.stringify(operationalDatabase));return false}
  if(flightPersistTimer)return true;
  flightPersistTimer=setTimeout(()=>{flightPersistTimer=null;queueFlightPersist().catch(e=>console.error('PilotLog IndexedDB write failed',e))},delay);
  return true;
}
async function flushFlightStore(){if(!flightStoreReady)return true;if(!flightPersistDirty&&!flightPersistTimer)return flightPersistActive?flightPersistPromise:true;if(flightPersistTimer){clearTimeout(flightPersistTimer);flightPersistTimer=null}await queueFlightPersist();return true}
// Display projections can reuse immutable copies. Editors still receive independent mutable copies.
const activityDisplayCopies=new WeakMap();
function immutableDisplayCopy(value){
  if(!value||typeof value!=='object')return value;
  if(Array.isArray(value))return Object.freeze(value.map(immutableDisplayCopy));
  return Object.freeze(Object.fromEntries(Object.entries(value).map(([key,item])=>[key,immutableDisplayCopy(item)])));
}
function displayActivityRows(){
  return (operationalDatabase?.records||[]).filter(isEntryActivityRecord).map(row=>{
    let view=activityDisplayCopies.get(row);
    if(!view){view=immutableDisplayCopy(row);activityDisplayCopies.set(row,view)}
    return view;
  });
}
function coreActivityRows(){return(operationalDatabase?.records||[]).filter(x=>x?.recordKind==='activity').map(copyJson)}
function isEntryActivityRecord(x){return x?.recordKind==='activity'}
function isDutyActivityRecord(x){return false}
function entryActivityRows(){return coreActivityRows().filter(isEntryActivityRecord)}
function rosterFlightRows(rows=coreActivityRows()){return rows.filter(x=>rawCanonicalType(x.dutyType)==='Flight').map(x=>({...x,std:x.schedOut||'',sta:x.schedIn||'',status:activityIncludedInResults(x)?'done':'planned'}))}
function dutyActivityRows(rows=coreActivityRows()){return rows.filter(x=>['Day OFF','Flight Duty','Duty','Sick Leave','Annual Leave'].includes(canonicalDutyType(x.dutyType||'','Duty'))).map(x=>({...x,type:x.dutyType||'Duty',report:x.onDuty||'',end:x.offDuty||'',minutes:entryDutyMins(x),notes:x.remarks||''}))}
function commitCoreActivities(rows){
  if(!operationalDatabase)operationalDatabase=emptyOperationalDatabase();
  const prior=new Map(operationalDatabase.records.map(r=>[r.id,r]));
  const next=normalizeCoreActivityRecords(Array.isArray(rows)?rows:[]).map(row=>{
    const old=prior.get(row.id);
    if(old&&(storedHash(old)===contentHash(row)||contentHash(normalizeCoreActivity(old))===contentHash(row)))return old;
    return {...row,_syncRev:old?recordRevision(old)+1:Math.max(1,recordRevision(row)),_updatedAt:new Date().toISOString()};
  });
  if(next.length===prior.size&&next.every(r=>prior.get(r.id)===r))return false;
  operationalDatabase.records=next;
  return touchOperationalDatabase();
}
function replaceActivitySelection(predicate,rows){
  // SAFETY INVARIANT: replacement/import may update supplied IDs but may not delete
  // existing records merely because they are absent from the incoming selection.
  const existing=coreActivityRows(),incoming=(rows||[]).map(x=>normalizeCoreActivity(x,isDutyActivityRecord(x)?'duties':'')),incomingIds=new Set(incoming.map(x=>String(x.id||'')).filter(Boolean));
  const kept=existing.filter(x=>!predicate(x)||!incomingIds.has(String(x.id||'')));
  return commitCoreActivities([...kept,...incoming]);
}
function upsertCoreActivity(source){
  const row=normalizeCoreActivity(source,isDutyActivityRecord(source)?'duties':''),records=coreActivityRows(),i=records.findIndex(x=>x.id===row.id);
  if(i>=0)records[i]=normalizeCoreActivity({...records[i],...row,id:records[i].id},isDutyActivityRecord(row)?'duties':'');else records.push(row);
  const refreshed=refreshProvisionalDutyRecords(records,row.id);
  commitCoreActivities(refreshed);
  refreshed.filter(r=>r.id!==row.id&&recordRevision(r)>recordRevision(records.find(previous=>previous.id===r.id))).forEach(r=>markCloudEdited('activities',r,'duty-recalculate'));
  return coreActivityRows().find(x=>x.id===row.id)||row;
}
function removeCoreActivities(predicate,options={}){if(options.manual!==true)throw new Error('PilotLog safety block: Core records can be removed only by an explicit manual user action.');return commitCoreActivities(coreActivityRows().filter(x=>!predicate(x)))}
async function persistCoreActivities(){await flushFlightStore();return true}
async function replaceEntryActivitiesDurable(rows){replaceActivitySelection(isEntryActivityRecord,(rows||[]).map(x=>({...x,activityCategory:'OPERATIONAL'})));await persistCoreActivities();return true}
function databaseValue(key,def){const v=operationalDatabase?.values?.[key];return v===undefined?def:v}
const load=k=>{if(BUSINESS_KEYS.has(k)){const v=databaseValue(k,[]);return Array.isArray(v)?copyJson(v):[]}try{return JSON.parse(localStorage.getItem(k)||'[]')}catch{return[]}};
function touchOperationalDatabase(){if(importStaging)return true;markLocalRevision();if(!operationalDatabase)operationalDatabase=emptyOperationalDatabase();operationalDatabase.updatedAt=new Date().toISOString();if(flightStoreReady)scheduleFlightPersist();else try{localStorage.setItem(FALLBACK_DB_KEY,JSON.stringify(operationalDatabase))}catch{};return true}
const AUTHORITATIVE_ENTRY_FIELDS=['date','flightNo','dep','arr','type','variant','reg','location','schedOut','schedIn','schedBlock','out','off','on','in','block','totalFlightTime','multiPilot','pic','picUS','sic','ifrTime','airTime','night','picNight','totalInstrument','simulatorTime','simulatorTimeImported','sfiSfe','dualGiven','dualReceived','groundInstruction','stbyTime','credit','totalDuty','timeOverrides','manualFields','fieldSources','rolePic','instruction','examiner','simInstructor','simExaminer','rhs','pf','callFromDayOff','role','seatPosition','trainerType','aerolineTrainerType','approachType','approaches','picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name','cabinCrew','observerCrew','studentName','remarks','courseType','delayMinutes','pax','fuelAboard','fuelBurned','fuelMinimum','dayTakeoffs','nightTakeoffs','dayLandings','nightLandings','onDuty','offDuty','sectors','locked','manualOverride','creditImported','sim','logtenCustom','customNote','goArounds'];
function setLocalJson(k,v){
  if(BUSINESS_KEYS.has(k)){if(!operationalDatabase)operationalDatabase=emptyOperationalDatabase();const old=operationalDatabase.values[k];
    if([TRIPS_KEY,EXPIRY_KEY].includes(k)&&Array.isArray(old)){
      const byId=new Map(old.map(row=>[row.id,row]));
      v=v.map(row=>{const prev=byId.get(row.id);return prev&&contentHash(prev)===contentHash(row)?prev:row});
    }
    if(contentHash(old)===contentHash(v)||old==null&&v&&typeof v==='object'&&!Object.keys(v).length)return false;
    operationalDatabase.values[k]=copyJson(v);return touchOperationalDatabase()}
  const text=JSON.stringify(v);
  try{localStorage.setItem(k,text);return true}
  catch(e){
    const quota=e?.name==='QuotaExceededError'||e?.name==='NS_ERROR_DOM_QUOTA_REACHED'||e?.code===22||e?.code===1014;
    
    throw e;
  }
}
const save=(k,v)=>{
  return setLocalJson(k,v);
};
const loadObject=(k,def={})=>{if(BUSINESS_KEYS.has(k)){const v=databaseValue(k,def);return v&&typeof v==='object'&&!Array.isArray(v)?copyJson(v):def}try{const v=JSON.parse(localStorage.getItem(k)||'null');return v&&typeof v==='object'&&!Array.isArray(v)?copyJson(v):def}catch{return def}};
function saveObject(k,v){return setLocalJson(k,v&&typeof v==='object'?v:{})}
function mergeOperationalFlightPair(entry,roster){
  return mergeCoreActivities(normalizeCoreActivity(entry,'entries'),normalizeCoreActivity(roster,'roster'));
}
function reconcileOperationalFlightIdentities(){
  if(!operationalDatabase?.records)return{merged:0,reviews:0};
  let records=[...operationalDatabase.records],merged=0;
  const normalized=normalizeCoreActivityRecords(records);merged=Math.max(0,records.length-normalized.length);records=normalized;
  const prior=Array.isArray(operationalDatabase.values?.[RECONCILIATION_REVIEW_KEY])?operationalDatabase.values[RECONCILIATION_REVIEW_KEY]:[];
  const unique=prior.filter(row=>row?.kind!=='ambiguous-flight-link');
  if(merged||JSON.stringify(prior)!==JSON.stringify(unique)){operationalDatabase.values[RECONCILIATION_REVIEW_KEY]=unique;commitCoreActivities(records)}
  return{merged,reviews:0};
}
function removeBusinessValue(k){if(!BUSINESS_KEYS.has(k)){localStorage.removeItem(k);return}return setLocalJson(k,BUSINESS_ARRAY_KEYS.has(k)?[]:null)}
async function replaceOperationalDatabase(database){const currentReplica=operationalDatabase?.replica162;
  operationalDatabase=normalizeOperationalDatabase(database);delete operationalDatabase.replica162;delete operationalDatabase.preSync162;
  if(currentReplica)operationalDatabase.replica162=currentReplica;
  touchOperationalDatabase();await flushFlightStore();return operationalDatabase}
function operationalDatabaseCounts(database=operationalDatabase){
  const records=database?.records||[],values=database?.values||{};
  return{activities:records.length,entries:records.filter(x=>activityIncludedInResults(x)&&(rawCanonicalType(x.dutyType)==='Flight'||rawCanonicalType(x.dutyType)==='Simulator')).length,roster:records.filter(x=>rawCanonicalType(x.dutyType)==='Flight').length,duties:records.filter(x=>String(x.activityCategory||'').toUpperCase()==='DUTY').length,trips:Array.isArray(values[TRIPS_KEY])?values[TRIPS_KEY].length:0,expiry:Array.isArray(values[EXPIRY_KEY])?values[EXPIRY_KEY].length:0};
}
const recordRevision=o=>Math.max(0,Number(o?._syncRev)||0);
const stamp=o=>({...o,_syncRev:recordRevision(o)+1,_updatedAt:new Date().toISOString()});
const today=()=>new Date().toISOString().slice(0,10);
function makeId(){
  if(globalThis.crypto?.randomUUID)return `pl-${crypto.randomUUID()}`;
  const a=new Uint32Array(4);if(globalThis.crypto?.getRandomValues)crypto.getRandomValues(a);else for(let i=0;i<4;i++)a[i]=(Math.random()*0xffffffff)>>>0;
  return `pl-${[...a].map(v=>v.toString(16).padStart(8,'0')).join('-')}`;
}
function makeActivityId(){
  const alphabet='0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  for(let attempt=0;attempt<8;attempt++){
    const bytes=new Uint8Array(10);if(globalThis.crypto?.getRandomValues)crypto.getRandomValues(bytes);else for(let i=0;i<bytes.length;i++)bytes[i]=(Math.random()*256)|0;
    const id=[...bytes].map(v=>alphabet[v&31]).join('');
    if(!(operationalDatabase?.records||[]).some(x=>x?.id===id))return id;
  }
  throw new Error('Unable to create a unique Core activity ID.');
}
function stableHash(value){let h=2166136261;const text=String(value||'');for(let i=0;i<text.length;i++){h^=text.charCodeAt(i);h=Math.imul(h,16777619)}return (h>>>0).toString(36)}
function stableHash128(value){
  const s=String(value||'');let h1=1779033703,h2=3144134277,h3=1013904242,h4=2773480762;
  for(let i=0,k;i<s.length;i++){k=s.charCodeAt(i);h1=h2^Math.imul(h1^k,597399067);h2=h3^Math.imul(h2^k,2869860233);h3=h4^Math.imul(h3^k,951274213);h4=h1^Math.imul(h4^k,2716044179)}
  h1=Math.imul(h3^(h1>>>18),597399067);h2=Math.imul(h4^(h2>>>22),2869860233);h3=Math.imul(h1^(h3>>>17),951274213);h4=Math.imul(h2^(h4>>>19),2716044179);
  const out=[(h1^h2^h3^h4)>>>0,(h2^h1)>>>0,(h3^h1)>>>0,(h4^h1)>>>0];return out.map(v=>v.toString(16).padStart(8,'0')).join('');
}
function stableSourceId(kind,sourceKey){return `src-${String(kind||'item')}-${stableHash128(`${kind}|${sourceKey}`)}`}
function importCoreId(sourceKey){
  if(!sourceKey)return makeActivityId();
  const alphabet='0123456789ABCDEFGHJKMNPQRSTVWXYZ';
  const bytes=stableHash128(sourceKey).match(/../g).slice(0,10);
  const id=bytes.map(b=>alphabet[parseInt(b,16)&31]).join('');
  const prior=operationalDatabase.records.find(r=>r.id===id);
  if(prior&&syncSourceKey(prior)!==sourceKey)throw Error('Import Core ID collision. Source data retained.');
  return id;
}
function syncDeviceId(){let id=localStorage.getItem(SYNC_DEVICE_KEY);if(!id){id=`dev-${stableHash(`${Date.now()}-${Math.random()}-${navigator.userAgent||''}`)}-${Date.now().toString(36)}`;try{localStorage.setItem(SYNC_DEVICE_KEY,id)}catch{}}return id||'device'}
function syncSourceKey(o){
  if(!o||typeof o!=='object')return'';
  if(o.source==='aeroline'&&o.aerolineKey)return `aeroline:${String(o.aerolineKey)}`;
  if(o.source==='logten'&&o.logtenUniqueId)return `logten:${String(o.logtenUniqueId)}`;
  if(o.source==='logten'&&o.sourceRowKey)return `logten-row:${String(o.sourceRowKey)}`;
  if(o.source==='calendar'&&o.calendarUid)return `calendar:${String(o.calendarUid)}`;
  if(o.source==='roster'&&o.sourceRowKey)return `roster:${String(o.sourceRowKey)}`;
  return'';
}
const mins=t=>{if(!t)return null;const [h,m]=String(t).split(':').map(Number);return Number.isFinite(h)&&Number.isFinite(m)?h*60+m:null};
const diff=(a,b)=>{if(a==null||b==null)return 0;let d=b-a;if(d<0)d+=1440;return d};
const timeSpanMins=(start,end)=>diff(mins(start),mins(end));
const fmt=m=>`${Math.floor(Math.max(0,Number(m)||0)/60)}:${String(Math.round(Math.max(0,Number(m)||0))%60).padStart(2,'0')}`;
const durationPickerValue=m=>{const n=Math.max(0,Math.round(Number(m)||0));return `${String(Math.floor(n/60)).padStart(2,'0')}:${String(n%60).padStart(2,'0')}`};
function setDurationPicker(el,value){if(!el)return;const n=numericTime(value);el.value=n===null?'':durationPickerValue(n)}
const PILOTLOG_RULES=Object.freeze({
  aliases:Object.freeze({
    'FLIGHT':'Flight','SIM':'Simulator','SIMULATOR':'Simulator','RT':'Simulator',
    'GROUND COURSE':'Ground Course','GROUND TRAINING':'Ground Course','GROUND TRAINING/COURSE':'Ground Course','GRT':'Ground Course',
    'DHD':'DHD','DHP':'DHP','STBY':'STBY','STANDBY':'STBY','HSBY':'STBY',
    'DAY OFF':'Day OFF','OFF':'Day OFF','FLIGHT DUTY':'Flight Duty','DUTY':'Duty','SICK LEAVE':'Sick Leave','ANNUAL LEAVE':'Annual Leave'
  }),
  activities:Object.freeze({
    'Flight':Object.freeze({editor:'flight',credit:'flight-schedule',trip:'duty',totals:'flight',rosterEditLocked:false}),
    'Simulator':Object.freeze({editor:'entry',credit:'editable-sim',trip:'duty',totals:'simulator',rosterEditLocked:true}),
    'Ground Course':Object.freeze({editor:'entry',credit:'editable-ground',trip:'fixed-ground',totals:'other',rosterEditLocked:true}),
    'DHD':Object.freeze({editor:'entry',credit:'editable',trip:'zero',totals:'other',rosterEditLocked:true}),
    'DHP':Object.freeze({editor:'entry',credit:'editable',trip:'credit',totals:'other',rosterEditLocked:true}),
    'STBY':Object.freeze({editor:'entry',credit:'stored',trip:'zero',totals:'other',rosterEditLocked:true}),
    'Sick Leave':Object.freeze({editor:'entry',credit:'zero',trip:'zero',totals:'other',rosterEditLocked:true}),
    'Annual Leave':Object.freeze({editor:'entry',credit:'zero',trip:'zero',totals:'other',rosterEditLocked:true}),
    'Day OFF':Object.freeze({editor:'entry',credit:'zero',trip:'zero',totals:'other',rosterEditLocked:true}),
    'Flight Duty':Object.freeze({editor:'entry',credit:'zero',trip:'duty',totals:'other',rosterEditLocked:true}),
    'Duty':Object.freeze({editor:'entry',credit:'zero',trip:'duty',totals:'other',rosterEditLocked:true})
  }),
  fixed:Object.freeze({groundTripDutyMins:300})
});
function canonicalDutyType(value,fallback='Flight'){
  const raw=String(value??'').trim();if(!raw)return fallback;
  const key=upper(raw).replace(/\s+/g,' ');
  return PILOTLOG_RULES.aliases[key]||raw;
}
function activityRule(value){
  const raw=value&&typeof value==='object'?(value.dutyType||value.type||''):value;
  const type=canonicalDutyType(raw,'Flight');
  return {type,...(PILOTLOG_RULES.activities[type]||{editor:'entry',credit:'stored',trip:'duty',totals:'other',rosterEditLocked:true})};
}
function rosterCanEditLockedActivity(value){const r=activityRule(value);return r.editor==='entry'&&r.rosterEditLocked===true}
const MOROCCO_IATA=new Set(['CMN','RAK','FEZ','TNG','RBA','OUD','AGA','NDR','ESU','OZZ','ERH','EUN','TTU','UAR','BEM']);
function isMoroccoCode(code){
  code=upper(code);
  if(MOROCCO_IATA.has(code))return true;
  const a=airportIndex?.[code];
  return !!(a&&(upper(a.country)==='MA'||a.tz==='Africa/Casablanca'||upper(a.icao||'').startsWith('GM')));
}
function moroccoLocalScheduledMinute(f){
  if(!f?.date||!f?.schedOut||!isMoroccoCode(f.dep))return null;
  const d=zuluDate(f.date,f.schedOut);
  if(!d)return null;
  return tzParts(d,'Africa/Casablanca');
}
function qualifiesMoroccoNightCredit(f){
  if(!isFlight(f)||!f.schedOut||!isMoroccoCode(f.dep))return false;
  const m=moroccoLocalScheduledMinute(f);
  if(m===null)return false;
  const st=appSettings(),start=mins(st.nightStart||'18:00'),end=mins(st.nightEnd||'05:00');
  if(start===null||end===null||start===end)return false;
  return start<end?m>=start&&m<end:m>=start||m<end;
}
function moroccoNightTrigger(f){
  // Reuse the existing, tested Morocco scheduled-departure helper.
  return qualifiesMoroccoNightCredit(f);
}
function sameFlightIdentity(a,b){
  return (a?.id&&b?.id&&a.id===b.id) ||
    (upper(a?.flightNo||'')===upper(b?.flightNo||'') &&
     upper(a?.dep||'')===upper(b?.dep||'') &&
     upper(a?.arr||'')===upper(b?.arr||'') &&
     String(a?.schedOut||'')===String(b?.schedOut||''));
}
function dutyGetsMoroccoNightPremium(f,allEntries=null){
  if(!isFlight(f)||!f.date)return false;
  if(isMoroccoCode(f.dep))return moroccoNightTrigger(f);

  const source=Array.isArray(allEntries)?allEntries:dayEntries(f.date);
  const flights=(source||[]).filter(isFlight).filter(x=>x.date===f.date).map(x=>({...x}));
  if(!flights.some(x=>sameFlightIdentity(x,f)))flights.push({...f});
  flights.sort((a,b)=>{
    const da=zuluDate(a.date,a.schedOut||a.out||'00:00')?.getTime()||0;
    const db=zuluDate(b.date,b.schedOut||b.out||'00:00')?.getTime()||0;
    return da-db;
  });
  const idx=flights.findIndex(x=>sameFlightIdentity(x,f));
  if(idx<=0)return false;
  const previous=flights[idx-1];
  return isMoroccoCode(previous.dep)&&moroccoNightTrigger(previous);
}
function flightCreditMins(f,allEntries=null){
  const base=scheduleBlockMins(f);
  if(!(base>0))return 0;
  const rounded=Math.ceil(base/30)*30;
  const premium=Math.max(0,Number(appSettings().nightPremiumPct??50));
  return dutyGetsMoroccoNightPremium(f,allEntries)?Math.round(rounded*(1+premium/100)):rounded;
}
const shiftTime=(t,delta)=>{const m=mins(t);if(m==null)return'';let x=(m+delta)%1440;if(x<0)x+=1440;return `${String(Math.floor(x/60)).padStart(2,'0')}:${String(x%60).padStart(2,'0')}`};
function signedMinuteDelta(from,to){const a=mins(from),b=mins(to);if(a==null||b==null)return 0;let d=b-a;if(d>720)d-=1440;if(d<-720)d+=1440;return d}
function formDelayMinutes(){if($('dutyTypeFlight')?.value!=='Flight')return 0;return Math.max(0,signedMinuteDelta($('schedOut')?.value,$('out')?.value))}
function updateDelayUI(){return formDelayMinutes()}
function syncOnDutyFromScheduleOut(){if(controlledDuty(coreActivityRows().find(f=>f.id===$('editId')?.value)))return;if(formTimeOverrides.onDuty||['aeroline','logten','calendar','screenshot','source','imported'].includes(String(formFieldSources.onDuty||'').toLowerCase()))return;const t=$('schedOut')?.value||'',positioning=isPositioning({dutyType:$('dutyTypeFlight')?.value||''});$('onDuty').value=t?(positioning?t:shiftTime(t,-60)):'';formFieldSources.onDuty=t?'calculated':''}
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]));
const csv=s=>`"${String(s??'').replace(/"/g,'""')}"`;
const dateOnly=d=>new Date(`${d}T00:00:00Z`);


const entrySuggestionDb={crew:[],types:[]};
function refreshEntrySuggestions(){
  const fs=entryActivityRows(),names=new Set(),types=new Set();
  fs.forEach(f=>{
    [...CREW_NAME_FIELDS.map(key=>f[key]),...(Array.isArray(f.cabinCrew)?f.cabinCrew:[]),...(Array.isArray(f.observerCrew)?f.observerCrew:[]),f.studentName].forEach(n=>{const v=upper(n||'').trim();if(v)names.add(v)});
    const t=upper(f.type||'').trim();if(t&&t!=='UNSPECIFIED')types.add(t);
  });
  const selfName=upper(appSettings().profileName||'');
  if(selfName)names.add(selfName);
  entrySuggestionDb.crew=[...names].sort();
  entrySuggestionDb.types=[...types].sort();
  if($('crewNameSuggestions'))$('crewNameSuggestions').innerHTML=entrySuggestionDb.crew.map(v=>`<option value="${esc(v)}"></option>`).join('');
  if($('aircraftTypeSuggestions'))$('aircraftTypeSuggestions').innerHTML=entrySuggestionDb.types.map(v=>`<option value="${esc(v)}"></option>`).join('');
}
function setupSmartAutocomplete(inputId,getValues){
  const input=$(inputId);if(!input)return;
  const host=input.parentElement;host.classList.add('autocomplete-host');
  let menu=host.querySelector('.smart-autocomplete');
  if(!menu){menu=document.createElement('div');menu.className='smart-autocomplete hidden';host.appendChild(menu)}
  const hide=()=>menu.classList.add('hidden');
  const render=()=>{
    const q=upper(input.value||'');
    const vals=(getValues()||[]).filter(v=>!q||v.startsWith(q)||v.includes(q)).slice(0,8);
    if(!vals.length){hide();return}
    menu.innerHTML=vals.map(v=>`<button type="button" data-autocomplete-value="${esc(v)}">${esc(v)}</button>`).join('');
    menu.classList.remove('hidden');
  };
  input.addEventListener('focus',()=>{if(CREW_NAME_FIELDS.includes(inputId)||inputId==='expiryEndorsedBy')refreshEntrySuggestions();render()});input.addEventListener('input',render);
  menu.addEventListener('mousedown',e=>e.preventDefault());
  menu.addEventListener('click',e=>{const b=e.target.closest('[data-autocomplete-value]');if(!b)return;input.value=b.dataset.autocompleteValue;hide();input.dispatchEvent(new Event('input',{bubbles:true}))});
  input.addEventListener('blur',()=>setTimeout(hide,120));
}

function displayDate(v){
  const s=String(v||'').slice(0,10);
  const m=s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m?`${m[3]}/${m[2]}/${m[1].slice(2)}`:s;
}
function displayDateTime(v){
  const s=String(v||'');
  if(!s)return'';
  const parts=s.replace('T',' ').split(' ');
  return `${displayDate(parts[0])}${parts[1]?' '+parts[1].slice(0,5):''}`;
}

const sum=(a,f)=>a.reduce((x,y)=>x+(Number(f(y))||0),0);
const upper=s=>String(s||'').trim().toUpperCase();
const durMins=v=>{v=String(v||'').trim();const m=v.match(/^(\d+):(\d{1,2})$/);return m?Number(m[1])*60+Number(m[2]):0};
const scheduleBlockMins=f=>{
  if(f?.schedOut&&f?.schedIn)return timeSpanMins(f.schedOut,f.schedIn);
  return 0;
};
function isFlight(f){return f?.sim!=='yes'&&canonicalDutyType(f?.dutyType||'Flight')==='Flight'}
function sourceLabel(source){
  const s=String(source||'manual').toLowerCase();
  return ({aeroline:'AeroLINE',logten:'LogTen',calendar:'Calendar',roster:'Roster',manual:'Manual','other_company':'Other Company',simulator:'Simulator'})[s]||String(source||'Manual');
}
function setEntrySourceDisplay(source){const el=$('entrySourceDisplay');if(el)el.value=sourceLabel(source)}

function showImportProgress(title='Importing data…'){
  const modal=$('importProgressModal');if(!modal)return;
  $('importProgressTitle').textContent=title;$('importProgressBar').style.width='0%';$('importProgressPercent').textContent='0%';$('importProgressDetail').textContent='Preparing…';modal.classList.remove('hidden');
}
function setImportProgress(percent,detail=''){
  const p=Math.max(0,Math.min(100,Math.round(Number(percent)||0)));if($('importProgressBar'))$('importProgressBar').style.width=`${p}%`;if($('importProgressPercent'))$('importProgressPercent').textContent=`${p}%`;if($('importProgressDetail')&&detail)$('importProgressDetail').textContent=detail;
}
function hideImportProgress(){const modal=$('importProgressModal');if(modal)modal.classList.add('hidden')}
function yieldToUi(){return new Promise(resolve=>setTimeout(resolve,0))}
function normalizedFlightKey(v){return cleanPrefix(composeFlightNo(v||''))}
function normalizedFlightDigits(v){
  const s=upper(v||'').replace(/\s+/g,'');
  const m=s.match(/(\d{2,4})$/);
  return m?m[1]:'';
}
function entryCompleteness(f){
  const weighted=[
    ['flightNo',5],['type',3],['reg',3],['dep',3],['arr',3],
    ['schedOut',4],['schedIn',4],['out',3],['off',3],['on',3],['in',3],
    ['onDuty',2],['offDuty',2],['block',4],['airTime',4],['credit',2],
    ['night',2],['role',1],['remarks',1],
    ['dayTakeoffs',1],['nightTakeoffs',1],['dayLandings',1],['nightLandings',1]
  ];
  return weighted.reduce((sum,[k,w])=>{
    const v=f?.[k];
    const present=v!==undefined&&v!==null&&String(v).trim()!=='';
    return sum+(present?w:0);
  },0)+(f?.locked?2:0);
}
function mergeDuplicateEntries(a,b){
  const ca=entryCompleteness(a),cb=entryCompleteness(b),primary=ca>=cb?a:b,secondary=primary===a?b:a;
  const merged={...secondary,...primary},primaryAll=!!primary?.locked,secondaryAll=!!secondary?.locked;
  const primaryLogTen=String(primary?.source||'').toLowerCase()==='logten',secondaryLogTen=String(secondary?.source||'').toLowerCase()==='logten';
  const chosenSecondary=new Set();
  for(const k of new Set([...Object.keys(primary||{}),...Object.keys(secondary||{})])){
    if(['id','manualFields','timeOverrides','fieldSources','_updatedAt','_syncRev'].includes(k))continue;
    const pManual=primaryAll||entryManualField(primary,k),sManual=secondaryAll||entryManualField(secondary,k);
    if(sManual&&!pManual&&Object.prototype.hasOwnProperty.call(secondary,k)){merged[k]=secondary[k];chosenSecondary.add(k)}
    else if(pManual&&!sManual)continue;
    else if(secondaryLogTen&&!primaryLogTen&&Object.prototype.hasOwnProperty.call(secondary,k)){merged[k]=secondary[k];chosenSecondary.add(k)}
    else if(primaryLogTen&&!secondaryLogTen)continue;
    else if((merged[k]===undefined||merged[k]===null||merged[k]==='')&&secondary[k]!==undefined&&secondary[k]!==null&&secondary[k]!=='')merged[k]=secondary[k];
  }
  merged.manualFields={...(secondary?.manualFields||{}),...(primary?.manualFields||{})};
  merged.timeOverrides={...(secondary?.timeOverrides||{}),...(primary?.timeOverrides||{})};
  merged.fieldSources={...(secondary?.fieldSources||{}),...(primary?.fieldSources||{})};
  for(const k of chosenSecondary){
    if(secondary?.fieldSources&&Object.prototype.hasOwnProperty.call(secondary.fieldSources,k))merged.fieldSources[k]=secondary.fieldSources[k];
    else if(secondaryLogTen)merged.fieldSources[k]='logten';
  }
  for(const k of Object.keys(merged.manualFields))if(merged.manualFields[k]===true)merged.fieldSources[k]='manual';
  for(const k of Object.keys(merged.timeOverrides))if(merged.timeOverrides[k]===true)merged.fieldSources[k]='manual';
  merged.id=primary.id||secondary.id||makeActivityId();merged.locked=!!(a.locked||b.locked);
  if(primaryLogTen||secondaryLogTen){merged.source='logten';merged.activityStatus=merged.locked?ACTIVITY_STATUS.COMPLETED:ACTIVITY_STATUS.OPEN}
  merged._syncRev=Math.max(recordRevision(a),recordRevision(b));merged._updatedAt=[a._updatedAt,b._updatedAt].filter(Boolean).sort().pop()||new Date().toISOString();return merged;
}
function exactOperationalMatch(a,b){
  if(!a||!b||a===b)return false;
  if(a.logtenUniqueId&&b.logtenUniqueId&&String(a.logtenUniqueId)!==String(b.logtenUniqueId))return false;
  if(a.date!==b.date||canonicalDutyType(a.dutyType||'Flight')!==canonicalDutyType(b.dutyType||'Flight'))return false;
  if(upper(a.dep||'')!==upper(b.dep||'')||upper(a.arr||'')!==upper(b.arr||''))return false;
  if(isFlight(a)&&isFlight(b)){
    const fa=normalizedFlightDigits(a.flightNo),fb=normalizedFlightDigits(b.flightNo);
    if(fa&&fb&&fa!==fb)return false;
    const timesA=[a.schedOut,a.schedIn,a.out,a.off,a.on,a.in].filter(Boolean);
    const timesB=[b.schedOut,b.schedIn,b.out,b.off,b.on,b.in].filter(Boolean);
    const commonTime=timesA.some(t=>timesB.includes(t));
    if(fa&&fb&&fa===fb)return true;
    if(commonTime)return true;
    const noIdentityA=!fa&&timesA.length===0,noIdentityB=!fb&&timesB.length===0;
    if(noIdentityA&&noIdentityB&&entryCompleteness(a)<=14&&entryCompleteness(b)<=14)return true;
    return false;
  }
  if(isDhd(a)&&isDhd(b)){
    const sa=a.schedOut||a.onDuty||'',sb=b.schedOut||b.onDuty||'';
    const ea=a.schedIn||a.offDuty||'',eb=b.schedIn||b.offDuty||'';
    return (!!sa&&sa===sb)||((!sa||!sb)&&!!ea&&ea===eb);
  }
  if(isSim(a)&&isSim(b)){
    return upper(a.type||'')===upper(b.type||'')&&((a.onDuty&&a.onDuty===b.onDuty)||(a.offDuty&&a.offDuty===b.offDuty));
  }
  if(isGround(a)&&isGround(b)){
    return upper(a.dep||'')===upper(b.dep||'')&&upper(a.courseType||a.remarks||'')===upper(b.courseType||b.remarks||'')&&((a.onDuty&&a.onDuty===b.onDuty)||(!a.onDuty&&!b.onDuty));
  }
  if(isStby(a)&&isStby(b))return (a.onDuty&&a.onDuty===b.onDuty)||(a.offDuty&&a.offDuty===b.offDuty)||(!a.onDuty&&!b.onDuty&&!a.offDuty&&!b.offDuty);
  return false;
}
function dedupeFlightEntriesSemantic(entries){
  // SAFETY INVARIANT: no automatic deduplication, merge, or deletion.
  // Possible duplicates may be reviewed by the user, but records remain untouched.
  return{entries:[...(entries||[])],removed:0,removedIds:[]};
}
function isSim(f){return canonicalDutyType(f?.dutyType||'')==='Simulator'||f?.sim==='yes'}
function isLogbookEntry(f){return isFlight(f)||isSim(f)}
// LogTen's locked/open flag protects editing only. It never controls visibility,
// totals, payroll, exports, or calculation authority for imported LogTen records.
function activityIncludedInResults(f){return !!f&&(String(f.source||'').toLowerCase()==='logten'||f.activityStatus===ACTIVITY_STATUS.COMPLETED)}
function simulatorCompleted(f,now=new Date()){
  if(!isSim(f)||!f?.date)return false;
  const startTime=f.onDuty||'00:00',endTime=f.offDuty||f.onDuty||'23:59';
  const pair=dutyDateEnd(f.date,startTime,endTime),end=pair?.[1];
  if(!end||!Number.isFinite(end.getTime()))return String(f.date)<isoDateLocal(now);
  return end<=now;
}
function isCompletedLogbookEntry(f,now=new Date()){
  return f?.activityStatus==='DRAFT'||(isFlight(f)||isSim(f))&&activityIncludedInResults(f);
}
function isGround(f){return canonicalDutyType(f?.dutyType||'')==='Ground Course'}
function isDhd(f){return canonicalDutyType(f?.dutyType||'')==='DHD'}
function isDhp(f){return canonicalDutyType(f?.dutyType||'')==='DHP'}
function isPositioning(f){return isDhd(f)||isDhp(f)}
function isStby(f){return canonicalDutyType(f?.dutyType||'')==='STBY'}
function picMins(f){return timeGroupFinalValue(f,'pic')}
function sicMins(f){return timeGroupFinalValue(f,'sic')}
function flightInstrMins(f){return timeGroupFinalValue(f,'dualGiven')}
function simInstrMins(f){return timeGroupFinalValue(f,'sfiSfe')}

function isA320Entry(f){
  const raw=upper(f?.type||'');
  const norm=raw.replace(/A32O/g,'A320').replace(/[^A-Z0-9]/g,'');
  return norm.includes('A320');
}
function totalFlightMins(f){return timeGroupFinalValue(f,'totalFlightTime')}

const TIME_GROUP_DEFS=Object.freeze([
  Object.freeze({key:'totalFlightTime',label:'Total Flight Time'}),
  Object.freeze({key:'multiPilot',label:'Multi-pilot'}),
  Object.freeze({key:'pic',label:'PIC'}),
  Object.freeze({key:'picUS',label:'PIC U/S'}),
  Object.freeze({key:'sic',label:'SIC'}),
  Object.freeze({key:'ifrTime',label:'IFR'}),
  Object.freeze({key:'airTime',label:'Air Time'}),
  Object.freeze({key:'night',label:'Night'}),
  Object.freeze({key:'picNight',label:'PIC Night'}),
  Object.freeze({key:'simulatorTime',label:'Simulator Time'}),
  Object.freeze({key:'sfiSfe',label:'SFI/SFE'}),
  Object.freeze({key:'dualGiven',label:'Dual Given'}),
  Object.freeze({key:'dualReceived',label:'Dual Received'}),
  Object.freeze({key:'groundInstruction',label:'Ground Instruction'}),
  Object.freeze({key:'stbyTime',label:'STBY'}),
  Object.freeze({key:'credit',label:'Credit Hours'}),
  Object.freeze({key:'totalDuty',label:'Total Duty'})
]);
const TIME_GROUP_KEYS=new Set(TIME_GROUP_DEFS.map(x=>x.key));
const TIME_GROUP_INPUT_IDS=Object.freeze({totalFlightTime:'totalFlightTimeDisplay',multiPilot:'multiPilotDisplay',pic:'picDisplay',picUS:'picUSDisplay',sic:'sicDisplay',ifrTime:'ifrTimeDisplay',airTime:'airTimeDisplay',night:'night',picNight:'picNightDisplay',simulatorTime:'simulatorTimeDisplay',sfiSfe:'sfiSfeDisplay',dualGiven:'dualGivenDisplay',dualReceived:'dualReceivedDisplay',groundInstruction:'groundInstructionDisplay',stbyTime:'stbyTimeDisplay',credit:'creditDisplay',totalDuty:'totalDutyDisplay'});
let formTimeOverrides={},formLoading=false;
let formManualFields={};
let formFieldSources={};
function emptyTimeGroupSettings(){return Object.fromEntries(TIME_GROUP_DEFS.map(x=>[x.key,{showTotal:false,showCard:false,autoSync:false}]))}
function timeGroupSettings(){
  const saved=appSettings().timeGroups||{},out=emptyTimeGroupSettings();
  TIME_GROUP_DEFS.forEach(({key})=>{const v=saved[key]||{};out[key]={showTotal:v.showTotal===true,showCard:v.showCard===true,autoSync:v.autoSync===true}});
  return out;
}
function numericTime(value){
  if(value===null||value===undefined||String(value).trim()==='')return null;
  if(typeof value==='string'&&/^\d+:\d{1,2}$/.test(value.trim()))return durMins(value);
  const n=Number(value);return Number.isFinite(n)&&n>=0?Math.round(n):null;
}
function optionalDuration(value){
  if(value===null||value===undefined||String(value).trim()==='')return null;
  if(typeof value==='number')return numericTime(value);
  const text=String(value).trim();return /^\d+:\d{1,2}$/.test(text)?durMins(text):null;
}
function fmtOptional(value){const n=numericTime(value);return n===null?'':fmt(n)}
function entryHasTimeValue(entry,key){return !!entry&&Object.prototype.hasOwnProperty.call(entry,key)&&numericTime(entry[key])!==null}
function entryTimeOverride(entry,key){
  return !!(entry?.timeOverrides&&typeof entry.timeOverrides==='object'&&entry.timeOverrides[key]===true);
}
function entryManualField(entry,key){
  if(entry?.manualFields&&typeof entry.manualFields==='object'&&entry.manualFields[key]===true)return true;
  return entryTimeOverride(entry,key);
}
function entryFieldSource(entry,key){return String(entry?.fieldSources?.[key]||'').toLowerCase()}
function entryFieldIsSource(entry,key){return ['aeroline','logten','calendar','screenshot','source','imported'].includes(entryFieldSource(entry,key))}
function sourceFieldMerge(existing,incoming){
  const out={...existing};
  const protectedAll=!!existing?.locked;
  const existingIsLogTen=String(existing?.source||'').toLowerCase()==='logten';
  const incomingIsLogTen=String(incoming?.source||'').toLowerCase()==='logten';
  const preserved=new Set();
  for(const [key,value] of Object.entries(incoming||{})){
    if(['id','manualFields','timeOverrides','fieldSources','manualOverride','locked','_updatedAt','_syncRev'].includes(key))continue;
    if(protectedAll||entryManualField(existing,key)){preserved.add(key);continue}
    /* Once LogTen is imported, keep its source record exactly as imported. Future AeroLINE/source merges
       must not reinterpret or replace LogTen fields. Manual editing remains field-authoritative. */
    if(existingIsLogTen&&!incomingIsLogTen&&Object.prototype.hasOwnProperty.call(existing,key)){preserved.add(key);continue}
    if(value===undefined||value===null||value==='')continue;
    out[key]=value;
  }
  out.manualFields={...(existing?.manualFields||{})};
  out.timeOverrides={...(existing?.timeOverrides||{})};
  out.fieldSources={...(existing?.fieldSources||{})};
  for(const [key,value] of Object.entries(incoming?.fieldSources||{}))if(!preserved.has(key)&&!entryManualField(existing,key)&&!protectedAll)out.fieldSources[key]=value;
  for(const key of Object.keys(out.manualFields))if(out.manualFields[key]===true)out.fieldSources[key]='manual';
  for(const key of Object.keys(out.timeOverrides))if(out.timeOverrides[key]===true)out.fieldSources[key]='manual';
  return out;
}
function entryRoleIsPic(entry){return entry?.rolePic===true||entry?.role==='PIC'}
function knownMulticrewAircraft(entry){return isA320Entry(entry)}
function userIsSimTrainer(entry){return !!(entry&&(entry.simInstructor===true||entry.simExaminer===true||(entry.source==='logten'&&entryHasTimeValue(entry,'sfiSfe')&&numericTime(entry.sfiSfe)>0)))}
function entryBlockMins(entry){
  if((entryTimeOverride(entry,'block')||entryManualField(entry,'block'))&&entryHasTimeValue(entry,'block'))return numericTime(entry.block);
  if(entryFieldIsSource(entry,'block')&&entryHasTimeValue(entry,'block'))return numericTime(entry.block);
  if(entry?.out&&entry?.in)return timeSpanMins(entry.out,entry.in);
  return entryHasTimeValue(entry,'block')?numericTime(entry.block):0;
}
function entryScheduleTotalMins(entry){return entry?.schedOut&&entry?.schedIn?timeSpanMins(entry.schedOut,entry.schedIn):0}
function resolvedSimulatorTimeMins(entry){
  if(!isSim(entry))return 0;
  const preserveSaved=entryTimeOverride(entry,'simulatorTime')||entryFieldIsSource(entry,'simulatorTime')||entry?.simulatorTimeImported===true;
  if(preserveSaved&&entryHasTimeValue(entry,'simulatorTime'))return numericTime(entry.simulatorTime);
  return entryScheduleTotalMins(entry);
}
function entryDutyMins(entry){
  const controlled=controlledDuty(entry);
  if(controlled)return entry.id===(controlled.control.totalDutyAtLast?dutyLastId(controlled):controlled.root.id)?controlledDutyMinutes(controlled.control):0;
  if(requiresManualDutyControl(entry))return 0;
  if(entryTimeOverride(entry,'totalDuty')&&entryHasTimeValue(entry,'totalDuty'))return numericTime(entry.totalDuty);
  if(entryFieldIsSource(entry,'totalDuty')&&entryHasTimeValue(entry,'totalDuty'))return numericTime(entry.totalDuty);
  if(entry?.onDuty&&entry?.offDuty)return timeSpanMins(entry.onDuty,entry.offDuty);
  if(isPositioning(entry)&&entry?.schedOut&&entry?.schedIn)return timeSpanMins(entry.schedOut,entry.schedIn);
  return entryHasTimeValue(entry,'totalDuty')?numericTime(entry.totalDuty):0;
}
function nightMinsForEntry(entry){
  if(!entry?.date||!entry?.out||!entry?.in)return 0;
  const dep=airportIndex[upper(entry.dep)],arr=airportIndex[upper(entry.arr)];if(!dep||!arr)return 0;
  const start=zuluDate(entry.date,entry.out),end=endZuluDate(entry.date,entry.out,entry.in);return nightRouteMinutes(start,end,dep,arr);
}
function resolvedNightMins(entry){
  if((entryManualField(entry,'night')||entryFieldSource(entry,'night')==='manual'||entryFieldSource(entry,'night')==='logten'||entry?.source==='logten')&&Object.prototype.hasOwnProperty.call(entry||{},'night'))return numericTime(entry.night)??0;
  if(entryFieldIsSource(entry,'night')&&entryHasTimeValue(entry,'night'))return numericTime(entry.night);
  if(timeGroupSettings().night.autoSync!==true&&entryHasTimeValue(entry,'night'))return numericTime(entry.night);
  return nightMinsForEntry(entry);
}
function calculatedTimeGroupValue(entry,key,allEntries=null){
  const block=entryBlockMins(entry),simulatorTime=resolvedSimulatorTimeMins(entry),night=resolvedNightMins(entry);
  switch(key){
    case'totalFlightTime':return isFlight(entry)?block:0;
    case'multiPilot':return isFlight(entry)&&knownMulticrewAircraft(entry)?block:0;
    case'pic':return isFlight(entry)&&entryRoleIsPic(entry)?block:0;
    case'picUS':return isFlight(entry)&&['PICUS','PIC U/S','PIC U/S'].includes(String(entry?.role||'').toUpperCase())?block:0;
    case'sic':return isFlight(entry)&&entry?.instruction!==true&&String(entry?.role||'').toUpperCase()==='SIC'?block:0;
    case'ifrTime':return isFlight(entry)?block:0;
    case'airTime':return isFlight(entry)&&entry?.off&&entry?.on?timeSpanMins(entry.off,entry.on):0;
    case'night':return isFlight(entry)?night:0;
    case'picNight':return isFlight(entry)&&entryRoleIsPic(entry)?night:0;
    case'simulatorTime':return simulatorTime;
    case'sfiSfe':return isSim(entry)&&userIsSimTrainer(entry)?simulatorTime:0;
    case'dualGiven':return isFlight(entry)&&entry?.instruction===true?block:0;
    case'dualReceived':return isFlight(entry)?block:0;
    case'groundInstruction':return 0;
    case'stbyTime':return isStby(entry)&&entry?.onDuty&&entry?.offDuty?timeSpanMins(entry.onDuty,entry.offDuty):0;
    case'credit':return creditMinsForEntry(entry,allEntries);
    case'totalDuty':return entryDutyMins(entry);
    default:return 0;
  }
}
function timeGroupFinalValue(entry,key,allEntries=null){
  if(key==='totalInstrument'){const value=Object.prototype.hasOwnProperty.call(entry||{},key)?entry[key]:entry?.logtenCustom?.totalInstrument;return numericTime(value)??0}
  if(!TIME_GROUP_KEYS.has(key))return 0;
  if(key==='totalDuty'&&(controlledDuty(entry)||isManualFlight(entry)))return entryDutyMins(entry);
  if((entryManualField(entry,key)||entryFieldSource(entry,key)==='manual')&&Object.prototype.hasOwnProperty.call(entry||{},key))return numericTime(entry[key])??0;
  if((entryFieldSource(entry,key)==='logten'||entry?.source==='logten')&&Object.prototype.hasOwnProperty.call(entry||{},key)&&Object.prototype.hasOwnProperty.call(LOGTEN_TIME_COLUMNS,key))return numericTime(entry[key])??0;
  if(entryFieldIsSource(entry,key)&&entryHasTimeValue(entry,key))return numericTime(entry[key]);
  if(key==='sfiSfe'&&entry?.source==='logten'&&entryHasTimeValue(entry,'sfiSfe'))return numericTime(entry.sfiSfe);
  if(key==='dualGiven'&&isFlight(entry)&&entry?.instruction===true)return calculatedTimeGroupValue(entry,key,allEntries);
  if(['sic','picUS'].includes(key)&&(entryFieldSource(entry,key)==='calculated'||timeGroupSettings()[key]?.autoSync===true))return calculatedTimeGroupValue(entry,key,allEntries);
  if(timeGroupSettings()[key]?.autoSync===true)return calculatedTimeGroupValue(entry,key,allEntries);
  if(entryHasTimeValue(entry,key))return numericTime(entry[key]);
  return 0;
}

const APP_DEFAULTS={homeBase:'CMN',flightPrefix:'MAC',aircraftPrefix:'CN-NM',profileName:'',profileRole:'Captain',profilePhotoData:'',profilePhotoUpdatedAt:'',nightStart:'18:00',nightEnd:'05:00',nightPremiumPct:50,currency:'MAD',payrollRegime:'IBC',macDependents:0,timeGroups:{}};
function appSettings(){return {...APP_DEFAULTS,...loadObject(APP_SETTINGS_KEY,{})}}
function saveAppSettings(v){if(v.profilePhotoData===appSettings().profilePhotoData)delete v.profilePhotoUpdatedAt;if(Object.entries(v).every(([k,x])=>contentHash(x)===contentHash(appSettings()[k])))return;saveObject(APP_SETTINGS_KEY,{...appSettings(),...v,_updatedAt:new Date().toISOString()})}
function payrollRegime(){return upper(appSettings().payrollRegime||'IBC')==='MAC'?'MAC':'IBC'}
function macDependents(){return Math.max(0,Math.min(6,Math.trunc(Number(appSettings().macDependents)||0)))}
const ISO_CURRENCIES=['AED','AFN','ALL','AMD','ANG','AOA','ARS','AUD','AWG','AZN','BAM','BBD','BDT','BGN','BHD','BIF','BMD','BND','BOB','BOV','BRL','BSD','BTN','BWP','BYN','BZD','CAD','CDF','CHE','CHF','CHW','CLF','CLP','CNY','COP','COU','CRC','CUC','CUP','CVE','CZK','DJF','DKK','DOP','DZD','EGP','ERN','ETB','EUR','FJD','FKP','GBP','GEL','GHS','GIP','GMD','GNF','GTQ','GYD','HKD','HNL','HTG','HUF','IDR','ILS','INR','IQD','IRR','ISK','JMD','JOD','JPY','KES','KGS','KHR','KMF','KPW','KRW','KWD','KYD','KZT','LAK','LBP','LKR','LRD','LSL','LYD','MAD','MDL','MGA','MKD','MMK','MNT','MOP','MRU','MUR','MVR','MWK','MXN','MXV','MYR','MZN','NAD','NGN','NIO','NOK','NPR','NZD','OMR','PAB','PEN','PGK','PHP','PKR','PLN','PYG','QAR','RON','RSD','RUB','RWF','SAR','SBD','SCR','SDG','SEK','SGD','SHP','SLE','SLL','SOS','SRD','SSP','STN','SVC','SYP','SZL','THB','TJS','TMT','TND','TOP','TRY','TTD','TWD','TZS','UAH','UGX','USD','USN','UYI','UYU','UYW','UZS','VED','VES','VND','VUV','WST','XAF','XAG','XAU','XBA','XBB','XBC','XBD','XCD','XDR','XOF','XPD','XPF','XPT','XSU','XTS','XUA','XXX','YER','ZAR','ZMW','ZWL'];
const QUICK_CASH_CURRENCIES=['USD','TRY','EUR'];
function appCurrency(){const c=upper(appSettings().currency||'MAD');return ISO_CURRENCIES.includes(c)?c:'MAD'}
function currencyOutputCode(code){return upper(code)==='MAD'?'DHM':upper(code)}
function moneyCurrency(value,code=appCurrency()){return `${money(value)} ${currencyOutputCode(code)}`}
function populateCurrencySelect(el,quick=[]){
  if(!el)return;const current=upper(el.value||'');el.innerHTML='';
  if(quick.length){const g=document.createElement('optgroup');g.label='Quick';quick.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c==='MAD'?'MAD / DHM':c;g.appendChild(o)});el.appendChild(g)}
  const all=document.createElement('optgroup');all.label='All currencies';ISO_CURRENCIES.filter(c=>!quick.includes(c)).forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c==='MAD'?'MAD / DHM':c;all.appendChild(o)});el.appendChild(all);
  if(current&&ISO_CURRENCIES.includes(current))el.value=current;
}
const FX_ENDPOINT_CURRENCY='eur';
function fxRateFromTable(table,from,to){from=upper(from);to=upper(to);if(from===to)return 1;const rates=table?.eur||{},fromPerEur=from==='EUR'?1:Number(rates[from.toLowerCase()]),toPerEur=to==='EUR'?1:Number(rates[to.toLowerCase()]);if(!(fromPerEur>0)||!(toPerEur>0))throw new Error(`FX pair unavailable: ${from}/${to}`);return toPerEur/fromPerEur}
async function fetchFxJson(url,timeoutMs=8000){
  const controller=typeof AbortController!=='undefined'?new AbortController():null,timer=controller?setTimeout(()=>controller.abort(),timeoutMs):null;
  try{const r=await fetch(url,{cache:'no-store',signal:controller?.signal});if(!r.ok)throw new Error(`FX HTTP ${r.status}`);const j=await r.json();if(!j?.eur||typeof j.eur!=='object')throw new Error('FX data unavailable');return j}finally{if(timer)clearTimeout(timer)}
}
async function fetchFxCandidates(urls){let last=null;for(const url of urls){try{return await fetchFxJson(url)}catch(e){last=e}}throw last||new Error('FX unavailable')}
function fxUrlsForDate(key){return[
  `https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@${key}/v1/currencies/${FX_ENDPOINT_CURRENCY}.min.json`,
  `https://${key}.currency-api.pages.dev/v1/currencies/${FX_ENDPOINT_CURRENCY}.min.json`,
  `https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@${key}/v1/currencies/${FX_ENDPOINT_CURRENCY}.json`,
  `https://${key}.currency-api.pages.dev/v1/currencies/${FX_ENDPOINT_CURRENCY}.json`
]}
async function fetchFxTableForDate(dateStr){
  if(!/^\d{4}-\d{2}-\d{2}$/.test(String(dateStr||'')))throw new Error('Invalid FX date');
  const table=await fetchFxCandidates(fxUrlsForDate(dateStr));
  if(table.date!==dateStr)throw Error('Requested historical FX date unavailable');
  return {table,date:dateStr};
}
async function fetchLatestFxTable(){const j=await fetchFxCandidates(fxUrlsForDate('latest'));return{table:j,date:String(j.date||today())}}
async function fxForDate(from,to,dateStr){from=upper(from);to=upper(to);if(from===to)return{rate:1,date:dateStr};const x=await fetchFxTableForDate(dateStr);return{rate:fxRateFromTable(x.table,from,to),date:x.date}}
async function latestFx(from,to){from=upper(from);to=upper(to);if(from===to)return{rate:1,date:today()};const x=await fetchLatestFxTable();return{rate:fxRateFromTable(x.table,from,to),date:x.date}}
function cleanPrefix(v){return upper(v).replace(/\s+/g,'')}
function composeFlightNo(raw){const prefix=cleanPrefix(appSettings().flightPrefix||'MAC'),s=cleanPrefix(raw);if(!s)return'';if(prefix&&s.startsWith(prefix))return s;if(/^3O\d/i.test(s))return prefix+s.slice(2);if(/^\d+$/.test(s))return prefix+s;return s}
function flightNoInput(full){const prefix=cleanPrefix(appSettings().flightPrefix||'MAC'),s=cleanPrefix(full);if(prefix&&s.startsWith(prefix))return s.slice(prefix.length);if(/^3O\d/i.test(s))return s.slice(2);return s}
function composeAircraftId(raw){const prefix=cleanPrefix(appSettings().aircraftPrefix||'CN-NM'),s=cleanPrefix(raw);if(!s)return'';if(prefix&&s.startsWith(prefix))return s;if(/^[A-Z]{1,3}-[A-Z0-9-]+$/.test(s))return s;return prefix+s}
function aircraftIdInput(full){const prefix=cleanPrefix(appSettings().aircraftPrefix||'CN-NM'),s=cleanPrefix(full);return prefix&&s.startsWith(prefix)?s.slice(prefix.length):s}
const aircraftRegistrationCache=new Map();let aircraftLookupSeq=0,aircraftLookupTimer=null;
async function aircraftByRegistration(registration){
  const reg=upper(registration||'').replace(/\s+/g,'');if(!reg)return null;
  if(aircraftRegistrationCache.has(reg))return aircraftRegistrationCache.get(reg);
  const controller=typeof AbortController!=='undefined'?new AbortController():null,timer=controller?setTimeout(()=>controller.abort(),6500):null;
  try{
    const response=await fetch(`https://api.adsbdb.com/v0/aircraft/${encodeURIComponent(reg)}`,{cache:'no-store',signal:controller?.signal});
    if(response.status===404)return null
    if(!response.ok)throw new Error(`Aircraft DB HTTP ${response.status}`);
    const payload=await response.json(),aircraft=payload?.response?.aircraft;
    if(!aircraft||typeof aircraft!=='object')return null
    const value={registration:upper(aircraft.registration||reg),type:upper(aircraft.type||''),icaoType:upper(aircraft.icao_type||'')};
    aircraftRegistrationCache.set(reg,value);return value;
  }finally{if(timer)clearTimeout(timer)}
}
async function applyAircraftRegistrationLookup(){
  if(formLoading||canonicalDutyType($('dutyTypeFlight')?.value||'Flight')!=='Flight')return false;
  if(coreActivityRows().find(f=>f.id===$('editId')?.value)?.locked)return false;
  const registration=composeAircraftId($('reg')?.value||'');if(!registration)return false;
  const seq=++aircraftLookupSeq,entryId=$('editId').value;
  try{
    const aircraft=await aircraftByRegistration(registration);if(seq!==aircraftLookupSeq||!aircraft||entryId!==$('editId').value||registration!==composeAircraftId($('reg').value))return false;
    // Fill missing data, including empty fields previously marked as manual.
    // Existing manual/source values remain authoritative.

    const variant=(aircraft.type||'').match(/\b[A-Z]\d{3}(?:-[A-Z0-9]+)+\b/)?.[0]||'';
    const resolved=aircraft.icaoType||variant.split('-')[0]||aircraft.type;if(!resolved)return false;
    if(coreActivityRows().find(f=>f.id===entryId)?.locked)return false;
    let changed=false;
    for(const [key,value] of [['type',resolved],['variant',variant]]){
      const el=$(key);if(!el||!value)continue;
      const empty=!String(el.value||'').trim(),owned=formFieldSources[key]==='aircraft-db'&&!formManualFields[key];
      if(!formManualFields[key]&&(empty||owned)){el.value=value;formFieldSources[key]='aircraft-db';formManualFields[key]=false;changed=true}
    }
    if(changed){calcEntry();saveEntryDraft()}return changed;
  }catch(err){console.warn('Aircraft registration lookup unavailable',err);return false}
}
function scheduleAircraftRegistrationLookup(){
  clearTimeout(aircraftLookupTimer);if(formLoading)return;
  aircraftLookupTimer=setTimeout(()=>applyAircraftRegistrationLookup(),450);
}
function updatePrefixUI(){const st=appSettings();$('flightPrefixLabel').textContent=cleanPrefix(st.flightPrefix||'MAC');$('aircraftPrefixLabel').textContent=cleanPrefix(st.aircraftPrefix||'CN-NM')}

function updateAppHeader(){
  const st=appSettings(),name=String(st.profileName||'').trim(),role=String(st.profileRole||'').trim();
  const descriptor=[role,name].filter(Boolean).join(' ');
  if($('appTitle'))$('appTitle').textContent=descriptor?`PilotLog of ${descriptor}`:'PilotLog';
  if($('appTagline'))$('appTagline').textContent='PilotLog — your personal LogBook';
  const photo=$('profileHeaderPhoto'),mark=document.querySelector('.brand-mark svg'),hasPhoto=!!st.profilePhotoData;
  if(photo){photo.classList.toggle('hidden',!hasPhoto);if(hasPhoto)photo.src=st.profilePhotoData;else photo.removeAttribute('src')}
  if(mark)mark.classList.toggle('hidden',hasPhoto);
  document.title=descriptor?`PilotLog of ${descriptor}`:'PilotLog';
}


/* Airport DB: fallback + IndexedDB cache of the public mwgg Airports dataset. */
const AIRPORT_SOURCE='https://raw.githubusercontent.com/mwgg/Airports/master/airports.json';
const AIRPORT_FALLBACK={
CMN:{iata:'CMN',icao:'GMMN',name:'Mohammed V International Airport',city:'Casablanca',country:'MA',lat:33.3675,lon:-7.58997,tz:'Africa/Casablanca'},
NDR:{iata:'NDR',icao:'GMMW',name:'Nador International Airport',city:'Nador',country:'MA',lat:34.9888,lon:-3.0282,tz:'Africa/Casablanca'},
TNG:{iata:'TNG',icao:'GMTT',name:'Tangier Ibn Battouta Airport',city:'Tangier',country:'MA',lat:35.7269,lon:-5.91689,tz:'Africa/Casablanca'},
RAK:{iata:'RAK',icao:'GMMX',name:'Marrakesh Menara Airport',city:'Marrakesh',country:'MA',lat:31.6069,lon:-8.0363,tz:'Africa/Casablanca'},
FEZ:{iata:'FEZ',icao:'GMFF',name:'Fès–Saïs Airport',city:'Fès',country:'MA',lat:33.9273,lon:-4.97796,tz:'Africa/Casablanca'},
BGY:{iata:'BGY',icao:'LIME',name:'Milan Bergamo Airport',city:'Bergamo',country:'IT',lat:45.6739,lon:9.70417,tz:'Europe/Rome'},
MXP:{iata:'MXP',icao:'LIMC',name:'Milan Malpensa Airport',city:'Milan',country:'IT',lat:45.6306,lon:8.72811,tz:'Europe/Rome'},
NAP:{iata:'NAP',icao:'LIRN',name:'Naples International Airport',city:'Naples',country:'IT',lat:40.886,lon:14.2908,tz:'Europe/Rome'},
CTA:{iata:'CTA',icao:'LICC',name:'Catania Fontanarossa Airport',city:'Catania',country:'IT',lat:37.4668,lon:15.0664,tz:'Europe/Rome'},
FCO:{iata:'FCO',icao:'LIRF',name:'Rome Fiumicino Airport',city:'Rome',country:'IT',lat:41.8003,lon:12.2389,tz:'Europe/Rome'},
BSL:{iata:'BSL',icao:'LFSB',name:'EuroAirport Basel Mulhouse Freiburg',city:'Basel',country:'CH',lat:47.59,lon:7.52991,tz:'Europe/Zurich'},
SAW:{iata:'SAW',icao:'LTFJ',name:'Istanbul Sabiha Gökçen Airport',city:'Istanbul',country:'TR',lat:40.8986,lon:29.3092,tz:'Europe/Istanbul'},
IST:{iata:'IST',icao:'LTFM',name:'Istanbul Airport',city:'Istanbul',country:'TR',lat:41.2753,lon:28.7519,tz:'Europe/Istanbul'},
SHJ:{iata:'SHJ',icao:'OMSJ',name:'Sharjah International Airport',city:'Sharjah',country:'AE',lat:25.3286,lon:55.5172,tz:'Asia/Dubai'},
DXB:{iata:'DXB',icao:'OMDB',name:'Dubai International Airport',city:'Dubai',country:'AE',lat:25.2528,lon:55.3644,tz:'Asia/Dubai'},
LYS:{iata:'LYS',icao:'LFLL',name:'Lyon Saint-Exupéry Airport',city:'Lyon',country:'FR',lat:45.7256,lon:5.08111,tz:'Europe/Paris'}
};
let airportIndex={...AIRPORT_FALLBACK}, airportDbLoaded=false;
function openAirportDb(){return new Promise((resolve,reject)=>{const r=indexedDB.open('pilotlog7-airports',1);r.onupgradeneeded=()=>{const db=r.result;if(!db.objectStoreNames.contains('cache'))db.createObjectStore('cache')};r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error)})}
async function idbGet(key){const db=await openAirportDb();return new Promise((resolve,reject)=>{const tx=db.transaction('cache','readonly'),q=tx.objectStore('cache').get(key);q.onsuccess=()=>resolve(q.result);q.onerror=()=>reject(q.error)})}
async function idbSet(key,val){const db=await openAirportDb();return new Promise((resolve,reject)=>{const tx=db.transaction('cache','readwrite');tx.objectStore('cache').put(val,key);tx.oncomplete=()=>resolve();tx.onerror=()=>reject(tx.error)})}
function buildAirportIndex(raw){const idx={...AIRPORT_FALLBACK};Object.values(raw||{}).forEach(a=>{if(!a)return;const rec={iata:upper(a.iata),icao:upper(a.icao),name:a.name||'',city:a.city||'',country:a.country||'',lat:Number(a.lat),lon:Number(a.lon),tz:a.tz||'UTC'};if(rec.iata)idx[rec.iata]=rec;if(rec.icao)idx[rec.icao]=rec});return idx}
async function ensureAirportDb(force=false){if(airportDbLoaded&&!force)return airportIndex;const status=$('airportDbStatus');try{if(!force){const cached=await idbGet('iata-index-v1');if(cached&&Object.keys(cached).length>1000){airportIndex={...AIRPORT_FALLBACK,...cached};airportDbLoaded=true;if(status)status.textContent=`Airport database: ${Object.keys(cached).length} codes cached offline.`;return airportIndex}}if(status)status.textContent='Airport database: downloading and building offline cache…';const r=await fetch(AIRPORT_SOURCE,{cache:'no-store'});if(!r.ok)throw new Error(`HTTP ${r.status}`);const raw=await r.json();const idx=buildAirportIndex(raw);airportIndex=idx;airportDbLoaded=true;await idbSet('iata-index-v1',idx);if(status)status.textContent=`Airport database: ${Object.keys(idx).length} IATA/ICAO codes cached offline.`;return airportIndex}catch(e){airportDbLoaded=true;if(status)status.textContent='Airport database: using built-in fallback; full database will retry when online.';return airportIndex}}
async function airport(code){await ensureAirportDb(false);return airportIndex[upper(code)]||null}
async function updateAirportInfo(){for(const [field,info] of [['dep','depInfo'],['arr','arrInfo'],['simLocation','simLocationInfo']]){if(!$(field)||!$(info))continue;const code=upper($(field).value);$(field).value=code;if(!code){$(info).textContent='';continue}const a=await airport(code);$(info).textContent=a?`${a.iata||a.icao} • ${a.name} • ${a.city} • ${a.tz}`:'Airport not found in current cache.'}}

/* Solar calculations. Night = civil twilight or darker (sun altitude below -6°). */
const rad=x=>x*Math.PI/180, deg=x=>x*180/Math.PI;
function julianDate(d){return d.getTime()/86400000+2440587.5}
function sunAltitude(date,lat,lon){
  const jd=julianDate(date), n=jd-2451545.0;
  const L=(280.46+0.9856474*n)%360, g=rad((357.528+0.9856003*n)%360);
  const lambda=rad((L+1.915*Math.sin(g)+0.020*Math.sin(2*g))%360);
  const eps=rad(23.439-0.0000004*n);
  const ra=Math.atan2(Math.cos(eps)*Math.sin(lambda),Math.cos(lambda));
  const dec=Math.asin(Math.sin(eps)*Math.sin(lambda));
  const T=(jd-2451545)/36525, gmst=(280.46061837+360.98564736629*(jd-2451545)+0.000387933*T*T-T*T*T/38710000)%360;
  const lst=rad((gmst+lon+360)%360), H=lst-ra;
  return deg(Math.asin(Math.sin(rad(lat))*Math.sin(dec)+Math.cos(rad(lat))*Math.cos(dec)*Math.cos(H)));
}
function zuluDate(date,time){if(!date||!time)return null;return new Date(`${date}T${time}:00Z`)}
function endZuluDate(date,start,end){const s=zuluDate(date,start),e=zuluDate(date,end);if(s&&e&&e<s)e.setUTCDate(e.getUTCDate()+1);return e}
function gcPoint(a,b,f){const φ1=rad(a.lat),λ1=rad(a.lon),φ2=rad(b.lat),λ2=rad(b.lon);const δ=2*Math.asin(Math.sqrt(Math.sin((φ2-φ1)/2)**2+Math.cos(φ1)*Math.cos(φ2)*Math.sin((λ2-λ1)/2)**2));if(!δ)return{lat:a.lat,lon:a.lon};const A=Math.sin((1-f)*δ)/Math.sin(δ),B=Math.sin(f*δ)/Math.sin(δ);const x=A*Math.cos(φ1)*Math.cos(λ1)+B*Math.cos(φ2)*Math.cos(λ2),y=A*Math.cos(φ1)*Math.sin(λ1)+B*Math.cos(φ2)*Math.sin(λ2),z=A*Math.sin(φ1)+B*Math.sin(φ2);return{lat:deg(Math.atan2(z,Math.sqrt(x*x+y*y))),lon:deg(Math.atan2(y,x))}}
function dateAtOrAfter(baseDate,time,reference=null){
  let d=zuluDate(baseDate,time);if(!d)return null;
  if(reference&&d<reference)d.setUTCDate(d.getUTCDate()+1);
  return d;
}
function nightFixedMinutes(start,end,loc,step=2){
  if(!start||!end||!loc||end<=start)return 0;
  const total=Math.round((end-start)/60000);let night=0;
  for(let m=0;m<total;m+=step){const chunk=Math.min(step,total-m),t=new Date(start.getTime()+(m+chunk/2)*60000);if(sunAltitude(t,loc.lat,loc.lon)<-6)night+=chunk}
  return night;
}
function nightRouteMinutes(start,end,dep,arr,step=2){
  if(!start||!end||!dep||!arr||end<=start)return 0;
  const total=Math.round((end-start)/60000);let night=0;
  for(let m=0;m<total;m+=step){const chunk=Math.min(step,total-m),f=(m+chunk/2)/total,p=gcPoint(dep,arr,f),t=new Date(start.getTime()+(m+chunk/2)*60000);if(sunAltitude(t,p.lat,p.lon)<-6)night+=chunk}
  return night;
}
function applyPfCounts(depNight,arrNight){
  const pf=!!$('pfCheck')?.checked;
  $('dayTakeoffs').value=pf&&!depNight?1:0;$('nightTakeoffs').value=pf&&depNight?1:0;
  $('dayLandings').value=pf&&!arrNight?1:0;$('nightLandings').value=pf&&arrNight?1:0;
}
async function calcNightForForm(){
  const date=$('date').value,dep=await airport($('dep').value),arr=await airport($('arr').value);
  const out=$('out').value,inn=$('in').value;
  if(!date||!dep||!arr||!out||!inn){
    if(!formTimeOverrides.night&&timeGroupSettings().night.autoSync)$('night').value='';
    applyPfCounts(false,false);
    $('nightStatus').textContent='Night auto: enter valid From/To and OUT/IN.';return 0;
  }
  const start=zuluDate(date,out),end=endZuluDate(date,out,inn),night=nightRouteMinutes(start,end,dep,arr),depNight=!!start&&sunAltitude(start,dep.lat,dep.lon)<-6,arrNight=!!end&&sunAltitude(end,arr.lat,arr.lon)<-6;
  if(!formTimeOverrides.night&&timeGroupSettings().night.autoSync)setDurationPicker($('night'),night);
  applyPfCounts(depNight,arrNight);
  $('nightStatus').textContent=`Night auto OUT–IN: ${fmt(night)} • take-off ${depNight?'night':'day'} • landing ${arrNight?'night':'day'}${$('pfCheck')?.checked?' • PF counts applied':' • PF not selected'}.`;
  return night;
}

/* Credit rules */
const PAY_DEFAULTS={joinDate:'2014-04-01',base:46500,allowance:8000,transport:2500,pos:4000,telephone:500,uniform:300,meal:520,deduction:-3000,seniority2:10,seniority5:null,seniority12:15,seniority25:null,groundCredit:5,simCredit:5,deductionMemo:null,t1Max:25,t1Rate:170,t2Max:50,t2Rate:370,t3Max:75,t3Rate:665,t4Rate:1400,trainingRate:500,layoverRate:25,simAllowance:1000,dayOffRate:1000};
function paySettings(){return {...PAY_DEFAULTS,...loadObject(PAY_SETTINGS_KEY,{})}}
function creditMinsForEntry(f,allEntries=null){
  const rule=activityRule(f);
  if(entryTimeOverride(f,'credit')&&entryHasTimeValue(f,'credit'))return numericTime(f.credit);
  if(entryFieldIsSource(f,'credit')&&entryHasTimeValue(f,'credit'))return numericTime(f.credit);
  if(rule.credit==='flight-schedule')return flightCreditMins({...f,dutyType:'Flight'},allEntries);
  if(rule.credit==='editable-sim'){
    if(f?.creditImported===true&&entryHasTimeValue(f,'credit'))return numericTime(f.credit);
    if(userIsSimTrainer(f))return 300;
    return 0;
  }
  if(rule.credit==='editable-ground'){
    if(entryHasTimeValue(f,'credit'))return numericTime(f.credit);
    const trainer=upper(f?.aerolineTrainerType||f?.trainerType||'');
    if(trainer==='TNE')return 0;
    if(trainer==='GI')return 300;
    return 0;
  }
  if(rule.credit==='zero')return 0;
  return entryHasTimeValue(f,'credit')?numericTime(f.credit):0;
}
function calculateActivityMetrics(f,allEntries=null){
  const dutyType=canonicalDutyType(f?.dutyType||'Flight'),flight=dutyType==='Flight',sim=dutyType==='Simulator';
  const schedBlock=entryScheduleTotalMins(f),block=entryBlockMins(f);
  const flightMins=flight&&f?.off&&f?.on?timeSpanMins(f.off,f.on):0;
  const simulatorTime=sim?resolvedSimulatorTimeMins(f):0;
  const totalDuty=entryDutyMins(f);
  const credit=creditMinsForEntry({...f,dutyType,schedBlock,simulatorTime},allEntries);
  return{dutyType,schedBlock,block,airTime:flightMins,simulatorTime,totalDuty,credit};
}
function formRoleValue(existing={}){
  if($('rolePicCheck')?.checked)return 'PIC';
  if(formManualFields.rolePic||existing.manualFields?.rolePic)return '';
  const role=String(existing.role||'').toUpperCase();
  return ['SIC','PICUS','PIC U/S'].includes(role)?role:'';
}
function formActivityInput(){
  const dutyType=canonicalDutyType($('dutyTypeFlight')?.value||'Flight');
  const existing=entryActivityRows().find(x=>x.id===$('editId')?.value)||{};
  const timeValues={};TIME_GROUP_DEFS.forEach(({key})=>{const id=TIME_GROUP_INPUT_IDS[key],value=optionalDuration($(id)?.value);if(value!==null)timeValues[key]=value});
  const blockValue=optionalDuration($('blockDisplay')?.value);
  const out=$('out')?.value||'',inn=$('in')?.value||'';
  return{...existing,...timeValues,id:$('editId')?.value||'',dutyType,date:$('date')?.value||'',flightNo:composeFlightNo($('flightNo')?.value||''),dep:upper($('dep')?.value||''),arr:upper($('arr')?.value||''),schedOut:$('schedOut')?.value||'',schedIn:$('schedIn')?.value||'',onDuty:$('onDuty')?.value||'',offDuty:$('offDuty')?.value||'',out,off:$('off')?.value||'',on:$('on')?.value||'',in:inn,block:blockValue===null?(out&&inn?timeSpanMins(out,inn):0):blockValue,timeOverrides:{...(existing.timeOverrides||{}),...formTimeOverrides},role:formRoleValue(existing),rolePic:!!$('rolePicCheck')?.checked,instruction:!!$('instructionCheck')?.checked,examiner:!!$('examinerCheck')?.checked,simInstructor:!!$('simInstructorCheck')?.checked,simExaminer:!!$('simExaminerCheck')?.checked,rhs:!!$('rhsCheck')?.checked,pf:!!$('pfCheck')?.checked?'yes':'no',trainerType:upper($('trainerTypeInput')?.value||existing?.trainerType||''),aerolineTrainerType:existing?.aerolineTrainerType||''};
}
function formCredit(dutyType){return PilotLogEngine.activity({...formActivityInput(),dutyType:canonicalDutyType(dutyType)}).credit}


function incrementFlightNumber(v){
  const raw=upper(v||'').trim();
  if(!raw)return'';
  const m=raw.match(/^(.*?)(\d+)$/);
  if(!m)return raw;
  return `${m[1]}${String(Number(m[2])+1).padStart(m[2].length,'0')}`;
}
function plusOneIsoDate(date){
  const d=dateOnly(date);
  d.setUTCDate(d.getUTCDate()+1);
  return d.toISOString().slice(0,10);
}
function returnFlightDateFromLeg(f){
  const date=f.date||today();
  const dep=f.schedOut||f.out||f.off||'';
  const arr=f.schedIn||f.in||f.on||'';
  if(dep&&arr&&mins(arr)<mins(dep))return plusOneIsoDate(date);
  return date;
}
function formLegSnapshot(){
  return {
    ...formCrewSnapshot(),
    dutyType:$('dutyTypeFlight')?.value||'Flight',
    date:$('date')?.value||today(),
    flightNo:composeFlightNo($('flightNo')?.value||''),
    type:upper($('type')?.value||''),
    reg:composeAircraftId($('reg')?.value||''),
    location:upper($('simLocation')?.value||''),
    dep:upper($('dep')?.value||''),
    arr:upper($('arr')?.value||''),
    schedOut:$('schedOut')?.value||'',
    schedIn:$('schedIn')?.value||'',
    out:$('out')?.value||'',
    off:$('off')?.value||'',
    on:$('on')?.value||'',
    in:$('in')?.value||'',
    rolePic:!!$('rolePicCheck')?.checked,
    picName:upper($('picName')?.value||''),
    sicName:upper($('sicName')?.value||''),
    soName:upper($('soName')?.value||''),
    instructorName:upper($('instructorName')?.value||''),
    instruction:!!$('instructionCheck')?.checked,
    examiner:!!$('examinerCheck')?.checked,
    rhs:!!$('rhsCheck')?.checked,
    pf:$('pfCheck')?.checked?'yes':'no',
    variant:upper($('variant')?.value||'')
  };
}
function latestFlightForReturn(){
  return coreActivityRows().filter(f=>isFlight(f)&&activityIncludedInResults(f)).sort((a,b)=>
    `${b.date||''}${b.schedOut||b.out||b.off||''}`.localeCompare(`${a.date||''}${a.schedOut||a.out||a.off||''}`)
  )[0]||null;
}
function createReturnFlight(){
  let src=formLegSnapshot();
  if(!src.dep||!src.arr){
    const latest=latestFlightForReturn();
    if(!latest)return alert('Enter or save the outbound flight first.');
    src=latest;
  }
  if(String(src.dutyType||'Flight')!=='Flight')return alert('Return Flight is available for Flight entries only.');

  saveEntryDraft();
  resetEntry(false);
  $('entryDraftId').value=makeId();
  $('dutyTypeFlight').value='Flight';
  $('date').value=returnFlightDateFromLeg(src);

  const prefix=(appSettings().flightPrefix||'MAC').toUpperCase();
  let no=String(src.flightNo||'');
  if(no.toUpperCase().startsWith(prefix))no=no.slice(prefix.length);
  $('flightNo').value=incrementFlightNumber(no);

  $('type').value=upper(src.type||'');
  $('variant').value=upper(src.variant||'');

  const regPrefix=(appSettings().aircraftPrefix||'CN-NM').toUpperCase();
  let reg=String(src.reg||'');
  if(reg.toUpperCase().startsWith(regPrefix))reg=reg.slice(regPrefix.length);
  $('reg').value=upper(reg);

  $('dep').value=upper(src.arr||'');
  $('arr').value=upper(src.dep||'');

  $('rolePicCheck').checked=src.rolePic===true||src.role==='PIC';
  $('picName').value=upper(src.picName||'');
  $('sicName').value=upper(src.sicName||'');
  $('soName').value=upper(src.soName||'');
  $('instructorName').value=upper(src.instructorName||'');
  $('instructionCheck').checked=src.instruction===true;$('examinerCheck').checked=src.examiner===true;$('rhsCheck').checked=src.rhs===true||upper(src.seatPosition||'')==='RIGHT';$('pfCheck').checked=src.pf==='yes';

  setEntryTypeUI();
  updateExaminerRemarkReminder();
  calcEntry();
  updateAirportInfo();
  syncProfileNameWithRole();
  beginCoreActivity('Flight',src);
  $('schedOut')?.focus();
}

function updateExaminerRemarkReminder(){
  const box=$('examinerRemarkReminder');if(!box)return;
  const active=$('dutyTypeFlight')?.value==='Flight' && $('role')?.value==='Examiner';
  box.classList.toggle('hidden',!active);
}

function setEntryTypeUI(){
  const dt=canonicalDutyType($('dutyTypeFlight').value||'Flight'),sim=dt==='Simulator',flight=dt==='Flight',positioning=dt==='DHD'||dt==='DHP',ground=dt==='Ground Course',standby=dt==='STBY',dayOff=dt==='Day OFF',genericDuty=dt==='Flight Duty'||dt==='Duty';
  $('dutyTypeFlight').value=dt;
  $('flightForm').dataset.activityType=dt;
  $('flightForm').classList.toggle('duty-layout',!flight);
  $('flightForm').classList.toggle('activity-layout',sim||positioning||ground);
  document.querySelector('.entry-counter-strip')?.classList.toggle('hidden',!flight);
  document.querySelectorAll('[data-entry-field]').forEach(el=>{el.classList.remove('hidden');el.querySelectorAll('input,select,textarea').forEach(x=>x.disabled=false)});
  if($('regLabel'))$('regLabel').textContent=sim?'SIM Registration':'Aircraft ID';
  if($('typeLabel'))$('typeLabel').textContent=sim?'SIM Type':'Aircraft Type';
  if($('depLabel'))$('depLabel').textContent=sim?'Location':ground||standby?'Location':'From — IATA/ICAO';
  if($('aircraftPrefixLabel'))$('aircraftPrefixLabel').style.display=sim?'none':'';
  if($('reg'))$('reg').placeholder=sim?'SIM-01':'Q';
  if($('onDutyLabel'))$('onDutyLabel').textContent=flight?'On Duty (Z)':'Duty On (Z)';
  if($('offDutyLabel'))$('offDutyLabel').textContent=flight?'Off Duty (Z)':'Duty Off (Z)';
  if($('schedOutLabel'))$('schedOutLabel').textContent=positioning?'Start (Z)':'Schedule OUT (Z)';
  if($('schedInLabel'))$('schedInLabel').textContent=positioning?'End (Z)':'Schedule IN (Z)';
  $('schedOut').readOnly=false;$('schedIn').readOnly=false;$('blockDisplay').readOnly=false;
  TIME_GROUP_DEFS.forEach(({key})=>{const id=TIME_GROUP_INPUT_IDS[key];if($(id))$(id).readOnly=false});

  const allowed=flight?new Set(['flightNo','reg','type','variant','dep','arr','schedOut','schedIn','schedBlock','onDuty','offDuty','out','off','on','in','block','picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name','approachType','approachCategory','pax','fuelAboard','fuelBurned','fuelMinimum']):
    sim?new Set(['reg','type','dep','schedOut','schedIn','schedBlock','onDuty','offDuty','picName','sicName','instructorName','examinerName']):
    positioning?new Set(['dep','arr','schedOut','schedIn','schedBlock','onDuty','offDuty']):
    ground?new Set(['dep','courseType','trainerType','onDuty','offDuty','block']):
    standby?new Set(['dep','onDuty','offDuty','block']):
    genericDuty?new Set(['onDuty','offDuty']):
    dayOff?new Set():new Set();
  document.querySelectorAll('[data-entry-field]').forEach(el=>{if(!allowed.has(el.dataset.entryField))el.classList.add('hidden')});

  const settings=timeGroupSettings(),applicable=flight?new Set(['totalFlightTime','multiPilot','pic','picUS','sic','ifrTime','airTime','night','picNight','dualGiven','dualReceived','credit','totalDuty']):sim?new Set(['simulatorTime','sfiSfe','credit','totalDuty']):ground?new Set(['groundInstruction','credit','totalDuty']):standby?new Set(['stbyTime','credit','totalDuty']):positioning?new Set(['credit','totalDuty']):genericDuty?new Set(['totalDuty']):new Set();
  document.querySelectorAll('[data-time-group]').forEach(el=>{const key=el.dataset.timeGroup;el.classList.toggle('hidden',!applicable.has(key)||(key!=='totalDuty'&&settings[key]?.showCard!==true))});
  document.querySelectorAll('[data-flight-counter]').forEach(el=>el.classList.toggle('hidden',!flight));
  const visibleTimeGroups=[...document.querySelectorAll('[data-time-group]')].some(el=>!el.classList.contains('hidden'));
  $('breakdownTitle')?.classList.toggle('hidden',!visibleTimeGroups&&!flight);$('breakdownGrid')?.classList.toggle('hidden',!visibleTimeGroups&&!flight);
  $('nightStatus')?.classList.toggle('hidden',!flight||settings.night.showCard!==true);
  $('remarksWrap')?.classList.remove('hidden');$('calcPreview')?.classList.remove('hidden');

  const checks={pic:flight,pf:flight,instruction:flight,examiner:flight,simTrainer:sim,rhs:flight};
  Object.entries(checks).forEach(([key,show])=>document.querySelector(`[data-entry-check="${key}"]`)?.classList.toggle('hidden',!show));
  $('checkboxTitle')?.classList.toggle('hidden',!(flight||sim||ground||standby||positioning));$('checkboxGrid')?.classList.toggle('hidden',!(flight||sim||ground||standby||positioning));
  if(!flight){$('rolePicCheck').checked=false;$('pfCheck').checked=false;$('instructionCheck').checked=false;$('examinerCheck').checked=false;$('rhsCheck').checked=false;}
  if(!sim){$('simInstructorCheck').checked=false;$('simExaminerCheck').checked=false;}
  if($('simTrainerCheck'))$('simTrainerCheck').checked=sim&&($('simInstructorCheck').checked||$('simExaminerCheck').checked);
  if(positioning&&!controlledDuty(coreActivityRows().find(f=>f.id===$('editId')?.value))){for(const [key,schedule] of [['onDuty','schedOut'],['offDuty','schedIn']]){if(!formTimeOverrides[key]&&!formManualFields[key]&&!['manual','aeroline','logten','calendar','screenshot','source','imported'].includes(String(formFieldSources[key]||'').toLowerCase()))$(key).value=$(schedule).value||''}}
  const current=coreActivityRows().find(x=>x.id===$('editId').value),duty=controlledDuty(current),manual=flight&&requiresManualDutyControl(current||{dutyType:'Flight',source:'manual'});
  $('offDuty').readOnly=!!(flight&&(manual||duty)&&duty?.control.state!=='CLOSED');
  $('totalDutyDisplay').readOnly=!!(flight&&(manual||duty));
  if(!flight)['dayTakeoffs','nightTakeoffs','dayLandings','nightLandings'].forEach(id=>$(id).value=0);
  updateDelayUI();renderEntryDraftShelf();updateDutyControls();
  if(coreActivityRows().find(x=>x.id===$('editId').value)?.locked)setEntryLockedUI(true);
}
function calcEntry(){
  const dutyType=canonicalDutyType($('dutyTypeFlight').value||'Flight');
  let input={...formActivityInput(),dutyType};
  const source=formFieldSources.credit;
  if(!formLoading&&!formManualFields.credit&&(source==='awaiting-role'||source==='entry-default')){
    input={...input,trainerType:$('trainerTypeInput').value,courseType:$('courseType').value};
    const setting=trainingCreditSetting(input);
    if(source==='awaiting-role'&&setting){
      const value=Math.round(paySettings()[setting]*60);
      setDurationPicker($('creditDisplay'),value);
      formTimeOverrides.credit=true;formFieldSources.credit='entry-default';
      input={...input,credit:value,timeOverrides:{...input.timeOverrides,credit:true}};
    }else if(source==='entry-default'&&!setting){
      setDurationPicker($('creditDisplay'),null);
      formTimeOverrides.credit=false;formFieldSources.credit='awaiting-role';
      input={...input,credit:null,timeOverrides:{...input.timeOverrides,credit:false}};
    }
  }
  let c=PilotLogEngine.activity(input),settings=timeGroupSettings();
  setDurationPicker($('schedBlockDisplay'),c.schedBlock);
  if(!formTimeOverrides.block&&dutyType==='Flight')setDurationPicker($('blockDisplay'),c.block);
  input={...input,block:optionalDuration($('blockDisplay')?.value)??c.block,totalDuty:optionalDuration($('totalDutyDisplay')?.value),simulatorTime:optionalDuration($('simulatorTimeDisplay')?.value),credit:optionalDuration($('creditDisplay')?.value)};
  const alwaysCalculated=new Set(['totalFlightTime','airTime','simulatorTime','sfiSfe','credit','totalDuty']);
  TIME_GROUP_DEFS.forEach(({key})=>{
    const el=$(TIME_GROUP_INPUT_IDS[key]);if(!el)return;
    if(key==='totalDuty'&&(controlledDuty(input)||requiresManualDutyControl(input))){
      const duty=controlledDuty(input),control=duty?provisionalDutyControl(duty.root,dutySequenceFor(duty.root).map(r=>r.id===input.id?input:r),duty.control):null;
      const lastId=control?.endEntryId||control?.memberIds?.at(-1)||duty?.root.id;
      setDurationPicker(el,control?.state==='CLOSED'&&input.id===(control.totalDutyAtLast?lastId:duty.root.id)?controlledDutyMinutes(control):null);
      if(control?.provisional&&control.state==='CLOSED'&&input.id===lastId&&!formTimeOverrides.offDuty)$('offDuty').value=hhmm(new Date(control.end));
      return;
    }
    if(key==='credit'&&formFieldSources.credit==='awaiting-role'&&['Simulator','Ground Course'].includes(dutyType)){
      setDurationPicker(el,null);return;
    }
    if(formTimeOverrides[key]||entryManualField(input,key)||entryFieldSource(input,key)==='manual')return;
    if(alwaysCalculated.has(key)||settings[key]?.autoSync===true){
      if(key==='simulatorTime'&&input.simulatorTimeImported===true&&el.value&&settings.simulatorTime.autoSync!==true)return;
      if(key==='credit'&&dutyType==='Simulator'&&!userIsSimTrainer(input)&&!input.creditImported&&!entryFieldIsSource(input,'credit')&&!entryManualField(input,'credit')){el.value='';return}
      if(key==='credit'&&dutyType==='Ground Course'&&!entryManualField(input,'credit')&&!entryFieldIsSource(input,'credit')&&!input.creditImported){
        const trainer=upper($('trainerTypeInput')?.value||'');
        setDurationPicker(el,trainer==='GI'?300:trainer==='TNE'?0:null);
        formFieldSources.credit='calculated';return;
      }
      if(key==='groundInstruction'&&!entryHasTimeValue(input,'groundInstruction')){el.value='';return}
      setDurationPicker(el,calculatedTimeGroupValue(input,key));formFieldSources[key]='calculated';
    }
  });
  c=PilotLogEngine.activity({...input,simulatorTime:optionalDuration($('simulatorTimeDisplay')?.value),totalDuty:optionalDuration($('totalDutyDisplay')?.value),credit:optionalDuration($('creditDisplay')?.value)});
  $('role').value=formRoleValue(input);$('seatPosition').value=input.rhs?'Right':'Left';$('ifr').value='';$('pf').value=input.pf;
  if(dutyType==='DHD'||dutyType==='DHP')$('calcPreview').textContent=`${dutyType} • ${$('dep').value||'—'} → ${$('arr').value||'—'} • ${$('schedOut').value||'--:--'}–${$('schedIn').value||'--:--'} • Credit ${fmt(c.credit)}`;
  else if(dutyType==='Ground Course')$('calcPreview').textContent=`Ground Course • ${$('dep').value||'—'} • ${$('courseType').value||'Course'}${$('onDuty').value?' • Start '+$('onDuty').value+' Z':''} • Credit ${fmt(c.credit)}`;
  else if(dutyType==='STBY')$('calcPreview').textContent=`STBY • ${$('dep').value||'—'} • ${$('onDuty').value||'--:--'}–${$('offDuty').value||'--:--'} • Credit ${fmt(c.credit)}`;
  else if(dutyType==='Simulator')$('calcPreview').textContent=`Schedule Block ${fmt(c.schedBlock)} • Simulator ${fmtOptional(optionalDuration($('simulatorTimeDisplay')?.value))||'—'} • Total Duty ${fmtOptional(optionalDuration($('totalDutyDisplay')?.value))||'—'} • Credit ${$('creditDisplay')?.value||'—'}`;
  else{const premium=dutyGetsMoroccoNightPremium({...formActivityInput(),dutyType:'Flight'})?` • Morocco Night +${appSettings().nightPremiumPct}%`:'';$('calcPreview').textContent=`Schedule Block ${fmt(c.schedBlock)} • Block ${fmt(c.block)} • Air Time ${$('airTimeDisplay')?.value||'—'} • Credit ${$('creditDisplay')?.value||'—'}${premium}`}
  updateDelayUI();document.querySelectorAll('[data-counter-shortcut]').forEach(button=>{const count=Number($(button.dataset.counterShortcut)?.value||0);button.querySelector('b').textContent=String(count);button.classList.toggle('count-active',count>0);button.disabled=!!$('dayTakeoffs')?.disabled});return c;
}
function updateSimulatorTrainerFromForm(){
  const checked=!!$('simTrainerCheck')?.checked;
  // Preserve the source role; the single checkbox controls the shared trainer rule.
  if(!checked){$('simInstructorCheck').checked=false;$('simExaminerCheck').checked=false}
  else if(!$('simInstructorCheck').checked&&!$('simExaminerCheck').checked)$('simInstructorCheck').checked=true;
  for(const key of ['simInstructor','simExaminer']){formManualFields[key]=true;formFieldSources[key]='manual'}
  calcEntry();silentAutosaveExisting();
}

/* Duty + EASA FTL — one deterministic operational-duty engine. */
function dtFromZulu(date,time){return zuluDate(date,time)}
function dutyDateEnd(date,start,end){const s=dtFromZulu(date,start),e=dtFromZulu(date,end);if(s&&e&&e<s)e.setUTCDate(e.getUTCDate()+1);return[s,e]}
function entryChrono(f){const t=f.out||f.schedOut||f.std||f.onDuty||'00:00';return `${f.date}T${t}`}
function compareLogbookEntries(a,b){return entryChrono(b).localeCompare(entryChrono(a))}
function dayEntries(date,fs=entryActivityRows()){return fs.filter(f=>f.date===date).sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)))}
function externalDutyForDate(date){return dutyActivityRows().filter(d=>d.date===date).sort((a,b)=>(a.report||'').localeCompare(b.report||''))[0]||null}

const MAX_REASONABLE_DUTY_MIN=18*60;
function hhmm(d){return `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}`}
function flightDutyWindow(f){
  if(!f||!f.date)return null;
  const explicit=controlledDuty(f);if(explicit){if(explicit.control.state!=='CLOSED')return null;const report=new Date(explicit.control.start),release=new Date(explicit.control.end);if(!Number.isFinite(+report)||!Number.isFinite(+release)||release<report)return null;return {entry:f,report,release,activityStart:report,activityEnd:release}}
  if(isPositioning(f))return null;
  const controlled=controlledDuty(f);if(controlled?.control.state==='OPEN'||requiresManualDutyControl(f)&&!controlled)return null;
  const scheduledDuty=isFlight(f)||isSim(f),reportTime=f.onDuty||(scheduledDuty&&f.schedOut?shiftTime(f.schedOut,-60):''),releaseTime=f.offDuty||(scheduledDuty&&f.schedIn?shiftTime(f.schedIn,30):'');
  if(!reportTime||!releaseTime)return null;
  const report=controlled?new Date(controlled.control.start):zuluDate(f.date,reportTime),release=controlled?new Date(controlled.control.end):dateAtOrAfter(f.date,releaseTime,report);if(!report||!release)return null;
  let activityStart=report,activityEnd=release;
  if(isFlight(f)){if(!f.out||!f.in)return null;activityStart=zuluDate(f.date,f.out);activityEnd=dateAtOrAfter(f.date,f.in,activityStart)}
  else if(isSim(f)){if(!f.schedOut||!f.schedIn)return null;activityStart=zuluDate(f.date,f.schedOut);activityEnd=dateAtOrAfter(f.date,f.schedIn,activityStart)}
  if(!activityStart||!activityEnd)return null;
  const own=Math.round((release-report)/60000);
  if(own<=0||!controlled&&own>MAX_REASONABLE_DUTY_MIN){console.warn('Ignoring unreasonable non-flight duty window',f.id,f.date,own);return null}
  return{entry:f,activityStart,activityEnd,report,release};
}
function buildDutySessions(fs=entryActivityRows()){
  const windows=(fs||[]).map(flightDutyWindow).filter(Boolean).sort((a,b)=>a.activityStart-b.activityStart);
  const groups=[];let cur=null;
  const push=()=>{if(cur)groups.push(cur);cur=null};
  for(const w of windows){
    if(!cur){cur={windows:[w],report:w.report,release:w.release,activityEnd:w.activityEnd};continue}
    const gap=Math.round((w.activityStart-cur.activityEnd)/60000);
    const proposedRelease=new Date(Math.max(cur.release.getTime(),w.release.getTime()));
    const proposedMinutes=Math.round((proposedRelease-cur.report)/60000);
    const control=controlledDuty(w.entry),previousControl=controlledDuty(cur.windows[0].entry);
    const sameControl=control&&previousControl&&control.root.id===previousControl.root.id;
    const sameDuty=control||previousControl?sameControl:gap>=-120&&gap<=300&&proposedMinutes>0&&proposedMinutes<=MAX_REASONABLE_DUTY_MIN;
    if(sameDuty){cur.windows.push(w);cur.release=proposedRelease;if(w.activityEnd>cur.activityEnd)cur.activityEnd=w.activityEnd}
    else{push();cur={windows:[w],report:w.report,release:w.release,activityEnd:w.activityEnd}}
  }
  push();
  return groups.map((g,i)=>{
    const entries=g.windows.map(x=>x.entry),flights=entries.filter(isFlight),minutes=Math.round((g.release-g.report)/60000);
    return{id:`duty-${g.report.toISOString()}-${i}`,reportDate:g.report.toISOString().slice(0,10),report:hhmm(g.report),endDate:g.release.toISOString().slice(0,10),end:hhmm(g.release),startDateTime:g.report,endDateTime:g.release,minutes,sectors:flights.length,entries,firstFlight:flights[0]||null,dep:flights[0]?.dep||entries[0]?.dep||'',arr:flights.at(-1)?.arr||entries.at(-1)?.arr||''};
  });
}
function totalDutyMinsForRows(rows){
  return (rows||[]).reduce((sum,row)=>sum+timeGroupFinalValue(row,'totalDuty',rows||[]),0);
}
// Duty button state is stored on the first flight in the same Core collection.
function isManualFlight(entry){return !!entry&&isFlight(entry)&&String(entry.source||'manual').toLowerCase()==='manual'}
// Preserve historical, explicitly entered duty times; never infer a new manual release.
function requiresManualDutyControl(entry){return !!entry&&isFlight(entry)&&(entry.manualDutyMode===true||isManualFlight(entry)&&(!entry.offDuty||entryFieldSource(entry,'offDuty')==='calculated'))}
// Only controls explicitly created/edited in 14.8 use a provisional endpoint.
function provisionalDutyControl(root,members,control){
  if(control.provisional!==true)return control;
  const ordered=[...members].sort((a,b)=>dutyActivityKey(a).localeCompare(dutyActivityKey(b))),last=ordered.at(-1);
  if(!last)return control;
  const finish=isFlight(last)?(last.in||last.schedIn):isSim(last)?last.schedIn:(last.dutyActivityEnd||last.schedIn||last.offDuty);
  const baseTime=isFlight(last)?last.out||last.schedOut:last.schedOut||last.onDuty;
  const base=finish?dateAtOrAfter(last.date,finish,zuluDate(last.date,baseTime)||new Date(control.start)):null;
  const end=base?new Date(+base+(isFlight(last)||isSim(last)?30:0)*60000):null;
  return {...control,memberIds:ordered.map(r=>r.id),endEntryId:last.id,state:end&&+end>=Date.parse(control.start)?'CLOSED':'OPEN',end:end&&+end>=Date.parse(control.start)?end.toISOString():'',endBasis:isFlight(last)&&last.in?'actual':'scheduled',totalDutyAtLast:true};
}
function refreshProvisionalDutyRecords(records,changedId){
  let result=records;
  const roots=records.filter(r=>r.dutyControl?.provisional===true&&(r.id===changedId||r.dutyControl.memberIds?.includes(changedId)));
  for(const root of roots){
    const ids=new Set(root.dutyControl.memberIds||[root.id]),members=result.filter(r=>ids.has(r.id)),control=provisionalDutyControl(root,members,root.dutyControl);
    result=result.map(r=>{
      if(!ids.has(r.id))return r;
      const next=controlledDutyFields(r,root,control);
      const changed=['onDuty','offDuty','totalDuty'].some(key=>next[key]!==r[key])||JSON.stringify(next.dutyControl)!==JSON.stringify(r.dutyControl)||next.coreInput?.totalDutyDisplay!==r.coreInput?.totalDutyDisplay;
      return changed?stamp(next):next;
    });
  }
  return result;
}
function totalDutyRowsForTotals(rows=coreActivityRows()){
  return rows.filter(r=>activityIncludedInResults(r)||controlledDuty(r)?.control.provisional!==undefined);
}
function controlledDutyMinutes(control){return control?.state==='CLOSED'?Math.max(0,Math.round((Date.parse(control.end)-Date.parse(control.start))/60000))||0:0}
function dutyEndTime(entry,fallback=''){
  const duty=controlledDuty(entry);
  if(duty)return duty.control.state==='CLOSED'?hhmm(new Date(duty.control.end)):'';
  return requiresManualDutyControl(entry)?'':entry?.offDuty||fallback;
}
function dutyEligible(entry){return !!entry&&canonicalDutyType(entry.dutyType||'Flight')!=='Day OFF'}
function dutySequenceFor(reference,rows=coreActivityRows()){
  const duty=controlledDuty(reference);if(!duty)return reference?[reference]:[];
  return [...new Set([duty.root.id,...(duty.control.memberIds||[])])].map(id=>rows.find(r=>r.id===id)).filter(Boolean);
}
function openDutyReference(reference=null){
  const own=controlledDuty(reference);if(own?.control.state==='OPEN')return own.root;
  const candidates=coreActivityRows().filter(f=>f.dutyControl?.state==='OPEN'&&(!reference?.screenshotDutyStart||f.dutyControl.start===reference.screenshotDutyStart));
  return candidates.length===1?candidates[0]:null;
}
function dutyLastId(duty){return duty.control.endEntryId||duty.control.memberIds?.at(-1)||duty.root.id}
function selectedDuty(){const root=coreActivityRows().find(r=>r.id===$('dutyTarget')?.value);return root?.dutyControl?{root,control:root.dutyControl}:null}
function dutyActivityKey(entry){return `${entry.date||''}T${entry.out||entry.schedOut||entry.dutyActivityReport||entry.onDuty||'00:00'}`}
function dutyRange(first,last,oldDuty){
  const from=dutyActivityKey(first),to=dutyActivityKey(last);
  if(to<from)throw new Error('The selected last activity is before the first activity. Correct the dates or press DUTY ON on the first activity.');
  const endpointDuty=controlledDuty(last);if(endpointDuty&&endpointDuty.root.id!==oldDuty?.root.id)throw new Error('The selected last activity belongs to another duty. Choose the duty of this activity.');
  const members=coreActivityRows().filter(r=>{const own=controlledDuty(r);return dutyEligible(r)&&dutyActivityKey(r)>=from&&dutyActivityKey(r)<=to&&(!own||own.root.id===oldDuty?.root.id)&&(!first.screenshotDutyStart||!r.screenshotDutyStart||r.screenshotDutyStart===first.screenshotDutyStart)}).sort((a,b)=>dutyActivityKey(a).localeCompare(dutyActivityKey(b)));
  return [first,...members.filter(r=>r.id!==first.id&&r.id!==last.id),...(last.id!==first.id?[last]:[])];
}
let dutyActionBusy=false;
async function runDutyAction(action){
  if(dutyActionBusy)return;dutyActionBusy=true;updateDutyControls();
  try{await action()}catch(error){console.error('Duty operation failed',error);alert(error.message||'Duty operation failed.')}finally{dutyActionBusy=false;updateDutyControls()}
}
async function saveDutyChange(first,members,control,oldDuty,selectedId){
  const before=coreActivityRows(),ids=new Set(members.map(r=>r.id)),oldIds=new Set(oldDuty?dutySequenceFor(oldDuty.root).map(r=>r.id):[]);
  const nextControl=provisionalDutyControl(first,members,{...control,totalDutyAtLast:true,id:control.id||makeActivityId(),memberIds:members.map(r=>r.id)});
  const changed=[];
  const after=before.map(row=>{
    if(ids.has(row.id)){const next=stamp(controlledDutyFields({...row,dutyControl:null},first,nextControl));changed.push(next);return next}
    if(oldIds.has(row.id)){const next=stamp({...row,dutyControl:null,dutyRootId:'',onDuty:'',offDuty:'',totalDuty:null,fieldSources:{...row.fieldSources,onDuty:'',offDuty:'',totalDuty:''},coreInput:row.coreInput?{...row.coreInput,onDuty:'',offDuty:'',totalDutyDisplay:''}:undefined});changed.push(next);return next}
    return row;
  });
  commitCoreActivities(after);
  try{await persistCoreActivities()}catch(error){commitCoreActivities(before);try{await persistCoreActivities()}catch{};throw new Error('Duty was not saved. The previous state has been restored; check storage and try again.')}
  changed.forEach(row=>markCloudEdited('activities',row,'duty-edit'));
  await persistCoreActivities();
  loadEntryToForm(coreActivityRows().find(r=>r.id===selectedId));scheduleAutoSync('duty-edit');
}
function dutyOn(){return runDutyAction(async()=>{
  saveEntryDraft();const current=coreActivityRows().find(f=>f.id===$('editId').value);
  if(!dutyEligible(current))return;
  const duty=selectedDuty()||controlledDuty(current);
  const departure=zuluDate(current.date,current.schedOut||'');
  const report=current.dutyActivityReport||current.onDuty;
  let start=duty?.root.id===current.id?new Date(duty.control.start):report?zuluDate(current.date,report):departure?new Date(+departure-(isFlight(current)||isSim(current)?60:0)*60000):null;
  if(duty?.root.id!==current.id&&report&&departure&&start>departure)start.setUTCDate(start.getUTCDate()-1);
  if(!start||!Number.isFinite(+start))throw new Error('Enter the reporting time and activity date before pressing DUTY ON.');
  const last=duty?coreActivityRows().find(r=>r.id===dutyLastId(duty)):current;
  if(!last)throw new Error('The last activity is missing. Correct the duty endpoint first.');
  const members=dutyRange(current,last,duty),control={...(duty?.control||{}),state:'OPEN',provisional:false,start:start.toISOString(),end:'',endEntryId:'',endBasis:''};
  if(control.state==='CLOSED'&&Date.parse(control.end)<+start)throw new Error('Duty start must not be after duty end. Correct the dates or times.');
  await saveDutyChange(current,members,control,duty,current.id);
})}
function dutyOff(){return runDutyAction(async()=>{
  saveEntryDraft();const current=coreActivityRows().find(f=>f.id===$('editId').value);
  if(!dutyEligible(current))return;
  const open=openDutyReference(current),duty=selectedDuty()||controlledDuty(current)||(open?{root:open,control:open.dutyControl}:null);
  if(!duty)throw new Error('Select the duty to correct below the buttons, or open a duty with DUTY ON first.');
  const members=dutyRange(duty.root,current,duty),start=new Date(duty.control.start);
  const finish=isFlight(current)?current.in:isSim(current)?current.schedIn:(current.dutyActivityEnd||current.offDuty||current.schedIn);
  let end;
  if(duty.control.state==='CLOSED'&&duty.control.provisional!==true&&dutyLastId(duty)===current.id)end=new Date(duty.control.end);
  else{
    if(!finish)throw new Error(isFlight(current)?'Enter Actual IN of this flight.':'Enter the end time of this activity.');
    const activityStart=zuluDate(current.date,current.out||current.schedOut||current.dutyActivityReport||'')||start;
    const base=dateAtOrAfter(current.date,finish,activityStart);end=base?new Date(+base+(isFlight(current)||isSim(current)?30:0)*60000):null;
  }
  if(!end||!Number.isFinite(+end)||end<start)throw new Error('Check the activity date and end time.');
  await saveDutyChange(duty.root,members,{...duty.control,state:'CLOSED',provisional:false,endBasis:'actual',end:end.toISOString(),endEntryId:current.id},duty,current.id);
})}
function updateDutyControls(){
  const entry=coreActivityRows().find(f=>f.id===$('editId')?.value),duty=controlledDuty(entry),eligible=dutyEligible(entry);
  for(const [id,active] of [['dutyOnBtn',!!duty&&entry.id===duty.root.id],['dutyOffBtn',!!duty&&duty.control.state==='CLOSED'&&!duty.control.provisional&&entry.id===dutyLastId(duty)]]){const b=$(id);if(b){b.classList.toggle('duty-confirmed',active);b.setAttribute('aria-pressed',String(active));b.disabled=!eligible||dutyActionBusy}}
  const target=$('dutyTarget');if(target){
    const entryChanged=target.dataset.entryId!==entry?.id,previous=entryChanged?'':target.value;
    target.dataset.entryId=entry?.id||'';
    const roots=coreActivityRows().filter(r=>r.dutyControl).sort((a,b)=>String(b.dutyControl.start).localeCompare(String(a.dutyControl.start)));
    target.innerHTML='<option value="">New duty / choose duty to correct</option>'+roots.map(r=>`<option value="${esc(r.id)}">${esc(r.date)} · ${esc(r.flightNo||r.dutyType)} · ${esc(r.dutyControl.provisional?'PROVISIONAL':r.dutyControl.state)}</option>`).join('');
    target.value=entryChanged?(duty?.root.id||''):previous;
    target.disabled=!eligible||dutyActionBusy;
  }
  for(const [field,key,endpoint] of [['dutyStartDate','start',!!duty&&entry.id===duty.root.id],['dutyEndDate','end',!!duty&&duty.control.state==='CLOSED'&&entry.id===dutyLastId(duty)]]){const el=$(field);if(el){el.hidden=!endpoint;el.disabled=!endpoint;el.value=endpoint?duty.control[key].slice(0,10):''}}
  if(duty){$('onDuty').disabled=false;$('offDuty').disabled=false}
  if($('onDuty'))$('onDuty').readOnly=!!duty&&entry.id!==duty.root.id;
  if($('offDuty'))$('offDuty').readOnly=!!duty?(duty.control.state==='CLOSED'?entry.id!==dutyLastId(duty):isFlight(entry)||isSim(entry)):false;
  const status=$('dutyState');if(status)status.textContent=duty?`${duty.control.provisional?'PROVISIONAL · '+(duty.control.endBasis==='actual'?'Actual IN':'Scheduled IN'):duty.control.state} · ${dutySequenceFor(duty.root).length} activities`:'';
}
function dutyClockOnDate(value,reference){
  if(!/^([01]\d|2[0-3]):[0-5]\d$/.test(value||'')||!Number.isFinite(reference?.getTime()))return null;
  const date=zuluDate(reference.toISOString().slice(0,10),value),delta=date-reference;
  if(delta>12*3600000)date.setUTCDate(date.getUTCDate()-1);
  else if(delta< -12*3600000)date.setUTCDate(date.getUTCDate()+1);
  return date;
}
function controlledDutyFields(entry,root,control){
  const closed=control.state==='CLOSED',timeOverrides={...(entry.timeOverrides||{})},manualFields={...(entry.manualFields||{})};
  for(const key of ['onDuty','offDuty','totalDuty']){if(key==='onDuty'&&entry.id===root.id&&control.startBasis==='manual')continue;delete timeOverrides[key];delete manualFields[key]}
  const lastId=control.endEntryId||(control.memberIds||[root.id]).at(-1);
  const onDuty=entry.id===root.id?hhmm(new Date(control.start)):'',offDuty=closed&&entry.id===lastId?hhmm(new Date(control.end)):'',totalDuty=closed?(entry.id===(control.totalDutyAtLast?lastId:root.id)?controlledDutyMinutes(control):(control.totalDutyAtLast?null:0)):null;
  return {...entry,dutyActivityReport:entry.dutyActivityReport||(entry.fieldSources?.onDuty!=='duty-control'?entry.onDuty:'')||'',dutyActivityEnd:entry.dutyActivityEnd||(entry.fieldSources?.offDuty!=='duty-control'?entry.offDuty:'')||'',onDuty,offDuty,totalDuty,timeOverrides,manualFields,fieldSources:{...(entry.fieldSources||{}),onDuty:entry.id===root.id&&control.startBasis==='manual'?'manual':'duty-control',offDuty:closed?'duty-control':'',totalDuty:'duty-control'},dutyRootId:root.id,...(entry.id===root.id?{dutyControl:control}:{}),...(entry.coreInput?{coreInput:{...entry.coreInput,onDuty,offDuty,totalDutyDisplay:totalDuty===null?'':fmt(totalDuty)}}:{})};
}
function storeDutyControl(root,members,control,reason){
  control={...control,memberIds:members.map(f=>f.id)};
  for(const f of members){const row=upsertCoreActivity(stamp(controlledDutyFields(f,root,control)));markCloudEdited('activities',row,reason)}
}
function applyDutyToEntry(entry){
  const duty=controlledDuty(entry);
  if(duty)return controlledDutyFields(entry,duty.root,duty.control);
  return requiresManualDutyControl(entry)?{...entry,offDuty:'',totalDuty:null}:entry;
}
// Clearing the reporting time removes the entire explicit duty, including hidden links.
function resetControlledDuty(duty){
  const ids=new Set(dutySequenceFor(duty.root).map(r=>r.id));
  const changed=[];
  const after=coreActivityRows().map(row=>{
    if(!ids.has(row.id))return row;
    const timeOverrides={...row.timeOverrides},manualFields={...row.manualFields},fieldSources={...row.fieldSources};
    for(const key of ['onDuty','offDuty','totalDuty']){delete timeOverrides[key];delete manualFields[key];fieldSources[key]=''}
    // Explicit empty clocks must not be repopulated by schedule helpers.
    timeOverrides.onDuty=true;timeOverrides.offDuty=true;
    const next=stamp({...row,dutyControl:null,dutyRootId:'',dutyActivityReport:'',dutyActivityEnd:'',onDuty:'',offDuty:'',totalDuty:null,
      ...(isFlight(row)?{manualDutyMode:true}:{}),timeOverrides,manualFields,fieldSources,
      ...(row.coreInput?{coreInput:{...row.coreInput,onDuty:'',offDuty:'',totalDutyDisplay:'',dutyStartDate:'',dutyEndDate:''}}:{})});
    changed.push(next);return next;
  });
  commitCoreActivities(after);
  changed.forEach(row=>markCloudEdited('activities',row,'duty-reset'));
  for(const key of ['onDuty','offDuty','totalDuty']){delete formTimeOverrides[key];delete formManualFields[key];delete formFieldSources[key]}
  formTimeOverrides.onDuty=true;formTimeOverrides.offDuty=true;
  for(const id of ['onDuty','offDuty','totalDutyDisplay','dutyStartDate','dutyEndDate'])if($(id))$(id).value='';
  if($('dutyTarget'))$('dutyTarget').value='';
  updateDutyControls();scheduleAutoSync('duty-reset');
}
function syncControlledDutyEdit(){
  const entry=coreActivityRows().find(f=>f.id===$('editId')?.value),duty=controlledDuty(entry);if(!duty)return;
  if(entry.id===duty.root.id&&formTimeOverrides.onDuty&&!$('onDuty').value&&!$('onDuty').validity?.badInput){resetControlledDuty(duty);return}
  const control={...duty.control};let changed=false;
  if(entry.id===dutyLastId(duty)&&control.state==='CLOSED'&&formTimeOverrides.offDuty&&!$('offDuty').value&&!$('offDuty').validity?.badInput){
    Object.assign(control,{state:'OPEN',provisional:false,end:'',endEntryId:'',endBasis:'',minutes:0});
    storeDutyControl(duty.root,dutySequenceFor(duty.root),control,'duty-reopen');
    if($('totalDutyDisplay'))$('totalDutyDisplay').value='';
    updateDutyControls();scheduleAutoSync('duty-reopen');return;
  }
  if(entry.id===duty.root.id&&formTimeOverrides.onDuty&&($('onDuty').value!==hhmm(new Date(control.start))||$('dutyStartDate').value!==control.start.slice(0,10))){const start=zuluDate($('dutyStartDate').value||control.start.slice(0,10),$('onDuty').value);if(start&&Number.isFinite(+start)){control.start=start.toISOString();changed=true}}
  if(entry.id===(control.endEntryId||control.memberIds?.at(-1))&&control.state==='CLOSED'&&formTimeOverrides.offDuty&&($('offDuty').value!==hhmm(new Date(control.end))||$('dutyEndDate').value!==control.end.slice(0,10))){const end=zuluDate($('dutyEndDate').value||control.end.slice(0,10),$('offDuty').value);if(end&&Number.isFinite(+end)){control.end=end.toISOString();control.provisional=false;control.endBasis='manual';changed=true}}
  if(changed&&(control.state==='OPEN'||Date.parse(control.end)>=Date.parse(control.start))){control.minutes=controlledDutyMinutes(control);storeDutyControl(duty.root,dutySequenceFor(duty.root),control,'duty-edit')}
}
function controlledDuty(entry){
  const storedRoot=entry?.dutyRootId&&!entry.dutyControl?operationalDatabase?.records.find(f=>isEntryActivityRecord(f)&&f.id===entry.dutyRootId):null;
  const root=entry?.dutyControl?entry:storedRoot?copyJson(storedRoot):null;
  return root?.dutyControl?{root,control:root.dutyControl}:null;
}

function reconcileAllDuties(){
  return buildDutySessions(entryActivityRows());
}

/* EASA FTL — basic maximum daily FDP, acclimatised, no extension. ORO.FTL.205 Table 2. */
const FDP_TABLE=[
{a:360,b:809,v:[780,750,720,690,660,630,600,570,540]},
{a:810,b:839,v:[765,735,705,675,645,615,585,555,540]},
{a:840,b:869,v:[750,720,690,660,630,600,570,540,540]},
{a:870,b:899,v:[735,705,675,645,615,585,555,540,540]},
{a:900,b:929,v:[720,690,660,630,600,570,540,540,540]},
{a:930,b:959,v:[705,675,645,615,585,555,540,540,540]},
{a:960,b:989,v:[690,660,630,600,570,540,540,540,540]},
{a:990,b:1019,v:[675,645,615,585,555,540,540,540,540]},
{a:1020,b:1439,v:[660,630,600,570,540,540,540,540,540]},
{a:0,b:299,v:[660,630,600,570,540,540,540,540,540]},
{a:300,b:314,v:[720,690,660,630,600,570,540,540,540]},
{a:315,b:329,v:[735,705,675,645,615,585,555,540,540]},
{a:330,b:344,v:[750,720,690,660,630,600,570,540,540]},
{a:345,b:359,v:[765,735,705,675,645,615,585,555,540]}
];
function fdpLimit(localStartMins,sectors){const row=FDP_TABLE.find(r=>localStartMins>=r.a&&localStartMins<=r.b);if(!row)return 0;const idx=Math.max(0,Math.min(8,(sectors<=2?0:sectors-2)));return row.v[idx]}
function tzParts(date,tz){try{const p=new Intl.DateTimeFormat('en-GB',{timeZone:tz,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(date);const h=Number(p.find(x=>x.type==='hour')?.value||0),m=Number(p.find(x=>x.type==='minute')?.value||0);return h*60+m}catch{return date.getUTCHours()*60+date.getUTCMinutes()}}
function startOfDays(n,now=new Date()){const d=new Date(now);d.setUTCHours(0,0,0,0);d.setUTCDate(d.getUTCDate()-n+1);return d}
function rollingFlight(n,now=new Date()){
  const c=startOfDays(n,now);
  return sum(entryActivityRows().filter(isFlight),f=>{const d=dateOnly(f.date);return d>=c&&d<=now?totalFlightMins(f):0});
}
function rollingDuty(n,now=new Date(),sessions=buildDutySessions()){
  const start=startOfDays(n,now);
  return sessions.reduce((total,s)=>{
    const a=new Date(Math.max(s.startDateTime.getTime(),start.getTime())),b=new Date(Math.min(s.endDateTime.getTime(),now.getTime()));
    return total+(b>a?Math.round((b-a)/60000):0);
  },0);
}
function calendarYearFlight(now=new Date()){const y=now.getUTCFullYear();return sum(entryActivityRows().filter(isFlight),f=>{const d=dateOnly(f.date);return d.getUTCFullYear()===y&&d<=now?totalFlightMins(f):0})}
function rollingCalendar12Flight(now=new Date()){const start=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth()-11,1));return sum(entryActivityRows().filter(isFlight),f=>{const d=dateOnly(f.date);return d>=start&&d<=now?totalFlightMins(f):0})}
function ftlLimitState(value,limit){
  const remaining=Number(limit||0)-Number(value||0);
  if(remaining<0)return{remaining,state:'red',textClass:'danger-text'};
  if(remaining<=300)return{remaining,state:'yellow',textClass:'warning'};
  return{remaining,state:'green',textClass:'success'};
}
function limitRow(name,value,limit){
  const s=ftlLimitState(value,limit);
  return `<div class="stat-row ftl-limit-row ftl-${s.state}"><span>${esc(name)}</span><b class="${s.textClass}">${fmt(value)} / ${fmt(limit)}</b></div>`;
}
async function sessionFdp(s){
  if(!s)return null;const dep=s.firstFlight?.dep||s.dep||'',a=dep?await airport(dep):null,localM=tzParts(s.startDateTime,a?.tz||'UTC'),sectors=Math.max(1,Number(s.sectors)||1),limit=fdpLimit(localM,sectors),margin=limit-s.minutes;
  return{date:s.reportDate,minutes:s.minutes,sectors,limit,margin,status:margin<0?'VIOLATION':margin<=300?'CAUTION':'OK',startDateTime:s.startDateTime};
}
async function rosterFdp(g){
  if(!g?.date||!g.start)return null;const start=zuluDate(g.date,g.start);if(!start)return null;const a=g.dep?await airport(g.dep):null,localM=tzParts(start,a?.tz||'UTC'),sectors=Math.max(1,Number(g.sectors)||1),minutes=g.end?timeSpanMins(g.start,g.end):0,limit=fdpLimit(localM,sectors),margin=limit-minutes;
  return{date:g.date,minutes,sectors,limit,margin,status:margin<0?'VIOLATION':margin<=300?'CAUTION':'OK',startDateTime:start};
}
async function nextFdpCandidate(sessions,now=new Date()){
  const futureSession=sessions.filter(s=>s.sectors>0&&s.startDateTime>now).sort((a,b)=>a.startDateTime-b.startDateTime)[0]||null;
  let futureRoster=null;
  const group=rosterGroups().filter(g=>g.kind==='flight').map(g=>({...g,_start:zuluDate(g.date,g.start)})).filter(g=>g._start&&g._start>now).sort((a,b)=>a._start-b._start)[0]||null;
  if(group)futureRoster=await rosterFdp(group);
  const entryCandidate=futureSession?await sessionFdp(futureSession):null;
  if(entryCandidate&&futureRoster)return entryCandidate.startDateTime<=futureRoster.startDateTime?entryCandidate:futureRoster;
  return entryCandidate||futureRoster;
}
async function renderFtl(containerId,compact=false){
  const now=new Date(),sessions=buildDutySessions(),d7=rollingDuty(7,now,sessions),d14=rollingDuty(14,now,sessions),d28=rollingDuty(28,now,sessions),f28=rollingFlight(28,now),fy=calendarYearFlight(now),f12=rollingCalendar12Flight(now);
  const currentSession=sessions.find(s=>s.sectors>0&&s.startDateTime<=now&&s.endDateTime>=now)||null;
  let current=currentSession?await sessionFdp(currentSession):null,dailyLabel=current?'Current FDP':'Next planned FDP';
  if(!current)current=await nextFdpCandidate(sessions,now);
  if(!current){const last=sessions.filter(s=>s.sectors>0&&s.endDateTime<now).sort((a,b)=>b.endDateTime-a.endDateTime)[0]||null;current=last?await sessionFdp(last):null;dailyLabel='Last recorded FDP'}
  const maxDaily=current?(()=>{const s=ftlLimitState(current.minutes,current.limit);return `<div class="stat-row ftl-limit-row ftl-${s.state}"><span>${esc(dailyLabel)} — ${esc(displayDate(current.date))} • ${current.sectors} sector${current.sectors===1?'':'s'}</span><b class="${s.textClass}">${fmt(current.minutes)} / ${fmt(current.limit)}</b></div>`})():'';
  const rows=maxDaily+limitRow('Duty — 7 consecutive days',d7,3600)+limitRow('Duty — 14 consecutive days',d14,6600)+limitRow('Duty — 28 consecutive days',d28,11400)+limitRow('Flight time — 28 consecutive days',f28,6000)+limitRow('Flight time — calendar year',fy,54000)+limitRow('Flight time — 12 calendar months',f12,60000);
  let dailyHtml='';
  if(!compact){const recent=sessions.filter(s=>s.sectors>0&&s.startDateTime<=now).sort((a,b)=>b.startDateTime-a.startDateTime).slice(0,10),detailed=[];for(const s of recent){const x=await sessionFdp(s);if(x&&x.limit)detailed.push(x)}dailyHtml=detailed.map(x=>{const s=ftlLimitState(x.minutes,x.limit);return `<div class="stat-row ftl-limit-row ftl-${s.state}"><span>${esc(displayDate(x.date))} • ${x.sectors} sector${x.sectors===1?'':'s'}</span><b class="${s.textClass}">${fmt(x.minutes)} / ${fmt(x.limit)} • ${x.status}</b></div>`}).join('')}
  $(containerId).innerHTML=rows+dailyHtml;
  return d7<=3600&&d14<=6600&&d28<=11400&&f28<=6000&&fy<=54000&&f12<=60000;
}

/* Roster local display and grouping */
function parseUtcForRoster(date,time){return zuluDate(date,time)}
function localTime(date,time,code){if(!time)return'';const a=airportIndex[upper(code)],d=parseUtcForRoster(date,time);if(!d)return time;try{return new Intl.DateTimeFormat('en-GB',{timeZone:a?.tz||'UTC',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(d)}catch{return time}}
function rosterFlightDigits(v){
  const s=cleanPrefix(v);
  let m=s.match(/^3O(\d+)$/i);if(m)return m[1];
  const prefix=cleanPrefix(appSettings().flightPrefix||'MAC');
  if(prefix&&s.startsWith(prefix)&&/^\d+$/.test(s.slice(prefix.length)))return s.slice(prefix.length);
  m=s.match(/(\d+)$/);return m?m[1]:s;
}
function rosterFlightLabel(v){
  const d=rosterFlightDigits(v);
  return /^\d+$/.test(d)?`3O${d}`:upper(v);
}
function rosterSectorKey(r){
  // Same physical sector can arrive twice (e.g. 3O485 and MAC485).
  return [r.date,rosterFlightDigits(r.flightNo),upper(r.dep),upper(r.arr),r.std||'',r.sta||''].join('|');
}
function dedupeRosterItems(items){
  const map=new Map();
  items.forEach(r=>{
    const k=rosterSectorKey(r),cur=map.get(k);
    if(!cur){map.set(k,r);return}
    // Prefer the 3O-labelled source for the roster display when both exist.
    const preferred=/^3O/i.test(cleanPrefix(r.flightNo))&&!/^3O/i.test(cleanPrefix(cur.flightNo))?r:cur;
    // The roster badge reflects the same Core activity: a locked Logbook copy
    // therefore remains green even when a roster-labelled duplicate supplies
    // the display text for that physical sector.
    map.set(k,{...preferred,locked:!!(cur.locked||r.locked),status:cur.status==='done'||r.status==='done'?'done':preferred.status});
  });
  return [...map.values()];
}

function rosterValueCompatible(a,b){return !a||!b||upper(a)===upper(b)}
function rosterTimeCompatible(a,b){return !a||!b||String(a)===String(b)}
function rosterSectorSameIdentity(a,b){
  if(!a||!b||a===b||a.date!==b.date)return false;
  const fa=rosterFlightDigits(a.flightNo||''),fb=rosterFlightDigits(b.flightNo||'');
  if(fa&&fb&&fa!==fb)return false;
  if(!rosterValueCompatible(a.dep,b.dep)||!rosterValueCompatible(a.arr,b.arr))return false;
  if(!rosterTimeCompatible(a.std,b.std)||!rosterTimeCompatible(a.sta,b.sta))return false;
  if(fa&&fb)return true;
  const sameRoute=upper(a.dep||'')===upper(b.dep||'')&&upper(a.arr||'')===upper(b.arr||'')&&!!(a.dep||a.arr);
  const sharedTime=(a.std&&a.std===b.std)||(a.sta&&a.sta===b.sta);
  return sameRoute&&(sharedTime||(!a.std&&!a.sta)||(!b.std&&!b.sta));
}
function rosterSectorScore(r){
  return ['flightNo','dep','arr','std','sta','type','reg','remarks'].reduce((n,k)=>n+(r?.[k]?1:0),0)+(r?.status==='done'?2:0);
}
function mergeRosterSector(a,b){
  const primary=rosterSectorScore(a)>=rosterSectorScore(b)?a:b,secondary=primary===a?b:a;
  const merged={...primary};
  ['flightNo','dep','arr','std','sta','type','reg','remarks','source'].forEach(k=>{if(!merged[k]&&secondary[k])merged[k]=secondary[k]});
  if(a.status==='done'||b.status==='done')merged.status='done';
  merged._syncRev=Math.max(recordRevision(a),recordRevision(b));
  merged._updatedAt=[a._updatedAt,b._updatedAt].filter(Boolean).sort().pop()||new Date().toISOString();
  return merged;
}
function rosterActivityStart(f){return f?.onDuty||f?.schedOut||f?.out||''}
function rosterActivityEnd(f){return f?.offDuty||f?.schedIn||f?.in||f?.on||''}
function rosterActivityType(f){return upper(f?.dutyType||'')}
function rosterActivityScore(f){
  return ['flightNo','dep','arr','onDuty','offDuty','schedOut','schedIn','out','in','type','courseType','remarks'].reduce((n,k)=>n+(f?.[k]?1:0),0)+(f?.locked?2:0);
}
function rosterActivitySameIdentity(a,b){
  if(!a||!b||a===b||a.date!==b.date)return false;
  if(rosterActivityType(a)!==rosterActivityType(b))return false;
  if(!(isPositioning(a)||isSim(a)||isStby(a)||isGround(a)))return false;
  const sa=rosterActivityStart(a),sb=rosterActivityStart(b),ea=rosterActivityEnd(a),eb=rosterActivityEnd(b);
  const oneUntimed=(!sa&&!ea)||(!sb&&!eb);
  const sharedTime=(sa&&sb&&sa===sb)||(ea&&eb&&ea===eb);
  if(!oneUntimed&&!sharedTime)return false;
  if(isPositioning(a)){
    if(!rosterValueCompatible(a.dep,b.dep)||!rosterValueCompatible(a.arr,b.arr))return false;
    const fa=rosterFlightDigits(a.flightNo||''),fb=rosterFlightDigits(b.flightNo||'');
    if(fa&&fb&&fa!==fb)return false;
    return true;
  }
  if(isSim(a))return rosterValueCompatible(a.type,b.type)&&rosterValueCompatible(a.dep,b.dep);
  if(isGround(a))return rosterValueCompatible(a.dep,b.dep)&&rosterValueCompatible(a.courseType||a.remarks,b.courseType||b.remarks);
  if(isStby(a))return rosterValueCompatible(a.dep,b.dep);
  return false;
}
function mergeRosterActivity(a,b){
  const primary=rosterActivityScore(a)>=rosterActivityScore(b)?a:b,secondary=primary===a?b:a;
  const merged={...primary};
  Object.keys(secondary).forEach(k=>{if((merged[k]===undefined||merged[k]===null||merged[k]==='')&&secondary[k]!==undefined&&secondary[k]!==null&&secondary[k]!=='')merged[k]=secondary[k]});
  merged.locked=!!(a.locked||b.locked);
  merged._syncRev=Math.max(recordRevision(a),recordRevision(b));
  merged._updatedAt=[a._updatedAt,b._updatedAt].filter(Boolean).sort().pop()||new Date().toISOString();
  return merged;
}
function cleanupRosterDuplicates(){
  // SAFETY INVARIANT: Roster rendering/import must never merge or delete activities.
  // Duplicate-looking records are preserved for explicit user review.
  return 0;
}

function rosterGroups(){
  const rows=displayActivityRows();
  const home=upper(appSettings().homeBase||'CMN');
  const rs=rosterFlightRows(rows).sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
  const ds=dutyActivityRows(rows),by={},groups=[];

  // AeroLINE crewScheduleBlockId is the strongest duty-group signal. Fall back to date when absent.
  rs.forEach(r=>{const key=r.screenshotDutyStart?`screenshot:${r.screenshotDutyStart}`:r.aerolineBlockId?`${r.date}|${r.aerolineBlockId}`:r.date;(by[key]||(by[key]=[])).push(r)});
  Object.entries(by).forEach(([,rawItems])=>{
    const date=rawItems[0]?.screenshotDutyStart?.slice(0,10)||rawItems[0]?.date||'';
    const items=dedupeRosterItems(rawItems);
    items.sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
    if(!items.length)return;

    const duty=ds.find(d=>d.date===date&&/flight duty/i.test(d.type||''));
    const first=items[0],last=items[items.length-1];
    const outboundDest=(items.find(x=>upper(x.dep)===home&&upper(x.arr)!==home)?.arr)||first.arr||last.arr||'';
    const start=first.onDuty||first.screenshotDutyStart?.slice(11,16)||duty?.report||shiftTime(first.std,-60);
    const end=dutyEndTime(last,duty?.end||shiftTime(last.sta,30));

    groups.push({
      kind:'flight',date,items,start,end,
      dep:first.dep||home,arr:outboundDest,
      sectors:items.filter(x=>x.flightNo).length,
      status:items.every(x=>x.status==='done')?'done':'planned',
      sortTime:start||first.std||'99:99'
    });
  });

  // SIM, STBY, DHD, DHP and Ground Course are stored as Entries, not roster sectors.
  const activities=rows.filter(f=>
    f?.date&&!f.rosterHidden&&(isSim(f)||isStby(f)||isDhd(f)||isDhp(f)||isGround(f))
  );

  activities.forEach(f=>{
    const start=f.onDuty||f.schedOut||f.out||'';
    const end=f.offDuty||f.schedIn||f.in||f.on||'';
    const dep=isPositioning(f)?upper(f.dep||''):(isSim(f)?upper(f.location||f.dep||home):upper(f.dep||home)),arr=isPositioning(f)?upper(f.arr||''):(isSim(f)?dep:upper(f.arr||f.dep||home));

    groups.push({
      kind:'activity',
      activityType:canonicalDutyType(f.dutyType||'Duty','Duty'),
      date:f.date,items:[],entryId:f.id,
      start,end,dep,arr,sectors:0,
      status:activityIncludedInResults(f)?'done':'planned',
      sortTime:start||'99:99',
      courseType:f.courseType||'',
      aircraftType:f.type||'',
      flightNo:f.flightNo||'',
      remarks:f.remarks||''
    });
  });

  // Duty-type activities are records in the same Core collection. Surface them
  // only when an equivalent operational activity is not already present.
  ds.filter(d=>d?.date&&!/flight duty/i.test(d.type||'')).forEach(d=>{
    const duplicate=activities.some(f=>{
      if(f.date!==d.date||canonicalDutyType(f.dutyType||'','Duty')!==canonicalDutyType(d.type||'','Duty'))return false;
      const fs=f.onDuty||f.schedOut||f.out||'',fe=f.offDuty||f.schedIn||f.in||f.on||'';
      return (!d.report||!fs||d.report===fs)&&(!d.end||!fe||d.end===fe);
    });
    if(duplicate)return;

    groups.push({
      kind:'duty',
      activityType:canonicalDutyType(d.type||'Duty','Duty'),
      date:d.date,items:[],dutyId:d.id,
      start:d.report||'',end:d.end||'',
      dep:home,arr:home,sectors:0,status:activityIncludedInResults(d)?'done':'planned',
      sortTime:d.report||'99:99',
      remarks:d.notes||''
    });
  });

  return groups.sort((a,b)=>
    `${a.date}${a.sortTime||'99:99'}${a.kind||''}`.localeCompare(
      `${b.date}${b.sortTime||'99:99'}${b.kind||''}`
    )
  );
}

function savedEntryForRosterSector(r){
  if(!r||!activityIncludedInResults(r))return null;
  return coreActivityRows().find(x=>x.id===r.id&&isFlight(x))||null;
}
function rosterSectorCompleted(r){return !!r&&activityIncludedInResults(r)}
const CREW_NAME_FIELDS=['picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name'];
function crewCarryData(entry){
  if(!entry)return null;
  const crew={};
  CREW_NAME_FIELDS.forEach(key=>{crew[key]=entry[key]??''});
  for(let i=0;i<3;i++){const key=`attendant${i+1}Name`;if(!Object.prototype.hasOwnProperty.call(entry,key))crew[key]=entry.cabinCrew?.[i]||''}
  return {...crew,role:entry.role||'PIC',rolePic:entry.rolePic===true||entry.role==='PIC',cabinCrew:[crew.attendant1Name,crew.attendant2Name,crew.attendant3Name],observerCrew:Array.isArray(entry.observerCrew)?[...entry.observerCrew]:[],studentName:entry.studentName||'',instruction:entry.instruction===true,examiner:entry.examiner===true};
}
function formCrewSnapshot(){
  const crew=crewCarryData(coreActivityRows().find(f=>f.id===$('editId')?.value)||{});
  CREW_NAME_FIELDS.forEach(key=>{if($(key))crew[key]=upper($(key).value||'')});
  return {...crew,role:$('rolePicCheck')?.checked?'PIC':'SIC',rolePic:!!$('rolePicCheck')?.checked,cabinCrew:[crew.attendant1Name,crew.attendant2Name,crew.attendant3Name],instruction:!!$('instructionCheck')?.checked,examiner:!!$('examinerCheck')?.checked};
}
function applyCrewToForm(crew){
  if(!crew)return;
  $('rolePicCheck').checked=crew.role==='PIC';$('role').value=$('rolePicCheck').checked?'PIC':'SIC';
  CREW_NAME_FIELDS.forEach(key=>{if($(key))$(key).value=upper(crew[key]||'')});
  $('instructionCheck').checked=crew.instruction===true;$('examinerCheck').checked=crew.examiner===true;
}
function carryCrewToRosterFlight(entry){
  const aeroline=entry.source==='aeroline';
  if(!isFlight(entry)||entry.locked||entry.crewCarryApplied&&!aeroline||String(entry.source||'').toLowerCase()==='logten')return entry;
  if(isManualFlight(entry)&&!controlledDuty(entry))return entry;
  const previous=(aeroline?previousAerolineReturnSector(entry):null)||rosterCarryDefaultsForItem(entry);if(!previous)return entry;
  const crew=crewCarryData(previous),existingCrew=crewCarryData(entry),row={...entry,crewCarryApplied:true};
  let changed=!entry.crewCarryApplied;
  if(!previous.reg||!entry.reg||upper(previous.reg)===upper(entry.reg)){
    for(const key of ['type','variant']){
      if(previous[key]&&!row[key]&&!entryManualField(entry,key)){
        row[key]=previous[key];changed=true;
        if(row.coreInput)row.coreInput={...row.coreInput,[key]:row[key]};
      }
    }
  }
  for(const [key,value] of Object.entries(crew)){
    if(entryManualField(entry,key))continue;
    const current=CREW_NAME_FIELDS.includes(key)?existingCrew[key]:entry[key],empty=current==null||current===''||Array.isArray(current)&&!current.length;
    if(entryFieldIsSource(entry,key)&&(!aeroline||!empty))continue;
    if(empty||['instruction','examiner'].includes(key)&&!current){
      const next=key==='cabinCrew'?[row.attendant1Name||'',row.attendant2Name||'',row.attendant3Name||'']:value;
      if(JSON.stringify(current)!==JSON.stringify(next)){row[key]=next;changed=true}
    }
  }
  if(!changed)return entry;
  if(['attendant1Name','attendant2Name','attendant3Name'].some(key=>row[key]!==entry[key])&&!entryManualField(entry,'cabinCrew'))row.cabinCrew=['attendant1Name','attendant2Name','attendant3Name'].map(key=>row[key]??existingCrew[key]??'');
  // Manual clears remain protected; empty unedited fields can receive names entered later.
  if(row.coreInput){row.coreInput={...row.coreInput};CREW_NAME_FIELDS.forEach(key=>row.coreInput[key]=row[key]||'');row.coreInput.instructionCheck=row.instruction===true;row.coreInput.examinerCheck=row.examiner===true}
  return row;
}
function previousAerolineReturnSector(entry){
  if(!isFlight(entry)||entry.source!=='aeroline'||!entry.date||!(entry.out||entry.schedOut)||!entry.dep||!entry.arr)return null;
  const earlier=coreActivityRows().filter(f=>f.id!==entry.id&&isFlight(f)&&f.source==='aeroline'&&f.date&&(f.out||f.schedOut)&&
    (f.date===entry.date||entry.aerolineBlockId&&f.aerolineBlockId===entry.aerolineBlockId||f.aerolineArrivalDate===entry.date)&&entryChrono(f)<entryChrono(entry))
    .sort((a,b)=>entryChrono(b).localeCompare(entryChrono(a)));
  const previous=earlier[0];
  if(!previous||earlier[1]&&entryChrono(earlier[1])===entryChrono(previous)||upper(previous.dep)!==upper(entry.arr)||upper(previous.arr)!==upper(entry.dep))return null;
  return previous;
}
function carryAircraftToAerolineReturn(entry){
  if(!isFlight(entry)||entry.source!=='aeroline'||entry.locked)return entry;
  const previous=previousAerolineReturnSector(entry);if(!previous)return entry;
  // A different aircraft already assigned to the return must not inherit this type.
  if(upper(entry.reg)&&upper(previous.reg)!==upper(entry.reg))return entry;
  const row={...entry,fieldSources:{...entry.fieldSources},manualFields:{...entry.manualFields}};let changed=false;
  for(const key of ['reg','type','variant']){
    const current=upper(entry[key]),prior=upper(previous[key]);
    const moreSpecific=key==='type'&&current&&prior.startsWith(current+'-');
    if(entryManualField(entry,key)||!prior||(current&&!moreSpecific))continue;
    if(key==='variant'&&upper(entry.type)&&upper(previous.type)&&aircraftIcaoType(entry)!==aircraftIcaoType(previous))continue;
    row[key]=upper(previous[key]);row.fieldSources[key]='previous-sector';row.manualFields[key]=false;changed=true;
    if(entry.coreInput){row.coreInput={...(row.coreInput||entry.coreInput),[key]:key==='reg'?aircraftIdInput(row[key]):row[key]}}
  }
  if(!changed)return entry;
  return row;
}
function rosterCarryDefaultsForItem(r){
  const duty=controlledDuty(r),group=duty?null:rosterGroups().find(g=>g.items.some(x=>x.id===r?.id));
  if(!duty&&!group)return null;
  const items=(duty?dutySequenceFor(duty.root):[...group.items]).sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
  const idx=items.findIndex(x=>x.id===r.id);
  if(idx<=0)return null;
  for(let i=idx-1;i>=0;i--){
    const prev=savedEntryForRosterSector(items[i]);
    if(!prev)continue;
    const previousDuty=controlledDuty(prev);if(previousDuty&&previousDuty.root.id!==duty?.root.id)continue;
    return{
      ...crewCarryData(prev),
      reg:prev.reg||'',type:prev.type||'',variant:prev.variant||'',role:prev.role||'PIC',rolePic:prev.rolePic===true||prev.role==='PIC',
      rhs:prev.rhs===true,seatPosition:prev.rhs===true?'Right':'',pf:'no'
    };
  }
  return null;
}

async function rosterGroupHtml(groups,interactive=false){
  if(!groups.length)return'<div class="empty">No roster activities for this day.</div>';
  let html='';

  for(const g of [...groups].sort((a,b)=>`${b.date}${b.sortTime||b.start||''}`.localeCompare(`${a.date}${a.sortTime||a.start||''}`))){
    const type=canonicalDutyType(g.activityType||'Duty','Duty'),u=upper(type);

    if(u==='DAY OFF'||u==='OFF'){
      const action=interactive&&g.dutyId?` data-edit-roster-duty="${g.dutyId}" role="button" tabindex="0"`:'';
      const deleteAction=interactive&&g.dutyId?`<button type="button" class="danger roster-delete-duty-btn" data-delete-roster-duty="${esc(g.dutyId)}">Delete duty</button>`:'';
      html+=`<div class="rowitem roster-agenda-row roster-day-off${action?' roster-agenda-clickable':''}"${action}><div><b>${esc(displayDate(g.date))} • Day OFF</b>${g.remarks?`<div class="small">${esc(g.remarks)}</div>`:''}</div><div class="meta"><span class="small">OFF</span>${deleteAction}</div></div>`;
      continue;
    }

    if(g.kind==='flight'){
      const dutyIds=(g.items||[]).map(x=>x.id).filter(Boolean).join(',');
      for(const r of [...g.items].sort(compareLogbookEntries)){
        const start=r.std?await localTime(r.date,r.std,r.dep):'--:--',end=r.sta?await localTime(r.date,r.sta,r.arr||r.dep):'--:--',done=rosterSectorCompleted(r);
        const info=r.aerolineTrainingLabel||r.aerolineTrainingDesc||'';
        const action=interactive?` data-roster-action="${r.id}" role="button" tabindex="0"`:'';
        html+=`<div class="rowitem roster-agenda-row${interactive?' roster-agenda-clickable':''}${done?' roster-all-completed':''}"${action}><div><b>${done?'✓ ':''}${esc(r.flightNo?rosterFlightLabel(r.flightNo):'Flight')}</b><div class="route">${esc(r.dep||'?')} → ${esc(r.arr||'?')}</div>${info?`<div class="small">${esc(info)}</div>`:''}</div><div class="meta"><b>${esc(start)} – ${esc(end)}</b><br><span class="small">local time${done?' • completed':''}</span></div></div>`;
      }
      if(interactive&&dutyIds)html+=`<div class="roster-duty-delete-row"><button type="button" class="danger roster-delete-duty-btn" data-delete-roster-duty="${esc(dutyIds)}">Delete duty</button></div>`;
      continue;
    }

    const start=g.start?await localTime(g.date,g.start,g.dep):'--:--';
    const end=g.end?await localTime(g.date,g.end,g.arr||g.dep):'--:--';

    let detail='';
    if(u==='DHD'||u==='DHP'){
      detail=(g.dep||g.arr)?`${esc(g.dep||'?')} → ${esc(g.arr||'?')}${g.flightNo?` • ${esc(g.flightNo)}`:''}`:'Route to complete';
    }else if(u==='SIMULATOR'){
      detail=[g.dep||'',g.aircraftType||'',g.remarks||g.courseType||''].filter(Boolean).map(esc).join(' • ');
    }else if(u==='GROUND COURSE'){
      detail=[g.dep||'',g.remarks||g.courseType||''].filter(Boolean).map(esc).join(' • ');
    }else{
      detail=esc(g.dep||appSettings().homeBase||'');
    }
    if(!detail&&g.remarks)detail=esc(g.remarks);

    let action='';
    if(interactive&&g.entryId)action=` data-edit-roster-entry="${g.entryId}" role="button" tabindex="0"`;
    else if(interactive&&g.dutyId)action=` data-edit-roster-duty="${g.dutyId}" role="button" tabindex="0"`;

    const deleteId=g.entryId||g.dutyId||'',deleteAction=interactive&&deleteId?`<button type="button" class="danger roster-delete-duty-btn" data-delete-roster-duty="${esc(deleteId)}">Delete duty</button>`:'';
    html+=`<div class="rowitem roster-agenda-row${action?' roster-agenda-clickable':''}"${action}><div><b>${esc(displayDate(g.date))} • ${esc(type)}</b>${detail?`<div class="small">${detail}</div>`:''}</div><div class="meta"><b>${esc(start)} – ${esc(end)}</b><br><span class="small">local time</span>${deleteAction}</div></div>`;
  }

  return html;
}

/* One activity collection. Roster and Logbook are filtered projections of the same records. */
function rosterMonthAvailable(month){
  if(!month)return false;
  return displayActivityRows().some(r=>inMonth(r.date,month)&&!r.rosterHidden);
}
function rosterPlanningEntriesForMonth(month){
  return displayActivityRows()
    .filter(r=>inMonth(r.date,month)&&!r.rosterHidden)
    .map(r=>activityIncludedInResults(r)?{...r,_payrollRosterSynthetic:false,_trainingAuthoritative:true}:{...r,onDuty:isFlight(r)?'':r.onDuty,offDuty:isFlight(r)?'':r.offDuty,_payrollRosterSynthetic:isFlight(r),_trainingAuthoritative:false})
    .sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
}
function operationalEntriesForMonth(month){
  return rosterPlanningEntriesForMonth(month);
}
function operationalEntriesForTrips(){
  const activities=displayActivityRows().filter(r=>!r.rosterHidden),byMonth=new Map();
  for(const r of activities){
    const month=String(r?.date||'').slice(0,7);if(!month)continue;
    if(!byMonth.has(month))byMonth.set(month,[]);
    byMonth.get(month).push(r);
  }
  const out=[];
  for(const month of [...byMonth.keys()].sort()){
    const rows=byMonth.get(month)||[];
    out.push(...rows.map(r=>activityIncludedInResults(r)?{...r,_payrollRosterSynthetic:false,_trainingAuthoritative:true}:{...r,onDuty:isFlight(r)?'':r.onDuty,offDuty:isFlight(r)?'':r.offDuty,_payrollRosterSynthetic:isFlight(r),_trainingAuthoritative:false}).sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b))));
  }
  return out.sort((a,b)=>(tripSequenceStart(a)?.getTime()||0)-(tripSequenceStart(b)?.getTime()||0));
}

/* Trips */
function overlapMins(a,b,s,e){const x=Math.max(a.getTime(),s.getTime()),y=Math.min(b.getTime(),e.getTime());return Math.max(0,Math.round((y-x)/60000))}
function tripChargeableDutyFromEntries(s,e,sourceEntries){
  const lower=new Date(s.getTime()-86400000).toISOString().slice(0,10),upperLimit=new Date(e.getTime()+86400000).toISOString().slice(0,10);
  let total=0;const seenDuties=new Set(),fs=(sourceEntries||[]).filter(f=>f.date&&f.date>=lower&&f.date<=upperLimit),dates=[...new Set(fs.map(f=>f.date).filter(Boolean))];
  dates.forEach(date=>{
    const entries=dayEntries(date,fs);
    for(const item of entries){
      const rule=activityRule(item),marker=tripSequenceStart(item)||entryStart(item)||zuluDate(date,item.onDuty||'00:00');
      if(!marker||marker<s||marker>e)continue;
      if(rule.trip==='fixed-ground')total+=PILOTLOG_RULES.fixed.groundTripDutyMins;
      else if(rule.trip==='credit')total+=creditMinsForEntry(item);
    }
    const dutyEntries=entries.filter(x=>{
      if(activityRule(x).trip!=='duty')return false;
      const duty=controlledDuty(x);
      if(duty){if(duty.control.state==='CLOSED'&&!seenDuties.has(duty.root.id)){seenDuties.add(duty.root.id);total+=overlapMins(new Date(duty.control.start),new Date(duty.control.end),s,e)}return false}
      return !requiresManualDutyControl(x);
    });if(!dutyEntries.length)return;
    const first=dutyEntries[0],last=dutyEntries[dutyEntries.length-1],startTime=first.onDuty||shiftTime(first.schedOut||first.out,-60),endTime=last.offDuty||shiftTime(last.schedIn||last.in,30);if(!startTime||!endTime)return;
    const [a,b]=dutyDateEnd(date,startTime,endTime);total+=overlapMins(a,b,s,e);
  });
  return total;
}
function tripChargeLabel(f){
  const rule=activityRule(f);if(rule.trip==='zero')return isStby(f)?'Not deducted':'Excluded from duty';if(rule.trip==='credit')return`Credit ${fmt(PilotLogEngine.credit(f))} deducted`;if(rule.trip==='fixed-ground')return`${fmt(PILOTLOG_RULES.fixed.groundTripDutyMins)} fixed duty`;return'Included in duty';
}

function tripChargeableDuty(s,e){return tripChargeableDutyFromEntries(s,e,operationalEntriesForTrips())}
function calculateTripMetrics(startValue,endValue,cashAmount=0,sourceEntries=null){
  const s=startValue instanceof Date?startValue:new Date(startValue),e=endValue instanceof Date?endValue:new Date(endValue);
  if(!Number.isFinite(s.getTime())||!Number.isFinite(e.getTime())||e<=s)return null;
  const trip=Math.round((e-s)/60000),duty=tripChargeableDutyFromEntries(s,e,sourceEntries||operationalEntriesForTrips()),layover=Math.max(0,trip-duty),allowance=Number(cashAmount||0)>0?0:(layover/60)*Number(paySettings().layoverRate||0);
  return{start:s,end:e,trip,duty,layover,allowance};
}
function calculateSavedTripProjection(savedTrip,sourceEntries=null){
  if(!savedTrip)return null;
  const metrics=calculateTripMetrics(savedTrip.start,savedTrip.end,Number(savedTrip.cashAmount||0),sourceEntries||operationalEntriesForTrips());
  if(!metrics)return null;
  const layover=savedTrip.tripOverrides?.layover??metrics.layover,allowance=Number(savedTrip.cashAmount||0)>0?0:savedTrip.tripOverrides?.allowance??(savedTrip.tripOverrides?.layover!=null?layover/60*Number(paySettings().layoverRate||0):metrics.allowance);
  return{...savedTrip,trip:savedTrip.trip??metrics.trip,duty:savedTrip.duty??metrics.duty,layover:savedTrip.layover??layover,allowance:savedTrip.allowance??allowance,calculated:metrics};
}
function calculateSavedTripProjections(savedTrips=null,sourceEntries=null){
  const entries=sourceEntries||operationalEntriesForTrips();
  return(savedTrips||load(TRIPS_KEY)).map(t=>calculateSavedTripProjection(t,entries)).filter(Boolean);
}
let tripFormOverrides={};
function tripCalc(){
  const cashAmount=Number($('tripCashAmount')?.value||0),result=PilotLogEngine.trip($('tripStart').value,$('tripEnd').value,cashAmount);if(!result){$('tripLayover').value='0:00';$('tripAllowance').value='0.00';return null}
  if(tripFormOverrides.layover!=null){result.layover=tripFormOverrides.layover;result.allowance=cashAmount>0?0:result.layover/60*Number(paySettings().layoverRate||0)}
  if(cashAmount<=0&&tripFormOverrides.allowance!=null)result.allowance=tripFormOverrides.allowance;
  $('tripLayover').value=fmt(result.layover);$('tripAllowance').value=cashAmount>0?moneyCurrency(0,appCurrency()):moneyCurrency(result.allowance,'MAD');return result
}
function tripStartDateFromValue(v){return String(v||'').slice(0,10)}
function tripFxCache(t,target,from,startDate){
  const x=t?.cashFxRates?.[target];if(x&&upper(x.from||'')===from&&(String(x.tripStartDate||'')===startDate||t?.createdDate===x.tripStartDate)&&Number(x.rate)>0)return x;
  if(target==='EUR'&&Number(t?.cashAmount)>0&&Number(t?.cashEur)>0&&String(t?.cashFxDate||'')&&upper(t?.cashCurrency||'')===from){return{from,to:'EUR',rate:Number(t.cashEur)/Number(t.cashAmount),date:String(t.cashFxDate),tripStartDate:startDate}}
  return null;
}
async function tripMoneyConversion(){
  const c=tripCalc();if(!c)return null;
  const amount=Number($('tripCashAmount')?.value||0),from=upper($('tripCashCurrency')?.value||''),startDate=tripStartDateFromValue($('tripStart')?.value),existing=load(TRIPS_KEY).find(x=>x.id===$('tripEditId')?.value);
  const to=existing?.cashFxTo&&from===upper(existing.cashCurrency)?existing.cashFxTo:appCurrency();
  if($('tripCashConvertedLabel'))$('tripCashConvertedLabel').textContent='Cash value in '+currencyOutputCode(to);
  if(amount>0&&!ISO_CURRENCIES.includes(from))return {...c,error:'Select a valid cash currency.'};
  const manual=tripFormOverrides.cashConverted,manualMatch=manual&&manual.from===from&&manual.to===to&&manual.date===startDate&&manual.amount===amount;
  const created=existing?existing.createdDate||'':today();
  const exact=existing?.cashFxRate>0&&upper(existing.cashFxFrom||existing.cashCurrency)===from&&(existing.cashFxTo===to||!existing.cashFxTo&&from===to)?{rate:existing.cashFxRate,date:existing.cashFxDate,manual:existing.cashFxManual}:tripFxCache(existing,to,from,startDate);
  const cached=exact;

  const rate=amount<=0?0:manualMatch?manual.value/amount:from===to?1:Number(cached?.rate||0);
  const provisional=false;
  const converted=rate>0?amount*rate:null;
  if($('tripCashConverted'))$('tripCashConverted').value=converted===null?'':moneyCurrency(converted,to);
  if($('tripAllowance'))$('tripAllowance').value=moneyCurrency(c.allowance,'MAD');
  return {...c,cashConverted:converted,cashRate:rate,cashFxDate:rate>0?(manualMatch||from===to?created:cached?.date||''):'',cashFrom:from,cashTo:to,madRate:1,tripStartDate:startDate,cashFxProvisional:provisional,cashFxPending:amount>0&&!rate,cashFxManual:!!manualMatch||!!(exact?.manual),error:''};
}
let tripFxRefreshBusy=false,tripFxRescan=false;
function tripFxIdentity(t){return JSON.stringify([t.start,t.cashAmount,t.cashCurrency,t.cashFxManual,t.cashFxRate,t.cashFxTo,t.cashFxDate])}
async function refreshPendingTripFx(){
  if(tripFxRefreshBusy){tripFxRescan=true;return {changed:false,failed:0}}
  if(navigator.onLine===false)return {changed:false,failed:0};
  tripFxRefreshBusy=true;let changed=false,failed=0;
  const tables=new Map();
  try{
    for(const snapshot of load(TRIPS_KEY).map(t=>({...t}))){
      const amount=Number(snapshot.cashAmount||0),from=upper(snapshot.cashCurrency||''),startDate=snapshot.createdDate,target=upper(snapshot.cashFxTo||''),exact=tripFxCache(snapshot,target,from,startDate);
      if(!snapshot.cashFxPending||snapshot.cashFxRate>0||!target||!(amount>0)||!from||!startDate||snapshot.cashFxManual||exact?.manual)continue;

      try{
        const key=startDate;
        let info=null,rate=1;
        if(from!==target){
          if(!tables.has(key))tables.set(key,fetchFxTableForDate(startDate));
          info=await tables.get(key);rate=fxRateFromTable(info.table,from,target);
        }
        if(!(rate>0))throw new Error('FX unavailable');
        // Re-read after network awaits so edits/deletions made meanwhile are never overwritten.
        const current=load(TRIPS_KEY),i=current.findIndex(t=>t.id===snapshot.id);
        if(i<0||tripFxIdentity(current[i])!==tripFxIdentity(snapshot))continue;
        const date=info?.date||startDate,provisional=false;
        const entry={from,to:target,rate,date,tripStartDate:startDate,provisional};
        const updated=stamp({...current[i],cashFxRates:{...current[i].cashFxRates,[target]:entry},cashFxFrom:from,cashFxTo:target,cashFxRate:rate,cashFxDate:date,cashFxTripStartDate:startDate,cashConverted:amount*rate,cashEur:target==='EUR'?amount*rate:current[i].cashEur,cashFxPending:false,cashFxProvisional:provisional});
        current[i]=updated;save(TRIPS_KEY,current);markCloudEdited('trips',updated,'trip-fx-update');changed=true;
      }catch{failed++}
    }
    if(changed){renderTrips();renderTripCashPayment($('payrollMonth')?.value||monthNow());if($('tripEditId')?.value)tripMoneyConversion();scheduleAutoSync('trip-fx-update')}
    return {changed,failed};
  }finally{tripFxRefreshBusy=false;if(tripFxRescan){tripFxRescan=false;queueMicrotask(refreshPendingTripFx)}}
}
function resetTrip(){tripFormOverrides={};$('tripForm').reset();$('tripEditId').value='';$('tripLayover').value='0:00';$('tripAllowance').value=moneyCurrency(0);if($('tripCashConverted'))$('tripCashConverted').value='';if($('tripCashCurrency'))$('tripCashCurrency').value='USD';updateCurrencyUI()}
function updateCurrencyUI(){
  const cur=appCurrency(),payCur=payrollConversionCurrency();if($('tripAllowanceLabel'))$('tripAllowanceLabel').textContent='Total Layover Allowance (DHM)';if($('tripCashConvertedLabel'))$('tripCashConvertedLabel').textContent=`Cash value in ${currencyOutputCode(cur)}`;if($('payTotalCurrencyLabel'))$('payTotalCurrencyLabel').textContent=`Converted total (${currencyOutputCode(payCur)})`;
}
function localDateTimeValue(d){const p=n=>String(n).padStart(2,'0');return `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`}
function entryStart(f){return zuluDate(f.date,f.onDuty||f.schedOut||f.out||'00:00')}
function tripSequenceStart(f){
  // For trip boundaries use the actual beginning of the activity, not a derived duty/reporting time.
  const t=f.schedOut||f.out||f.onDuty||'00:00';
  return zuluDate(f.date,t);
}
function tripSequenceEnd(f){
  // For trip boundaries use the actual end of the final activity.
  const start=f.schedOut||f.out||f.onDuty||'00:00';
  const end=f.schedIn||f.in||f.offDuty||f.on||start;
  return endZuluDate(f.date,start,end);
}
function positioningCashEligible(entries){
  return (entries||[]).some(f=>isPositioning(f)&&isMoroccoCode(f.dep)&&f.arr&&!isMoroccoCode(f.arr));
}
function autoDetectTrips(showAlert=true,rerender=true){
  const base=upper(appSettings().homeBase||'CMN');
  const fs=operationalEntriesForTrips().filter(f=>f.date);

  let ts=load(TRIPS_KEY),created=0,updated=0;

  for(let i=0;i<fs.length;i++){
    const f=fs[i];

    // A trip can be opened by DHD or DHP leaving Home Base and ending away from base.
    // Normal operating flights must never create a trip by themselves.
    if(!isPositioning(f) || upper(f.dep)!==base || !f.arr || upper(f.arr)===base) continue;

    const start=tripSequenceStart(f);
    if(!start) continue;

    let end=null,last=i,stations=new Set(),includedIds=[];
    for(let j=i;j<fs.length;j++){
      const x=fs[j];
      includedIds.push(x.id);
      last=j;

      if(x.dep&&upper(x.dep)!==base) stations.add(upper(x.dep));
      if(x.arr&&upper(x.arr)!==base) stations.add(upper(x.arr));

      // Trip closes only when the sequence physically returns to home base.
      if(j>i && upper(x.arr)===base){
        end=tripSequenceEnd(x);
        break;
      }
    }

    if(!end||end<=start) continue;

    const startStr=localDateTimeValue(start),endStr=localDateTimeValue(end);
    const sig=`AUTO|${startStr}|${endStr}|${base}`;
    const k=ts.findIndex(t=>t.autoSig===sig||t.autoSourceId===f.id);
    if(k>=0){i=last;continue}
    const existingCash=k>=0?Number(ts[k].cashAmount||0):0;
    const metrics=PilotLogEngine.trip(start,end,existingCash,fs);if(!metrics)continue;
    const{trip,duty,layover}=metrics;
    const allowance=metrics.allowance;

    const obj=stamp({
      id:makeId(),createdDate:today(),
      base,
      stations:[...stations].filter(Boolean).join(', '),
      start:startStr,
      end:endStr,
      trip,
      duty,
      layover,
      allowance,
      remarks:'Automatically detected from out-of-base sequence',
      auto:true,
      autoSourceId:f.id,
      autoSig:sig,
      includedEntryIds:includedIds,
      cashEligible:positioningCashEligible(fs.filter(x=>includedIds.includes(x.id))),
      cashAmount:k>=0?Number(ts[k].cashAmount||0):0,
      cashCurrency:k>=0?upper(ts[k].cashCurrency||''): '',
      cashConverted:k>=0?Number(ts[k].cashConverted||0):0,
      cashEur:k>=0?Number(ts[k].cashEur||0):0,
      cashFxFrom:k>=0?upper(ts[k].cashFxFrom||ts[k].cashCurrency||''):'',
      cashFxTo:k>=0?upper(ts[k].cashFxTo||''):'',
      cashFxRate:k>=0?Number(ts[k].cashFxRate||0):0,
      cashFxDate:k>=0?String(ts[k].cashFxDate||''):'',
      cashFxTripStartDate:k>=0?String(ts[k].cashFxTripStartDate||''):'',
      cashFxRates:k>=0?{...(ts[k].cashFxRates||{})}:{},
      cashFxPending:k>=0?!!ts[k].cashFxPending:false,cashFxProvisional:k>=0?!!ts[k].cashFxProvisional:false,cashFxManual:k>=0?!!ts[k].cashFxManual:false,
      tripFxTo:k>=0?upper(ts[k].tripFxTo||''):'',tripFxMadRate:k>=0?Number(ts[k].tripFxMadRate||0):0,tripFxDate:k>=0?String(ts[k].tripFxDate||''):'',tripFxTripStartDate:k>=0?String(ts[k].tripFxTripStartDate||''):''
    });

    if(k>=0){ts[k]={...ts[k],...obj};updated++}
    else{ts.push(obj);created++}

    i=last;
  }

  save(TRIPS_KEY,ts);
  if(rerender){renderTrips();renderPayroll()}
  if(showAlert)alert(`Automatic Trips: ${created} created, ${updated} updated.`);
  return{created,updated};
}

/* Imports */
function unfoldIcs(text){return text.replace(/\r?\n[ \t]/g,'')}
function icsVal(line){const i=line.indexOf(':');return i<0?'':line.slice(i+1).replace(/\\n/g,'\n').replace(/\\,/g,',').replace(/\\;/g,';')}
function parseIcsDate(v){v=String(v||'').trim();const m=v.match(/^(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})?Z?)?/);if(!m)return null;return{date:`${m[1]}-${m[2]}-${m[3]}`,time:m[4]?`${m[4]}:${m[5]}`:''}}
function parseIcs(text){return unfoldIcs(text).split('BEGIN:VEVENT').slice(1).map(x=>x.split('END:VEVENT')[0]).map(b=>{const o={};b.split(/\r?\n/).forEach(line=>{const k=line.split(':')[0].split(';')[0];if(['SUMMARY','DESCRIPTION','DTSTART','DTEND','UID'].includes(k))o[k]=icsVal(line)});o._start=parseIcsDate(o.DTSTART);o._end=parseIcsDate(o.DTEND);return o}).filter(e=>e._start)}
function classify(summary){
  const s=upper(summary),fl=s.match(/\b([A-Z0-9]{2,3}\s?\d{2,4})\b.*?\b([A-Z]{3})\b\s*(?:→|->|–|-)\s*\b([A-Z]{3})\b/);
  if(fl)return{kind:'flight',flightNo:fl[1].replace(/\s+/g,''),dep:fl[2],arr:fl[3]};
  const ac=(s.match(/\bA(319|320|321)\b/)||[])[0]||'';
  if(/\bRT[ -]?A3(19|20|21)\b|RECURRENT TRAINING|\bSIM(?:ULATOR)?\b|\bOPC\b|\bLPC\b/.test(s))return{kind:'entry',dutyType:'Simulator',aircraftType:ac};
  if(/\bSTBY\b|STANDBY|\bHSBY\b/.test(s))return{kind:'entry',dutyType:'STBY',aircraftType:ac};
  if(/\bDHD\b|DEADHEAD|\bDHP\b/.test(s)){const r=s.match(/\b([A-Z]{3})\b\s*(?:→|->|–|-)\s*\b([A-Z]{3})\b/);return{kind:'entry',dutyType:'DHD',aircraftType:ac,dep:r?r[1]:'',arr:r?r[2]:''}};
  if(/^SOD$/.test(s))return{kind:'entry',dutyType:'Ground Course',aircraftType:ac};
  if(/\bGRT\b|GROUND|COURSE|TRAINING/.test(s))return{kind:'entry',dutyType:'Ground Course',aircraftType:ac};
  if(/^DUTY\b|\bDUTY\s*[•:-]/.test(s))return{kind:'duty',dutyType:'Flight Duty'};
  if(/^(?:OFF|DAY\s*OFF|DAYOFF)\b/.test(s))return{kind:'duty',dutyType:'Day OFF'};return{kind:'unknown'}
}
function zuluTimes(desc){const d=String(desc||'');return{out:(d.match(/Scheduled (?:take-off\/)?departure\s+(\d{1,2}:\d{2})Z/i)||[])[1]||'',inn:(d.match(/Scheduled arrival\s+(\d{1,2}:\d{2})Z/i)||[])[1]||'',report:(d.match(/Reporting\s+(\d{1,2}:\d{2})Z/i)||[])[1]||'',end:(d.match(/(?:Release|End of duty)\s+(\d{1,2}:\d{2})Z/i)||[])[1]||''}}
function entrySig(f){return [f.date,f.dutyType,f.flightNo,f.dep,f.arr,f.schedOut,f.schedIn,f.onDuty,f.offDuty].map(x=>upper(x)).join('|')}
function dutySig(d){return [d.date,d.type,d.report,d.end,d.notes].map(x=>upper(x)).join('|')}
let importStaging=false;
function commitImport(operation){
  const previous=operationalDatabase;
  operationalDatabase={...previous,values:{...previous.values}};importStaging=true;
  try{
    const result=operation();
    importStaging=false;
    if(operationalDatabase.records!==previous.records||Object.keys(operationalDatabase.values).some(k=>operationalDatabase.values[k]!==previous.values[k]))touchOperationalDatabase();
    else operationalDatabase=previous;
    return result;
  }catch(error){operationalDatabase=previous;throw error}
  finally{importStaging=false}
}
function importCalendar(events){return commitImport(()=>applyCalendarImport(events))}
function applyCalendarImport(events){
  const activities=coreActivityRows(),seenActivities=new Set(activities.map(entrySig)),seenDuties=new Set(dutyActivityRows().map(dutySig));let sectors=0,duties=0,other=0,skipped=0;
  events.forEach(ev=>{const c=classify(ev.SUMMARY),z=zuluTimes(ev.DESCRIPTION),date=ev._start.date;if(['ignore','unknown'].includes(c.kind)){skipped++;return}
    if(c.kind==='flight'){
      const schedOut=z.out||ev._start.time||'',schedIn=z.inn||(ev._end?.time||''),sourceExisting=ev.UID?activities.find(x=>x.source==='calendar'&&x.calendarUid===ev.UID):null,f=stamp({id:sourceExisting?.id||importCoreId(ev.UID?'calendar:'+ev.UID:''),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:'Flight',date,flightNo:composeFlightNo(c.flightNo),dep:c.dep,arr:c.arr,type:'',reg:'',schedOut,schedIn,schedBlock:timeSpanMins(schedOut,schedIn),onDuty:z.report||shiftTime(schedOut,-60),offDuty:z.end||shiftTime(schedIn,30),out:'',off:'',on:'',in:'',block:null,credit:creditMinsForEntry({dutyType:'Flight',date,dep:c.dep,schedOut,schedIn}),rolePic:appSettings().profileRole==='Captain',role:appSettings().profileRole==='Captain'?'PIC':'SIC',instruction:false,simInstructor:false,simExaminer:false,night:null,sim:'no',ifr:'',pf:'no',approachType:'',dayTakeoffs:0,nightTakeoffs:0,dayLandings:0,nightLandings:0,remarks:'Imported from calendar',source:'calendar',calendarUid:ev.UID||''});
      const existingIndex=activities.findIndex(x=>x.id===f.id);if(existingIndex>=0){activities[existingIndex]=mergeCoreActivities(activities[existingIndex],f);skipped++;return}
      const signature=entrySig(f);if(!seenActivities.has(signature)){activities.push(f);seenActivities.add(signature);sectors++}else skipped++;return;
    }
    if(c.kind==='duty'){
      const rep=z.report||ev._start.time||'',end=z.end||(ev._end?.time||''),sourceExisting=ev.UID?activities.find(x=>x.source==='calendar'&&x.calendarUid===ev.UID):null,d=stamp({id:sourceExisting?.id||importCoreId(ev.UID?'calendar:'+ev.UID:''),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:c.dutyType,date,onDuty:rep,offDuty:end,totalDuty:timeSpanMins(rep,end),remarks:ev.SUMMARY||'',source:'calendar',calendarUid:ev.UID||''}),sd=dutySig({date,type:c.dutyType,report:rep,end,notes:ev.SUMMARY||''});
      const existingIndex=activities.findIndex(x=>x.id===d.id);if(existingIndex>=0){activities[existingIndex]=mergeCoreActivities(activities[existingIndex],d);skipped++;return}
      if(!seenDuties.has(sd)){activities.push(d);seenDuties.add(sd);duties++}else skipped++;return;
    }
    if(c.kind==='entry'){
      const start=ev._start.time||'',end=ev._end?.time||'',sim=c.dutyType==='Simulator',positioning=c.dutyType==='DHD'||c.dutyType==='DHP',ground=c.dutyType==='Ground Course',sourceExisting=ev.UID?activities.find(x=>x.source==='calendar'&&x.calendarUid===ev.UID):null,f=stamp({id:sourceExisting?.id||importCoreId(ev.UID?'calendar:'+ev.UID:''),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:c.dutyType,date,flightNo:'',dep:c.dep||'',arr:c.arr||'',type:c.aircraftType||'',reg:'',schedOut:sim||positioning?start:'',schedIn:sim||positioning?end:'',schedBlock:sim||positioning?timeSpanMins(start,end):0,onDuty:sim?shiftTime(start,-60):start,offDuty:sim?shiftTime(end,30):end,totalDuty:timeSpanMins(sim?shiftTime(start,-60):start,sim?shiftTime(end,30):end),out:'',off:'',on:'',in:'',block:null,simulatorTime:sim?timeSpanMins(start,end):null,credit:null,rolePic:false,instruction:false,simInstructor:false,simExaminer:false,night:null,sim:sim?'yes':'no',ifr:'',pf:'no',approachType:'',dayTakeoffs:0,nightTakeoffs:0,dayLandings:0,nightLandings:0,courseType:ground?upper(ev.SUMMARY||''):'',remarks:`Imported from calendar: ${ev.SUMMARY||''}`,source:'calendar',calendarUid:ev.UID||''});
      const existingIndex=activities.findIndex(x=>x.id===f.id);if(existingIndex>=0){activities[existingIndex]=mergeCoreActivities(activities[existingIndex],f);skipped++;return}
      const signature=entrySig(f);if(!seenActivities.has(signature)){activities.push(initialTrainingCreditSource(f));seenActivities.add(signature);other++}else skipped++;
    }
  });commitCoreActivities(activities);reconcileAllDuties();return{sectors,duties,other,skipped}
}

/* AeroLINE native roster JSON import */
function aerolineRoot(payload){
  const candidates=[payload,payload?.data,payload?.result,payload?.response,payload?.body];
  return candidates.find(x=>x&&typeof x==='object'&&Array.isArray(x.mainCrewScheduleInfoList))||null;
}
function aerolineDateTime(v){
  const s=String(v||'').trim();
  let m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if(m)return{date:`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`,time:`${m[4].padStart(2,'0')}:${m[5]}`};
  m=s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{1,2}):(\d{2}))?/);
  return m?{date:`${m[1]}-${m[2]}-${m[3]}`,time:m[4]?`${m[4].padStart(2,'0')}:${m[5]}`:''}:{date:'',time:''};
}
function aerolineMonthLabel(root){
  const d=String(root?.schStartDate||'').slice(0,10);
  if(!/^\d{4}-\d{2}-\d{2}$/.test(d))return'';
  try{return new Intl.DateTimeFormat('en-GB',{month:'long',year:'numeric',timeZone:'UTC'}).format(new Date(`${d}T00:00:00Z`)).toUpperCase()}
  catch{return d.slice(0,7)}
}
function aerolineFlightKey(r){return `FLIGHT|${r.flightLegId||''}|${upper(r.flightNo||'')}|${upper(r.origin||'')}|${upper(r.destination||'')}|${String(r.flightDepTimeString||'')}`}
function aerolineEventKey(r,code){return `EVENT|${upper(code||'')}|${r.crewScheduleBlockId||''}|${String(r.eventStartTimeString||'')}|${String(r.eventEndTimeString||'')}|${upper(r.eventStartLoc||'')}|${upper(r.eventEndLoc||'')}`}
function aerolineTrainingKey(r){return `TRAINING|${r.trainingSlotId||''}|${r.crewScheduleBlockId||''}|${upper(r.trainingProgramCode||'')}|${String(r.trainingStartTimeString||'')}|${String(r.trainingEndTimeString||'')}`}
function aerolineNormalizePersonName(v){
  return String(v||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toUpperCase().replace(/[^A-Z0-9 ]+/g,' ').split(/\s+/).filter(Boolean).sort().join(' ');
}
function aerolineSamePerson(a,b){const x=aerolineNormalizePersonName(a),y=aerolineNormalizePersonName(b);return !!x&&!!y&&x===y}
function aerolineConfirmCrewIdentity(root){
  const rosterName=String(root?.crewName||'').trim(),profileName=String(appSettings().profileName||'').trim();
  if(!rosterName)return true;
  if(profileName&&aerolineSamePerson(rosterName,profileName))return true;
  return confirm(`Confirm this is ${rosterName}.`);
}
function aerolinePeople(v){return String(v||'').split(',').map(x=>x.trim()).filter(Boolean)}
function aerolineSelfName(root){return String(appSettings().profileName||root?.crewName||'').trim()}
function aerolineOtherCrew(r,root){
  const self=aerolineSelfName(root);
  return (Array.isArray(r?.joininCrew)?r.joininCrew:[]).map(x=>String(x||'').trim()).filter(x=>x&&!aerolineSamePerson(x,self));
}
function aerolineRegistration(r){
  const keys=['aircraftRegistration','aircraftReg','aircraftID','aircraftId','registration','reg','tailNo','tailNumber'];
  for(const k of keys){const v=upper(r?.[k]||'');if(v)return v}
  return'';
}
function aerolineTrainingMeta(root){
  return Array.isArray(root?.trainingCrewScheduleInfoList)?root.trainingCrewScheduleInfoList:[];
}
function aerolineTrainingForFlight(root,date,flightNo){
  const digits=rosterFlightDigits(flightNo||'');
  return aerolineTrainingMeta(root).find(t=>normalDate(t.trainDate||'')===date&&rosterFlightDigits(t.trainLocOrFlight||'')===digits)||null;
}
function aerolineTrainingForActivity(root,r,start){
  const code=upper(r?.trainingProgramCode||''),loc=upper(r?.trainingLocation||'');
  return aerolineTrainingMeta(root).find(t=>{
    const td=aerolineDateTime(t.trainDateString||t.trainDate||'');
    if(td.date!==start.date)return false;
    if(td.time&&start.time&&td.time!==start.time)return false;
    if(code&&upper(t.trainingType||'')&&upper(t.trainingType)!==code)return false;
    if(loc&&upper(t.trainLocOrFlight||'')&&upper(t.trainLocOrFlight)!==loc)return false;
    return true;
  })||null;
}
function aerolineTrainingLabel(tm,r){
  const s=upper([tm?.trainingType,tm?.trainingTypeDesc,r?.trainingProgramCode,r?.trainingProgramDesc,r?.trainingSlotName].filter(Boolean).join(' '));
  if(/\bALC\b|ANNUAL LINE CHECK/.test(s))return'ALC • ANNUAL LINE CHECK';
  if(upper(tm?.trainingType||r?.trainingProgramCode||'')==='LTG'||/\bLINE TRAINING\b/.test(s))return'LTG • LINE TRAINING';
  if(upper(tm?.trainingType||r?.trainingProgramCode||'')==='RT'||/RECURRENT TRAINING/.test(s))return'RT • RECURRENT TRAINING';
  return upper(tm?.trainingTypeDesc||r?.trainingProgramDesc||tm?.trainingType||r?.trainingProgramCode||'');
}
function aerolineIsAlc(tm,r){return /\bALC\b|ANNUAL LINE CHECK/.test(upper([tm?.trainingType,tm?.trainingTypeDesc,r?.trainingProgramCode,r?.trainingProgramDesc,r?.trainingSlotName].filter(Boolean).join(' ')))}
function aerolineIsLineTraining(tm,r){return !aerolineIsAlc(tm,r)&&(upper(tm?.trainingType||r?.trainingProgramCode||'')==='LTG'||/\bLINE TRAINING\b/.test(upper([tm?.trainingTypeDesc,r?.trainingProgramDesc,r?.trainingSlotName].filter(Boolean).join(' '))))}
function aerolineIsSimulatorTraining(r,tm=null){
  const s=upper([tm?.trainingType,tm?.trainingTypeDesc,r.trainingProgramCode,r.trainingProgramDesc,r.trainingSlotName].filter(Boolean).join(' '));
  return /\bSIM\b|SIMULATOR|\bOPC\b|\bLPC\b|RECURRENT TRAINING|\bRT\b/.test(s);
}
function aerolineTrainingCrewFields(root,tm,sim){
  if(!sim||!tm)return{picName:'',sicName:'',instructorName:'',trainerType:'',selfIsTrainer:false};
  const joining=aerolinePeople(tm.joiningPilot),trainers=aerolinePeople(tm.trainerName),self=aerolineSelfName(root);
  const selfTrainer=trainers.find(name=>aerolineSamePerson(name,self))||'';
  const singleTrainer=trainers.length===1?trainers[0]:'';
  return{
    picName:upper(joining[0]||''),
    sicName:upper(joining[1]||''),
    instructorName:upper(selfTrainer||singleTrainer),
    trainerType:upper(tm.trainerType||''),
    selfIsTrainer:!!selfTrainer
  };
}
const EXPIRY_COURSE_ALIASES=Object.freeze({
  CRM:['CRM','CREW RESOURCE MANAGEMENT'],SMS:['SMS','SAFETY MANAGEMENT SYSTEM'],GRT:['GRT','GROUND RECURRENT TRAINING'],DGR:['DGR','DANGEROUS GOODS'],AVSEC:['AVSEC','AVIATION SECURITY'],SEP:['SEP','SAFETY AND EMERGENCY PROCEDURES','SAFETY EMERGENCY PROCEDURES'],FIRSTAID:['FIRST AID','FIRSTAID'],UPRT:['UPRT','UPSET PREVENTION AND RECOVERY TRAINING'],EBTCBTA:['EBT/CBTA','EBT CBTA','CBTA','EVIDENCE BASED TRAINING'],OPC:['OPC','OPERATOR PROFICIENCY CHECK'],PPC:['PPC','PILOT PROFICIENCY CHECK']
});
function normalizeExpiryText(v){return upper(v||'').replace(/AEROLINE|LOGTEN|TYPE|ENDORSEMENT|COURSE|VALIDATION/g,' ').replace(/[^A-Z0-9]+/g,' ').replace(/\s+/g,' ').trim()}
function expiryCourseCode(x){
  const explicit=upper(x?.aerolineValidationCode||'');if(explicit)return explicit.replace(/[^A-Z0-9/]+/g,'');
  const text=normalizeExpiryText([x?.name,x?.remarks,x?.aerolineValidationDesc].filter(Boolean).join(' '));
  for(const [code,aliases] of Object.entries(EXPIRY_COURSE_ALIASES))if(aliases.some(a=>{const n=normalizeExpiryText(a);return text===n||text.includes(` ${n} `)||text.startsWith(`${n} `)||text.endsWith(` ${n}`)}))return code;
  return normalizeExpiryText(x?.name||'').replace(/\s+/g,'');
}
function expiryIdentity(x){
  const category=String(x?.category||'course').toLowerCase();
  if(category==='course'){const code=expiryCourseCode(x);return code?`course|${code}`:''}
  const authority=normalizeExpiryText(x?.authority||''),name=normalizeExpiryText(x?.name||x?.medicalClass||x?.aircraftType||x?.aerolineValidationCode||'');if(!name)return'';return `${category}|${name}|${authority}`;
}
function findExpirySemanticMatch(rows,candidate){const key=expiryIdentity(candidate);return key&&key!=='course|'?rows.find(x=>expiryIdentity(x)===key):null}
function mergeExpiryDuplicate(a,b){
  const aProtected=!!(a?.locked||a?.manualOverride||a?.source==='manual'),bProtected=!!(b?.locked||b?.manualOverride||b?.source==='manual'),preferred=bProtected&&!aProtected?b:a,other=preferred===a?b:a,out={...other,...preferred};
  out.id=preferred.id||other.id;out.locked=!!(a?.locked||b?.locked);out.manualOverride=!!(a?.manualOverride||b?.manualOverride);
  if(!aProtected&&!bProtected){
    if(!out.expiry||String(other.expiry||'')>String(out.expiry||''))out.expiry=other.expiry||out.expiry||'';
    ['aerolineValidationCode','aerolineValidationDesc','logtenUniqueId','courseDate','lastCheck','number','authority','issuer','photoData'].forEach(k=>{if(!out[k]&&other?.[k])out[k]=other[k]});
  }
  if(preferred.source==='manual')out.source='manual';else if(a?.source==='aeroline'||b?.source==='aeroline')out.source='aeroline';else out.source=preferred.source||other.source||'manual';
  out._updatedAt=new Date().toISOString();return out;
}
function dedupeExpiryRecords(input=load(EXPIRY_KEY)){
  const out=[],index=new Map();let removed=0;
  for(const row of (Array.isArray(input)?input:[])){
    const key=expiryIdentity(row);if(!key||key==='course|'||!index.has(key)){index.set(key,out.length);out.push(row);continue}
    const i=index.get(key);out[i]=mergeExpiryDuplicate(out[i],row);removed++;
  }
  return{rows:out,removed};
}
function migrateExpirySemanticDuplicates(){const d=dedupeExpiryRecords(load(EXPIRY_KEY));if(d.removed)save(EXPIRY_KEY,d.rows);return d.removed}
const AEROLINE_VALIDATION_SKIP=new Set(['CMC','A320/IR','JSIM','TRN','WP']);
function aerolineValidationCategory(code){
  code=upper(code);
  if(code==='MED')return'medical';
  if(code==='ELP')return'english';
  if(code==='LVC')return'license';
  return'course';
}
function parseAerolineValidations(root){
  const seen=new Set(),out=[];
  for(const v of (root?.validationsInfoDTO||[])){
    const code=upper(v?.validationCode||'');if(!code||AEROLINE_VALIDATION_SKIP.has(code)||seen.has(code))continue;
    seen.add(code);
    const expiry=normalDate(String(v?.expDate||'').slice(0,10));
    out.push({code,description:String(v?.validationDesc||code).trim(),expiry:/^\d{4}-\d{2}-\d{2}$/.test(expiry)?expiry:''});
  }
  return out;
}
function importAerolineValidations(root){
  const initial=dedupeExpiryRecords(load(EXPIRY_KEY)),rows=[...initial.rows],validations=parseAerolineValidations(root);let added=0,updated=0,kept=initial.removed,emptySkipped=0;
  for(const v of validations){
    const category=aerolineValidationCategory(v.code),candidate={category,name:category==='english'?'LPC ENGLISH':v.code,aerolineValidationCode:v.code,aerolineValidationDesc:v.description};
    const match=rows.find(x=>upper(x.aerolineValidationCode||'')===v.code)||findExpirySemanticMatch(rows,candidate);
    if(!v.expiry){if(match){match.aerolineValidationCode=v.code;match.aerolineValidationDesc=v.description;kept++}else emptySkipped++;continue}
    if(!match){
      rows.push(stamp({id:stableSourceId('expiry',`aeroline-validation:${v.code}`),category,name:category==='english'?'LPC ENGLISH':v.code,authority:'',number:'',lastCheck:'',courseDate:'',medicalClass:'',englishLevel:'',aircraftType:'',endorsedBy:'',expiry:v.expiry,issuer:'',remarks:`AEROLINE • ${upper(v.description)}`,photoData:'',locked:false,source:'aeroline',aerolineValidationCode:v.code,aerolineValidationDesc:v.description}));
      added++;continue;
    }
    match.aerolineValidationCode=v.code;match.aerolineValidationDesc=v.description;
    if(!match.expiry){if(!(match.locked||match.manualOverride)){match.expiry=v.expiry;updated++}else kept++;match._updatedAt=new Date().toISOString();continue}
    if(match.expiry===v.expiry){match._updatedAt=new Date().toISOString();kept++;continue}
    if(match.locked){kept++;continue}
    const ok=confirm(`AeroLINE shows a different expiry date for ${v.code}.\n\nCurrent PilotLog expiry: ${displayDate(match.expiry)}\nNew AeroLINE expiry: ${displayDate(v.expiry)}\n\nDo you want to update it?`);
    if(ok){match.expiry=v.expiry;match._updatedAt=new Date().toISOString();updated++}else kept++;
  }
  const final=dedupeExpiryRecords(rows);save(EXPIRY_KEY,final.rows);return{added,updated,kept:kept+final.removed,emptySkipped,total:validations.length};
}
function parseAerolineRosterObject(payload){
  const root=aerolineRoot(payload);if(!root)throw new Error('This JSON does not contain an AeroLINE crew schedule.');
  const main=root.mainCrewScheduleInfoList||[];
  const flights=[],activities=[],daysOff=[],unsupported=[];
  const seenFlights=new Set(),seenEvents=new Set(),seenTraining=new Set();

  main.forEach(r=>{
    if(r?.flightNo&&r?.origin&&r?.destination){
      const dep=aerolineDateTime(r.flightDepTimeString),arr=aerolineDateTime(r.flightArrTimeString),key=aerolineFlightKey(r);
      if(dep.date&&!seenFlights.has(key)){
        seenFlights.add(key);
        const tm=aerolineTrainingForFlight(root,dep.date,r.flightNo),crew=aerolineOtherCrew(r,root),self=upper(aerolineSelfName(root));
        const alc=aerolineIsAlc(tm,r),lineTraining=aerolineIsLineTraining(tm,r),trainingLabel=aerolineTrainingLabel(tm,r),twoPilotCrew=crew.length===1;
        flights.push({
          aerolineKey:key,aerolineFlightLegId:String(r.flightLegId||''),aerolineBlockId:String(r.crewScheduleBlockId||''),
          date:dep.date,flightNo:rosterFlightLabel(r.flightNo),dep:upper(r.origin),arr:upper(r.destination),std:dep.time,sta:arr.time,
          type:upper(r.fleetType||''),reg:aerolineRegistration(r),status:'planned',source:'aeroline',aerolineRosterPlanned:true,
          aerolineArrivalDate:arr.date||dep.date,aerolineCrew:crew.map(upper),picName:twoPilotCrew?self:'',sicName:twoPilotCrew?upper(crew[0]||''):'',aerolineCrewAssignmentAmbiguous:crew.length>1,
          aerolineTrainingType:upper(tm?.trainingType||''),aerolineTrainingDesc:upper(tm?.trainingTypeDesc||''),aerolineTrainingLabel:trainingLabel,
          aerolineTrainerType:upper(tm?.trainerType||''),aerolineJoiningPilot:upper(tm?.joiningPilot||''),aerolineTrainerName:upper(tm?.trainerName||''),
          aerolineLineTraining:lineTraining,aerolineAlc:alc,instructorName:'',instruction:lineTraining,remarks:trainingLabel||''
        });
      }
    }

    let eventCode=upper(r?.eventCode||'');
    if(!eventCode&&upper(r?.airportStandby)==='Y')eventCode='STBY';
    if(eventCode){
      const key=aerolineEventKey(r,eventCode);
      if(!seenEvents.has(key)){
        seenEvents.add(key);
        const start=aerolineDateTime(['OFF','ALV','SLV'].includes(eventCode)?(r.leaveStartTimeString||r.eventStartTimeString):r.eventStartTimeString);
        const end=aerolineDateTime(['OFF','ALV','SLV'].includes(eventCode)?(r.leaveEndTimeString||r.eventEndTimeString):r.eventEndTimeString);
        if(eventCode==='OFF'){
          if(start.date)daysOff.push({aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),date:start.date,type:'Day OFF',report:'',end:'',minutes:0,notes:'AeroLINE • OFF',source:'aeroline',aerolineRosterPlanned:true});
        }else if(eventCode==='ALV'||eventCode==='SLV'){
          if(start.date)activities.push({aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),dutyType:eventCode==='ALV'?'Annual Leave':'Sick Leave',date:start.date,onDuty:'',offDuty:'',totalDuty:0,credit:0,remarks:`AeroLINE • ${eventCode}`,source:'aeroline',aerolineRosterPlanned:true});
        }else if(eventCode==='SOD'){
          if(start.date)activities.push({aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),date:start.date,dutyType:'Ground Course',courseType:'SOD',trainerType:'SOD',onDuty:start.time,offDuty:end.time,source:'aeroline',aerolineRosterPlanned:true});
        }else if(eventCode==='MED'){
          if(start.date)activities.push({aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),dutyType:'Duty',date:start.date,onDuty:start.time,offDuty:end.time,totalDuty:timeSpanMins(start.time,end.time),credit:0,location:upper(r.eventEndLoc||r.eventStartLoc||appSettings().homeBase||'CMN'),remarks:'AeroLINE • MED • Medical examination',source:'aeroline',aerolineRosterPlanned:true,fieldSources:{onDuty:'aeroline',offDuty:'aeroline'}});
        }else if(eventCode==='DHD'||eventCode==='DHP'||eventCode==='HSBY'||eventCode==='STBY'){
          if(start.date){const positioning=eventCode==='DHD'||eventCode==='DHP';activities.push({
            aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),dutyType:positioning?eventCode:'STBY',date:start.date,
            flightNo:'',dep:upper(r.eventStartLoc||''),arr:upper(r.eventEndLoc||''),type:upper(r.fleetType||''),reg:aerolineRegistration(r),
            schedOut:positioning?start.time:'',schedIn:positioning?end.time:'',schedBlock:positioning?timeSpanMins(start.time,end.time):0,
            onDuty:start.time,offDuty:end.time,totalDuty:timeSpanMins(start.time,end.time),fieldSources:{onDuty:'aeroline',offDuty:'aeroline',schedOut:'aeroline',schedIn:'aeroline',totalDuty:'calculated'},
            out:'',off:'',on:'',in:'',block:null,credit:0,rolePic:false,instruction:false,simInstructor:false,simExaminer:false,night:null,sim:'no',ifr:'',pf:'no',approachType:'',
            dayTakeoffs:0,nightTakeoffs:0,dayLandings:0,nightLandings:0,courseType:'',remarks:`AeroLINE • ${eventCode}`,source:'aeroline',aerolineRosterPlanned:true,rosterHidden:false,locked:false
          })}
        }else unsupported.push(eventCode);
      }
    }

    if(r?.trainingProgramCode||r?.trainingSlotName){
      const key=aerolineTrainingKey(r);
      if(!seenTraining.has(key)){
        seenTraining.add(key);
        const start=aerolineDateTime(r.trainingStartTimeString),end=aerolineDateTime(r.trainingEndTimeString);
        if(start.date){
          const tm=aerolineTrainingForActivity(root,r,start),sim=aerolineIsSimulatorTraining(r,tm),dutyType=sim?'Simulator':'Ground Course',crew=aerolineTrainingCrewFields(root,tm,sim),trainerType=upper(tm?.trainerType||crew.trainerType||'');
          const fullName=upper(r.trainingSlotName||r.trainingProgramDesc||tm?.trainingTypeDesc||r.trainingProgramCode||'AEROLINE TRAINING');
          const desc=upper(r.trainingProgramDesc||tm?.trainingTypeDesc||'');
          const trainingLocationDirect=sim?upper(tm?.trainLocOrFlight||''):upper(r.trainingLocation||'');
          activities.push({
            aerolineKey:key,aerolineBlockId:String(r.crewScheduleBlockId||''),dutyType,date:start.date,flightNo:'',location:sim?trainingLocationDirect:'',dep:trainingLocationDirect,arr:'',type:upper(r.fleetType||''),reg:aerolineRegistration(r),
            schedOut:start.time,schedIn:end.time,schedBlock:timeSpanMins(start.time,end.time),onDuty:shiftTime(start.time,sim?-90:-60),offDuty:shiftTime(end.time,30),fieldSources:{schedOut:'aeroline',schedIn:'aeroline',schedBlock:'calculated',onDuty:'calculated',offDuty:'calculated'},out:'',off:'',on:'',in:'',block:null,
            simulatorTime:sim?timeSpanMins(start.time,end.time):null,totalDuty:timeSpanMins(shiftTime(start.time,sim?-90:-60),shiftTime(end.time,30)),credit:null,
            rolePic:false,instruction:false,simInstructor:sim&&crew.selfIsTrainer===true,simExaminer:false,night:null,sim:sim?'yes':'no',ifr:'',pf:'no',approachType:'',dayTakeoffs:0,nightTakeoffs:0,dayLandings:0,nightLandings:0,
            courseType:upper(r.trainingProgramCode||tm?.trainingType||r.trainingSlotName||''),remarks:desc&&desc!==fullName?`${fullName} • ${desc}`:fullName,source:'aeroline',aerolineRosterPlanned:true,rosterHidden:false,locked:false,
            picName:crew.picName,sicName:crew.sicName,soName:'',instructorName:crew.instructorName,aerolineTrainerType:trainerType,
            aerolineJoiningPilot:upper(tm?.joiningPilot||''),aerolineTrainerName:upper(tm?.trainerName||''),aerolineTrainingLabel:aerolineTrainingLabel(tm,r),aerolineTrainingSlotName:fullName
          });
        }
      }
    }
  });

  return{root,flights,activities,daysOff,validations:parseAerolineValidations(root),unsupported:[...new Set(unsupported)],start:String(root.schStartDate||'').slice(0,10),end:String(root.schEndDate||'').slice(0,10),monthLabel:aerolineMonthLabel(root)};
}
function aerolineWithinRange(date,start,end){return !!date&&(!start||date>=start)&&(!end||date<=end)}
function aerolineTimeDistance(a,b){const x=mins(a),y=mins(b);if(x==null||y==null)return 9999;const d=Math.abs(x-y);return Math.min(d,1440-d)}
function aerolineRosterMatch(rows,incoming){
  if(incoming.aerolineFlightLegId){const exact=rows.find(x=>String(x.aerolineFlightLegId||'')===incoming.aerolineFlightLegId);if(exact)return exact}
  const candidates=rows.filter(x=>x.date===incoming.date&&rosterFlightDigits(x.flightNo||'')===rosterFlightDigits(incoming.flightNo||'')&&upper(x.dep||'')===incoming.dep&&upper(x.arr||'')===incoming.arr);
  if(candidates.length===1)return candidates[0];
  if(candidates.length>1)return [...candidates].sort((a,b)=>aerolineTimeDistance(a.std,incoming.std)-aerolineTimeDistance(b.std,incoming.std))[0];
  return null;
}
function aerolineActivityMatch(rows,incoming){
  const exact=rows.find(x=>x.aerolineKey&&x.aerolineKey===incoming.aerolineKey);if(exact)return exact;
  return rows.find(x=>rosterActivitySameIdentity(x,incoming))||null;
}
function aerolineThanksDates(root){
  const out=new Set();
  for(const m of (root?.memoListDTO||root?.memoList||[])){
    if(!/\bTHANKS?\b/i.test(String(m?.message||'')))continue;
    const d=normalDate(String(m?.memoDate||m?.memoDateString||'').slice(0,10));
    if(/^\d{4}-\d{2}-\d{2}$/.test(d))out.add(d);
    else{
      const x=aerolineDateTime(m?.memoDateString||'');if(x.date)out.add(x.date);
    }
  }
  return out;
}
function importAerolineRosterObject(payload){return commitImport(()=>applyRosterImport(payload))}
function applyRosterImport(payload){
  const parsed=parseAerolineRosterObject(payload),{start,end}=parsed;
  if(!aerolineConfirmCrewIdentity(parsed.root))throw new Error('AeroLINE import cancelled: crew identity not confirmed.');

  const expiryImport=importAerolineValidations(parsed.root);
  let activities=coreActivityRows();
  const priorOffDates=new Set(activities.filter(x=>x.aerolineRosterPlanned&&/^(DAY OFF|OFF)$/i.test(x.dutyType||'')).map(x=>x.date));
  const thanksDates=aerolineThanksDates(parsed.root);
  const incomingKeys=new Set();
  let flightsAdded=0,flightsUpdated=0,activitiesAdded=0,activitiesUpdated=0,offAdded=0,offUpdated=0,staleRemoved=0;

  parsed.flights.forEach(r=>{
    incomingKeys.add(r.aerolineKey);
    const hadPriorOff=priorOffDates.has(r.date),hasThanks=thanksDates.has(r.date);
    if(hasThanks){
      r.aerolinePossibleDayOffPaid=true;
      r.aerolineDayOffPaidSignal=hadPriorOff?'OFF replaced by flight + Thanks memo':'Thanks memo on flight day';
    }
    const stableId=importCoreId('aeroline:'+r.aerolineKey),candidate=normalizeCoreActivity({id:stableId,...r,dutyType:'Flight',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED},'roster');
    if(importSuppressedByDeletion('activities',candidate))return;
    const match=activities.find(x=>x.aerolineKey&&x.aerolineKey===r.aerolineKey)||activities.find(x=>x.aerolineFlightLegId&&r.aerolineFlightLegId&&String(x.aerolineFlightLegId)===String(r.aerolineFlightLegId))||activities.find(x=>x.id===stableId);
    if(match){
      const i=activities.findIndex(x=>x.id===match.id),merged=sourceFieldMerge(match,candidate);
      activities[i]=normalizeCoreActivity(stamp({...merged,id:match.id,aerolineKey:r.aerolineKey,aerolineFlightLegId:r.aerolineFlightLegId,aerolineBlockId:r.aerolineBlockId,aerolineRosterPlanned:true,activityStatus:match.activityStatus||ACTIVITY_STATUS.PLANNED}),'roster');
      flightsUpdated++;
    }else{activities.push(stamp({...candidate,rolePic:appSettings().profileRole==='Captain',role:appSettings().profileRole==='Captain'?'PIC':'SIC'}));flightsAdded++}
  });

  parsed.activities.forEach(a=>{
    incomingKeys.add(a.aerolineKey);
    const stableId=importCoreId('aeroline:'+a.aerolineKey),candidate=normalizeCoreActivity({id:stableId,...a,activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED},'entries');
    if(importSuppressedByDeletion('activities',candidate))return;
    const match=activities.find(x=>x.aerolineKey&&x.aerolineKey===a.aerolineKey)||activities.find(x=>x.id===stableId);
    if(match){
      const i=activities.indexOf(match),preserveGroundCredit=canonicalDutyType(match.dutyType||'')==='Ground Course'&&match.groundCreditExplicit===true;
      const incoming=preserveGroundCredit?{...a,credit:match.credit,groundCreditExplicit:true}:a,merged=sourceFieldMerge(match,incoming);
      activities[i]=normalizeCoreActivity(stamp({...applyNewTrainingCredit(merged),id:match.id,aerolineKey:a.aerolineKey,aerolineBlockId:a.aerolineBlockId,aerolineRosterPlanned:true,locked:!!match.locked,activityStatus:match.activityStatus||ACTIVITY_STATUS.PLANNED}),'entries');
      activitiesUpdated++;
    }else{activities.push(stamp(initialTrainingCreditSource(candidate)));activitiesAdded++}
  });

  parsed.daysOff.forEach(d=>{
    incomingKeys.add(d.aerolineKey);
    const stableId=importCoreId('aeroline:'+d.aerolineKey),candidate=normalizeCoreActivity({id:stableId,...d,activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED},'duties');
    if(importSuppressedByDeletion('activities',candidate))return;
    let match=activities.find(x=>x.aerolineKey===d.aerolineKey)||activities.find(x=>x.id===stableId);
    if(match){
      const i=activities.indexOf(match),merged=sourceFieldMerge(match,candidate);
      activities[i]=normalizeCoreActivity(stamp({...merged,id:match.id,aerolineKey:d.aerolineKey,aerolineBlockId:d.aerolineBlockId,aerolineRosterPlanned:true,activityStatus:match.activityStatus||ACTIVITY_STATUS.PLANNED}),'duties');offUpdated++;
    }else{activities.push(stamp(candidate));offAdded++}
  });

  // SAFETY INVARIANT: a roster import may add/update exact source identities only.
  // Missing rows in a later roster file are never interpreted as a deletion request.
  staleRemoved=0;

  snapshotFlights('before-aeroline-import');
  commitCoreActivities(activities);
  cleanupRosterDuplicates();reconcileOperationalFlightIdentities();reconcileAllDuties();refreshEntrySuggestions();
  return{...parsed,flightsAdded,flightsUpdated,activitiesAdded,activitiesUpdated,offAdded,offUpdated,staleRemoved,expiryImport};
}
function aerolineImportStatusText(){
  const months=[];
  coreActivityRows().filter(x=>x.aerolineRosterPlanned&&x.date).forEach(x=>months.push(x.date.slice(0,7)));
  if(!months.length)return'Import the AeroLINE roster JSON. Re-importing the same month updates the existing roster instead of creating duplicates.';
  const latest=months.sort().pop(),d=new Date(`${latest}-01T00:00:00Z`);
  let label=latest;try{label=new Intl.DateTimeFormat('en-GB',{month:'long',year:'numeric',timeZone:'UTC'}).format(d)}catch{}
  return`AeroLINE roster available through ${label}. Importing the same month performs an update, not a duplicate import.`;
}

function parseCsv(text){const rows=[];let row=[],cell='',q=false;for(let i=0;i<text.length;i++){const c=text[i],n=text[i+1];if(c==='"'&&q&&n==='"'){cell+='"';i++}else if(c==='"')q=!q;else if(c===','&&!q){row.push(cell.trim());cell=''}else if((c==='\n'||c==='\r')&&!q){if(c==='\r'&&n==='\n')i++;row.push(cell.trim());cell='';if(row.some(x=>x!==''))rows.push(row);row=[]}else cell+=c}row.push(cell.trim());if(row.some(x=>x!==''))rows.push(row);return rows}
function parseTab(text){return String(text||'').replace(/^\uFEFF/,'').split(/\r?\n/).filter(x=>x.trim()!=='').map(line=>line.split('\t'))}
function normalDate(v){v=String(v||'').trim();if(/^\d{4}-\d{2}-\d{2}$/.test(v))return v;const m=v.match(/^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2,4})$/);if(m){const y=m[3].length===2?'20'+m[3]:m[3];return`${y}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`}return v}
function logTenImport(text){return commitImport(()=>applyLogTenTab(text))}
function applyLogTenTab(text){
  const rows=parseTab(text);if(rows.length<2)throw new Error('No LogTen rows found.');const headers=rows[0].map(x=>String(x||'').trim()),ix={};headers.forEach((h,i)=>ix[h]=i);if(!('flight_flightDate'in ix))throw new Error('This does not look like a LogTen Export Flights (Tab) file.');const g=(r,k)=>ix[k]===undefined?'':String(r[ix[k]]||'').trim();
  let fs=entryActivityRows(),imported=0,updated=0,sims=0,other=0;rows.slice(1).forEach(r=>{const date=normalDate(g(r,'flight_flightDate'));if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return;
    const category=upper(g(r,'aircraftType_selectedCategory')),
      remarks=g(r,'flight_remarks'),
      flightNoRaw=g(r,'flight_flightNumber'),
      logTenType=String(g(r,'flight_type')||'').trim(),
      simDur=durMins(g(r,'flight_simulator')),
      groundDur=durMins(g(r,'flight_ground')),
      transferText=[remarks,flightNoRaw,g(r,'flight_from'),g(r,'flight_to')].join(' ').toUpperCase(),
      isSimulator=logTenType==='3'||category==='SIMULATOR'||simDur>0,
      isGround=logTenType==='2'||(!isSimulator&&groundDur>0),
      isStandby=logTenType==='7',
      isTransferType=logTenType==='1',
      looksLikeTransfer=/\bDHD\b|\bDHP\b|DEADHEAD|\bTGV\b|\bRAM\b|PICK\s*UP|ROAD|TRANSFER|POSITIONING/.test(transferText),
      isDhpTransfer=isTransferType&&/\bDHP\b/.test(transferText);
    let dutyType=isSimulator?'Simulator':isGround?'Ground Course':isStandby?'STBY':'Flight';
    if(!isSimulator&&!isGround&&!isStandby&&isTransferType&&(looksLikeTransfer||!flightNoRaw)){
      const proposed=isDhpTransfer?'DHP':'DHD';
      const evidence=[remarks,flightNoRaw,g(r,'flight_from'),g(r,'flight_to')].filter(Boolean).join(' | ');
      const ok=confirm(`LogTen ${date}${flightNoRaw?` • ${flightNoRaw}`:''} appears to be ${proposed}.\n\nSource evidence: ${evidence||'LogTen transfer type'}\n\nClassify this record as ${proposed}?\nCancel leaves it as Flight for manual review.`);
      if(ok)dutyType=proposed;
    }
    const out=g(r,'flight_actualDepartureTime'),inn=g(r,'flight_actualArrivalTime'),off=g(r,'flight_takeoffTime'),on=g(r,'flight_landingTime'),schedOut=g(r,'flight_scheduledDepartureTime'),schedIn=g(r,'flight_scheduledArrivalTime'),schedBlock=schedOut&&schedIn?timeSpanMins(schedOut,schedIn):0,block=dutyType==='Flight'&&out&&inn?timeSpanMins(out,inn):null,dual=durMins(g(r,'flight_dualGiven')),sfiSfe=durMins(g(r,'flight_sfiSfe')),picRaw=g(r,'flight_pic'),sicRaw=g(r,'flight_sic'),pic=durMins(picRaw),sic=durMins(sicRaw),ifrTime=durMins(g(r,'flight_ifr')),creditRaw=g(r,'flight_credit'),creditImported=creditRaw!=='',dayLd=Number(g(r,'flight_dayLandings')||0),nightLd=Number(g(r,'flight_nightLandings')||0),dayTo=Number(g(r,'flight_dayTakeoffs')||0),nightTo=Number(g(r,'flight_nightTakeoffs')||0),onDuty=g(r,'flight_onDutyTime'),offDuty=g(r,'flight_offDutyTime');
    const f=stamp({dutyType,date,flightNo:dutyType==='Flight'?composeFlightNo(flightNoRaw):'',dep:upper(g(r,'flight_from')),arr:upper(g(r,'flight_to')),reg:upper(g(r,'aircraft_aircraftID')),type:upper(g(r,'aircraftType_type')),schedOut,schedIn,schedBlock,onDuty,offDuty,totalDuty:onDuty&&offDuty?timeSpanMins(onDuty,offDuty):null,out:dutyType==='Flight'?out:'',off:dutyType==='Flight'?off:'',on:dutyType==='Flight'?on:'',in:dutyType==='Flight'?inn:'',block,airTime:dutyType==='Flight'&&off&&on?timeSpanMins(off,on):null,totalFlightTime:dutyType==='Flight'?durMins(g(r,'flight_totalTime')):null,simulatorTime:isSimulator?simDur:null,simulatorTimeImported:isSimulator&&g(r,'flight_simulator')!=='',credit:creditImported?durMins(creditRaw):null,creditImported,rolePic:pic>0,role:pic>0?'PIC':sic>0?'SIC':'',pic:picRaw!==''?pic:null,sic:sicRaw!==''?sic:null,instruction:dutyType==='Flight'&&dual>0,simInstructor:false,simExaminer:false,night:g(r,'flight_night')||null,ifrTime,dualGiven:dual,sfiSfe,sim:isSimulator?'yes':'no',ifr:'',dayTakeoffs:dayTo,nightTakeoffs:nightTo,dayLandings:dayLd,nightLandings:nightLd,courseType:isGround?upper(remarks):'',remarks,picName:upper(g(r,'flight_selectedCrewPIC')||''),sicName:upper(g(r,'flight_selectedCrewSIC')||''),soName:upper(g(r,'flight_selectedCrewStudent')||''),instructorName:upper(g(r,'flight_selectedCrewInstructor')||''),examinerName:'',pf:(dayTo+nightTo+dayLd+nightLd)>0?'yes':'no',approachType:upper(g(r,'flight_selectedApproach1')||''),fieldSources:{...(schedOut?{schedOut:'logten'}:{}),...(schedIn?{schedIn:'logten'}:{}),...(schedOut&&schedIn?{schedBlock:'calculated'}:{}),...(onDuty?{onDuty:'logten'}:{}),...(offDuty?{offDuty:'logten'}:{}),...(onDuty&&offDuty?{totalDuty:'calculated'}:{}),...(out?{out:'logten'}:{}),...(off?{off:'logten'}:{}),...(on?{on:'logten'}:{}),...(inn?{in:'logten'}:{}),...(dutyType==='Flight'&&out&&inn?{block:'calculated'}:{}),...(dutyType==='Flight'&&off&&on?{airTime:'calculated'}:{}),...(g(r,'flight_totalTime')?{totalFlightTime:'logten'}:{}),...(isSimulator&&g(r,'flight_simulator')?{simulatorTime:'logten'}:{}),...(creditRaw?{credit:'logten'}:{}),...(picRaw?{pic:'logten'}:{}),...(sicRaw?{sic:'logten'}:{}),...(g(r,'flight_night')?{night:'logten'}:{}),...(g(r,'flight_ifr')?{ifrTime:'logten'}:{}),...(g(r,'flight_dualGiven')?{dualGiven:'logten'}:{}),...(g(r,'flight_sfiSfe')?{sfiSfe:'logten'}:{})},source:'logten',sourceRowType:logTenType,sourceRowKey:[date,logTenType,flightNoRaw,g(r,'flight_from'),g(r,'flight_to'),g(r,'flight_scheduledDepartureTime'),g(r,'flight_scheduledArrivalTime'),onDuty,offDuty,remarks].map(x=>upper(x)).join('|')});
    if(importSuppressedByDeletion('activities',f))return;
    // SAFETY INVARIANT: TAB import may update only the exact stable sourceRowKey.
    // Similar date/route/time/type records are never merged automatically.
    let match=fs.findIndex(x=>x.source==='logten'&&x.sourceRowKey&&x.sourceRowKey===f.sourceRowKey);
    if(match>=0){
      f.id=fs[match].id;
      fs[match]=mergeDuplicateEntries(fs[match],f);
      updated++;
    }else{
      f.id=importCoreId(syncSourceKey(f));
      fs.push(f);
      imported++;
    }
    if(isSimulator)sims++;
    if(!['Flight','Simulator'].includes(dutyType))other++;
  });

  replaceActivitySelection(isEntryActivityRecord,fs);
  reconcileAllDuties();
  return{imported,updated,sims,other,duplicatesRemoved:0}
}


// Read-only SQLite/Core Data import. WAL checksums and commit boundaries follow
// https://sqlite.org/fileformat.html; SHM is retained as the original WAL index.
function logTenSqliteSnapshot(database,wal,shm){
  const u8=new Uint8Array(database),w=new Uint8Array(wal),s=new Uint8Array(shm);
  if(u8.length<100||new TextDecoder().decode(u8.subarray(0,16))!=='SQLite format 3\0')throw new Error('Invalid LogTen SQLite database.');
  const d=new DataView(u8.buffer,u8.byteOffset,u8.byteLength),pageSize=d.getUint16(16)===1?65536:d.getUint16(16);
  if(pageSize<512||pageSize>65536||(pageSize&(pageSize-1))||u8.length%pageSize)throw new Error('Invalid SQLite page size.');
  if(s.length&&s.length%32768)throw new Error('Incomplete LogTen SHM file. Select a consistent copy of all three files.');
  if(!w.length)return{database:u8.slice(),walFrames:0};
  if(w.length<32)throw new Error('Incomplete LogTen WAL header.');
  const v=new DataView(w.buffer,w.byteOffset,w.byteLength),magic=v.getUint32(0),little=magic===0x377f0682;
  if(![0x377f0682,0x377f0683].includes(magic)||v.getUint32(4)!==3007000||v.getUint32(8)!==pageSize)throw new Error('LogTen WAL does not match the database format.');
  let a=0,b=0;
  const checksum=(start,length)=>{for(let i=start;i<start+length;i+=8){a=(a+v.getUint32(i,little)+b)>>>0;b=(b+v.getUint32(i+4,little)+a)>>>0}};
  checksum(0,24);
  if(a!==v.getUint32(24)||b!==v.getUint32(28))throw new Error('LogTen WAL header checksum failed.');
  let committed=0,pages=0,valid=0;
  for(let p=32;p+24+pageSize<=w.length;p+=24+pageSize){
    if(v.getUint32(p+8)!==v.getUint32(16)||v.getUint32(p+12)!==v.getUint32(20))break;
    checksum(p,8);checksum(p+24,pageSize);
    if(a!==v.getUint32(p+16)||b!==v.getUint32(p+20))break;
    if(!v.getUint32(p))throw new Error('Invalid WAL page number.');
    valid++;if(v.getUint32(p+4)){committed=valid;pages=v.getUint32(p+4)}
  }
  // A copied, initialized SHM must not claim committed frames that were lost
  // or corrupted in the supplied WAL. Stale/uncommitted tail frames are ignored.
  if(s.length>=96){
    const sv=new DataView(s.buffer,s.byteOffset,s.byteLength),le=sv.getUint32(0,true)===3007000;
    if(s[12]===1&&sv.getUint32(0,le)===3007000){
      for(let i=0;i<48;i++)if(s[i]!==s[i+48])throw new Error('LogTen SHM changed while being copied. Copy the database files again.');
      for(let i=0;i<8;i++)if(s[32+i]!==w[16+i])throw new Error('LogTen WAL and SHM belong to different snapshots.');
      if(sv.getUint32(16,le)>committed)throw new Error('LogTen WAL is incomplete or damaged; committed records would be missing.');
    }
  }
  if(!committed){if(w.length>32)throw new Error('No complete committed LogTen WAL transaction found.');return{database:u8.slice(),walFrames:0}}
  if(pages*pageSize>512*1024*1024)throw new Error('LogTen database exceeds the supported import size.');
  const out=new Uint8Array(pages*pageSize);out.set(u8.subarray(0,out.length));
  for(let i=0;i<committed;i++){const p=32+i*(24+pageSize),n=v.getUint32(p);if(n<=pages)out.set(w.subarray(p+24,p+24+pageSize),(n-1)*pageSize)}
  // Export a standalone checkpointed snapshot; original sql/wal/shm are also archived.
  const ov=new DataView(out.buffer);out[18]=1;out[19]=1;ov.setUint32(28,pages);ov.setUint32(92,ov.getUint32(24));
  return{database:out,walFrames:committed};
}
function readLogTenSqlite(bytes){
  const b=new Uint8Array(bytes),v=new DataView(b.buffer,b.byteOffset,b.byteLength),size=v.getUint16(16)===1?65536:v.getUint16(16),usable=size-b[20];
  const encoding=({1:'utf-8',2:'utf-16le',3:'utf-16be'})[v.getUint32(56)];
  if(!encoding)throw new Error('Unsupported SQLite text encoding.');
  const decoder=new TextDecoder(encoding),limit=b.length/size;
  const page=n=>{if(!Number.isInteger(n)||n<1||n>limit)throw new Error('Invalid SQLite page reference.');return(n-1)*size};
  const varint=(buf,state)=>{let n=0;for(let i=0;i<9;i++){if(state.p>=buf.length)throw new Error('Truncated SQLite integer.');const x=buf[state.p++];n=n*(i===8?256:128)+(i===8?x:x&127);if(!(x&128)||i===8){if(!Number.isSafeInteger(n))throw new Error('SQLite integer exceeds safe precision.');return n}}};
  const record=payload=>{
    const state={p:0},end=varint(payload,state),types=[];if(end>payload.length)throw new Error('Invalid SQLite record header.');
    while(state.p<end)types.push(varint(payload,state));let p=end;const view=new DataView(payload.buffer,payload.byteOffset,payload.byteLength);
    return types.map(t=>{
      if(t===0)return null;if(t===8)return 0;if(t===9)return 1;if(t===10||t===11)throw new Error('Invalid SQLite serial type.');
      const n=t>=12?Math.floor((t-12)/2):[0,1,2,3,4,6,8,8][t];if(p+n>payload.length)throw new Error('Truncated SQLite record.');
      let value;if(t===7)value=view.getFloat64(p);else if(t<7){let x=0n;for(let i=0;i<n;i++)x=(x<<8n)|BigInt(payload[p+i]);if(payload[p]&128)x-=1n<<BigInt(n*8);value=Number(x);if(!Number.isSafeInteger(value))throw new Error('SQLite integer exceeds safe precision.')}
      else value=t%2?decoder.decode(payload.subarray(p,p+n)):payload.slice(p,p+n);p+=n;return value;
    });
  };
  const table=root=>{
    const rows=[],visited=new Set();
    const walk=n=>{
      if(visited.has(n))throw new Error('Cyclic SQLite table.');visited.add(n);
      const base=page(n),h=base+(n===1?100:0),type=b[h],count=v.getUint16(h+3),header=type===5?12:8;
      if(![5,13].includes(type)||h+header+count*2>base+usable)throw new Error('Unsupported or damaged LogTen table.');
      for(let i=0;i<count;i++){
        let p=base+v.getUint16(h+header+i*2);if(p<base||p>=base+usable)throw new Error('Invalid SQLite cell.');
        if(type===5){walk(v.getUint32(p));continue}
        const st={p},length=varint(b,st),rowid=varint(b,st);p=st.p;
        if(length>b.length)throw new Error('Invalid SQLite payload length.');
        const min=Math.floor((usable-12)*32/255)-23,k=min+(length-min)%(usable-4),local=length<=usable-35?length:k<=usable-35?k:min;
        if(p+local+(local<length?4:0)>base+usable)throw new Error('Invalid SQLite cell payload.');
        const payload=new Uint8Array(length);payload.set(b.subarray(p,p+local));let copied=local,next=local<length?v.getUint32(p+local):0;const seen=new Set();
        while(copied<length){if(seen.has(next))throw new Error('Cyclic SQLite overflow.');seen.add(next);const q=page(next),take=Math.min(length-copied,usable-4);payload.set(b.subarray(q+4,q+4+take),copied);copied+=take;next=v.getUint32(q)}
        rows.push({rowid,values:record(payload)});
      }
      if(type===5)walk(v.getUint32(h+8));
    };walk(root);return rows;
  };
  const schema=table(1).map(r=>r.values),tables=schema.filter(r=>r[0]==='table');
  const read=name=>{
    const spec=tables.find(r=>r[1]===name);if(!spec)return[];
    // Core Data uses flat column declarations, with no expressions/default clauses.
    const sql=String(spec[4]),body=sql.slice(sql.indexOf('(')+1,sql.lastIndexOf(')')),defs=body.split(',');
    const cols=defs.map(s=>s.trim().match(/^([A-Z_][A-Z0-9_]*)\s+(INTEGER|TIMESTAMP|FLOAT|VARCHAR|BLOB|REAL|TEXT|DOUBLE)\b/i)?.[1]);
    if(cols.some(x=>!x))throw new Error(`Unsupported LogTen schema: ${name}.`);
    return table(spec[3]).map(r=>Object.fromEntries(cols.map((key,i)=>[key,key==='Z_PK'?r.rowid:(r.values[i]??null)])));
  };
  return{read,tableCount:tables.length};
}
const LOGTEN_TIME_COLUMNS=Object.freeze({totalFlightTime:'TOTALTIME',pic:'PIC',sic:'SIC',night:'NIGHT',picNight:'PICNIGHT',ifrTime:'IFR',multiPilot:'MULTIPILOT',dualGiven:'DUALGIVEN',dualReceived:'DUALRECEIVED',picUS:'P1US',simulatorTime:'SIMULATOR',sfiSfe:'SFI',totalInstrument:'TOTALINSTRUMENT'});
function logTenSqliteEntries(db){
  const index=t=>new Map(db.read(t).map(r=>[r.Z_PK,r])),types=index('ZAIRCRAFTTYPE'),aircraft=index('ZAIRCRAFT'),places=index('ZPLACE'),crew=index('ZFLIGHTCREW'),people=index('ZPERSON');
  const date=n=>n==null?'':new Date((Number(n)+978307200)*1000).toISOString(),clock=n=>date(n).slice(11,16);
  const place=id=>{const r=places.get(id)||{};return r.ZPLACE_IATAID||r.ZPLACE_ICAOID||r.ZPLACE_IDENTIFIER||''};
  const person=id=>{const r=people.get(id)||{};return r.ZPERSON_NAME||r.ZPERSON_FULLNAME||[r.ZPERSON_FIRSTNAME,r.ZPERSON_LASTNAME].filter(Boolean).join(' ')};
  const rows=db.read('ZFLIGHT');if(!rows.length)throw new Error('No LogTen ZFLIGHT records found.');
  return rows.map(r=>{
    const g=k=>r['ZFLIGHT_'+k],kind=Number(g('TYPE')),a=aircraft.get(g('AIRCRAFT'))||{},t=types.get(g('AIRCRAFTTYPE'))||{},c=crew.get(g('FLIGHTCREW'))||{};
    const e={logtenPk:r.Z_PK,logtenType:kind,logtenUniqueId:r.ZLOGTEN_UNIQUEID,date:date(g('FLIGHTDATE')).slice(0,10),dutyType:({0:'Flight',1:'Duty',2:'Ground Course',3:'Simulator',7:'STBY'})[kind],flightNo:g('FLIGHTNUMBER')||'',dep:place(g('FROMPLACE')),arr:place(g('TOPLACE')),type:t.ZAIRCRAFTTYPE_TYPE||'',reg:a.ZAIRCRAFT_AIRCRAFTID||'',remarks:g('REMARKS')||'',locked:!!g('ISLOCKED'),source:'logten',fieldSources:{},logtenCustom:{}};
    if(!e.dutyType)throw new Error(`Unsupported LogTen activity type ${kind}.`);
    for(const [key,col] of Object.entries(LOGTEN_TIME_COLUMNS)){e[key]=g(col)??null;e.fieldSources[key]='logten'}
    e.logtenCustom.totalInstrument=e.totalInstrument;
    for(const [key,col] of Object.entries({out:'ACTUALDEPARTURETIME',in:'ACTUALARRIVALTIME',off:'TAKEOFFTIME',on:'LANDINGTIME',schedOut:'SCHEDULEDDEPARTURETIME',schedIn:'SCHEDULEDARRIVALTIME',onDuty:'ONDUTYTIME',offDuty:'OFFDUTYTIME'})){e[key]=clock(g(col));if(g(col)!=null)e.fieldSources[key]='logten'}
    e.block=e.totalFlightTime;e.fieldSources.block='logten';e.rolePic=(e.pic||0)>0;e.role=e.rolePic?'PIC':(e.sic||0)>0?'SIC':'';e.instruction=(e.dualGiven||0)>0;
    for(const [key,col] of Object.entries({dayTakeoffs:'DAYTAKEOFFS',nightTakeoffs:'NIGHTTAKEOFFS',dayLandings:'DAYLANDINGS',nightLandings:'NIGHTLANDINGS',airTime:'DURATION',totalDuty:'TOTALDUTYTIME',groundInstruction:'GROUND',pax:'PAXCOUNT',fuelAboard:'FUELTOTALABOARD',fuelBurned:'FUELBURNED',fuelMinimum:'FUELMINIMUMFORDIVERSION'})){e[key]=g(col)??null;e.fieldSources[key]='logten'}
    for(const [key,col] of Object.entries({picName:'PIC',sicName:'SIC',instructorName:'INSTRUCTOR',studentName:'STUDENT',purserName:'PURSER',attendant1Name:'FLIGHTATTENDANT2',attendant2Name:'FLIGHTATTENDANT3',attendant3Name:'FLIGHTATTENDANT4'}))e[key]=person(c['ZFLIGHTCREW_'+col]);
    return e;
  });
}
async function parseLogTenCompleteFiles(files){
  const list=Array.from(files||[]);
  if(list.length===1&&/\.pilotlog-logten$/i.test(list[0].name))return parseLogTenMigrationPackage(await list[0].arrayBuffer());
  const main=list.filter(f=>/\.sql$/i.test(f.name));
  if(main.length!==1)throw new Error('Select LogTenCoreDataStore.sql together with its .sql-wal and .sql-shm files.');
  const file=main[0],wal=list.find(f=>f.name===file.name+'-wal'),shm=list.find(f=>f.name===file.name+'-shm');
  if(!wal||!shm||list.length!==3)throw new Error('Select all three LogTen files with the same basename: .sql, .sql-wal and .sql-shm.');
  const [originalDatabase,originalWal,originalShm]=await Promise.all([file,wal,shm].map(f=>f.arrayBuffer()));
  const snapshot=logTenSqliteSnapshot(originalDatabase,originalWal,originalShm),db=readLogTenSqlite(snapshot.database),entries=logTenSqliteEntries(db),dates=entries.map(r=>r.date).sort();
  return{database:snapshot.database,originalFiles:{database:originalDatabase,wal:originalWal,shm:originalShm},payload:{entries,manifest:{sourceName:file.name,flightRows:entries.length,sqliteTables:db.tableCount,dateFrom:dates[0],dateTo:dates.at(-1),walFrames:snapshot.walFrames}}};
}

/* Complete LogTen SQLite migration archive */
const LOGTEN_MIGRATION_MAGIC='PLGTN001';
const LOGTEN_ARCHIVE_DB='pilotlog7-logten-archive';
const LOGTEN_ARCHIVE_STORE='archives';

function logTenArchiveMeta(){return loadObject(LOGTEN_ARCHIVE_META_KEY,{})}
function logTenArchiveStatusText(){
  const m=logTenArchiveMeta();
  if(!m?.storedAt)return'No complete LogTen database archived yet.';
  const size=Number(m.sourceBytes||0)/(1024*1024),rows=Number(m.flightRows||0),tables=Number(m.sqliteTables||0);
  const span=m.dateFrom&&m.dateTo?` • ${displayDate(m.dateFrom)}–${displayDate(m.dateTo)}`:'';
  return`Full LogTen archive stored locally • ${rows.toLocaleString('en-US')} logbook rows${span}${tables?` • ${tables} tables`:''}${size?` • ${size.toFixed(1)} MB`:''}. Original SQLite bytes retained; not sent to cloud.`;
}
function openLogTenArchiveDb(){
  return new Promise((resolve,reject)=>{
    const req=indexedDB.open(LOGTEN_ARCHIVE_DB,1);
    req.onupgradeneeded=()=>{const db=req.result;if(!db.objectStoreNames.contains(LOGTEN_ARCHIVE_STORE))db.createObjectStore(LOGTEN_ARCHIVE_STORE)};
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error||new Error('LogTen archive database unavailable'));
  });
}
async function storeLogTenArchive(databaseBytes,manifest={},sourceFileName='LogTenCoreDataStore.sql',originalFiles=null){
  const db=await openLogTenArchiveDb();
  const record={
    storedAt:new Date().toISOString(),
    sourceFileName:sourceFileName||manifest.sourceName||'LogTenCoreDataStore.sql',
    sourceBytes:Number(manifest.sourceBytes)||databaseBytes.byteLength||0,
    sourceSha256:String(manifest.sourceSha256||''),
    flightRows:Number(manifest.flightRows)||0,
    sqliteTables:Number(manifest.sqliteTables)||0,
    dateFrom:String(manifest.dateFrom||''),dateTo:String(manifest.dateTo||''),
    manifest,
    database:new Blob([databaseBytes],{type:'application/x-sqlite3'}),
    ...(originalFiles?{originalFiles:Object.fromEntries(Object.entries(originalFiles).map(([k,v])=>[k,new Blob([v])]))}:{})
  };
  try{
    await new Promise((resolve,reject)=>{
      const tx=db.transaction(LOGTEN_ARCHIVE_STORE,'readwrite');
      tx.objectStore(LOGTEN_ARCHIVE_STORE).put(record,'latest');
      tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error||new Error('LogTen archive write aborted'));
    });
  }finally{db.close()}
  saveObject(LOGTEN_ARCHIVE_META_KEY,{
    storedAt:record.storedAt,sourceFileName:record.sourceFileName,sourceBytes:record.sourceBytes,sourceSha256:record.sourceSha256,
    flightRows:record.flightRows,sqliteTables:record.sqliteTables,dateFrom:record.dateFrom,dateTo:record.dateTo
  });
  return record;
}
async function getLogTenArchive(){
  const db=await openLogTenArchiveDb();
  try{
    return await new Promise((resolve,reject)=>{
      const tx=db.transaction(LOGTEN_ARCHIVE_STORE,'readonly'),req=tx.objectStore(LOGTEN_ARCHIVE_STORE).get('latest');
      req.onsuccess=()=>resolve(req.result||null);req.onerror=()=>reject(req.error);
    });
  }finally{db.close()}
}
async function exportArchivedLogTenDatabase(){
  const r=await getLogTenArchive();
  if(!r?.database)return alert('No complete LogTen database is archived on this device.');
  const name=String(r.sourceFileName||'LogTenCoreDataStore.sql').replace(/[\\/:*?"<>|]+/g,'_')||'LogTenCoreDataStore.sql';
  download(name,r.database,'application/x-sqlite3');
}
function parseLogTenMigrationPackage(buffer){
  const u8=new Uint8Array(buffer);if(u8.byteLength<16)throw new Error('Migration file is too small.');
  const magic=new TextDecoder('ascii').decode(u8.subarray(0,8));
  if(magic!==LOGTEN_MIGRATION_MAGIC)throw new Error('This is not a PilotLog complete LogTen migration file.');
  const jsonLen=new DataView(buffer,8,4).getUint32(0,true),jsonStart=12,jsonEnd=jsonStart+jsonLen;
  if(jsonLen<20||jsonEnd>u8.byteLength)throw new Error('Migration package header is damaged.');
  let payload;try{payload=JSON.parse(new TextDecoder().decode(u8.subarray(jsonStart,jsonEnd)))}catch{throw new Error('Migration package metadata is not valid JSON.')} 
  if(payload?.schema!=='PilotLog LogTen Migration'||Number(payload?.formatVersion)!==1||!Array.isArray(payload?.entries))throw new Error('Unsupported LogTen migration format.');
  const database=u8.slice(jsonEnd);if(database.byteLength<100||new TextDecoder('ascii').decode(database.subarray(0,16))!=='SQLite format 3\u0000')throw new Error('The embedded original LogTen SQLite database is missing or damaged.');
  return{payload,database};
}
function logTenLegacyRowKey(e){
  return[e.date,e.logtenType,e.flightNo,e.dep,e.arr,e.schedOut,e.schedIn,e.onDuty,e.offDuty,e.remarks].map(x=>upper(x)).join('|');
}
function logTenMigrationEntry(raw){
  const {logtenPk,logtenType,...data}=raw||{};const uid=String(data.logtenUniqueId||'').trim();
  if(!uid)throw new Error('A LogTen row is missing its stable unique ID.');
  const f={...data,fieldSources:{...(data.fieldSources||{})}};
  for(const key of Object.keys(LOGTEN_TIME_COLUMNS))if(Object.prototype.hasOwnProperty.call(f,key))f.fieldSources[key]='logten';
  if(!Object.prototype.hasOwnProperty.call(f,'totalInstrument')&&Object.prototype.hasOwnProperty.call(f.logtenCustom||{},'totalInstrument')){f.totalInstrument=f.logtenCustom.totalInstrument;f.fieldSources.totalInstrument='logten'}
  f.source='logten';f.logtenUniqueId=uid;f.id=importCoreId('logten:'+uid);
  f.dutyType=canonicalDutyType(f.dutyType||'Flight');f.date=String(f.date||'').slice(0,10);
  if(isFlight(f))f.schedBlock=scheduleBlockMins(f);
  ['flightNo','dep','arr','reg','type','courseType','picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name','approachType'].forEach(k=>{if(f[k]!=null)f[k]=upper(f[k])});
  if(Array.isArray(f.approaches))f.approaches=f.approaches.map(a=>({
    type:upper(a?.type||''),quantity:Math.max(1,Number(a?.quantity)||1),night:!!a?.night,simulated:!!a?.simulated,
    precision:a?.precision==null?null:!!a.precision,termination:Number(a?.termination)||0,place:upper(a?.place||''),comment:String(a?.comment||'')
  })).filter(a=>a.type);
  if(isSim(f)){
    f.sim='yes';
    f.dep=upper(f.dep||f.location||'');f.location='';f.out='';f.off='';f.on='';f.in='';f.block=null;f.simInstructor=false;f.simExaminer=false;
    if(Object.prototype.hasOwnProperty.call(f,'simulatorTime'))f.simulatorTimeImported=true;
    if(f.creditImported!==true)f.credit=null;
  }else delete f.sim;
  if(f.pf==='no')delete f.pf;
  if(!f.locked)delete f.locked;
  f._logtenImportType=Number(logtenType||0);f._logtenLegacyKey=logTenLegacyRowKey({...raw,logtenType:Number(logtenType||0)});
  return f;
}
function logTenMigrationFallbackMatch(existing,incoming){
  if(!existing||existing.date!==incoming.date)return false;
  if(existing.logtenUniqueId)return String(existing.logtenUniqueId)===String(incoming.logtenUniqueId);
  const same=(key)=>!!existing[key]&&!!incoming[key]&&upper(existing[key])===upper(incoming[key]);
  const compatible=(key)=>!existing[key]||!incoming[key]||same(key);
  if(isFlight(existing)&&isFlight(incoming)){
    if(!same('dep')||!same('arr')||!compatible('reg'))return false;
    if(same('out')&&same('in'))return true;
    const block=numericTime(incoming.totalFlightTime),existingBlock=entryBlockMins(existing);
    if((same('out')||same('in'))&&block>0&&block===existingBlock)return true;
    if(same('schedOut')&&same('schedIn'))return true;
    // A source placeholder has no operational time: only a unique route may match.
    if(!incoming.flightNo&&!incoming.out&&!incoming.in&&!incoming.schedOut&&!incoming.schedIn&&!block)return true;
    return false;
  }
  if(isSim(existing)&&isSim(incoming)){
    if(same('schedOut')&&same('schedIn')&&numericTime(existing.simulatorTime)===numericTime(incoming.simulatorTime))return true;
    return same('reg')&&same('onDuty')&&same('offDuty');
  }
  const a=canonicalDutyType(existing.dutyType),b=canonicalDutyType(incoming.dutyType);
  if(a!==b&&!(b==='Duty'&&['DHD','DHP'].includes(a)))return false;
  if(a==='Flight'||a==='Simulator')return false;
  if(!compatible('dep')||!compatible('arr'))return false;
  // Non-flight rows are accepted only when this date/type identifies one candidate.
  return true;
}
function mergeLogTenMigration(existing,incoming){
  // An explicit complete-source import repairs source fields, including locked
  // LogTen records. Per-field manual edits remain authoritative; lock still protects UI edits.
  const merged={...existing,fieldSources:{...(existing.fieldSources||{})}},conflicts=[];
  const keys=[...Object.keys(LOGTEN_TIME_COLUMNS),'type'];
  for(const key of keys){
    if(!Object.prototype.hasOwnProperty.call(incoming,key))continue;
    const value=incoming[key],manual=entryManualField(existing,key)||entryFieldSource(existing,key)==='manual'||(key==='totalInstrument'&&entryManualField(existing,'logtenCustom'));
    if(key==='type'&&!value)continue;
    if(manual){if(existing[key]!==value)conflicts.push({field:key,manual:existing[key],logten:value});continue}
    merged[key]=value;merged.fieldSources[key]='logten';
  }
  merged.logtenCustom={...(existing.logtenCustom||{})};
  if(!entryManualField(existing,'logtenCustom')&&!entryManualField(existing,'totalInstrument')&&entryFieldSource(existing,'totalInstrument')!=='manual'){
    if(Object.prototype.hasOwnProperty.call(incoming,'totalInstrument'))merged.logtenCustom.totalInstrument=incoming.totalInstrument;
    else if(Object.prototype.hasOwnProperty.call(incoming.logtenCustom||{},'totalInstrument')){merged.logtenCustom.totalInstrument=incoming.logtenCustom.totalInstrument;merged.totalInstrument=incoming.logtenCustom.totalInstrument;merged.fieldSources.totalInstrument='logten'}
  }
  merged.id=existing.id||incoming.id;merged.logtenUniqueId=incoming.logtenUniqueId;
  if(existing.source!=='manual')merged.source='logten';
  merged.logtenImportConflicts=conflicts;
  if(Object.prototype.hasOwnProperty.call(incoming,'simulatorTime'))merged.simulatorTimeImported=true;
  merged._updatedAt=new Date().toISOString();
  return merged;
}
function reconcileLogTenEntries(existingRows,rawEntries){
  const fs=(existingRows||[]).map(row=>({...row})),uidIndex=new Map();let imported=0,updated=0,protectedMerged=0,conflicts=0;
  for(let i=0;i<fs.length;i++){
    const uid=String(fs[i].logtenUniqueId||'').trim();
    if(!uid)continue;
    if(uidIndex.has(uid))throw new Error(`Duplicate existing LogTen ID: ${uid}. No entries were changed.`);
    uidIndex.set(uid,i);
  }
  const seen=new Set();
  for(const raw of rawEntries){
    const incoming=logTenMigrationEntry(raw),uid=String(incoming.logtenUniqueId||'').trim();
    if(!/^\d{4}-\d{2}-\d{2}$/.test(incoming.date))throw new Error('Invalid LogTen flight date. No entries were imported.');
    if(!uid)throw new Error(`LogTen row without Unique ID on ${incoming.date}. No entries were imported.`);
    if(seen.has(uid))throw new Error(`Duplicate LogTen ID in source: ${uid}. No entries were imported.`);seen.add(uid);
    const idx=uidIndex.has(uid)?uidIndex.get(uid):-1;
    if(idx>=0){
      fs[idx]=mergeLogTenMigration(fs[idx],incoming);updated++;
      const n=fs[idx].logtenImportConflicts.length;if(n){protectedMerged++;conflicts+=n}
    }else{
      delete incoming._logtenImportType;delete incoming._logtenLegacyKey;incoming._updatedAt=new Date().toISOString();
      const next=fs.length;fs.push(initialTrainingCreditSource(incoming));uidIndex.set(uid,next);imported++;
    }
  }
  return{rows:fs,imported,updated,protectedMerged,conflicts,removedIds:[]};
}
function mergeLogTenSupplementalRows(key,kind,rawRows){
  if(!Array.isArray(rawRows)||!rawRows.length)return{imported:0,updated:0,total:0};
  let rows=kind==='expiry'?dedupeExpiryRecords(load(key)).rows:load(key), now=new Date().toISOString();
  let imported=0,updated=0;
  for(const raw of (Array.isArray(rawRows)?rawRows:[])){
    const uid=String(raw?.logtenUniqueId||'').trim();if(!uid)continue;
    const stableId=stableSourceId(kind,`logten:${uid}`),incoming={...raw,id:stableId,source:'logten',logtenUniqueId:uid,_updatedAt:now};
    if(importSuppressedByDeletion(kind,incoming))continue;
    let idx=rows.findIndex(x=>String(x?.logtenUniqueId||'')===uid||x?.id===stableId);
    if(idx<0&&kind==='expiry'){const semantic=findExpirySemanticMatch(rows,incoming);if(semantic)idx=rows.indexOf(semantic)}
    if(idx>=0){
      const existing=rows[idx],protectedExisting=!!(existing?.locked||existing?.manualOverride||existing?.source==='manual');
      if(protectedExisting){const preserved={...incoming,...existing};preserved.id=existing.id||stableId;preserved.logtenUniqueId=uid;preserved.source=existing.source||'manual';preserved.manualOverride=!!existing.manualOverride;preserved.locked=!!existing.locked;preserved._updatedAt=now;rows[idx]=preserved}
      else if(kind==='expiry'&&existing.source==='aeroline'){rows[idx]={...incoming,...existing,id:existing.id||stableId,logtenUniqueId:uid,_updatedAt:now}}
      else rows[idx]={...existing,...incoming,id:existing.id||stableId};
      updated++;
    }else{rows.push(incoming);imported++}
  }
  if(kind==='expiry')rows=dedupeExpiryRecords(rows).rows;
  save(key,rows);return{imported,updated,total:Array.isArray(rawRows)?rawRows.length:0};
}

async function importLogTenMigrationPackage(file){
  setImportProgress(2,'Reading complete LogTen migration package…');await yieldToUi();
  const parsed=await parseLogTenCompleteFiles(file?.arrayBuffer?[file]:file),manifest=parsed.payload.manifest||{},rawEntries=parsed.payload.entries;
  setImportProgress(5,`Package read • ${rawEntries.length.toLocaleString('en-US')} logbook rows found`);await yieldToUi();
  if(manifest.creditHoursOnlyPatch===true){
    throw new Error('Credit-Hours-only LogTen patches are not used in v8.6. Flight Credit H is calculated automatically from Scheduled Block Hours.');
  }
  setImportProgress(6,'Archiving original LogTen database…');
  await storeLogTenArchive(parsed.database,manifest,manifest.sourceName||'LogTenCoreDataStore.sql',parsed.originalFiles);
  snapshotFlights('before-logten-complete-import');
  setImportProgress(82,'Preparing import…');await yieldToUi();
  const {rows:fs,imported,updated,protectedMerged,conflicts,removedIds,expiryImport}=commitImport(()=>{
    const result=reconcileLogTenEntries(coreActivityRows(),rawEntries);
    replaceActivitySelection(isEntryActivityRecord,result.rows);
    const ids=new Set(coreActivityRows().map(r=>r.logtenUniqueId));
    if(rawEntries.some(r=>r.logtenUniqueId&&!ids.has(r.logtenUniqueId)))throw Error('Incomplete LogTen import');
    return {...result,expiryImport:mergeLogTenSupplementalRows(EXPIRY_KEY,'expiry',parsed.payload.expiry)};
  });
  const tripImport={imported:0,updated:0,total:0};
  await flushFlightStore();
  setImportProgress(94,'Verifying imported LogTen identities…');await yieldToUi();
  const persisted=entryActivityRows(),wanted=new Set(rawEntries.map(x=>String(x?.logtenUniqueId||'')).filter(Boolean)),present=new Set(persisted.map(x=>String(x?.logtenUniqueId||'')).filter(Boolean));
  const missing=[...wanted].filter(x=>!present.has(x));if(missing.length)throw new Error(`Local LogTen verification failed: ${missing.length} source rows are missing after import. Nothing has been sent to cloud.`);
  // Complete migration packages already carry stable LogTen identities. Avoid an O(n²)
  // semantic duplicate pass immediately after a 7k+ row import; it can stall mobile browsers.
  localStorage.setItem('pilotlog7_dedupe_semantic_v1','1');
  refreshEntrySuggestions();renderExpiry();renderTrips();setImportProgress(98,'Import verified • rebuilding PilotLog views…');await yieldToUi();
  return{imported,updated,protectedMerged,conflicts,conflictDetails:fs.flatMap(f=>(f.logtenImportConflicts||[]).map(c=>`${f.date} ${f.flightNo||f.dutyType} — ${c.field}: ${c.manual??"NULL"} (LogTen: ${c.logten??"NULL"})`)),total:rawEntries.length,manifest,archiveStored:true,expiryImport,tripImport,verified:wanted.size,localTotal:persisted.length};
}
/* Export */



const EASA_ROWS_PER_PAGE=26;

function euDate(v){
  const s=String(v||'').slice(0,10),m=s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return m?`${m[3]}/${m[2]}/${m[1].slice(2)}`:s;
}
function blankIfZero(v){const n=Number(v)||0;return n>0?fmt(n):''}
function blankCount(v){const n=Number(v)||0;return n>0?String(n):''}
function experienceFlights(){
  return coreActivityRows().filter(f=>isFlight(f)&&activityIncludedInResults(f)).sort((a,b)=>`${a.date||''}${a.off||a.out||a.schedOut||''}`.localeCompare(`${b.date||''}${b.off||b.out||b.schedOut||''}`));
}
function experienceSims(){
  return coreActivityRows().filter(f=>isSim(f)&&activityIncludedInResults(f)&&simulatorCompleted(f)).sort((a,b)=>`${a.date||''}${a.onDuty||''}`.localeCompare(`${b.date||''}${b.onDuty||''}`));
}
function experienceRows(){
  const includeSim=($('easaIncludeSim')?.value||'yes')==='yes';
  return [
    ...experienceFlights().map(f=>({...f,_experienceKind:'flight'})),
    ...(includeSim?experienceSims().map(f=>({...f,_experienceKind:'sim'})):[])
  ].sort((a,b)=>{
    const ta=a._experienceKind==='flight'?(a.off||a.out||a.schedOut||'00:00'):(a.onDuty||'00:00');
    const tb=b._experienceKind==='flight'?(b.off||b.out||b.schedOut||'00:00'):(b.onDuty||'00:00');
    return `${a.date||''}${ta}`.localeCompare(`${b.date||''}${tb}`);
  });
}
function easaSimulatorMinutes(f){
  return entryHasTimeValue(f,'simulatorTime')?numericTime(f.simulatorTime):timeGroupFinalValue(f,'simulatorTime');
}
function slotSinglePilot(f,minutes,multiEngine){
  const engine=upper(f.engineType||f.engineClass||'');
  const minutesLogged=numericTime(multiEngine?f.singlePilotME:f.singlePilotSE);
  if(minutesLogged!==null)return blankIfZero(minutesLogged);
  const explicit=multiEngine?/^(ME|MULTI[- ]ENGINE)$/:/^(SE|SINGLE[- ]ENGINE)$/;
  return explicit.test(engine)?blankIfZero(minutes):'';
}
function expTotals(list){
  const flights=list.filter(x=>x._experienceKind!=='sim'&&isFlight(x));
  const sims=list.filter(x=>x._experienceKind==='sim'||isSim(x));
  return{
    total:sum(flights,totalFlightMins),pic:sum(flights,picMins),sic:sum(flights,sicMins),
    instr:sum(flights,flightInstrMins),night:sum(flights,f=>timeGroupFinalValue(f,'night')),
    ifr:sum(flights,f=>timeGroupFinalValue(f,'ifrTime')),
    ldDay:sum(flights,f=>Number(f.dayLandings)||0),ldNight:sum(flights,f=>Number(f.nightLandings)||0),
    sim:sum(sims,easaSimulatorMinutes),
    picUS:sum(flights,f=>timeGroupFinalValue(f,'picUS')),dual:sum(flights,f=>timeGroupFinalValue(f,'dualReceived')),
    multi:sum(flights,f=>timeGroupFinalValue(f,'multiPilot')),
    se:sum(flights,f=>durMins(slotSinglePilot(f,Math.max(0,totalFlightMins(f)-timeGroupFinalValue(f,'multiPilot')),false)||'0:00')),
    me:sum(flights,f=>durMins(slotSinglePilot(f,Math.max(0,totalFlightMins(f)-timeGroupFinalValue(f,'multiPilot')),true)||'0:00')),
    toDay:sum(flights,f=>Number(f.dayTakeoffs)||0),toNight:sum(flights,f=>Number(f.nightTakeoffs)||0)
  };
}
function plusTotals(a,b){const o={};Object.keys(a).forEach(k=>o[k]=(Number(a[k])||0)+(Number(b[k])||0));return o}
function selectedExperiencePageRange(){
  const all=experienceRows(),mode=$('easaExportMode')?.value||'last2';
  if(mode==='period'){
    const from=$('easaFrom')?.value||'',to=$('easaTo')?.value||'';
    return{all,selected:all.filter(f=>(!from||f.date>=from)&&(!to||f.date<=to)),firstPage:null,lastPages:false};
  }
  if(mode==='all')return{all,selected:all,firstPage:0,lastPages:false};
  const n=Number(mode.replace('last',''))||2,totalPages=Math.max(1,Math.ceil(all.length/EASA_ROWS_PER_PAGE)),firstPage=Math.max(0,totalPages-n);
  return{all,selected:all.slice(firstPage*EASA_ROWS_PER_PAGE),firstPage,lastPages:true};
}

/* Minimal PDF writer: produces a real fixed-size A4 PDF, so Safari print scaling
   no longer changes the logbook geometry. */
function pdfEsc(v){
  return String(v??'')
    .replace(/\\/g,'\\\\').replace(/\(/g,'\\(').replace(/\)/g,'\\)')
    .replace(/[^\x20-\x7E]/g,c=>{
      const cp1252={'€':128,'‘':145,'’':146,'“':147,'”':148,'–':150,'—':151,'…':133};
      const n=cp1252[c]??c.charCodeAt(0);
      return n>=128&&n<=255?'\\'+n.toString(8).padStart(3,'0'):c==='\n'?' ':'?';
    });
}
function pdfText(x,y,size,text,align='left',width=0,bold=false){
  const s=pdfEsc(text);
  let tx=x;
  if(align!=='left'&&width){
    const approx=s.length*size*0.49;
    if(align==='center')tx=x+(width-approx)/2;
    if(align==='right')tx=x+width-approx-2;
  }
  return `BT /${bold?'F2':'F1'} ${size} Tf 1 0 0 1 ${tx.toFixed(2)} ${y.toFixed(2)} Tm (${s}) Tj ET\n`;
}
function pdfLine(x1,y1,x2,y2,w=.35){return `${w} w ${x1} ${y1} m ${x2} ${y2} l S\n`}
function pdfRect(x,y,w,h,line=.35){return `${line} w ${x} ${y} ${w} ${h} re S\n`}
function easaCell(c,x1,x2,y,text,size=8,align='center',bold=false){
  if(!text)return;
  const value=String(text).replace(/\s+/g,' ').trim();
  // Upper bound on the width of the printable characters in Helvetica.
  const width=[...value].reduce((n,ch)=>n+('MW@%'.includes(ch)?1.02:/[A-Z]/.test(ch)?.77:/[ilI.,:;! |']/.test(ch)?.34:.62),0);
  const fitted=Math.min(size,(x2-x1-5)/Math.max(width,1));
  const estimatedWidth=width*fitted;
  const x=align==='left'?x1+2.5:(x1+x2-estimatedWidth)/2;
  c.push(pdfText(x,y-fitted*.32,fitted.toFixed(2),value,'left',0,bold));
}

function easaRemarksCell(c,x1,x2,centerY,remarks){
  if(!remarks)return;
  const source=String(remarks).replace(/\r/g,'').trim();
  // Keep every word, including those in a long instructor/trainee endorsement.
  const words=source.split(/\s+/),lines=[''];
  for(const word of words){
    const last=lines.length-1,candidate=(lines[last]+' '+word).trim();
    if(lines[last]&&candidate.length>75&&lines.length<2)lines.push(word);
    else lines[last]=candidate;
  }
  const longest=Math.max(...lines.map(line=>[...line].reduce((w,ch)=>w+('MW@%'.includes(ch)?1.02:/[A-Z]/.test(ch)?.77:/[ilI.,:;! |']/.test(ch)?.34:.62),0)));
  const size=Math.min(7,(x2-x1-5)/Math.max(longest,1));
  lines.forEach((line,i)=>c.push(pdfText(x1+2.5,centerY+(lines.length-1-2*i)*Math.min(size*0.56,3.5)-size*.32,size.toFixed(2),line)));
}

function buildEasaPdfPage(rows,prior,pageTotal,toDate,pageNo,side){
  const isA=side==='A',c=[];
  const x=isA
    ?[18.4,58.4,120.9,152.5,214.9,246.5,324.2,401.8,433.8,465,506.6,548.3,641.1,672.3,703.5,734.8,766]
    :[76,114.8,159.3,203.3,237.3,276.5,310.5,364.6,407,489.8,537.9,823.6];
  const left=x[0],right=x.at(-1),top=576,bodyTop=499,step=16,bottom=bodyTop-EASA_ROWS_PER_PAGE*step;
  const cell=(a,b,y,s,size=8,align='center',bold=false)=>easaCell(c,x[a],x[b],y,s,size,align,bold);
  c.push(isA?'q /GridA Do Q\n':'q /GridB Do Q\n');
  for(let i=0;i<EASA_ROWS_PER_PAGE;i++){
    const y=bodyTop-i*step;
    const f=rows[i];
    if(!f)continue;
    const sim=f._experienceKind==='sim'||isSim(f);
    if(sim){
      if(!isA){
        const type=[f.type,f.reg].map(v=>String(v||'').trim()).filter(Boolean).join(' ');
        cell(7,8,y-step/2,euDate(f.date));
        cell(8,9,y-step/2,type);
        cell(9,10,y-step/2,blankIfZero(easaSimulatorMinutes(f)));
        easaRemarksCell(c,x[10],x[11],y-step/2,f.remarks);
      }
      continue;
    }
    const total=totalFlightMins(f),multi=timeGroupFinalValue(f,'multiPilot'),single=Math.max(0,total-multi);
    const values=isA
      ?[euDate(f.date),f.dep,f.out||f.off,f.arr,f.in||f.on,f.variant||f.type,f.reg,slotSinglePilot(f,single,false),slotSinglePilot(f,single,true),blankIfZero(multi),blankIfZero(total),f.picName,blankCount(f.dayTakeoffs),blankCount(f.nightTakeoffs),blankCount(f.dayLandings),blankCount(f.nightLandings)]
      :[blankIfZero(timeGroupFinalValue(f,'night')),blankIfZero(timeGroupFinalValue(f,'ifrTime')),blankIfZero(picMins(f)),blankIfZero(timeGroupFinalValue(f,'picUS')),blankIfZero(sicMins(f)),blankIfZero(timeGroupFinalValue(f,'dualReceived')),blankIfZero(flightInstrMins(f)),'','','',f.remarks];
    values.forEach((v,j)=>cell(j,j+1,y-step/2,v,8,!isA&&j===10?'left':'center'));
  }
  [pageTotal,prior,toDate].forEach((tot,i)=>{
    const y=bottom-i*step;
    if(isA){
      [tot.se,tot.me,tot.multi,tot.total].forEach((v,j)=>cell(j+7,j+8,y-step/2,blankIfZero(v)));
      [tot.toDay,tot.toNight,tot.ldDay,tot.ldNight].forEach((v,j)=>cell(j+12,j+13,y-step/2,blankCount(v)));
    }else{
      [tot.night,tot.ifr,tot.pic,tot.picUS,tot.sic,tot.dual,tot.instr].forEach((v,j)=>cell(j,j+1,y-step/2,blankIfZero(v)));
      cell(9,10,y-step/2,blankIfZero(tot.sim));
    }
  });
  c.push(pdfText(right-62,23,8,`PAGE ${pageNo}${side}`,'right',62,true));
  return c.join('');
}
function makePdfDocument(pageStreams){
  const objects=[];
  const add=o=>{objects.push(o);return objects.length};
  const catalog=add('');
  const pagesObj=add('');
  const font1=add('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>');
  const font2=add('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>');
  // Static table geometry and labels from the supplied A4 reference, reused on every page pair.
  const grids=["GhS^Z9lD>uO[]$@'jTIC?)m@n\\)^(rM3sT4BMeg8nBteK\")'JPc``tBAjk3#RO;u`@hphSpJp%=)4C63(@CP%gGD(ko:'1Wk.Z>4f<sAqojR:#IdA/2[Hm_giICn)i?-r.75:BkFmS7)\\1!*:n+L=kP%j.2@D,:!o!*=qSh^ds\\)6oC.HA'ab:lr<[WUnX]1Af=m`N]sakRE_Fup@3A(SJ@d6(.ZhQQZ[`aQDMetjqJq4bFP*Nt<cIdP]HQHX\\7Y%OK`f^8:\\b?@(foXClf2j/%EVK%G<><d8RgkV&,m^?\\d,X*f8EpBE60h!oN-AeURT2MS@T2MT+,gj!'UeZi_.%$=L$VlV`>VIa\\`2CR\"dBq\\UJSAOaSX)P#e%u`iQ@A2/@?.5('[a:T,aoKt8t?`H8t?`]7+9#93\\C?*?s6;Y3%_jb_6a=XT,*VYdrOm\\VM-===osOV$VP;>VQK=<03^loieh,I>S9_'n._L\\i$giA/d\\C$OSH34(\\+lfH(erO/'$*O8ShR8&kpKfbPTa$8:4SbM+pVcnS+EbDV`?H>\"fN&Mb0XWiMY1>iMY1>0hF2R/?.+X0++T<oRbQO.i1lP+Rs/#8Mi$n?=6r=;Pth&pp@9DVr\\2m7[34rBNe(B9aa1^:(':_:5_]<BjjT((4T18/Z0DT(14U[=osOVMb0XW\\b?0S_NHr3J]+I?,rR@82L\"I\";6DF$BlQ]u/\\tA5>(W[I[(%BKQ9aHY;I@AkEp&.RV61e69-gcVEChi9(GG5Q9-Eb=M'Ou2J]+Ht*m:Q9dBo8bRMM)Te&\"n[e&\"n[dpeJR>$+\\#`Es<QbNT2.`E+[ZQ9aHY@ZkD.m_-ECYbY#e+Rs/SM,2;kaC_qNdpe2J>\"fQ'$U:I_$U:I_Mb0XW@7d\"*Io.mOV\\bj=?b'QJ!f9^28YTq0pB&g\\Oub1G`2A;82D)gIaJPS]BlQQq/M7@,-$Y;C9CNXf(7,0Q&kICRlAKmjm_$oRe<T'.M'Ou2/d\\E:ioU,Q#p_\\dOi4+COSH1&qlk\"d[$W&+(6_/8_G@/[_G@/[@ZkD.=Al'9?PH2Wkln!(=\"]l+5iU7%P%\\%e]th\"ZUeXQ*nS)Eg:J9B@AK.@X0h!oN-AeURT2MS@T2MT+,gj!'UeZi_.%$=L$VlV`>VIa\\`2CR\"dBq\\UJSAOaSX)P#e%u`eQ@8+c-%]A8\"LDQp?rZn=4t6C<*u%WVF2A8+'7UBcam\"G4`#\"AF,Z*-LQm.oZHQV\\e#V19d!elU=/'9i9iV;t^'9E/ioL%<BBeTM!%1d%-bQ$&3kYCXDKCY]2DTn-507C<o9rUAGhu%jRRlkWXI/^?2's$Es_c0M)c@?;XU(YsWm.D*)B>5E!'U/bAk!&9B!qpRJ]^lJAEnuK<2U:[*\"mFp)J7Gm?E_C4[38&ojfY^_)0i:cEUta')1(Ujn*4a6C>[U?h!fkADb!@]<#i.lDDE*]D6fA)nJeW/t'[5lJY*ZqV$dcd1!QS^]jjK[ri,#t)kM_DNA$Vb4mF1J%D!l4J@=lrq/DuoD\"($S.]#uZabZf$Va]m$nI#5,D:,dO@hK&N$A^X@5H:Zq61+%@Pe'/r>\\\"&D6!^GN'JaY\"4bK6.<_+qIbi&aQfE\\\\sKr,e]hoIVG#Z3NJI3\"P7X:<$gq>p-$M\\>/+:-Y#aZ88<Ub$T&>@7)*e5a]m$nI#5,D:,dO@hK&N$A^X@5H:Zq61+%@Pe'/r>\\\"&D6!^GN'JaY\"4bK6.<_+qIbi&aQfE\\\\sKr,e]hoIVG#Z3NJI3\"P7X:<$gq>p-$M\\>/+:-Y&md^.l1mJ,d*+pPSbCk&'RAFUB`UGn$3mD`JTcnRiBcCQMns9`\"9L&*[k+`afU'r7OI2)O)sRi5a_;n0KEb5oGkLf^Um2I#5s\\:rYnS++NpE^'8.Bp0bo_k956_b*X`a$fpp[Q^GDiAP<e3j4%p=h2?.=k\"9G+JNZ:+jCS>ZK&_o'LLFX&/jKqc\"<<Ug%!+Ab[_'5?Y=n87F\"^u'a$dc68)4&HB6'QSEOu,MW<qnt;SY$?_q92)1@R?7!8$1hJ7J0/h7rjq\"C)0.3Ng6lkF@Y<YLW+9;\\>87rVI[9qt24`NWE^d*rjAofV1TdQ=s\\.-3k;gV$89PCr,R:0\"k3:6lI[g:c;aY)OSUoKa`h?DcJa4E$4[;#%\\HKX)<OR5k9U;M?<6H2N\\1P)3ditZ1:Sa#t\"o.JMlba#lk*b(c!KH]NX+o'7sa')&*82X_]0j))<V0Y]@G3%U]E2))<TI]Ns=r6\\<5b)3dit_t$Eq3C=#41]5t4]F[Z[O==?%aoV10,#D+/Td%X.*N$P1M#LtI7L`EQ8d#K6$20LYNSlD:BuLDNOU$n7>5$oC_*6;kO[tCf$5M]UR6b=Q+t`\\2XtUrV%@TBlBi.aT!e'.g,E\\:?3*kHijS:$_Q5'lHV[q\\jN?F(bW2PY`4r>G\"Kd>*@U1+?'aFd#-/c0M83;3SA6F03,/Li5dQk*d8N?a:em)+)Nf-=':M6qZ9%68AfZ:pF;apOmR^VbjW,aRYa\\r)O\\g7s.KRm7Wc\"Q7*'Y@@3f7h^?0FBY^^I5IUR:<Pi6MV1K@9lsQ<e1*?u!ENkjd:q9h=0_Q,YX5+A:5b!5`TX&t9lsS4iDkj.d=<Q)/M\"Jk:EP6))GP<W<7EBU*s#DU1sp#7;6(O^1n]\\#b<ERrc[?t<ctK0&U7t\\nq$=2kUPVah5.lkp4W;[8Vra+ZKoF3;-5.Db'o!0HLET_i16FS>'sD++gnN]*8>(eg!q9TO=#'TnUC\"\\(3iuD@4rVd9Ve$rV`U\"h1-9<_.C)*5!!%Tmpkd9]oX4Ve&=J=,1VaX%V@eA&u-9<aUn1)HRBL!6%Q5*`FV[pWON@$\\<.V]]fNrU[eRcq,WW)\\d?)ULmMAKP6tkIqL.BJe+N;,OCHHj!QFd6smD+5Y#I*XueWe+1Vh6H8+XPDP=B$H!(__iQAoRE1o0MTbpYIIo$-#rFA*!$862.V9cJ1uYo7VmVE07g#4D[-Y]2R!n&C7&NIBX^^l6@\"h&`SX+\\K'q:6a.l<^Y[&Bp'N'80,7&OT<?H\"lG%;;KbZGN[;oKcTQd6sm$7BL%j&R^;^':fmR[>N@%\".JG\\m),n!g_[g7N@$\\<.V]]fNrU[eRcq-F_qQ$[UB+)2>!CV?aLE4(1=8s%@G'L^(ik<U':Dd#nSR^6+oDcZN@(M.^k3A.`U\"h1-N\"YDZrSpXBL!6%Q@3umZ]@5;kd7:e<GXi#XO(XTe$9Lf@e11>Kgl`H()=67d74;-kd9]oWu1#p!DRZ><d5@O4pWYqRcq,WW)[Xt!j'a^U=5j2<\\aGa5SK]SI<ZL!qa+DYd6sm$7BL%j&R^;^':kG;GT5[h1n]\\#b9u$O)4)RB-9<V,$lf*S6`uM],'1mV55loZa!(?i%ZMg4#f)'-<lKWN4IYbDUf4I+p<eC;12YseW]9L17h_;V'jelI,JXYG-8KGr_lD@;\"B_AF7&FOL=#'Sc5SKPDn6o>@o?i(812W\\g._bL?=J=,1VIbH_Z24g26DKB2b6kF^p<eC;12YseWq^I=#V.a#aL&&<.Wum7<bO=PB`fI.`k3?%BHkn0-i:<5Gra;`,,5@\"=)=gZ(ik<U'9uKtnMli8DHm:FctMDG#*2%.i5%V*BG.sH3j%=k54WBM8BX1:4P3.B9bM/;;8L.t0NMjD8Yfo>%NabbYsP3W+tY8oGbC*[RO$=e%gNW8O$&jVd^$a+j7E@SU!Y-J()7QHbQhX37[$5D<)i?P+*f\\7Uh(VX*p:6;<bO<]Ud[]U&+dV>U!Y-JQ@1`;i-OJ3,a\"Bg[XeZ98!Zg];-@>ciHH,YKd7>&;cdtL+3/)UYX5+A.0B!HKi^#+aJ0'V767PNN'80,7&OT<?H)7UO!Zn>.V]]fNrU[eRcq-F_lMF;\"B_AF7&NIBX^^qMJ0m</qsZ)Tq.N#,SX(!s`MX6`$B6An/LkKMRu[P\"1n*X8W2N^T4rVd9Ve$rV`TOrH<bO>;Bi.a.fR*g(U!Z9+AKTdK\\/l)mU=8\\T[R/!q8<\\mjapQ#j^Vc-`,al`D\\r)F:bQ!Mb@tdAc;Zu$p$Emq4O[g(6Mf`.'(k\"66M,)5X]SbB4*_g`[<7EBU*s#DU1sp$jLKCbV#d?XjM,&td>/f^#!DRZ>p>K#1o?i(83\\;kpMGG=I'cKeg>#a!$2BMs#BJe+N;,OA2I5IUR:<Pi6MU-\\oXO(XTe\"WR;Z2\"[06DKB4b<ERuEBP5e7&T.1D2!qlOX:_^Pnf#_IYT(J82?;fFVSh92kGNlo6XT5dDQ_\"Gd,&cR!n&CK\\muEega-2%;;KbZ4F-Jp5qiYSJDsJMTnYA'cKeg>5$oTCd%p%ctK0&U7t[CqIi,-SX+\\K(#sZuTSZFe$ErI<#@W<U':fmR[>N4!J/&tsFBVYn.RkKhBVNt]&$Ou6?O//=Rcq,WW)[Xt!j'a^U=6CYDe!%5\"uO_Aq.*\\Qj)Us;6q:gB@>!HGa;'k3A56FL\\%\\6^,HT7*@Yt.J=Q*9(54WC(Dime1'r-fo1sp$jLK?5-aJ4SeMi(]Wjs.YKN@$\\<qsZ'n*s#DU1sp#7;6(O^1n]\\#b<ERrEBY;f7&OUM=7PO-YX5+A:5b!5`TAD\\$B6An/LkKMUQ5U9d:q9h=7NgSKG>D\"FBVYn.Q/@L<bO>;C\"\\9N0s4\"KRcq,+ZE94.3RVH4UU%b@_=')=$:uR\"Vl\\nB\\E_Z72`NX_Qm)M@<qtRFbs(]3_JbQ3leeFr4rVd9V@@qJhk\\L\\nn95rZD.>8g?'Wq$B6AfD;`>37W%`/@\"h&`R?eT\\Dq.<!%;6s7#!Vs'f#IN!5SKPD;#\"DUIJP(fctFWPZGNZpI0<ZGd6sjCKi^#+BL!6!2Xk-TI((b^'cKeW>#ok_]8lqB1n)LmJIcO+?B5:IO_KH3rE@:T*,.0j;$4cf2Ujes\\O)I9b,MFd)TT#=8UqNo9jGNm&L\\V]fLHiSK\"\":K'g0i5*_d8]fnY1bqadJR+tY8oGaK#]$B6Bh[-Y]H_gF.=BJ`;V77@@joT;Y-6sB<Ci.:K^8!?>E<]G>o4od)iRO$=UU4S0r@'%^gP\":Lg$6JCUO!]^jm)-[\"g_[a5N@'LhW2SW_4r>e,RO$=e%gNVMO$&jVddkL<nA+/,P\"?iI.h\"Q<%O8-<A>4/1.\\??OgnWcb8>(eg!q9TO=#'TnUC\"\\(3iuD@4rVd9Ve$rV`U\"h1-9<_.C)*5!!%Tmpkd9]oX4Ve&=J=,1VaX%V@eA&u-9<aUn1)HRBL!6%Q5*`FV[pWON@$\\<.V]]fNrU[eRcq,WW)\\d?)ULmMAKP6tkIqL.BJe+N;,OCHHj!QFd6smD+5Y#I*XueWe+1Vh6H8+XPDP=B$H!(__iQAoRE1o0MO1jmDp`KB,gaID!I1icX-e=rd=gC$S^o8[*t;nXe$9Lf@ePpS':fmR[>N4!J/&tsFB\\Cs<GXi#XO(XTe\"WR;1&>QK':kG;GST7d1n]\\#b6kH4dg'<87MD=.'t&qD7t;<m9lsQ<e1*p0%;;KbZ4F.uF'f2'1n*X8W2N^44p%h4BVNt]%rYr_Ns7GgC&):D+ldU=a[AX1KjfVk@E91H9Opn(`EQ\\C7)-R#5SI'!i5#c(@ePpS'7Pj]>,rKg1n]\\#b<ERrfgd)XFBVYn.Q/@L<bO>;C\"\\8CZ2\"[06DKB4Mi(]WkbA/QFB\\Cs<K-OsJ0m<//%ga8++t@I9lsQ<e1*?u!ENkjd:lHTWu-7ATSZFe5.ld!I3G]=BVNtMU/nPp#d?XjM,)5X]T$no)ULmMjcaP8N6@>1'-3>QL(/Te+]h2iOZ@P!mqXlTiGW[K1bhE!*j\"i\\Afl:TiO;@U[>N@%\".JG\\m),lkIH8dDVaX%V@e11>Kgl`HQ@3umRu[P\"1n*X8W2N^T4rVd9Ve$rV`AeqKBVNt]_Q3J(6!0i.7&NIBX^^l6@\"h&`SJDsJMU=orRcq-F_lNRf(W$N=-9<_.C)*5!!%Tmpkd9]ogDJP;!DRZ>51ruX\\/]3<d8=%Tf\\grjEePbPR8[/VglL8JKYP?sdDQ_\"Gd,&b3:N&#'7Pj]l;c%l)ULmL%=D7m#<q]l,1Ej7X4Vh'=J=,1V@@oT't]M,.l<^9[&iaHAfhIq,1?`[XGV9e)ULmL%2;j^G<Q9_1n)LmXRI]_*s#DU1m)KTiDn)c)ULmL%=D7mhsCe[Kgl`DQ5*`FmN]LSN@$Dti5=99f:r@@AD5sP^D8RX8\"*+^e3NP]%J:2uY)m1gEp>-GbG1phq@*po-E^bG#S[ailm%K:_-<_a$6JCUO!]^jm)-YlIAG79OVSZH4Nn%?Kgq7o>5$q5@6PMY1n#]<U8!5EqT,F'+t`\\2nA+k@UIcX].i^ZH++P(E9bM/;;8L.t0NMjD8YedD\"V:6f8!?>Ep>OqLmL*EV7[$5D<)i?@+,gpQ9bM0nLP#i77ikA;C&)9YGi3UQaWqI`'a>_.LCmTY1=8+)'d*IGU>Om\"TSY*!n6grO1&K%e$,8niX]:<D)ULmMAKP6tlm%Q<3\\;gr'q:6a.l<^Y[-Y]2fR*g(U!Z9+77F=fo[/193\\C6u.`V9t5SK]S'jekW&&J[5-9<_.C)*5!!%Tmpkd7:e<K'*[cpXdC+'uqL^(==Y1sp#7;6/>s\"B_AF7&OT<hT!&H%;;Kbo#\\=,`oTaTM2o^c_ZmlnO=MTEaIKi!QbJiCIY/kr-Dh'aK,%_3lm%Q<r\"eP(<\\aGa5SK]SI<ZL5O$&lp[&Bp'Cd%p%ctK0&Z4F-Jo+>Aa7MD=.'t&qH7g#4D[-Y]2)!@Xp9lsQ>GSQi7#V.a#kd7:e<GXi#XO(XTe\"WR;1&>QK':kG;GSQkJ6<Kr/7&NIBX^^l6@\"h&`SX+\\K)rl6$TSZFe-eetGQQo3O3[uWg)uL15Y)m0<F6Y5]f5@%Phr%%uM(&Mq@\"hZ:qoGN=^I:2c<K'*[cpXdC+'uqQ,EF04/LkKMRu[P\"1n*X8m),mVHj!QFd6sm$7BL%kd=<Q)/c5#e`qa:5':kF(?H&fQ_?u8L3\\;gr'q:6a.l<^Y[&Bp'N'80,7&OT<?H)*A&6<I$d:lHTX4Ve&=J=,1Ve$t,`jd'!BVNuHagOOUj[bSlNlh^H#/A';.jUP(S@XUPZ2A[#YX5+A.0B!HKi^#+aJ0'V767PNN'80,7&OT<?H)7UO!Zn>.V]]fNrU[eRcq-F_lMF;\"B_AF7&NIBX^^qMJ0m</qsZ)Tq.N#,SX(!s`MX6`$B6An/LkKMRu[P\"1n*X8W2N^T4rVd9Ve$rV`TOrH<bO>;Bi.a.fR*g(U!Z9+AKTdK\\/l)mU=8\\T[R/!q8<\\mjapQ#j^Vc-`,al`D\\r(EQ2m)%WE+h'_70\"C1n1&#lGr4Kh8>ikQd74;-kd9]oX-e=rd6sm$7BL%j&R^;^':kG;GSQ^/!%Tmpkd7:e<K'*[cpXdC+'uoG*t;nXe$?ikq\"-jSU!Z9+77F=fo+>Aa7MD=.'t&qH7g#4D[-Y]2ptQ\\&$B6An/LkKMRu[P\"1n*X8m).Qpf:r@@FBUl]<lhcjBen(5=^ft@G8P@S,SQSE*pI7k\\mYu]([3Hq'X'_>Dr!H*mpF]8.-sL^q4R%F?ZGuZl-td$qZs>PIDs=8XRIIZ4rUR`R5XPC8,@+1k^(ZYS4q*55LSsE\\=W=--dr8`lr<S&1gG#U/M$0qo5^XF3#RtrcLK(_3-l,8$ZGlgc/gPK;XTJ-c*nHh(rlkFo&\"6X/^gV>4Srge?6do'5)eM(+8P@_12UK-Q<Q<'hAa>@4/WhkXd0]E4j_7((\"3Nh2TZ=+?K45$mqDQEhIY6hTj*H4@-?qmrk6=\"\"Q.W!pH3n_FQuuD4rV/uag?U0LC^#<\\a-0`4g_0^X]2t4a<9T5()=7Lc,[&i?,(7FFh0d_+aM+boNLS3-[\"kNH$c_gr>AcNj@+dS*85YdK2N7KA]602f-s,sQQXU1qHb&SGGpjFl,Mae)9Y1TNpkh$5!<'^>c+HuQ^Bel.nX/ir\"e\\,?=N&LY:+hD-.8N^*[]d4^+B0PrnZKoUEr34<7Ksh:4g)$EPq\"YW2O@OB7qoCIDP-[]Ol?^d73t]d8=%Tf8%[$h%'\")d5b(N5Q(,Bs*/hap;?B=4DTCjq8^C%lYb1hbPs_i,>nGYGaR'\"l-\"+5T%\\'No#JY?L-Io8r6N:0MuN3BkbjNgjjR`=e$lH,9:@N+T$j7&`^6EsIeiW+rq(U)i,WtSUd)PW>B];8mp>UX^5_67?S7u-d\\#C=6!VRs^&R_bTAIbFHN37#9s1,2i(e\\ZQ%p>$rr2HLM*>::Q<q)f=a0c6-RE9ur:K+BJ,@^Ps8MC-Ish-sJ,Sc\\ki4(Yqrd>-483uIM(CUb5/-l<qW78=bAX#FJhT^O4#>tI_`h@to(`%PoC%_^1!n:4<k9BJj'(<KlSG+sJ+rd/_b73]K`geAWSb>LFuAjVo?TXKI/<.^cQ>Bb\"E)fFj[%X#etp.JcVM<(I!P]A5,&>?4CTc6_0XcYAR`86@2YB:8>V'%n%Qi'aqJ)F]nD;)c.=>-f%lC.ZBcjhY>9EBl070ULLAQ-iVJM=\\Q]rZj1hU;$t4G$LQBbGr:gme@_(jmZE;q@?#BN;=D[p+r#P3BWL(fbY\"QV1[@Hp#S>CX^c1:Y%:S0gup_B?aLKp-e.neTWf8\"a$r;$4F,P[A\\E\\7%?`QD/glGuna^?,0^5Mhb,pbp5j^8Bekk[Q;JbOYAUa!*-'\\Tc5C*2!3`Ied;.n+-#I\\?<upVkO%Lp#A970>4c0bH3>[II_$X^:`:=o'Y8<?\\8:hXkW6&os/j58)>Ql&RW7rqssI0?Q-@X1s9WH+i!2EC%Qj\\^NRqaNur$0lh:8Oo^QeZ/,+EA*'I'+YMA_d0%]GUiEs\"Ua1D_cIdN_Vq=q5]?a](;Vf0VR1OEFtqcZQqEI;$L6b1q&f+.T;j5Y>Z;&rp`0T9Qjb)LfKK]Do'4\"0i@2]d2\"='HM7qt0iU[STG3^HS=KmB(l?JSBNaTqBK8qj3)15(3J'nLs[1\\Q^\\=:i^q$TB!!%A:*kDj6,UGT3hXUD]iIL^:Pt`1nUChGlIBtoAe$c-_0F<kk7ZKbsT%X%q<(rd'$4G28/8J\\TH5Jh%sDHTr=:#l_f>Qa7knZCKgS@fH4FKI/`ftQcC3C=oDB]d</`mJs']dQ>W24]4ZQer9:,7?Tp^Qrqu#if8\"Cc*QrH!qK_AWj*tS3<dla[4$8;]rrE_BkKs~>", "GhS.JhiHRDQ%\\&n=7k5o&#MoVg\"h$TXKaQ`HXo\\7\"Teg/\"GuL<@\\![=kJ@&In*SN%1W,@?HoD8h+&dT+eZ$`Q<86[ZL[B=Kr:nt@1lV$L=AJ$f>u$18A[kFg@;>ga]GuBGJ,&!/Bj-87E_4BY%*Ld%-0mJ5kOH)nHm+ouj\"PECdmd%eSbJ<-GrUi1]X1+7GaY&WFhUhU*2g(?BMHTpoC>4g>))_JjCO``U1kuX`m_//a-9ik2M2g/b;NUcbL7r-[aDCEX=_h2IBkqAf&=:.#GV?\\l`_GNZ@J$9\"[#@m!SXJ#5a%iA^If7QP,\\N8/IDbT<Ie+!$4'4'5b$(\\/S+r+W=!m5fE/EW;1Ko\\!Ig:b+='qfZ<J\"SP,\\Ma;A9\\*U4D9L)O>6@$BV&3i0hLuO;mus;W8'4<`&m\\'Et:<?[sCj!P+(@5a&-tQ?KeYb5t\".(%<KR\"_a[*!(i%Lr$ki78?)`%[jW@E<<6:\"-m0d8!TSH=[u.JAOAlBG8SeF]W$jooU&_r#((cY/!8/q2#YA$H<[e@]8Q!9N?AE*_WWFFp$@!n;\"lr.eJ<]g1MH*W+,Xh`Jf,#Q$;*_;D$]q/6?o\\_a?<n<XP,a$[M(Lt3'a7-+']<'7!^]d\"+M1&C+fPgq.Ra@]W=#$HD.4u5_NIk%4@s>ig8HDbP+hrk8equ$WW:^($4'4'J6\"*+#Y@n/FDeREZ+LB9;AAUoiX3bdHiY\\d\"Ve#9!(i%Ld]P5gP+mJ3`>9Xg<P[&\"?cnkB,ZOk@&mKlE.LM95.)<$L\"b`[$5^&%e6;ePk<JgiE;ACmf6[Mug$OB=(^lu\\PTT+g1,Uj)iX(25SW=\"aCU4A>)\"_a0qJ<1HK>`Q^A/apNL.)u:_E7<DkIeZh9g&!W`qG7liQW$-D:_\\!&)Lh%Hpsdg6p6Xgua2c/b[b*W\"kE#(D>hWZIE+qP]h/7gS$\\+[LnDPa3X35QCbO&0Z$#YmW(-+K6f;E3:D%VA%!<o+JJkY$YR9D>66TZp&QtKVoTK/+La7LG,nX8,$#nQFu>h\\^n^-r<:\"Vga!<cC.];`j(!TcN^I[+dp!=9/RhVYAGdZF*>&?*pq!O1Qu1TIrdn@sYuD0\"c$Rjq%SYLV:LD[\"<\"/(L+eGk&@AJGO-X&*(]!tH4LJ!bED$s=kg4=D\\3%r+G#Sf\"moB1V`Bf*,P,X,G5PT>^s@EuUrV`T3-m\"jL_L:aDDAkYU_S\\,iP@;].I+AbPG3ip?l=b7`L,LLcm(Yt\"sQJjVA4n.iP@;].>fjnLS0F6YREpW@SAE\\HMBMm;A<VEJ`rdf=9]s<0rF[V!0IN05bogJ[\"<guWc/LpLgih!=F4N5*ZmGtKtIZ;M[)OKI/$;1`S3Ks56rl_6cqB7JJ].T4Gf3^+n.e\"5R\\#2T#A]j^BL#>___Tk@UqL=oH&TmiPC]p=cP5=5R\\#2T#A]jN9NJ._pjAd0sVd/H9j7/c_6]e(t/i10[7Fs(g2kR]naCj4BN4@b;#k/\"DUqj6SY8S1EsZDbHROt'o3aiU%nDV=fAb10e?RS&gPTe(2-SS$P/iIaE!5R\"_*k]?qH;.a-b^N0J#>2&gPRSVA4nqiPC]p=cP5MLL?L\\YTu](@n\\N]LA<k%Z5d@XJJb7E=:uk%0rF[V(m>1t@UqL=[\":QFWc0@5P[d0.fM/k=^4U;pbVTiaB=/H[@^=rqN&8\"jTM&-U-9R5!k8;\\UKX%&#V(qN\\K6e3I0q%Mj7\\F6f.hKd4_XeV$N^:@8+Bd9(JW*.?TO&0X.jiZeF:;mf@TcIC+rR)76+r-5=dlQg7]Krp.gY?N)ii'/F\\IW?RuV!bYZ4Bb!4sMJgKp$#%O[jkN-;rD4&'mR&!(>\"HUh8?8:51oQHfp6NjRYu#Tc/lTUG<jBrrR&*^2]])=nk_]ao*f]i[rakr[=>Ki%4#d1,u5ZrW;Gf/tUcRJ._9]ak-L4tNlTq&O1O6;1H4NnF^:Ni7oIV:G>8h\\19IGE>$7\"kKe\"5o39'BJ]GN0'kD\\(Hr1>1\\`u4<@$cb%hEL]5H\"0<\"M4)\"5b#U9BrrRF(Hr1>1\\`u4pE@n_(Ct?e5H\"/]J^2T:+8hY[.M?ilLPId?T@8*.@\\%mkBJ]EDX]5YLWl7jT\\^++jF/O<9[dr8B*qG)^^q*Mkke]mlD$Ic2Kq0RS_FFWD#!]#nM?KL4iM\"NSkHs*$5SlTkBJ]EDX]6Y1_FFWD#!]$Y4p3q>ih=WTkHnP=d.qtZK:gUr7=n;+nBbe:F'A$\"XHO/*D$E5&[0f3&[9./gpGj^5m^s\"h=M?NH6iN/]=LX.j>rX:+c/?(CYgW_\\paDf[i9.IJOeC/c4L?\"(SC[QKke]gjD$E5&[>G)mpaDf[i9.K(LHqbD4L?\"(SCT1n3^8[Sn8hd68C2&l*nh$O:@!=aFn4Lbc/@cl<Zrll(/&N*5\"B2.D<ilNoc*'7j:=h5FfS'[Kp)Sl%\">er4&+(UNrn#DISOYn#s->K6^tp<BtY%./pnA[B(0kFX%:TkNrn#DIST3W#^#@N6^tp<BtZj+oCM/N5GtoH\"J+]6U!;NY[,-R?-QaD`%\":re'nW`+Z.)82F&^*5m<>Ul;uO^]#Pg_b@,3:83]SX^c/?(CYZ$*J4pksZpbAmP#]r';%rDM8VCd/ukiALh2M3+#HA+U3*sK\"hr0Xq^&YR<R4L?\"(R'ofok_aZXK-/QgKg;!rnBbe:A(ndiYV(?KBJ]E<X]GXiZPH2Nj??%Ebg#bTnMe?%bjHUg,Tc8XO]W,A*nh$O^C*+tXG7;sD$GJ[>,t2qCE=8F%/u9ohM8<V5!61ir#KLR,-`1Q4L?\"(*GN1t@9pQKBJ]EFdX$VOmcVt,rf4?dO]^3t/@6;m*GN1t@9l&JZl/c^Brs^(=\"THdaSh-tN4Hr]nQ<MFhF%:('Nhi%\"C3u9TZuE(]<K`e?2,h@c/@cl<fs'(4?Ttgr1)@8&Epu$!Gr$*VmVBb?2,h@c/@clhM4@o?9GS4r1)=g\"?Jc.IdhD^X%:TO*Zj#DIST3WLir:IU!;NYZrS=uWtn?Fj??(FbcTVR0'+&84ld,FJT\\\"ad\"RYbg'iGB$Th)0KoZ;h%\":re'aZqG_aBp0cY:''JM)<act;cg>,t3@KoZ;h%\":t;I0ar[`C$-2cY:'ZT_%d=#=#,oM?KL4iM\"NSkHs*$==513g'iF*CDDH,C9c8XmW2@I>[(uKb;[^na<-QHlW0.@$RZ+b)>nXoG+,'3*sK\"hr1)=f&p0RtLGjdWds<);?2,h@c/@cl<fs$_*sK\"hr1)@8&EqW%LGjdWds?^5kNC5'ISOYn#s->K6^tp<BtY%^:-LeI)>p#U.Kj>5@]>@BkGX?MD/(]g2pOM++-H]k<fs$_-O$jpr,rm\"R\\1$1\"gf0FSCT/&FIIu?2M7XNH632opaDf[i9.IJOe<((\"gf0FSC[QKkgYNG2M7XNHA+WIkKsfh:2:RN3mGQ0S&*Eb]aqAC5!61ir1)=f\"D-+mWK#]rW//\"==3$8t;p\"A9nqX$V\\lIghAquQOg'iGB$TlU/c41p\"Zl/e/egp=?ZS`7(D$E5%/UZaq[)5B7D$E5%onrY]mI3t<S&(/\"$Qo3jD=un^c/?XLKgME/mH;k-ISB&Bq2*goLPId?+4CuhoYK8PiM\"NSa0]0H%ffZ_QJ0E.N67ML2XHIeV>U*m-GLfIlX#]U%k#>?3[\"4r0'kF\"*^5JMF[5G?UpZ)u]&qtdgB#.JOk0C@f#(#Jr=0,4NjMiL=:ZLmG<@#C448Es_Rb'`ht)BVmmuaZ_Rc@02u#J_>\")n+oC1Z3JDflel[9')o&7@T!Q!ZUegc>qeRH;gJ[jSLPqglk+#!;bRWjiX,R)c$,!d#?$>;6jm4R`^5o2]lBJ]EDX]6Y11_1+6#!]$Y4p8Id;*^VecY9L*5o2]lBJ]EDXnO!cW>?(^#!]$Y$A$LKUEBE(eg_;SRYQYVK:gV=+(CdgG\\Ao.3NZ9eb,WEY)nF9l)0>+)N8%Gi]7^#).Bm\\)$+b(R5b#T.G@@BU]'rQ^S&*EbXWg'/H$F!Ypc5JN+jbq(!nn'3:2:RN]'rQ^S&*Eb]aoZh]m+3Hpc5HX#]tM:rSZkG<fs'(4?Ttgr1)@8&F8Dp6^tp<Brs]u<\\9WkaF0)kQoe*g`MI*g#Pg_b@,3:FFQ'$G2M9oT_G1bdiM1o]K:gUr7=n;+nBbe:F'A$\"TF2hFZl/e3eg_;SiM1o]K:gV=*s/!Zn^(n;F'>aYkkmR>_9Zht,!i,PG\\Ao.3N`$!f#`]&2M7XN>(rWN=tI(Dq](kUG<$2_=MJk462lr[=LX.j>rX:+c/?(CYgW_\\paDf[i9.IJOeC/c4L?\"(SC[QKke]gjD$E5&[>G)mpaDf[i9.K(LHqbD4L?\"(SCT1n3^8[Sn8hd68C2&l*nh$O:@!=aFn4Lbc/@cl<Zrll(/&N*5\"A&c/`UhalB9g@j:=h5FfQ);jJ7;aKAZ+6:'E]S.%niCa0].rd,dAPZl/e/[dXm%PE,e!%\":n9'`sqR'[UI]A(ndiJ1c3=ct;cWg-QCE/*>4iISB&B!aX*h6^toQBaZ$!](FsTc/?XL?4g#Z=tI(Dq\\YSQ[nL!f7cbTP*TWR*A)/XJlf#cLS&+Q;@AetTnDU(ci9s3P'o@(X_a9j/hiZLe&D\"aSLGje\",HXrkf#*8u2M8ahXnO!?Wtu.\\#(K-HY7T:`4M&AdOk(upl^ET_2M8ahXnO!?bD1d0K>/Y5<=%oj`DISbh[u,.G?fpf=MAe36iN/]=LX.j>rX:+c/?(CYgW_\\paDf[i9.IJOeC/c4L?\"(SC[QKke]gjD$E5&[>G)mpaDf[i9.K(LHqbD4L?\"(SCT1n3^8[Sn8hd68C2&l*nh$O:@!=aFn4Lbc/@cl<Zrll(/&N*5\"B2.X]':<^2`!srf@gr[u(;o3%:NFi4$>&df`tO.%niCkHn9PT[Q\"cZl/e3f)$$B_FFWD#!]#nM?I5[.%niCkHs*$5c7pNZl/e3eggka5EC9_F'>V8ctOOBg'iF*l[=S3o&6:ei9.IJ+XjB,95W`R7qKo8cL!TXDk-Q`59pE`XWg%I:'tYjq!B^#1dMpA$>;6j3NZ:,kVNe\\D$E5&of`Min52H?_9Zht,!d#/$>;6j3N`$!d&@flD$E5&oa68qc_DT[SCT/&FIIu?2M7XNH66UeI!B<\\pc5HV#g00d;B*0m;&OG7:\"*:\\ESa\"\\O;(g+M?I3E.A4rD\\%Aln\"C:dNTZuEXZrW<^dl]b\")>p#Uq[hu.7fdH25GtoH\"C:dNTZuEX[,-R'Vd;Km)>p#U.g.mtO+-S>>,t3@Bc\\>L%\":t;I>E%:iM\"NSkHn85.'\"+VD<*lNA^##G0V.Jfc9')hOojrMOe<(0\"gf0FFr,=jTF2hFZl/e3eg_;SRYQYVK:gV=*s,aC.%niCkHn9PTF2hFZl/e3f)$$Be;K)?K:gV=Kg=had1:\\$CD@.:c,2o<_9Zi/&$\\kn4L?\"(SCT.mA^t7hNSN]q33IC1L@_tP9]hAq'8d;bRSUgV*C\"s$I&t])jJ/ZFO,s$d:5.GGh\\qNAlGJ*5DEka=RbtCGrT)5q@tekP+2t@7Da/[eF=Q).4T)=6Da0Vtn.E0qO5ea,[i\\<0'uI)#'XnRdrF\"?r7pJpEMnaE(m0D*d7:*I^j4sY-\\:*YBe<S:9T[[OTR^miMWt=N9*Zj!nI8&rAJ`Jp7QJ\"fWDZXb_0C-1YgK$P>G<F1>p,Pjbkr?A$<k!t)7lZp^c0\"-[.6;-Q*KlKLcbrkHLJ0L<aI1%DhESA<LZIKDfW1$'(qI#`qn^$,6RXA\"o\"`s_Nuu4&qsQL5P2u5Rq!sbm?BG<t-WU0eVo\\;D??9kM8^W%..H89HTn:1<566\"`336,=LEAbXe+N4L=4?-VronI5rbuPs$gfj#rP(_3_7)Y+__eL_cJ>@D>)-fr&(dEaJ(XaF\\Gl6]m`M`)fCL65T>,[j]3i9'k@$Esqj1+700ckVg\\H6R^O12Xhi>Ua5(EAd%F]D<i&mM_gZ,?lI!>>Yeb/pd_lI_B12ZBiAQj;Ep%CZ\"DJba%ELAK;4M8DC/T\\GYIsYg0]\"MK1c+2ZiQQ$_nm3egPV7sSAk0<Sgeb9S]np9V[J4k4r3A/K-iOcie>lT1q^&RYBIJ`=/iVqeO4776)eN@4,AoCjs8RLBRa8M?KmgegShS!_(XL2cpcF`Am@p>_1F_iAZVU3n?HhZp.\\,ZHqnEStTE<PFII[tI<h0r-p_c3DRj#VApYM[!@GPCL\\_tBQX['c+(CU'FjANHoPZ,cJ.\\B`\"jf71M<-ZaXMgKd?%anbXK0V>Ids4(=h?lYKgFJm=3SuZ$t[t!:a)5#(Or;PYr\\&pOf_gL/iF2#<gfPJe!cf\\4Ha)A^EL,t<rKA@aCdXG?H)IDTMq`,a.OjAq:m=B6NqXuGF[9_DO8`t?PT!Hu`Rb?LRju2,!*SKW)I/SJ5<R/LpZZ)GgJN1?Aj5JhemIssohq1s^mD9fdb;@/]0.-5ceb-*7ESKj5!k1+![r\"']^]*16s7kU:T>11\"pV6^T3a%2S\\'WQp1tQ_s6Ebn7m]O(\\VVOY+gPenJ?'OZEl`4qrW3g2le+r)\\lalKc^%mNcd-=l^0@/m1C<MPfenp2s&X`tBlgAMW)qn'Sd-Y1hpt$=sg/C0mG3md^DcCI2Bb)H;)COV8p\\2H#p<sPe++F*j^\\ua_n,;5D5C`RlVt[qIIem^SgRipXr6PEBGkbLT^]*rM^YTM\"\\#T.lEF*Na7/PGgMu,_cro^K<[_qNj3Sba;55]<2osd+iErX_KI/ilWX6]Lu<V$.lMk$R/~>"].map(data=>add(`<< /Type /XObject /Subtype /Form /BBox [0 0 842 595] /Resources << /Font << /F1 ${font1} 0 R >> >> /Filter [/ASCII85Decode /FlateDecode] /Length ${data.length} >>\nstream\n${data}\nendstream`));
  const pageRefs=[];
  for(const stream of pageStreams){
    const content=add(`<< /Length ${new TextEncoder().encode(stream).length} >>\nstream\n${stream}endstream`);
    const page=add(`<< /Type /Page /Parent ${pagesObj} 0 R /MediaBox [0 0 842 595] /Resources << /XObject << /GridA ${grids[0]} 0 R /GridB ${grids[1]} 0 R >> /Font << /F1 ${font1} 0 R /F2 ${font2} 0 R >> >> /Contents ${content} 0 R >>`);
    pageRefs.push(page);
  }
  objects[catalog-1]=`<< /Type /Catalog /Pages ${pagesObj} 0 R >>`;
  objects[pagesObj-1]=`<< /Type /Pages /Kids [${pageRefs.map(n=>`${n} 0 R`).join(' ')}] /Count ${pageRefs.length} >>`;

  let pdf='%PDF-1.4\n%\xE2\xE3\xCF\xD3\n',offsets=[0];
  for(let i=0;i<objects.length;i++){
    offsets.push(new TextEncoder().encode(pdf).length);
    pdf+=`${i+1} 0 obj\n${objects[i]}\nendobj\n`;
  }
  const xref=new TextEncoder().encode(pdf).length;
  pdf+=`xref\n0 ${objects.length+1}\n0000000000 65535 f \n`;
  for(let i=1;i<offsets.length;i++)pdf+=String(offsets[i]).padStart(10,'0')+' 00000 n \n';
  pdf+=`trailer\n<< /Size ${objects.length+1} /Root ${catalog} 0 R >>\nstartxref\n${xref}\n%%EOF`;
  return new Blob([new TextEncoder().encode(pdf)],{type:'application/pdf'});
}
function exportEasaStylePdf(){
  const {all,selected,firstPage,lastPages}=selectedExperiencePageRange();
  if(!selected.length)return alert('No professional experience matches this selection.');
  const chunks=[];for(let i=0;i<selected.length;i+=EASA_ROWS_PER_PAGE)chunks.push(selected.slice(i,i+EASA_ROWS_PER_PAGE));
  const streams=chunks.flatMap((rows,pageIndex)=>{
    let prior,pageNo;
    if(lastPages){
      const absolutePage=(firstPage||0)+pageIndex;
      prior=expTotals(all.slice(0,absolutePage*EASA_ROWS_PER_PAGE));
      pageNo=absolutePage+1;
    }else{
      const idx=all.findIndex(x=>x.id===rows[0]?.id&&x._experienceKind===rows[0]?._experienceKind);
      prior=expTotals(idx>0?all.slice(0,idx):[]);
      pageNo=idx>=0?Math.floor(idx/EASA_ROWS_PER_PAGE)+1:pageIndex+1;
    }
    const pageTotal=expTotals(rows),toDate=plusTotals(prior,pageTotal);
    return ['A','B'].map(side=>buildEasaPdfPage(rows,prior,pageTotal,toDate,pageNo,side));
  });
  const blob=makePdfDocument(streams);
  const name=`PilotLog_EASA_${today()}.pdf`;
  if(typeof window!=='undefined'&&window.PilotLogNative?.isNative){
    return window.PilotLogNative.exportFile(name,blob,'application/pdf').catch(error=>{
      if(!/cancel|annul/i.test(error.message||''))alert('Esportazione PDF non riuscita: '+error.message);
    });
  }
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;a.target='_blank';a.rel='noopener';a.download=name;
  document.body.appendChild(a);a.click();a.remove();
  setTimeout(()=>URL.revokeObjectURL(url),60000);
}

function exportExperiencePdf(){
  const entries=[...experienceFlights(),...experienceSims()];
  if(!entries.length)return alert('No professional experience to export');
  const profile=appSettings(),pages=[];
  let stream='',y=0;
  function page(){
    if(stream)pages.push(stream);
    stream=pdfText(40,542,22,profile.profileName||'Professional experience','left',0,true);
    stream+=pdfText(40,516,13,[profile.profileRole,'Flight experience'].filter(Boolean).join('  |  '));
    y=466;
  }
  function heading(label){
    if(y<128)page();
    stream+=pdfText(40,y,14,label,'left',0,true);y-=27;
  }
  const columns=['Total flight','IFR','SP','MP','PIC','SIC','PICUS'];
  function header(){
    columns.forEach((label,i)=>stream+=pdfText(212+i*84,y,10,label,'left',0,true));y-=22;
  }
  function row(label,records){
    if(y<50){page();heading('Aircraft experience');header()}
    const t=expTotals(records);
    stream+=pdfText(40,y,11,label);
    [t.total,t.ifr,t.se+t.me,t.multi,t.pic,t.sic,t.picUS].forEach((n,i)=>stream+=pdfText(212+i*84,y,11,fmt(n)));
    y-=24;
  }
  page();heading('General');header();row('All aircraft',entries);
  y-=12;heading('Aircraft experience');header();
  const groups=new Map();
  for(const entry of entries.filter(isFlight)){
    const label=aircraftIcaoType(entry)||'Unspecified';
    if(!groups.has(label))groups.set(label,[]);
    groups.get(label).push(entry);
  }
  for(const [label,records] of [...groups].sort(([a],[b])=>a.localeCompare(b)))row(label,records);
  y-=12;heading('Training experience');
  const flight=sum(entries.filter(isFlight),flightInstrMins),sim=sum(entries.filter(isSim),simInstrMins);
  for(const [label,n] of [['Total',flight+sim],['In flight',flight],['In simulator',sim]]){
    stream+=pdfText(40,y,11,label)+pdfText(212,y,11,fmt(n));y-=22;
  }
  pages.push(stream);
  download('PilotLog_Experience_'+today()+'.pdf',makePdfDocument(pages),'application/pdf');
}
function exportFullBackupJson(){
  const payload={schema:'PilotLog Backup',version:VERSION,architecture:'single-authoritative-database',exportedAt:new Date().toISOString(),database:normalizeOperationalDatabase(operationalDatabase)};
  delete payload.database.replica162;delete payload.database.preSync162;
  download(`pilotlog_backup_${today()}.json`,JSON.stringify(payload,null,2),'application/json')
}
function validateFullBackupPayload(payload){
  if(!payload||typeof payload!=='object'||payload.schema!=='PilotLog Backup')throw new Error('This is not a PilotLog Full Backup JSON.');
  if(payload.database!=null&&!isOperationalDatabase(payload.database))throw new Error('Invalid authoritative database section.');
  if(!payload.database)for(const k of ['flights','roster','duties','trips','expiry','syncLedger'])if(payload[k]!=null&&!Array.isArray(payload[k]))throw new Error(`Invalid backup section: ${k}.`);
  const database=migrateSnapshotToOperationalDatabase(payload),counts=operationalDatabaseCounts(database);
  return{database,...counts};
}
async function restoreFullBackupJsonFile(file){
  const text=await file.text();let payload;try{payload=JSON.parse(String(text||'').replace(/^\uFEFF/,''))}catch{throw new Error('The selected JSON file cannot be read.')}
  const counts=validateFullBackupPayload(payload);
  const ok=confirm(`Restore this PilotLog backup?\n\nVersion: ${payload.version||'unknown'}\nExported: ${payload.exportedAt?displayDateTime(payload.exportedAt):'unknown'}\nUnique operational activities: ${counts.activities}\nLogbook / entry view: ${counts.entries}\nRoster view: ${counts.roster}\nDuty view: ${counts.duties}\nTrips: ${counts.trips}\nExpiry: ${counts.expiry}\n\nCurrent local PilotLog data on this device will be replaced. Nothing will be uploaded automatically.`);
  if(!ok)return false;
  await replaceOperationalDatabase(counts.database);
  reconcileOperationalFlightIdentities();await flushFlightStore();reconcileAllDuties();markLocalOnlyImport(`Full backup restored locally — ${counts.activities} unique activities`);await render();await renderRoster();renderDuty();renderTrips();renderPayroll();renderSettings();resetEntry(false);
  alert(`Full Backup restored locally.\n\n${counts.activities} unique activities • ${counts.roster} roster • ${counts.trips} trips • ${counts.expiry} expiry records.\n\nAuto Sync is OFF. Review this device before pressing Sync now.`);return true;
}


let screenshotReviewRows=[];
let screenshotObjectUrl='';
let screenshotOcrLayout=null;
let screenshotSmartCells=[];

function normalizeScreenshotDate(v){
  const s=String(v||'').trim();
  let m=s.match(/^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2,4})$/);
  if(m){
    let y=m[3]; if(y.length===2)y=`20${y}`;
    return `${y}-${String(m[2]).padStart(2,'0')}-${String(m[1]).padStart(2,'0')}`;
  }
  m=s.match(/^(\d{4})[\/.\-](\d{1,2})[\/.\-](\d{1,2})$/);
  if(m)return `${m[1]}-${String(m[2]).padStart(2,'0')}-${String(m[3]).padStart(2,'0')}`;
  return normalDate(s);
}
function normalizeOcrTime(v){
  const s=String(v||'').trim().replace('.',':');
  const m=s.match(/\b([01]?\d|2[0-3])[:h]([0-5]\d)\b/i);
  return m?`${String(m[1]).padStart(2,'0')}:${m[2]}`:'';
}
function classifyScreenshotDuty(text){
  const s=upper(text||'');
  if(/\bSTBY\b|STANDBY|\bHSBY\b/.test(s))return'STBY';
  if(/\bDHP\b/.test(s))return'DHP';
  if(/\bDHD\b|DEADHEAD|POSITIONING|TRANSFER|\bTGV\b|PICK\s*UP/.test(s))return'DHD';
  if(/\bSIM\b|SIMULATOR|FSTD|FFS/.test(s))return'Simulator';
  if(/GROUND\s*COURSE|\bGRT\b|\bCRM\b|\bSEP\b|COURSE|TRAINING\s*GROUND/.test(s))return'Ground Course';
  if(/\bDUTY\b/.test(s)&&!/\bFLIGHT\b/.test(s))return'Duty';
  return'Flight';
}
function screenshotFlightNumbers(text){
  const out=[];
  const seen=new Set();
  const re=/\b(?:3O|MAC)\s*[- ]?(\d{2,4})\b/gi;
  let m;
  while((m=re.exec(text))){
    const x=`3O${m[1]}`;
    if(!seen.has(x)){seen.add(x);out.push(x)}
  }
  return out;
}
function screenshotRoutes(text){
  const routes=[];
  const re=/\b([A-Z]{3})\s*(?:→|->|–>|-|TO)\s*([A-Z]{3})\b/g;
  let m;
  while((m=re.exec(upper(text||''))))routes.push([m[1],m[2]]);
  return routes;
}
function screenshotTimes(text){
  const out=[];
  const re=/\b(?:[01]?\d|2[0-3])[:.]([0-5]\d)\b/g;
  let m;
  while((m=re.exec(text))){
    const t=normalizeOcrTime(m[0]);
    if(t&&!out.includes(t))out.push(t)
  }
  return out;
}
function screenshotAirports(text){
  const stop=new Set(['THE','AND','PIC','SIC','SIM','FSTD','OFF','OUT','DAY','NIGHT','DUTY','TOTAL','FROM','DATE','TIME','FLIGHT','COURSE','CRM','GRT','SEP','MAC']);
  return [...new Set((upper(text||'').match(/\b[A-Z]{3}\b/g)||[]).filter(x=>!stop.has(x)))];
}
function ocrDateMatches(line){
  return [...String(line||'').matchAll(/\b(?:\d{1,2}[\/.\-]\d{1,2}[\/.\-]\d{2,4}|\d{4}[\/.\-]\d{1,2}[\/.\-]\d{1,2})\b/g)].map(m=>m[0]);
}
function screenshotMissingFields(r){
  const missing=[],dutyType=r?.dutyType==='To complete'?'To complete':canonicalDutyType(r?.dutyType||'');
  if(!r?.date)missing.push('date');
  if(!r?.dutyType||dutyType==='To complete')missing.push('dutyType');

  if(dutyType==='Flight'){
    if(!r.flightNo)missing.push('flightNo');
    if(!r.dep)missing.push('dep');
    if(!r.arr)missing.push('arr');
    if(r.target==='roster'){
      if(!r.start)missing.push('start');
      if(!r.end)missing.push('end');
    }
  }else if(dutyType==='DHD'||dutyType==='DHP'){
    if(!r.dep)missing.push('dep');
    if(!r.arr)missing.push('arr');
    if(!r.start)missing.push('start');
    if(!r.end)missing.push('end');
  }else if(dutyType==='STBY'||dutyType==='Simulator'){
    if(!r.start)missing.push('start');
    if(!r.end)missing.push('end');
  }else if(dutyType==='Ground Course'){
    if(!r.start)missing.push('start');
  }

  return [...new Set(missing)];
}
function screenshotRowReady(r){return screenshotMissingFields(r).length===0}
function makeScreenshotRow(o={}){
  const row={
    include:o.include!==false,
    target:o.target||'entries',
    date:o.date||'',
    dutyType:o.dutyType&&o.dutyType!=='To complete'?canonicalDutyType(o.dutyType):'To complete',
    flightNo:o.flightNo||'',
    dep:upper(o.dep||''),
    arr:upper(o.arr||''),
    start:o.start||'',
    end:o.end||'',
    remarks:o.remarks||'',
    rawText:o.rawText||o.remarks||'',
    parserNote:o.parserNote||''
  };
  row.needsReview=!screenshotRowReady(row);
  return row;
}
function screenshotReviewSummaryData(){
  const selected=screenshotReviewRows.filter(r=>r.include);
  const incomplete=selected.filter(r=>!screenshotRowReady(r));
  return{selected:selected.length,incomplete:incomplete.length,ready:selected.length-incomplete.length};
}

const OCR_MONTHS={
  JAN:1,JANUARY:1,FEB:2,FEBRUARY:2,MAR:3,MARCH:3,APR:4,APRIL:4,MAY:5,
  JUN:6,JUNE:6,JUL:7,JULY:7,AUG:8,AUGUST:8,SEP:9,SEPT:9,SEPTEMBER:9,
  OCT:10,OCTOBER:10,NOV:11,NOVEMBER:11,DEC:12,DECEMBER:12
};
const OCR_DAY_NAMES=new Set(['MON','MONDAY','TUE','TUES','TUESDAY','WED','WEDNESDAY','THU','THUR','THURS','THURSDAY','FRI','FRIDAY','SAT','SATURDAY','SUN','SUNDAY']);

function screenshotBlockWords(blocks){
  const out=[];
  (blocks||[]).forEach(block=>(block.paragraphs||[]).forEach(p=>(p.lines||[]).forEach(line=>
    (line.words||[]).forEach(w=>{
      const b=w.bbox||{},text=String(w.text||'').trim();
      if(!text||!Number.isFinite(b.x0)||!Number.isFinite(b.y0)||!Number.isFinite(b.x1)||!Number.isFinite(b.y1))return;
      out.push({text,x0:b.x0,y0:b.y0,x1:b.x1,y1:b.y1,cx:(b.x0+b.x1)/2,cy:(b.y0+b.y1)/2});
    })
  )));
  return out;
}
function screenshotWordLines(words){
  if(!words.length)return[];
  const hs=words.map(w=>Math.max(1,w.y1-w.y0)).sort((a,b)=>a-b);
  const medianH=hs[Math.floor(hs.length/2)]||12,tol=Math.max(5,medianH*.8);
  const sorted=[...words].sort((a,b)=>a.cy-b.cy||a.x0-b.x0),lines=[];
  for(const w of sorted){
    let line=lines.find(l=>Math.abs(l.cy-w.cy)<=tol);
    if(!line){line={words:[],cy:w.cy};lines.push(line)}
    line.words.push(w);
    line.cy=(line.cy*(line.words.length-1)+w.cy)/line.words.length;
  }
  return lines.sort((a,b)=>a.cy-b.cy).map(l=>{
    l.words.sort((a,b)=>a.x0-b.x0);
    l.text=l.words.map(w=>w.text).join(' ');
    l.x0=Math.min(...l.words.map(w=>w.x0));l.x1=Math.max(...l.words.map(w=>w.x1));
    l.y0=Math.min(...l.words.map(w=>w.y0));l.y1=Math.max(...l.words.map(w=>w.y1));
    return l;
  });
}
function crewScheduleYear(text){
  const years=[...String(text||'').matchAll(/\b(20\d{2})\b/g)].map(m=>Number(m[1]));
  return years[0]||new Date().getFullYear();
}
function crewScheduleDefaultMonth(text){
  const s=upper(text||'');
  for(const [name,num] of Object.entries(OCR_MONTHS)){
    if(new RegExp(`\\b${name}\\b`).test(s))return num;
  }
  return new Date().getMonth()+1;
}
function cleanOcrToken(v){return upper(v||'').replace(/[^A-Z0-9]/g,'')}
function crewScheduleDateHeaders(words,text){
  const lines=screenshotWordLines(words),year=crewScheduleYear(text),defaultMonth=crewScheduleDefaultMonth(text),headers=[];
  for(const line of lines){
    const ws=line.words;
    for(let i=0;i<ws.length;i++){
      const token=cleanOcrToken(ws[i].text),isDayName=OCR_DAY_NAMES.has(token);
      let day=null,month=null,start=i,end=i;
      if(isDayName){
        for(let j=i+1;j<=Math.min(i+3,ws.length-1);j++){
          const t=cleanOcrToken(ws[j].text);
          if(day===null&&/^\d{1,2}$/.test(t)&&Number(t)>=1&&Number(t)<=31){day=Number(t);end=j;continue}
          if(day!==null&&OCR_MONTHS[t]){month=OCR_MONTHS[t];end=j;break}
        }
        if(day!==null&&!month)month=defaultMonth;
      }else if(/^\d{1,2}$/.test(token)&&Number(token)>=1&&Number(token)<=31){
        const next=cleanOcrToken(ws[i+1]?.text);
        if(OCR_MONTHS[next]){day=Number(token);month=OCR_MONTHS[next];end=i+1}
      }
      if(day!==null&&month){
        const chosen=ws.slice(start,end+1);
        headers.push({
          date:`${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`,
          x0:Math.min(...chosen.map(w=>w.x0)),x1:Math.max(...chosen.map(w=>w.x1)),
          y0:Math.min(...chosen.map(w=>w.y0)),y1:Math.max(...chosen.map(w=>w.y1)),
          cx:chosen.reduce((s,w)=>s+w.cx,0)/chosen.length,
          cy:chosen.reduce((s,w)=>s+w.cy,0)/chosen.length
        });
        i=end;
      }
    }
  }
  const dedup=[];
  headers.sort((a,b)=>a.cy-b.cy||a.cx-b.cx).forEach(h=>{
    if(!dedup.some(x=>x.date===h.date&&Math.abs(x.cx-h.cx)<20&&Math.abs(x.cy-h.cy)<20))dedup.push(h);
  });
  return dedup;
}
function clusterCrewHeaderRows(headers){
  if(!headers.length)return[];
  const hs=headers.map(h=>Math.max(1,h.y1-h.y0)).sort((a,b)=>a-b);
  const tol=Math.max(8,(hs[Math.floor(hs.length/2)]||12)*1.6),rows=[];
  [...headers].sort((a,b)=>a.cy-b.cy||a.cx-b.cx).forEach(h=>{
    let row=rows.find(r=>Math.abs(r.cy-h.cy)<=tol);
    if(!row){row={headers:[],cy:h.cy};rows.push(row)}
    row.headers.push(h);
    row.cy=(row.cy*(row.headers.length-1)+h.cy)/row.headers.length;
  });
  rows.forEach(r=>r.headers.sort((a,b)=>a.cx-b.cx));
  return rows.sort((a,b)=>a.cy-b.cy);
}
function crewScheduleFooterY(words,lastHeaderY){
  const footer=words.filter(w=>w.cy>lastHeaderY&&/^(REPORTING|MEMO)$/i.test(cleanOcrToken(w.text))).map(w=>w.y0);
  return footer.length?Math.min(...footer):null;
}
function crewScheduleCellWords(words,row,rowIndex,rows,headerIndex,footerY=null){
  const hs=row.headers,h=hs[headerIndex];
  const gaps=hs.slice(1).map((x,i)=>x.cx-hs[i].cx).filter(x=>x>10).sort((a,b)=>a-b);
  const spacing=gaps[Math.floor(gaps.length/2)]||Math.max(50,h.x1-h.x0+20);
  const left=headerIndex>0?(hs[headerIndex-1].cx+h.cx)/2:h.cx-spacing/2;
  const right=headerIndex<hs.length-1?(h.cx+hs[headerIndex+1].cx)/2:h.cx+spacing/2;
  const nextRowY=rowIndex<rows.length-1?Math.min(...rows[rowIndex+1].headers.map(x=>x.y0)):null;
  const rowEnd=nextRowY||footerY||Math.max(...words.map(w=>w.y1))+10;
  const rowHeight=Math.max(30,rowEnd-h.y1);
  const contentEnd=rowEnd-Math.max(10,rowHeight*.14);
  return words.filter(w=>w.cx>=left&&w.cx<right&&w.cy>h.y1&&w.cy<contentEnd);
}
function gridLineFlightNos(line){
  const text=upper(line||'').replace(/[|]/g,' ');
  const out=[],re=/\b(?:3[O0Q]|MAC)\s*[- ]?(\d{2,4})\b/g;
  let m;while((m=re.exec(text)))out.push(`3O${m[1]}`);
  return out;
}
function inferScreenshotRoute(flightNo){
  const digits=rosterFlightDigits(flightNo||'');
  if(!digits)return{dep:'',arr:''};
  const candidates=coreActivityRows().filter(isFlight).filter(x=>rosterFlightDigits(x.flightNo||'')===digits&&x.dep&&x.arr)
   .sort((a,b)=>String(b.date||'').localeCompare(String(a.date||'')));
  return candidates.length?{dep:upper(candidates[0].dep),arr:upper(candidates[0].arr)}:{dep:'',arr:''};
}
function gridCellRows(date,cellWords){
  if(!cellWords.length)return[];
  const lines=screenshotWordLines(cellWords),
    raw=lines.map(l=>l.text).join(' ').replace(/\s+/g,' ').trim(),
    blob=upper(raw),
    rows=[];

  const hasFlights=lines.some(l=>gridLineFlightNos(l.text).length);
  if(/\bOFF\b/.test(blob)&&!hasFlights){
    return[makeScreenshotRow({
      target:'duty',date,dutyType:'Day OFF',
      remarks:raw||'OFF',rawText:raw||'OFF'
    })];
  }

  for(const line of lines){
    const txt=upper(line.text),flights=gridLineFlightNos(txt),times=screenshotTimes(txt);

    if(flights.length){
      flights.forEach((flightNo,i)=>{
        const route=inferScreenshotRoute(flightNo);
        rows.push(makeScreenshotRow({
          target:'roster',date,dutyType:'Flight',flightNo,
          dep:route.dep,arr:route.arr,
          start:times[Math.min(i,times.length-1)]||'',end:'',
          remarks:line.text,rawText:line.text,
          parserNote:(!route.dep||!route.arr)?'Complete From/To.':''
        }));
      });
      continue;
    }

    if(/\bDHP\b|\bDHD\b|DEADHEAD/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({
        target:'entries',date,dutyType:/\bDHP\b/.test(txt)?'DHP':'DHD',
        dep:airports[0]||'',arr:airports[1]||'',
        start:times[0]||'',end:times[1]||'',
        remarks:line.text,rawText:line.text
      }));
      continue;
    }

    if(/\bHSBY\b|\bSTBY\b|STANDBY/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({
        target:'entries',date,dutyType:'STBY',
        dep:airports[0]||'',
        start:times[0]||'',end:times[1]||'',
        remarks:line.text,rawText:line.text
      }));
      continue;
    }

    if(/\bSIM\b|SIMULATOR|FSTD|FFS/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({
        target:'entries',date,dutyType:'Simulator',
        dep:airports[0]||'',
        start:times[0]||'',end:times[1]||'',
        remarks:line.text,rawText:line.text
      }));
      continue;
    }

    if(/\bGRT\b|\bCRM\b|\bSMS\b|\bSEP\b|\bDGR\b|\bAVSEC\b|FIRST\s*AID|\bUPRT\b|GROUND\s*COURSE|COURSE/.test(txt)){
      rows.push(makeScreenshotRow({
        target:'entries',date,dutyType:'Ground Course',
        start:times[0]||'',end:times[1]||'',
        remarks:line.text,rawText:line.text
      }));
    }
  }

  // If we understood at least one activity, keep the original OCR in Remarks
  // of those rows. Missing fields will be highlighted in Review.
  if(rows.length)return rows;

  // Never silently discard a non-empty cell.
  // Ignore only obvious bottom-of-cell totals made exclusively of numbers/times.
  const totalsOnly=/^(?:\d{1,2}[:.]\d{2}|\d+(?:[.,]\d+)?|\s|-)+$/.test(raw);
  if(raw&&!totalsOnly){
    return[makeScreenshotRow({
      target:'entries',date,dutyType:'To complete',
      remarks:raw.slice(0,220),rawText:raw,
      parserNote:'PilotLog could not classify this cell.'
    })];
  }
  return[];
}
function parseCrewScheduleGrid(layout,text){
  const words=screenshotBlockWords(layout?.blocks||[]);
  if(words.length<10)return[];
  const headers=crewScheduleDateHeaders(words,text),rows=clusterCrewHeaderRows(headers);
  if(headers.length<4||rows.length<1)return[];
  const footerY=crewScheduleFooterY(words,Math.max(...headers.map(h=>h.cy)));
  const out=[];
  rows.forEach((row,rowIndex)=>row.headers.forEach((h,i)=>{
    out.push(...gridCellRows(h.date,crewScheduleCellWords(words,row,rowIndex,rows,i,footerY)));
  }));
  const seen=new Set();
  return out.filter(r=>{
    const k=[r.target,r.date,r.dutyType,r.flightNo,r.dep,r.arr,r.start,r.end].join('|');
    if(seen.has(k))return false;
    seen.add(k);return true;
  });
}


function crewScheduleUnparsedRows(layout,text){
  const words=screenshotBlockWords(layout?.blocks||[]);
  const lines=screenshotWordLines(words)
    .map(l=>l.text.replace(/\s+/g,' ').trim())
    .filter(Boolean)
    .filter(t=>!/INDIVIDUAL\s+CREW\s+SCHEDULE|CURRENT\s+MONTH|REPORTING\s+TIME|MEMO|CREW\s+NAME|CREW\s+ID/i.test(t));

  const meaningful=lines.filter(t=>
    /\b(?:3[O0Q]|MAC)\s*[- ]?\d{2,4}\b|\bOFF\b|\bDHD\b|\bSTBY\b|\bHSBY\b|\bSIM\b|\bGRT\b|\bCRM\b|\bSMS\b|\bSEP\b|\bDGR\b|\bAVSEC\b|\bA320\b|\d{1,2}[:.]\d{2}/i.test(t)
  );

  const source=meaningful.length?meaningful:String(text||'').replace(/\s+/g,' ').trim()?[String(text||'').replace(/\s+/g,' ').trim()]:[];
  return source.slice(0,40).map(t=>makeScreenshotRow({
    target:'entries',date:'',dutyType:'To complete',
    remarks:t.slice(0,220),rawText:t,
    parserNote:'OCR recovered text but could not safely assign its date/type.'
  }));
}

function parseScreenshotText(text){
  const source=$('screenshotSource')?.value||'auto';
  const defaultTarget=$('screenshotTarget')?.value||'auto';
  const isCrewGrid=/INDIVIDUAL\s+CREW\s+SCHEDULE\s+REPORT/i.test(String(text||''));
  if(screenshotSmartCells.length&&(source==='roster'||isCrewGrid)){
    const smartRows=parseSmartCrewScheduleCells(screenshotSmartCells);
    if(smartRows.length)return smartRows;
  }
  if(screenshotOcrLayout&&(source==='roster'||isCrewGrid)){
    const gridRows=parseCrewScheduleGrid(screenshotOcrLayout,text);
    if(gridRows.length)return gridRows;
    const fallbackRows=crewScheduleUnparsedRows(screenshotOcrLayout,text);
    if(fallbackRows.length)return fallbackRows;
  }
  const cleaned=String(text||'')
    .replace(/[—–]/g,'-')
    .replace(/[|]/g,' ')
    .replace(/\r/g,'')
    .split('\n')
    .map(x=>x.replace(/\s+/g,' ').trim())
    .filter(Boolean);

  if(!cleaned.length)return[];

  // Split OCR into date-based blocks. This works well for roster cards and LogTen list screenshots.
  const blocks=[];
  let cur=null;
  for(const line of cleaned){
    const dates=ocrDateMatches(line);
    if(dates.length){
      if(cur)blocks.push(cur);
      cur={date:normalizeScreenshotDate(dates[0]),lines:[line]};
    }else if(cur){
      cur.lines.push(line);
    }else{
      cur={date:'',lines:[line]};
    }
  }
  if(cur)blocks.push(cur);

  const home=upper(appSettings().homeBase||'CMN');
  const rows=[];

  for(const block of blocks){
    const blob=block.lines.join(' ');
    const dutyType=classifyScreenshotDuty(blob);
    const flights=screenshotFlightNumbers(blob);
    const routes=screenshotRoutes(blob);
    const times=screenshotTimes(blob);
    const airports=screenshotAirports(blob);
    const autoRoster=source==='roster'||(/\bROSTER\b/i.test(text))||(/\b\d+\s*FLIGHTS?\b/i.test(blob));
    const target=defaultTarget==='auto'?(autoRoster?'roster':'entries'):defaultTarget;

    // For roster duty cards, the visible start/end is often the duty window, not sector times.
    // Preserve it as an internal Duty row and keep sector times blank unless they are clearly sector-level.
    const dutyCount=(blob.match(/\b(\d+)\s*FLIGHTS?\b/i)||[])[1];
    const isRosterDutyBlock=target==='roster' && (Number(dutyCount)>1 || flights.length>1);

    if(isRosterDutyBlock && block.date){
      rows.push(makeScreenshotRow({
        target:'duty',date:block.date,dutyType:'Duty',
        start:times[0]||'',end:times[1]||'',
        remarks:'Imported from screenshot roster'
      }));
    }

    if(dutyType!=='Flight' && dutyType!=='Duty' && !flights.length){
      const route=routes[0]||[];
      rows.push(makeScreenshotRow({
        target:target==='roster'?'entries':target,
        date:block.date,dutyType,
        dep:route[0]||airports[0]||'',
        arr:route[1]||'',
        start:times[0]||'',end:times[1]||'',
        remarks:blob.slice(0,140)
      }));
      continue;
    }

    if(flights.length){
      for(let i=0;i<flights.length;i++){
        let dep='',arr='';
        if(routes[i]){dep=routes[i][0];arr=routes[i][1]}
        else if(routes.length===1 && flights.length===2){
          if(i===0){dep=routes[0][0];arr=routes[0][1]}
          else{dep=routes[0][1];arr=routes[0][0]}
        }else if(routes.length===1){
          dep=routes[0][0];arr=routes[0][1]
        }else if(airports.length>=2){
          dep=airports[Math.min(i,airports.length-2)]||airports[0];
          arr=airports[Math.min(i+1,airports.length-1)]||airports[1];
        }

        rows.push(makeScreenshotRow({
          target,
          date:block.date,
          dutyType:'Flight',
          flightNo:flights[i],
          dep,arr,
          start:isRosterDutyBlock?'':(times[i*2]||times[0]||''),
          end:isRosterDutyBlock?'':(times[i*2+1]||times[1]||''),
          remarks:'Imported from screenshot'
        }));
      }
      continue;
    }

    if(routes.length){
      for(let i=0;i<routes.length;i++){
        rows.push(makeScreenshotRow({
          target,date:block.date,dutyType:'Flight',
          dep:routes[i][0],arr:routes[i][1],
          start:times[i*2]||'',end:times[i*2+1]||'',
          remarks:'Imported from screenshot'
        }));
      }
      continue;
    }

    // Last-resort editable row. Nothing is imported until the user reviews it.
    if(block.date || dutyType!=='Flight' || blob.trim()){
      const safeType=(dutyType==='Flight'&&!flights.length&&!routes.length)?'To complete':dutyType;
      rows.push(makeScreenshotRow({
        target:target==='roster'?'entries':target,
        date:block.date,dutyType:safeType,
        dep:airports[0]||'',arr:airports[1]||'',
        start:times[0]||'',end:times[1]||'',
        remarks:blob.slice(0,220),rawText:blob.slice(0,220),
        parserNote:safeType==='To complete'?'Unrecognized OCR block.':''
      }));
    }
  }

  // Remove exact duplicate review rows.
  const seen=new Set();
  return rows.filter(r=>{
    const k=[r.target,r.date,r.dutyType,r.flightNo,r.dep,r.arr,r.start,r.end].join('|');
    if(seen.has(k))return false;
    seen.add(k);return true;
  });
}
function renderScreenshotReview(){
  const wrap=$('screenshotReviewWrap'),body=$('screenshotReviewBody'),summary=$('screenshotReviewSummary');
  if(!screenshotReviewRows.length){
    wrap.classList.add('hidden');body.innerHTML='';if(summary)summary.textContent='';return;
  }

  screenshotReviewRows.forEach(r=>r.needsReview=!screenshotRowReady(r));
  const stat=screenshotReviewSummaryData();
  if(summary){
    summary.className=`screenshot-review-summary ${stat.incomplete?'has-incomplete':'all-ready'}`;
    summary.textContent=stat.incomplete
      ?`${stat.ready} ready • ${stat.incomplete} row${stat.incomplete===1?'':'s'} to complete`
      :`${stat.ready} ready to import`;
  }

  const missingClass=(r,field)=>screenshotMissingFields(r).includes(field)?'shot-missing':'';
  const label={dutyType:'Type',flightNo:'Flight',dep:'From',arr:'To',start:'Start',end:'End',date:'Date'};

  body.innerHTML=screenshotReviewRows.map((r,i)=>{
    const missing=screenshotMissingFields(r);
    const status=missing.length?`Complete: ${missing.map(x=>label[x]||x).join(', ')}`:'Ready';
    return `<tr data-shot-review-row="${i}" class="${missing.length?'shot-needs-review':'shot-ready'}">
      <td><input type="checkbox" data-shot-row="${i}" data-shot-field="include" ${r.include?'checked':''}></td>
      <td><select data-shot-row="${i}" data-shot-field="target">
        <option value="entries" ${r.target==='entries'?'selected':''}>Entries</option>
        <option value="roster" ${r.target==='roster'?'selected':''}>Roster</option>
        <option value="duty" ${r.target==='duty'?'selected':''}>Duty</option>
      </select></td>
      <td class="${missingClass(r,'date')}"><input type="date" data-shot-row="${i}" data-shot-field="date" value="${esc(r.date)}"></td>
      <td class="${missingClass(r,'dutyType')}"><select data-shot-row="${i}" data-shot-field="dutyType">
        ${['To complete','Flight','DHD','DHP','STBY','Ground Course','Simulator','Day OFF','Duty'].map(x=>`<option ${r.dutyType===x?'selected':''}>${x}</option>`).join('')}
      </select></td>
      <td class="${missingClass(r,'flightNo')}"><input data-upper data-shot-row="${i}" data-shot-field="flightNo" value="${esc(r.flightNo)}"></td>
      <td class="${missingClass(r,'dep')}"><input data-upper maxlength="4" data-shot-row="${i}" data-shot-field="dep" value="${esc(r.dep)}"></td>
      <td class="${missingClass(r,'arr')}"><input data-upper maxlength="4" data-shot-row="${i}" data-shot-field="arr" value="${esc(r.arr)}"></td>
      <td class="${missingClass(r,'start')}"><input type="time" data-shot-row="${i}" data-shot-field="start" value="${esc(r.start)}"></td>
      <td class="${missingClass(r,'end')}"><input type="time" data-shot-row="${i}" data-shot-field="end" value="${esc(r.end)}"></td>
      <td><input data-shot-row="${i}" data-shot-field="remarks" value="${esc(r.remarks)}"><div class="shot-row-status">${esc(status)}${r.parserNote?` • ${esc(r.parserNote)}`:''}</div></td>
    </tr>`;
  }).join('');
  wrap.classList.remove('hidden');
}
function updateScreenshotReviewFromControl(el){
  const i=Number(el.dataset.shotRow),field=el.dataset.shotField;
  if(!Number.isInteger(i)||!screenshotReviewRows[i]||!field)return;
  screenshotReviewRows[i][field]=field==='include'?!!el.checked:(field==='dep'||field==='arr'||field==='flightNo'?upper(el.value):el.value);
  screenshotReviewRows[i].needsReview=!screenshotRowReady(screenshotReviewRows[i]);
}
function clearScreenshotReview(){
  screenshotReviewRows=[];
  screenshotOcrLayout=null;
  screenshotSmartCells=[];
  $('screenshotReviewBody').innerHTML='';
  $('screenshotReviewWrap').classList.add('hidden');
}
async function loadTesseractBrowser(){
  if(window.Tesseract)return window.Tesseract;
  await new Promise((resolve,reject)=>{
    const existing=document.querySelector('script[data-pilotlog-tesseract]');
    if(existing){
      existing.addEventListener('load',resolve,{once:true});
      existing.addEventListener('error',()=>reject(new Error('OCR library could not be loaded.')),{once:true});
      return;
    }
    const s=document.createElement('script');
    s.src='https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js';
    s.async=true;s.dataset.pilotlogTesseract='1';
    s.onload=resolve;s.onerror=()=>reject(new Error('OCR library could not be loaded. Check the internet connection.'));
    document.head.appendChild(s);
  });
  if(!window.Tesseract)throw new Error('OCR library unavailable.');
  return window.Tesseract;
}

async function prepareScreenshotForOcr(file){
  const bitmap=await createImageBitmap(file);
  try{
    const pixels=Math.max(1,bitmap.width*bitmap.height),maxPixels=12_000_000;
    const scale=Math.min(2.5,Math.max(1,Math.sqrt(maxPixels/pixels)));
    const canvas=document.createElement('canvas');
    canvas.width=Math.max(1,Math.round(bitmap.width*scale));
    canvas.height=Math.max(1,Math.round(bitmap.height*scale));
    const ctx=canvas.getContext('2d',{alpha:false});
    ctx.fillStyle='#fff';ctx.fillRect(0,0,canvas.width,canvas.height);
    try{ctx.filter='grayscale(1) contrast(1.28)'}catch{}
    ctx.drawImage(bitmap,0,0,canvas.width,canvas.height);
    try{ctx.filter='none'}catch{}
    return canvas;
  }finally{
    if(bitmap.close)bitmap.close();
  }
}


function crewScheduleCellRects(layout,text,width,height){
  const words=screenshotBlockWords(layout?.blocks||[]);
  if(words.length<10)return[];
  const headers=crewScheduleDateHeaders(words,text),rows=clusterCrewHeaderRows(headers);
  if(headers.length<4||!rows.length)return[];
  const footerY=crewScheduleFooterY(words,Math.max(...headers.map(h=>h.cy)))||height;
  const rects=[];

  rows.forEach((row,rowIndex)=>{
    const hs=row.headers;
    const gaps=hs.slice(1).map((x,i)=>x.cx-hs[i].cx).filter(x=>x>15).sort((a,b)=>a-b);
    const spacing=gaps[Math.floor(gaps.length/2)]||Math.max(60,width/Math.max(1,hs.length));
    const nextTop=rowIndex<rows.length-1?Math.min(...rows[rowIndex+1].headers.map(x=>x.y0)):footerY;
    const rowBottom=Math.min(height,nextTop);
    const contentTop=Math.max(...hs.map(h=>h.y1))+2;
    const rowHeight=Math.max(30,rowBottom-contentTop);
    const bodyBottom=rowBottom-Math.max(8,rowHeight*.12);

    hs.forEach((h,i)=>{
      const left=i>0?(hs[i-1].cx+h.cx)/2:h.cx-spacing/2;
      const right=i<hs.length-1?(h.cx+hs[i+1].cx)/2:h.cx+spacing/2;
      const x=Math.max(0,Math.floor(left+2)),y=Math.max(0,Math.floor(contentTop));
      const r=Math.min(width,Math.ceil(right-2)),b=Math.min(height,Math.floor(bodyBottom));
      if(r-x>20&&b-y>18)rects.push({date:h.date,x,y,width:r-x,height:b-y});
    });
  });

  const seen=new Set();
  return rects.filter(r=>{if(seen.has(r.date))return false;seen.add(r.date);return true})
    .sort((a,b)=>a.date.localeCompare(b.date));
}
function cropRosterCellCanvas(source,rect){
  const scale=2;
  const canvas=document.createElement('canvas');
  canvas.width=Math.max(1,Math.round(rect.width*scale));
  canvas.height=Math.max(1,Math.round(rect.height*scale));
  const ctx=canvas.getContext('2d',{alpha:false});
  ctx.fillStyle='#fff';ctx.fillRect(0,0,canvas.width,canvas.height);
  try{ctx.filter='grayscale(1) contrast(1.48)'}catch{}
  ctx.drawImage(source,rect.x,rect.y,rect.width,rect.height,0,0,canvas.width,canvas.height);
  try{ctx.filter='none'}catch{}
  return canvas;
}
function smartCellUsefulText(text){
  const s=String(text||'').replace(/\s+/g,' ').trim();
  if(!s)return false;
  if(/^(?:\d{1,2}[:.]\d{2}|\d+(?:[.,]\d+)?|\s|-)+$/.test(s))return false;
  return /[A-Z]{2,}|\d{2,}/i.test(s);
}
async function recognizeCrewScheduleCells(worker,T,source,layout,text,status){
  const rects=crewScheduleCellRects(layout,text,source.width,source.height);
  if(rects.length<4)return[];
  const cells=[];

  if(T.PSM?.SINGLE_BLOCK){
    try{await worker.setParameters({tessedit_pageseg_mode:T.PSM.SINGLE_BLOCK})}catch{}
  }

  for(let i=0;i<rects.length;i++){
    const rect=rects[i];
    status.textContent=`Smart roster scan — reading day ${i+1}/${rects.length}…`;
    const crop=cropRosterCellCanvas(source,rect);
    try{
      const result=await worker.recognize(crop);
      const cellText=String(result?.data?.text||'').replace(/\r/g,'').split('\n')
        .map(x=>x.replace(/\s+/g,' ').trim()).filter(Boolean).join('\n');
      if(smartCellUsefulText(cellText))cells.push({date:rect.date,text:cellText,confidence:Number(result?.data?.confidence||0)});
    }catch(e){
      console.warn('Smart cell OCR failed',rect.date,e);
      cells.push({date:rect.date,text:'',confidence:0,error:true});
    }
  }
  return cells;
}
function gridCellTextRows(date,text){
  const raw=String(text||'').replace(/\r/g,'').trim();
  if(!raw)return[];
  const lines=raw.split('\n').map(x=>x.replace(/\s+/g,' ').trim()).filter(Boolean);
  const blob=upper(lines.join(' ')),rows=[];
  const hasFlight=lines.some(x=>gridLineFlightNos(x).length);

  if(/\bOFF\b/.test(blob)&&!hasFlight){
    return[makeScreenshotRow({target:'duty',date,dutyType:'Day OFF',remarks:raw.slice(0,220),rawText:raw,parserNote:'Smart Cell Scan'})];
  }

  for(const line of lines){
    const txt=upper(line),flightNos=gridLineFlightNos(txt),times=screenshotTimes(txt);
    if(flightNos.length){
      flightNos.forEach((flightNo,i)=>{
        const route=inferScreenshotRoute(flightNo);
        rows.push(makeScreenshotRow({
          target:'roster',date,dutyType:'Flight',flightNo,dep:route.dep,arr:route.arr,
          start:times[Math.min(i,times.length-1)]||'',end:'',remarks:line,rawText:line,
          parserNote:(!route.dep||!route.arr)?'Smart Cell Scan • complete From/To and clipped times.':'Smart Cell Scan • complete clipped times.'
        }));
      });
      continue;
    }
    if(/\bDHD\b|DEADHEAD/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({target:'entries',date,dutyType:'DHD',dep:airports[0]||'',arr:airports[1]||'',start:times[0]||'',end:times[1]||'',remarks:line,rawText:line,parserNote:'Smart Cell Scan'}));
      continue;
    }
    if(/\bHSBY\b|\bSTBY\b|STANDBY/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({target:'entries',date,dutyType:'STBY',dep:airports[0]||'',start:times[0]||'',end:times[1]||'',remarks:line,rawText:line,parserNote:'Smart Cell Scan'}));
      continue;
    }
    if(/\bSIM\b|SIMULATOR|\bFSTD\b|\bFFS\b/.test(txt)){
      const airports=screenshotAirports(txt);
      rows.push(makeScreenshotRow({target:'entries',date,dutyType:'Simulator',dep:airports[0]||'',start:times[0]||'',end:times[1]||'',remarks:line,rawText:line,parserNote:'Smart Cell Scan'}));
      continue;
    }
    if(/\bGRT\b|\bCRM\b|\bSMS\b|\bSEP\b|\bDGR\b|\bAVSEC\b|FIRST\s*AID|\bUPRT\b|GROUND\s*COURSE|COURSE/.test(txt)){
      rows.push(makeScreenshotRow({target:'entries',date,dutyType:'Ground Course',start:times[0]||'',end:times[1]||'',remarks:line,rawText:line,parserNote:'Smart Cell Scan'}));
    }
  }

  if(rows.length)return rows;
  if(smartCellUsefulText(raw)){
    return[makeScreenshotRow({target:'entries',date,dutyType:'To complete',remarks:raw.slice(0,220),rawText:raw,parserNote:'Smart Cell Scan read this day but could not classify it.'})];
  }
  return[];
}
function parseSmartCrewScheduleCells(cells){
  const out=[];
  (cells||[]).forEach(cell=>out.push(...gridCellTextRows(cell.date,cell.text)));
  const seen=new Set();
  return out.filter(r=>{
    const k=[r.target,r.date,r.dutyType,r.flightNo,r.dep,r.arr,r.start,r.end,r.remarks].join('|');
    if(seen.has(k))return false;seen.add(k);return true;
  });
}

async function runScreenshotOcr(file){
  const status=$('screenshotImportStatus');
  status.textContent='Loading OCR engine…';
  const T=await loadTesseractBrowser();
  let worker=null;
  try{
    worker=await T.createWorker('eng',T.OEM?.LSTM||1,{
      logger:m=>{
        if(m.status==='recognizing text'&&!/^Smart roster scan/.test(status.textContent||''))status.textContent=`Reading roster structure… ${Math.round((m.progress||0)*100)}%`;
        else if(m.status&&!/^Smart roster scan/.test(status.textContent||''))status.textContent=`OCR: ${m.status}`;
      }
    });
    status.textContent='Preparing screenshot…';
    let input=file;
    try{input=await prepareScreenshotForOcr(file)}catch(e){console.warn('Screenshot preprocessing skipped',e)}
    if(T.PSM?.SPARSE_TEXT){try{await worker.setParameters({tessedit_pageseg_mode:T.PSM.SPARSE_TEXT})}catch{}}
    status.textContent='Reading roster structure… 0%';
    const result=await worker.recognize(input,{}, {blocks:true});
    screenshotOcrLayout={blocks:result?.data?.blocks||[]};
    screenshotSmartCells=[];
    const text=result?.data?.text||'';
    const source=$('screenshotSource')?.value||'auto';
    const looksCrew=/INDIVIDUAL\s+CREW\s+SCHEDULE\s+REPORT/i.test(text);
    if((source==='roster'||looksCrew)&&input?.width&&input?.height){
      screenshotSmartCells=await recognizeCrewScheduleCells(worker,T,input,screenshotOcrLayout,text,status);
    }
    status.textContent=screenshotSmartCells.length?`Smart roster scan complete — ${screenshotSmartCells.length} populated day(s) read.`:'Roster structure read. Preparing review…';
    return text;
  }finally{
    if(worker)try{await worker.terminate()}catch{}
  }
}
async function importReviewedScreenshot(){
  const selected=screenshotReviewRows.filter(r=>r.include);
  if(!selected.length)return alert('No reviewed rows selected.');

  const incomplete=selected.filter(r=>!screenshotRowReady(r));
  if(incomplete.length){
    renderScreenshotReview();
    return alert(`${incomplete.length} selected row${incomplete.length===1?' is':'s are'} incomplete. Complete the yellow fields or uncheck the row before importing.`);
  }

  snapshotFlights('before-screenshot-import');

  const activities=coreActivityRows();
  let entryCount=0,rosterCount=0,dutyCount=0,duplicates=0;

  for(const r of selected){
    const date=normalizeScreenshotDate(r.date);
    if(!date){continue}

    if(r.target==='duty' || r.dutyType==='Duty' || r.dutyType==='Day OFF'){
      const type=r.dutyType==='Day OFF'?'Day OFF':'Flight Duty';
      const exists=activities.find(x=>isDutyActivityRecord(x)&&x.date===date&&upper(x.dutyType||'')===upper(type)&&String(x.onDuty||'')===String(r.start||'')&&String(x.offDuty||'')===String(r.end||''));
      if(exists){duplicates++;continue}
      activities.push(stamp({id:makeActivityId(),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:type,date,onDuty:r.start||'',offDuty:r.end||'',totalDuty:r.start&&r.end?timeSpanMins(r.start,r.end):0,remarks:r.remarks||'Imported from screenshot',source:'screenshot'}));
      dutyCount++;continue;
    }

    if(r.target==='roster'){
      const flightNo=r.flightNo?rosterFlightLabel(r.flightNo):'';
      const exists=activities.find(x=>isFlight(x)&&x.date===date&&upper(x.flightNo)===upper(flightNo)&&upper(x.dep)===upper(r.dep)&&upper(x.arr)===upper(r.arr));
      if(exists){duplicates++;continue}
      activities.push(stamp({id:makeActivityId(),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:'Flight',date,flightNo,dep:upper(r.dep),arr:upper(r.arr),schedOut:r.start||'',schedIn:r.end||'',source:'screenshot'}));
      rosterCount++;continue;
    }

    const dutyType=r.dutyType||'Flight';
    const flightNo=(dutyType==='Flight')?(r.flightNo?composeFlightNo(r.flightNo):''):'';
    const exists=activities.find(x=>x.source==='screenshot'&&x.date===date&&x.dutyType===dutyType&&upper(x.flightNo)===upper(flightNo)&&upper(x.dep)===upper(r.dep)&&upper(x.arr)===upper(r.arr)&&String(x.schedOut||x.onDuty||'')===String(r.start||''));
    if(exists){duplicates++;continue}

    const obj=stamp({
      id:makeActivityId(),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.COMPLETED,source:'screenshot',dutyType,date,flightNo,reg:'',type:dutyType==='Simulator'?'A320':'',
      dep:upper(r.dep),arr:upper(r.arr),
      schedOut:['Flight','Simulator','DHD','DHP'].includes(dutyType)?r.start||'':'',
      schedIn:['Flight','Simulator','DHD','DHP'].includes(dutyType)?r.end||'':'',
      schedBlock:['Flight','Simulator','DHD','DHP'].includes(dutyType)&&r.start&&r.end?timeSpanMins(r.start,r.end):0,
      onDuty:['Flight','Simulator','Ground Course','STBY','DHD','DHP'].includes(dutyType)&&r.start?(dutyType==='Flight'||dutyType==='Simulator'?shiftTime(r.start,-60):r.start):'',
      offDuty:['Flight','Simulator','Ground Course','STBY','DHD','DHP'].includes(dutyType)&&r.end?(dutyType==='Flight'||dutyType==='Simulator'?shiftTime(r.end,30):r.end):'',
      out:'',off:'',on:'',in:'',block:null,simulatorTime:dutyType==='Simulator'&&r.start&&r.end?timeSpanMins(r.start,r.end):null,
      credit:null,
      rolePic:false,instruction:false,simInstructor:false,simExaminer:false,night:null,sim:dutyType==='Simulator'?'yes':'no',ifr:'',
      dayTakeoffs:0,nightTakeoffs:0,dayLandings:0,nightLandings:0,
      courseType:dutyType==='Ground Course'?upper(r.remarks||''):'',
      remarks:r.remarks||'Imported from screenshot',locked:false
    });
    if(!['Simulator','Ground Course'].includes(dutyType))obj.credit=creditMinsForEntry(obj);
    activities.push(initialTrainingCreditSource(obj));entryCount++;
  }

  commitCoreActivities(activities);
  reconcileAllDuties();
  
  await render();
  renderTrips();
  $('screenshotImportStatus').textContent=`Imported ${entryCount} entries, ${rosterCount} roster sectors, ${dutyCount} duties. ${duplicates} duplicates skipped.`;
  alert(`Screenshot import complete: ${entryCount} entries • ${rosterCount} roster • ${dutyCount} duties • ${duplicates} duplicates skipped`);
}

function download(name,text,type='text/plain'){if(window.PilotLogNative?.isNative){window.PilotLogNative.exportFile(name,text,type).catch(e=>alert('Export not completed: '+(e.message||e)));return;}const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type}));a.download=name;document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove()},500)}
function logTenExport(){
  const fs=coreActivityRows().filter(isCompletedLogbookEntry);if(!fs.length)return alert('No entries to export');
  const h=['flight_flightDate','flight_type','flight_flightNumber','flight_from','flight_to','flight_scheduledDepartureTime','flight_actualDepartureTime','flight_takeoffTime','flight_landingTime','flight_scheduledArrivalTime','flight_actualArrivalTime','flight_totalTime','flight_pic','flight_sic','flight_dualGiven','flight_simulator','flight_sfiSfe','flight_ground','flight_ifr','flight_night','flight_dayTakeoffs','flight_nightTakeoffs','flight_dayLandings','flight_nightLandings','flight_onDutyTime','flight_offDutyTime','flight_remarks','aircraft_aircraftID','aircraftType_type','flight_selectedApproach1'];
  const value=(f,key)=>{const n=timeGroupFinalValue(f,key,fs);return n>0?fmt(n):''};
  const rows=fs.map(f=>[f.date,'',f.flightNo||'',f.dep||'',f.arr||'',f.schedOut||'',f.out||'',f.off||'',f.on||'',f.schedIn||'',f.in||'',value(f,'totalFlightTime'),value(f,'pic'),value(f,'sic'),value(f,'dualGiven'),value(f,'simulatorTime'),value(f,'sfiSfe'),value(f,'groundInstruction'),value(f,'ifrTime'),value(f,'night'),f.dayTakeoffs||0,f.nightTakeoffs||0,f.dayLandings||0,f.nightLandings||0,f.onDuty||'',f.offDuty||'',f.remarks||'',f.reg||'',f.type||'',f.approachType||f.approaches?.[0]?.type||'']);
  download(`PilotLog_LogTen_${today()}.txt`,[h.join('\t'),...rows.map(r=>r.map(v=>String(v??'').replace(/[\t\r\n]+/g,' ')).join('\t'))].join('\n'),'text/tab-separated-values');
}

/* Payroll */
function monthNow(){return new Date().toISOString().slice(0,7)}
function inMonth(date,month){return String(date||'').slice(0,7)===month}
function money(v){return new Intl.NumberFormat('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}).format(Number(v)||0)}
function instructionSectorForPayroll(f){return isFlight(f)&&f.instruction===true&&(!f._payrollRosterSynthetic||f._trainingAuthoritative===true)}
function tierPay(hours,st){hours=Math.max(0,Number(hours)||0);const a=Number(st.t1Max)||0,b=Math.max(a,Number(st.t2Max)||a),c=Math.max(b,Number(st.t3Max)||b);let left=hours,total=0,h=Math.min(left,a);total+=h*Number(st.t1Rate||0);left-=h;h=Math.min(Math.max(0,left),b-a);total+=h*Number(st.t2Rate||0);left-=h;h=Math.min(Math.max(0,left),c-b);total+=h*Number(st.t3Rate||0);left-=h;if(left>0)total+=left*Number(st.t4Rate||0);return total}
function seniorityPct(st,month){if(!st.joinDate)return 0;const start=new Date(`${st.joinDate}T00:00:00Z`),[y,m]=month.split('-').map(Number),end=new Date(Date.UTC(y,m,0,23,59,59));const years=(end-start)/(365.2425*86400000);if(years>=25&&st.seniority25!=null)return Number(st.seniority25);if(years>=5&&years<=12&&st.seniority5!=null)return Number(st.seniority5);return years>12?Number(st.seniority12||0):years>2?Number(st.seniority2||0):0}
function fixedSalaryAmount(st,seniority){return Number(st.base||0)+Number(st.allowance||0)+Number(st.transport||0)+seniority+Number(st.pos||0)+Number(st.telephone||0)+Number(st.uniform||0)+Number(st.meal||0)+Number(st.deduction||0)}
function showFixedSalary(st){
  const month=$('payrollMonth')?.value||monthNow();
  const amount=fixedSalaryAmount(st,Number(st.base||0)*seniorityPct(st,month)/100);
  $('fixedSalarySum').textContent=amount.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2})+' DHM';
}
function monthExtras(month){const all=loadObject(PAY_MONTH_KEY,{});return {dayOffCount:0,arrears:0,dayOffRemarks:{},...(all[month]||{})}}
function saveMonthExtras(month,x){if(contentHash(x)===contentHash(monthExtras(month)))return;const all=loadObject(PAY_MONTH_KEY,{});all[month]=x;all._updatedAt=new Date().toISOString();saveObject(PAY_MONTH_KEY,all)}
function payrollSnapshotProjection(p){
  const keys=['fixed','creditMins','nonFlightCreditCount','nonFlightCreditMins','flightPay','training','trainingPay','layMins','layoverPay','sims','simPay','dayOffCount','dayOffPay','arrears','total','netPayroll','contractTarget','grossPayroll','taxableGross','cimrBase'];
  const result=Object.fromEntries(keys.map(key=>[key,p[key]]));
  result.st={...p.st};
  result.extras={dayOffRemarks:{...(p.extras.dayOffRemarks||{})}};
  result.dayOffEvents=p.dayOffEvents.map(ev=>({date:ev.date,entries:ev.entries.map(f=>({dutyType:f.dutyType,dep:f.dep,arr:f.arr,flightNo:f.flightNo}))}));
  if(p.tax)result.tax={...p.tax};
  return result;
}

function dayOffEventsForMonth(month,sourceEntries=null){
  const flagged=(sourceEntries||entryActivityRows())
    .filter(f=>inMonth(f.date,month)&&f.callFromDayOff)
    .sort((a,b)=>`${a.date}${a.onDuty||a.schedOut||a.out||''}`.localeCompare(`${b.date}${b.onDuty||b.schedOut||b.out||''}`));

  const byDate=new Map();
  flagged.forEach(f=>{
    if(!byDate.has(f.date))byDate.set(f.date,{date:f.date,entries:[]});
    byDate.get(f.date).entries.push(f);
  });
  return [...byDate.values()];
}
function saveDayOffRemark(month,date,remark){
  const ex=monthExtras(month);
  if(ex.validated)return;
  ex.dayOffRemarks={...(ex.dayOffRemarks||{}),[date]:String(remark||'')};
  saveMonthExtras(month,ex);
}
function dayOffEventLabel(ev){
  const entries=ev.entries||[],first=entries[0]||{};
  const flights=entries.filter(isFlight);
  if(flights.length){
    const a=flights[0],b=flights.at(-1);
    return `${a.dep||''}${b.arr?' → '+b.arr:''}${a.flightNo?' • '+a.flightNo:''}`;
  }
  return `${first.dutyType||'Duty'}${first.dep?' • '+first.dep:''}`;
}

function calculatePayrollProjection(month){
  const st=paySettings(),
    rosterPrimary=rosterMonthAvailable(month),
    fs=operationalEntriesForMonth(month),
    sourceName=rosterPrimary?'Roster':(fs.some(f=>String(f.source||'').toLowerCase()==='logten')?'LogTen Pro':'Logbook'),
    trips=load(TRIPS_KEY).filter(t=>String(t.start||'').slice(0,7)===month),
    tripResults=calculateSavedTripProjections(trips,operationalEntriesForTrips()),
    extras=monthExtras(month),
    dayOffEvents=dayOffEventsForMonth(month,fs),
    dayOffCount=dayOffEvents.length,
    calculatedCreditMins=fs.reduce((n,f)=>n+creditMinsForEntry(f,fs),0),
    nonFlightCredit=fs.reduce((acc,f)=>{const c=creditMinsForEntry(f,fs);if(!isFlight(f)&&c>0){acc.count++;acc.mins+=c}return acc},{count:0,mins:0}),
    creditMins=calculatedCreditMins,
    training=fs.filter(instructionSectorForPayroll).length,
    sims=fs.filter(f=>isSim(f)&&userIsSimTrainer(f)).length,
    layMins=tripResults.reduce((n,t)=>n+(Number(t.cashAmount||0)>0?0:Number(t.layover)||0),0),
    layHours=layMins/60,
    seniorPct=seniorityPct(st,month),
    seniority=Number(st.base||0)*seniorPct/100,
    fixed=Number(st.base||0)+Number(st.allowance||0)+Number(st.transport||0)+seniority+Number(st.pos||0)+Number(st.telephone||0)+Number(st.uniform||0)+Number(st.meal||0)+Number(st.deduction||0),
    flightPay=tierPay(creditMins/60,st),
    trainingPay=training*Number(st.trainingRate||0),
    layoverPay=tripResults.reduce((n,t)=>n+(Number(t.allowance)||0),0),
    simPay=sims*Number(st.simAllowance||0),
    dayOffPay=dayOffCount*Number(st.dayOffRate||0),
    arrears=Number(extras.arrears||0),
    total=fixed+flightPay+trainingPay+layoverPay+simPay+dayOffPay+arrears;

  return{st,extras,sourceName,rosterPrimary,dayOffEvents,dayOffCount,creditMins,nonFlightCreditCount:nonFlightCredit.count,nonFlightCreditMins:nonFlightCredit.mins,training,sims,layMins,layHours,tripResults,seniorPct,seniority,fixed,flightPay,trainingPay,layoverPay,simPay,dayOffPay,arrears,total};
}

// v12.5 MAC payroll model.
// IBC calculation above is intentionally unchanged. MAC starts from the same contractual
// amounts, applies the taxability observed in 2026 Air Arabia Maroc payslips, and grosses
// protected regular taxable pay so that its net target is preserved. Positive arrears /
// adjustments are treated as taxable, non-protected extra pay.
const MAC_TAX={cnssRate:.0448,cnssCap:6000,amoRate:.0226,healthRate:.0112,cimrRate:.0375,professionalExpenseMonthlyCap:2917,professionalExpenseRate:.45,companyTopIrRate:.37,companyTopIrDeduction:2272.92,familyDeductionMonthlyPerPerson:41.67};
function macIrFromRni(rni,dependents=0){
  rni=Math.max(0,Number(rni)||0);dependents=Math.max(0,Math.min(6,Math.trunc(Number(dependents)||0)));
  let ir=0;
  // The upper bracket reproduces the regular Jul/Aug 2026 company payslips supplied for validation.
  // Lower brackets retain the 2025/2026 Moroccan progressive schedule.
  if(rni<=3333.33)ir=0;
  else if(rni<=5000)ir=rni*.10-333.33;
  else if(rni<=6666.67)ir=rni*.20-833.33;
  else if(rni<=8333.33)ir=rni*.30-1500;
  else if(rni<=15000)ir=rni*.34-1833.33;
  else ir=rni*MAC_TAX.companyTopIrRate-MAC_TAX.companyTopIrDeduction;
  ir-=dependents*MAC_TAX.familyDeductionMonthlyPerPerson;
  return Math.max(0,ir);
}
function macTaxFromGross(taxableGross,cimrBase,dependents=0){
  taxableGross=Math.max(0,Number(taxableGross)||0);cimrBase=Math.max(0,Math.min(taxableGross,Number(cimrBase)||0));
  const cnss=Math.min(taxableGross,MAC_TAX.cnssCap)*MAC_TAX.cnssRate;
  const amo=taxableGross*MAC_TAX.amoRate;
  const health=taxableGross*MAC_TAX.healthRate;
  const cimr=cimrBase*MAC_TAX.cimrRate;
  const contributions=cnss+amo+health+cimr;
  const professionalExpense=Math.min(taxableGross*MAC_TAX.professionalExpenseRate,MAC_TAX.professionalExpenseMonthlyCap);
  const netTaxable=Math.max(0,taxableGross-contributions-professionalExpense);
  const ir=macIrFromRni(netTaxable,dependents);
  return{cnss,amo,health,cimr,contributions,professionalExpense,netTaxable,ir};
}
function calculateMacPayrollProjection(p){
  const st=p.st,dependents=macDependents();
  // Confirmed/observed non-taxable categories in the supplied MAC payslips:
  // transport, telephone, uniform/salissure, meal and layover/DHD allowance.
  const nonTaxableRegular=Number(st.transport||0)+Number(st.telephone||0)+Number(st.uniform||0)+Number(st.meal||0)+Number(p.layoverPay||0);
  const postTaxFixed=Number(st.deduction||0);
  // Base + seniority are CIMR-bearing. Other regular protected pay is taxable but not CIMR-bearing.
  const cimrTarget=Math.max(0,Number(st.base||0)+Number(p.seniority||0));
  const otherTaxableTarget=Math.max(0,Number(st.allowance||0)+Number(st.pos||0)+Number(p.flightPay||0)+Number(p.trainingPay||0)+Number(p.simPay||0)+Number(p.dayOffPay||0));
  const regularTarget=Number(p.total||0)-Number(p.arrears||0);
  const factorCimr=1/((1-MAC_TAX.amoRate-MAC_TAX.healthRate-MAC_TAX.cimrRate)*(1-MAC_TAX.companyTopIrRate));
  const factorOther=1/((1-MAC_TAX.amoRate-MAC_TAX.healthRate)*(1-MAC_TAX.companyTopIrRate));
  const baseCimrGross0=cimrTarget*factorCimr,otherGross0=otherTaxableTarget*factorOther;
  function regularAt(scale){
    const cimrGross=baseCimrGross0*scale,otherGross=otherGross0*scale,taxableGross=cimrGross+otherGross;
    const tax=macTaxFromGross(taxableGross,cimrGross,dependents);
    const net=taxableGross+nonTaxableRegular+postTaxFixed-tax.contributions-tax.ir;
    return{scale,cimrGross,otherGross,taxableGross,tax,net};
  }
  let lo=.25,hi=3;
  for(let i=0;i<80;i++){const mid=(lo+hi)/2;if(regularAt(mid).net<regularTarget)lo=mid;else hi=mid}
  const regular=regularAt((lo+hi)/2);
  const taxableExtra=Math.max(0,Number(p.arrears||0)),postTaxExtra=Math.min(0,Number(p.arrears||0));
  const taxableGross=regular.taxableGross+taxableExtra,cimrBase=regular.cimrGross;
  const tax=macTaxFromGross(taxableGross,cimrBase,dependents);
  const grossPayroll=taxableGross+nonTaxableRegular+postTaxFixed+postTaxExtra;
  const netPayroll=grossPayroll-tax.contributions-tax.ir;
  return{...p,regime:'MAC',dependents,contractTarget:p.total,regularTarget,nonTaxableRegular,postTaxFixed,cimrTarget,otherTaxableTarget,cimrGross:regular.cimrGross,otherTaxableGross:regular.otherGross,taxableExtra,postTaxExtra,taxableGross,cimrBase,grossPayroll,netPayroll,tax,grossUpScale:regular.scale};
}

const payrollRequests=new Map(),failedPayrollQuotes=new Set();
let monthEndTimer;
function recoverPendingFx(){
  failedPayrollQuotes.clear();refreshPendingTripFx();
  const keys=new Set([...Object.keys(operationalDatabase.fxRequests||{}),...Object.entries(fxStore()).filter(([k,v])=>!v.locked&&k.split(':')[1]<monthNow()).map(([k])=>k)]);
  for(const key of keys){const [,month,,target]=key.split(':');if(month&&target)getMonthFx(month,target).catch(()=>{})}
  schedulePayrollClose();
}
function schedulePayrollClose(){
  clearTimeout(monthEndTimer);
  if(!Object.entries(fxStore()).some(([k,v])=>v?.provisional&&k.split(':')[1]>=monthNow()))return;
  const now=new Date(),end=Date.UTC(now.getUTCFullYear(),now.getUTCMonth()+1,1);
  monthEndTimer=setTimeout(recoverPendingFx,Math.min(2147483647,end-Date.now()+100));
}
function fxStore(){return loadObject(FX_KEY,{})}
function payrollFxTargetDate(month){const [y,m]=month.split('-').map(Number),day=new Date(Date.UTC(y,m,0)).getUTCDate();return `${y}-${String(m).padStart(2,'0')}-${String(day).padStart(2,'0')}`}
function payrollConversionCurrency(){const cur=appCurrency();return cur==='MAD'?'EUR':cur}
async function getMonthFx(month,target=appCurrency()){
  target=upper(target);if(target==='MAD')return{rate:1,date:payrollFxTargetDate(month),locked:true,provisional:false,from:'MAD',to:'MAD'};
  const key='payroll:'+month+':MAD:'+target,final=month<monthNow(),prior=fxStore()[key];
  if(prior?.locked||prior&&!final)return prior;
  if(payrollRequests.has(key))return payrollRequests.get(key);
  if(failedPayrollQuotes.has(key))throw Error('FX pending');
  const owner=cloudStoredSession()?.user?.id,waiting=operationalDatabase.fxRequests??={};
  if(!waiting[key]){waiting[key]=today();scheduleFlightPersist()}
  const task=(async()=>{
    if(navigator.onLine===false)throw Error('Offline');
    const requested=final?payrollFxTargetDate(month):waiting[key],got=await fxForDate('MAD',target,requested);
    if(waiting!==operationalDatabase.fxRequests||owner!==cloudStoredSession()?.user?.id)throw Error('Local archive changed');
    if(!(got.rate>0))throw Error('FX unavailable');
    const data=fxStore();
    if(data[key]?.locked||data[key]&&!final)return data[key];
    const rate={...got,locked:final,provisional:!final,from:'MAD',to:target};
    delete waiting[key];data[key]=rate;saveObject(FX_KEY,data);schedulePayrollClose();return rate;
  })().catch(error=>{failedPayrollQuotes.add(key);throw error}).finally(()=>payrollRequests.delete(key));payrollRequests.set(key,task);return task;
}
const PAY_MAP={setSeniority5:'seniority5',setSeniority25:'seniority25',setGroundCredit:'groundCredit',setSimCredit:'simCredit',setDeductionMemo:'deductionMemo',setJoinDate:'joinDate',setBase:'base',setAllowance:'allowance',setTransport:'transport',setPos:'pos',setTelephone:'telephone',setUniform:'uniform',setMeal:'meal',setDeduction:'deduction',setSeniority2:'seniority2',setSeniority12:'seniority12',setT1Max:'t1Max',setT1Rate:'t1Rate',setT2Max:'t2Max',setT2Rate:'t2Rate',setT3Max:'t3Max',setT3Rate:'t3Rate',setT4Rate:'t4Rate',setTrainingRate:'trainingRate',setLayoverRate:'layoverRate',setSimAllowance:'simAllowance',setDayOffRate:'dayOffRate'};
function fillPaySettings(){const st=paySettings();Object.entries(PAY_MAP).forEach(([id,k])=>{if($(id))$(id).value=st[k]??''});showFixedSalary(st)}
function readPaySettings(){const st={};Object.entries(PAY_MAP).forEach(([id,k])=>st[k]=k==='joinDate'?$(id).value:$(id).value===''&&PAY_DEFAULTS[k]===null?null:Number($(id).value||0));return st}
function renderTripCashPayment(month){
  const container=$('payTripCash');if(!container)return;
  const frozen=monthExtras(month).validated;
  if(frozen){container.innerHTML=frozen.tripCashHtml||'<div class="empty">No trip cash in this month.</div>';return}
  const target=appCurrency(),trips=load(TRIPS_KEY).filter(t=>String(t.start||'').slice(0,7)===month&&Number(t.cashAmount)>0);
  container.innerHTML='<h3>Trip cash — fuori busta</h3>'+(!trips.length?'<div class="empty">No trip cash in this month.</div>':trips.map(t=>{
    const fx=tripFxCache(t,target,upper(t.cashCurrency||''),tripStartDateFromValue(t.start));
    const conversion=fx?`${moneyCurrency(Number(t.cashAmount)*fx.rate,target)} · ${fx.provisional?'Provisional FX':'FX'} ${esc(displayDate(fx.date))}`:'FX pending';
    return `<div class="stat-row"><span>${esc(displayDate(tripStartDateFromValue(t.start)))} · ${esc(t.stations||'Trip')}<br><span class="small">${conversion}</span></span><b>${moneyCurrency(t.cashAmount,t.cashCurrency)}</b></div>`;
  }).join(''));
}
let payrollRenderToken=0;
async function renderPayroll(){
  const payrollVisible=!$('payrollView')?.classList.contains('hidden');
  const token=++payrollRenderToken,month=$('payrollMonth').value||monthNow(),ex=monthExtras(month),frozen=ex.validated||null,displayCur=frozen?.currency||appCurrency(),cur=frozen?.conversionCurrency||payrollConversionCurrency();$('payrollMonth').value=month;updateCurrencyUI();
  $('payTotalCurrencyLabel').textContent=`Converted total (${currencyOutputCode(cur)})`;
  $('validatePayroll').classList.toggle('hidden',!!frozen);$('unlockPayroll').classList.toggle('hidden',!frozen);
  $('payrollValidationStatus').textContent=frozen?`Validated ${new Date(frozen.validatedAt).toLocaleString()} • values frozen`:'Not validated • values may change';
  $('payArrears').disabled=!!frozen;$('recalcPayroll').disabled=!!frozen;
  renderTripCashPayment(month);
  const regime=frozen?.regime||payrollRegime(),baseProjection=frozen?null:PilotLogEngine.payroll(month),p=frozen?.projection||(regime==='MAC'?calculateMacPayrollProjection(baseProjection):baseProjection);
  if($('payrollRegimeBadge'))$('payrollRegimeBadge').textContent=regime;
  $('payMacSummaryWrap')?.classList.toggle('hidden',regime!=='MAC');
  $('payArrears').value=ex.arrears||0;
  const sourceTotal=regime==='MAC'?p.netPayroll:p.total;
  $('payTotalSource').textContent=moneyCurrency(sourceTotal,'MAD');$('payTotalMain').textContent='…';$('payFxStatus').textContent='Loading FX conversion…';
  let fx;try{fx=frozen?.fx||(payrollVisible?await getMonthFx(month,cur):fxStore()['payroll:'+month+':MAD:'+cur]);if(!fx)return;}catch{if(token!==payrollRenderToken)return;$('payTotalMain').textContent='FX unavailable';$('payFxStatus').textContent='FX unavailable.';return}
  if(token!==payrollRenderToken)return;const cv=v=>(Number(v)||0)*Number(fx.rate||1),displayValue=v=>displayCur==='MAD'?(Number(v)||0):cv(v),unit=currencyOutputCode(displayCur);
  $('payTotalMain').textContent=moneyCurrency(cv(sourceTotal),cur);
  if(regime==='MAC'){
    const t=p.tax;
    $('payMacSummary').innerHTML=[
      ['Contract / protected net target',p.contractTarget],['Estimated gross payroll',p.grossPayroll],['Taxable gross',p.taxableGross],['CIMR base',p.cimrBase],['CNSS',-t.cnss],['AMO',-t.amo],['CIMR',-t.cimr],['Ass. Maladie',-t.health],['Professional expense deduction',-t.professionalExpense],['Net imposable',t.netTaxable],['IR',-t.ir],['MAC net payroll',p.netPayroll]
    ].map(([n,v])=>`<div class="mac-summary-row"><span>${esc(n)}</span><b>${money(displayValue(v))} ${unit}</b></div>`).join('');
    $('payBreakdown').innerHTML=[
      ['Fix Salary','',p.fixed],['Credit H',fmt(p.creditMins),p.flightPay],['NF CH',String(p.nonFlightCreditCount),fmt(p.nonFlightCreditMins),'credit-info'],['Training sectors',String(p.training),p.trainingPay],['Layover (non-taxable)',fmt(p.layMins),p.layoverPay],['Simulator allowance',String(p.sims),p.simPay],['Call from Day OFF',String(p.dayOffCount),p.dayOffPay],['Arrears / adjustments (taxable, not protected)','',p.arrears]
    ].map(([n,q,v,kind],i)=>`<div class="pay-breakdown-row ${i%2?'pay-breakdown-alt':''}"><span class="pay-breakdown-label">${esc(n)}</span><span class="pay-breakdown-qty">${esc(q)}</span><b class="money pay-breakdown-amount">${kind==='credit-info'?esc(v):money(displayValue(v))+' '+unit}</b></div>`).join('');
  }else{
    if($('payMacSummary'))$('payMacSummary').innerHTML='';
    $('payBreakdown').innerHTML=[
      ['Fix Salary','',p.fixed],['Credit H',fmt(p.creditMins),p.flightPay],['NF CH',String(p.nonFlightCreditCount),fmt(p.nonFlightCreditMins),'credit-info'],['Training sectors',String(p.training),p.trainingPay],['Layover',fmt(p.layMins),p.layoverPay],['Simulator allowance',String(p.sims),p.simPay],['Call from Day OFF',String(p.dayOffCount),p.dayOffPay],['Arrears / adjustments','',p.arrears]
    ].map(([n,q,v,kind],i)=>`<div class="pay-breakdown-row ${i%2?'pay-breakdown-alt':''}"><span class="pay-breakdown-label">${esc(n)}</span><span class="pay-breakdown-qty">${esc(q)}</span><b class="money pay-breakdown-amount">${kind==='credit-info'?esc(v):money(displayValue(v))+' '+unit}</b></div>`).join('');
  }
  const remarks=p.extras.dayOffRemarks||{};$('payDayOffDetails').innerHTML=p.dayOffEvents.length?p.dayOffEvents.map(ev=>{const remark=remarks[ev.date]||'';return `<div class="pay-dayoff-row"><div class="pay-dayoff-main"><b>${esc(displayDate(ev.date))}</b><div class="small">${esc(dayOffEventLabel(ev))}</div><div class="small">${moneyCurrency(displayValue(p.st.dayOffRate||0),displayCur)} premium</div></div><div class="pay-dayoff-remark-wrap"><label>Remark</label><input class="pay-dayoff-remark" data-dayoff-date="${esc(ev.date)}" value="${esc(remark)}" placeholder="e.g. Not paid / email sent"></div></div>`}).join(''):'<div class="empty">No Day OFF calls in this payroll month.</div>';
  $('payDayOffDetails').querySelectorAll('.pay-dayoff-remark').forEach(input=>input.disabled=!!frozen);
  $('payFxStatus').innerHTML=`MAD → ${esc(cur)} ${Number(fx.rate).toFixed(6)} • ${esc(displayDate(fx.date))} ${frozen||fx.locked?'<span class="fx-lock">LOCKED</span>':'<span class="fx-live">LIVE</span>'}${fx.provisional?' • provisional':''}`;
  return{projection:p,fx,regime,currency:displayCur,conversionCurrency:cur,tripCashHtml:$('payTripCash').innerHTML};
}

/* Expiry / validity tracking */
const EXPIRY_CATEGORIES=['license','medical','instructor','course','aircraft','english'];
let expiryPendingPhotoData='';

function expiryDaysRemaining(expiry,now=new Date()){
  if(!expiry)return null;
  const end=new Date(`${expiry}T00:00:00`);
  if(!Number.isFinite(end.getTime()))return null;
  const start=new Date(now.getFullYear(),now.getMonth(),now.getDate());
  return Math.round((Date.UTC(end.getFullYear(),end.getMonth(),end.getDate())-Date.UTC(start.getFullYear(),start.getMonth(),start.getDate()))/86400000);
}
function expiryStatus(expiry,now=new Date(),record={}){
  const days=expiryDaysRemaining(expiry,now);
  if(days===null)return{days:null,state:'unknown',label:'NO EXPIRY'};
  if(days<0)return{days,state:'expired',label:`EXPIRED ${Math.abs(days)} DAY${Math.abs(days)===1?'':'S'} AGO`};
  const title=upper([record.courseType,record.name,record.title,record.ratingType,record.licenseType,record.englishType,expiryTitle(record)].filter(Boolean).join(' '));
  const monthEnd=new Date(now.getFullYear(),now.getMonth()+2,0).getDate(),monthAhead=new Date(now.getFullYear(),now.getMonth()+1,Math.min(now.getDate(),monthEnd));
  const threshold=record.category==='instructor'?365:/OPC|LPC/.test(title)?90:record.category==='medical'?45:/EPC/.test(title)?expiryDaysRemaining(isoDateLocal(monthAhead),now):15;
  if(days<=threshold)return{days,state:'warning',label:`${days} DAY${days===1?'':'S'} REMAINING`};
  return{days,state:'valid',label:`${days} DAYS REMAINING`};
}
function expiryCategoryLabel(category){
  return category==='license'?'LICENCE':
    category==='medical'?'MEDICAL':
    category==='instructor'?'INSTRUCTOR RATING':
    category==='course'?'RECURRENT COURSE':
    category==='aircraft'?'AIRCRAFT ENDORSED':
    category==='english'?'LPC ENGLISH':'EXPIRY';
}
function expiryRecords(category){
  return load(EXPIRY_KEY)
    .filter(x=>x.category===category)
    .sort((a,b)=>String(a.expiry||'9999-99-99').localeCompare(String(b.expiry||'9999-99-99')));
}
function expiryTitle(x){
  if(x.category==='medical')return [x.medicalClass||'MEDICAL','MEDICAL',x.authority].filter(Boolean).join(' • ');
  if(x.category==='course')return upper(x.name||'COURSE');
  if(x.category==='instructor')return upper(x.name||'INSTRUCTOR RATING');
  if(x.category==='aircraft')return ['AIRCRAFT ENDORSED',x.aircraftType||x.name].filter(Boolean).join(' • ');
  if(x.category==='english')return ['LPC ENGLISH',x.englishLevel?`LEVEL ${x.englishLevel}`:''].filter(Boolean).join(' • ');
  return [x.authority,x.name||'LICENCE'].filter(Boolean).join(' • ');
}
function expirySubtitle(x){
  const parts=[];
  if(x.number)parts.push(`NO. ${upper(x.number)}`);
  if(x.lastCheck)parts.push(`${x.category==='aircraft'?'ENDORSED':'LAST CHECK'} ${displayDate(x.lastCheck)}`);
  if(x.courseDate)parts.push(`${x.category==='english'?'DATE PERFORMED':'COURSE'} ${displayDate(x.courseDate)}`);
  if(x.endorsedBy)parts.push(`ENDORSED BY ${upper(x.endorsedBy)}`);
  if(x.issuer)parts.push(upper(x.issuer));
  if(x.category==='aircraft'&&x.authority)parts.push(upper(x.authority));
  if(x.category==='english'&&x.authority)parts.push(upper(x.authority));
  return parts.join(' • ');
}
function expiryRowHtml(x){
  const s=expiryStatus(x.expiry,new Date(),x);
  const noExpiry=x.category==='english'&&String(x.englishLevel)==='6'&&!x.expiry;
  const actions=[];
  if(x.photoData)actions.push(`<button class="secondary" data-view-expiry-photo="${x.id}">VIEW</button>`);
  return `<div class="expiry-row expiry-${s.state}${x.locked?' expiry-locked':''}" data-edit-expiry="${x.id}" role="button" tabindex="0">
    <div class="expiry-main">
      <div class="expiry-title-line"><b>${esc(upper(expiryTitle(x)))}</b>${x.locked?'<span class="pill expiry-lock-pill">LOCKED</span>':''}</div>
      ${expirySubtitle(x)?`<div class="small">${esc(upper(expirySubtitle(x)))}</div>`:''}
      ${x.remarks?`<div class="small">${esc(upper(x.remarks))}</div>`:''}
    </div>
    <div class="expiry-meta">
      <b>${x.expiry?esc(displayDate(x.expiry)):(noExpiry?'NO EXPIRY':'—')}</b>
      <span>${esc(upper(s.label))}</span>
      <div class="list-actions">${actions.join('')}</div>
    </div>
  </div>`;
}
function renderExpiry(){
  const map={
    license:'expiryLicences',
    medical:'expiryMedicals',
    instructor:'expiryInstructorRatings',
    course:'expiryCourses',
    aircraft:'expiryAircraft',
    english:'expiryEnglish'
  };
  Object.entries(map).forEach(([category,id])=>{
    const rows=expiryRecords(category);
    $(id).innerHTML=rows.length?rows.map(expiryRowHtml).join(''):'<div class="empty">NO RECORDS YET.</div>';
  });
}
function setExpiryFieldVisible(id,visible){
  const el=$(id)?.closest('[data-expiry-field]');
  if(el)el.classList.toggle('hidden',!visible);
}
function setExpiryFields(ids){
  const all=['expiryName','expiryAuthority','expiryNumber','expiryLastCheck','expiryCourseDate','expiryMedicalClass','expiryEnglishLevel','expiryAircraftType','expiryEndorsedBy','expiryExpiry','expiryIssuer','expiryPhotoInput','expiryRemarks'];
  all.forEach(id=>setExpiryFieldVisible(id,ids.includes(id)));
}
function addYearsIso(date,years){
  const m=String(date||'').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if(!m)return'';
  const y=Number(m[1])+Number(years),mo=Number(m[2]),d=Number(m[3]);
  const last=new Date(Date.UTC(y,mo,0)).getUTCDate();
  return `${String(y).padStart(4,'0')}-${String(mo).padStart(2,'0')}-${String(Math.min(d,last)).padStart(2,'0')}`;
}
function easaEnglishExpiry(date,level){
  if(String(level)==='4')return addYearsIso(date,4);
  if(String(level)==='5')return addYearsIso(date,6);
  if(String(level)==='6')return'';
  return'';
}
function applyEnglishExpiryDefault(){
  if($('expiryCategory').value!=='english')return;
  const authority=upper($('expiryAuthority').value||'');
  if(authority&&authority!=='EASA')return;
  const date=$('expiryCourseDate').value,level=$('expiryEnglishLevel').value;
  if(!date||!level)return;
  $('expiryExpiry').value=easaEnglishExpiry(date,level);
  $('expiryExpiryHint').textContent=String(level)==='6'
    ?'EASA FCL.055: LEVEL 6 DOES NOT REQUIRE REASSESSMENT. EXPIRY REMAINS EDITABLE FOR OTHER AUTHORITIES.'
    :`EASA FCL.055 DEFAULT: LEVEL ${level} • ${level==='4'?'4':'6'} YEARS. EXPIRY REMAINS EDITABLE.`;
}
function configureExpiryEditor(category){
  const c=category||'license';
  $('expiryCategory').value=c;
  $('expiryEditorTitle').textContent=`${$('expiryEditId').value?'EDIT':'ADD'} ${expiryCategoryLabel(c)}`;
  $('expiryExpiry').required=true;
  $('expiryExpiryHint').textContent='';
  $('expiryNameLabel').textContent='LICENCE / TYPE';
  $('expiryNumberLabel').textContent='LICENCE / CERTIFICATE NUMBER';
  $('expiryLastCheckLabel').textContent='LAST CHECK';
  $('expiryCourseDateLabel').textContent='COURSE DATE';
  $('expiryIssuerLabel').textContent='ISSUED BY';

  if(c==='license'){
    setExpiryFields(['expiryName','expiryAuthority','expiryNumber','expiryLastCheck','expiryExpiry','expiryRemarks']);
    $('expiryName').placeholder='ATPL';
  }else if(c==='medical'){
    setExpiryFields(['expiryAuthority','expiryMedicalClass','expiryNumber','expiryLastCheck','expiryExpiry','expiryIssuer','expiryRemarks']);
    $('expiryNumberLabel').textContent='MEDICAL CERTIFICATE NUMBER';
  }else if(c==='instructor'){
    setExpiryFields(['expiryName','expiryAuthority','expiryNumber','expiryLastCheck','expiryExpiry','expiryRemarks']);
    $('expiryNameLabel').textContent='INSTRUCTOR RATING';
    $('expiryName').placeholder='TRI / SFI / CRMI / TRE';
  }else if(c==='course'){
    setExpiryFields(['expiryName','expiryAuthority','expiryCourseDate','expiryExpiry','expiryIssuer','expiryRemarks']);
    $('expiryNameLabel').textContent='COURSE';
    $('expiryName').placeholder='CRM / SMS / GRT / DGR';
    $('expiryIssuerLabel').textContent='PROVIDER / ISSUER';
  }else if(c==='aircraft'){
    setExpiryFields(['expiryAircraftType','expiryAuthority','expiryLastCheck','expiryEndorsedBy','expiryExpiry','expiryPhotoInput','expiryRemarks']);
    $('expiryLastCheckLabel').textContent='DATE ENDORSED';
  }else if(c==='english'){
    setExpiryFields(['expiryAuthority','expiryCourseDate','expiryEnglishLevel','expiryExpiry','expiryRemarks']);
    $('expiryCourseDateLabel').textContent='DATE PERFORMED';
    if(!$('expiryEditId').value&&!$('expiryAuthority').value)$('expiryAuthority').value='EASA';
    $('expiryExpiryHint').textContent='EASA FCL.055: LEVEL 4 = 4 YEARS • LEVEL 5 = 6 YEARS • LEVEL 6 = NO REASSESSMENT. EXPIRY IS EDITABLE.';
    if($('expiryEnglishLevel').value==='6')$('expiryExpiry').required=false;
  }
  updateExpiryPhotoControls();
}
function resetExpiryEditor(category='license'){
  $('expiryForm').reset();
  $('expiryEditId').value='';
  $('expiryCategory').value=category;
  expiryPendingPhotoData='';
  $('expiryPhotoInput').value='';
  configureExpiryEditor(category);
}
function updateExpiryPhotoControls(){
  const has=!!expiryPendingPhotoData;
  $('expiryFormPhotoViewBtn')?.classList.toggle('hidden',!has);
  $('expiryFormPhotoRemoveBtn')?.classList.toggle('hidden',!has);
}
function openExpiryEditor(category,item=null){
  resetExpiryEditor(category);
  if(item){
    $('expiryEditId').value=item.id;
    $('expiryCategory').value=item.category;
    $('expiryName').value=upper(item.name||'');
    $('expiryAuthority').value=upper(item.authority||'');
    $('expiryNumber').value=upper(item.number||'');
    $('expiryLastCheck').value=item.lastCheck||'';
    $('expiryCourseDate').value=item.courseDate||'';
    $('expiryMedicalClass').value=upper(item.medicalClass||'');
    $('expiryEnglishLevel').value=String(item.englishLevel||'');
    $('expiryAircraftType').value=upper(item.aircraftType||'');
    $('expiryEndorsedBy').value=upper(item.endorsedBy||'');
    $('expiryExpiry').value=item.expiry||'';
    $('expiryIssuer').value=upper(item.issuer||'');
    $('expiryRemarks').value=upper(item.remarks||'');
    expiryPendingPhotoData=item.photoData||'';
    configureExpiryEditor(item.category);
  }
  setExpiryEditorLocked(!!item?.locked);expiryBaseline=expiryContent();
  $('expiryEditorWrap').classList.remove('hidden');
  $('expiryEditorWrap').scrollIntoView({behavior:'smooth',block:'start'});
}
function setExpiryEditorLocked(locked){
  const form=$('expiryForm');
  form?.querySelectorAll('input:not(#expiryEditId):not(#expiryCategory),select').forEach(el=>el.disabled=!!locked);
  const id=$('expiryEditId')?.value,record=id?load(EXPIRY_KEY).find(x=>x.id===id):null;
  $('expiryToggleLockBtn').disabled=!record;
  $('expiryDeleteBtn').disabled=!record;
  $('expiryToggleLockBtn').textContent=locked?'UNLOCK':'LOCK';
}
let expiryBaseline='',tripBaseline='';
function formContent(id){
  return JSON.stringify([...$(id).elements].filter(el=>el.id&&el.type!=='button'&&el.type!=='submit'&&el.type!=='file').map(el=>[el.id,el.value]));
}
function expiryContent(){return formContent('expiryForm')+expiryPendingPhotoData}
function tripContent(){return formContent('tripForm')+JSON.stringify(tripFormOverrides)}
function saveExpiryFromForm(options={}){
  if(expiryBaseline===expiryContent())return;
  const silent=options.silent===true;
  const category=$('expiryCategory').value;
  const expiry=$('expiryExpiry').value;
  const level=$('expiryEnglishLevel').value;
  if(!silent&&category==='medical'&&!$('expiryMedicalClass').value)return alert('SELECT CLASS 1 OR CLASS 2.');
  if(!silent&&category==='aircraft'&&!$('expiryAircraftType').value.trim())return alert('ENTER THE AIRCRAFT ENDORSED.');
  if(!silent&&category==='english'&&!$('expiryCourseDate').value)return alert('ENTER THE DATE PERFORMED.');
  if(!silent&&category==='english'&&!level)return alert('SELECT THE ENGLISH LEVEL.');
  if(!silent&&!expiry&&!(category==='english'&&level==='6'))return alert('PLEASE ENTER AN EXPIRY DATE.');

  const rows=load(EXPIRY_KEY),id=$('expiryEditId').value||makeId(),existing=rows.find(x=>x.id===id);
  $('expiryEditId').value=id;
  if(existing?.locked)return alert('THIS EXPIRY ITEM IS LOCKED.');

  const item=stamp({
    id,
    category,
    name:category==='english'?'LPC ENGLISH':category==='aircraft'?'AIRCRAFT ENDORSED':upper($('expiryName').value.trim()),
    authority:upper($('expiryAuthority').value.trim()),
    number:upper($('expiryNumber').value.trim()),
    lastCheck:$('expiryLastCheck').value,
    courseDate:$('expiryCourseDate').value,
    medicalClass:upper($('expiryMedicalClass').value),
    englishLevel:level,
    aircraftType:upper($('expiryAircraftType').value.trim()),
    endorsedBy:upper($('expiryEndorsedBy').value.trim()),
    expiry,
    issuer:upper($('expiryIssuer').value.trim()),
    remarks:upper($('expiryRemarks').value.trim()),
    photoData:expiryPendingPhotoData||'',
    locked:!!existing?.locked,
    source:existing?.source||'manual',manualOverride:true
  });
  const i=rows.findIndex(x=>x.id===id);
  item._syncRev=Number(existing?._syncRev||0)+1;
  if(i>=0)rows[i]={...rows[i],...item};else rows.push(item);
  save(EXPIRY_KEY,rows);markCloudEdited('expiry',item,existing?'edit':'create');
  renderExpiry();
  scheduleAutoSync('expiry-save');
  setExpiryEditorLocked(false);expiryBaseline=expiryContent();
  return item;
}
async function expiryPhotoDataFromFile(file){
  if(!file||!String(file.type||'').startsWith('image/'))throw new Error('SELECT AN IMAGE FILE.');
  const bitmap=await createImageBitmap(file);
  try{
    const max=1400,scale=Math.min(1,max/Math.max(bitmap.width,bitmap.height));
    const canvas=document.createElement('canvas');
    canvas.width=Math.max(1,Math.round(bitmap.width*scale));
    canvas.height=Math.max(1,Math.round(bitmap.height*scale));
    const ctx=canvas.getContext('2d',{alpha:false});
    ctx.fillStyle='#fff';ctx.fillRect(0,0,canvas.width,canvas.height);
    ctx.drawImage(bitmap,0,0,canvas.width,canvas.height);
    let data=canvas.toDataURL('image/jpeg',.72);
    if(data.length>900000)data=canvas.toDataURL('image/jpeg',.55);
    if(data.length>1400000)throw new Error('IMAGE TOO LARGE. CHOOSE A SMALLER PHOTO.');
    return data;
  }finally{if(bitmap.close)bitmap.close()}
}
async function profilePhotoDataFromFile(file){
  if(!file||!String(file.type||'').startsWith('image/'))throw new Error('SELECT AN IMAGE FILE.');
  const bitmap=await createImageBitmap(file);
  try{
    const max=512,scale=Math.min(1,max/Math.max(bitmap.width,bitmap.height)),size=Math.max(1,Math.round(Math.min(bitmap.width,bitmap.height)*scale));
    const canvas=document.createElement('canvas');canvas.width=size;canvas.height=size;
    const sourceSize=Math.min(bitmap.width,bitmap.height),sx=(bitmap.width-sourceSize)/2,sy=(bitmap.height-sourceSize)/2,ctx=canvas.getContext('2d',{alpha:false});
    ctx.fillStyle='#fff';ctx.fillRect(0,0,size,size);ctx.drawImage(bitmap,sx,sy,sourceSize,sourceSize,0,0,size,size);
    const data=canvas.toDataURL('image/jpeg',.78);if(data.length>500000)throw new Error('IMAGE TOO LARGE. CHOOSE A SMALLER PHOTO.');return data;
  }finally{if(bitmap.close)bitmap.close()}
}
function updateProfilePhotoSettings(){
  const data=appSettings().profilePhotoData||'',preview=$('profilePhotoPreview');
  if(preview){preview.classList.toggle('hidden',!data);if(data)preview.src=data;else preview.removeAttribute('src')}
  $('profilePhotoFallback')?.classList.toggle('hidden',!!data);$('removeProfilePhoto')?.classList.toggle('hidden',!data);
}
function showExpiryPhoto(data){
  if(!data)return;
  $('expiryPhotoViewerImg').src=data;
  $('expiryPhotoViewer').classList.remove('hidden');
  $('expiryPhotoViewer').setAttribute('aria-hidden','false');
}
function hideExpiryPhoto(){
  $('expiryPhotoViewer').classList.add('hidden');
  $('expiryPhotoViewer').setAttribute('aria-hidden','true');
  $('expiryPhotoViewerImg').removeAttribute('src');
}
function hideExpiryAddMenu(){
  $('expiryAddMenu').classList.add('hidden');
  $('expiryAddMenu').setAttribute('aria-hidden','true');
}
function showExpiryAddMenu(){
  $('expiryAddMenu').classList.remove('hidden');
  $('expiryAddMenu').setAttribute('aria-hidden','false');
}
function bindExpiryEvents(){
  $('expiryAddBtn').addEventListener('click',showExpiryAddMenu);
  $('expiryAddCancel').addEventListener('click',hideExpiryAddMenu);
  $('expiryAddMenu').addEventListener('click',e=>{
    if(e.target===$('expiryAddMenu'))hideExpiryAddMenu();
    const b=e.target.closest('[data-add-expiry]');
    if(!b)return;
    hideExpiryAddMenu();
    openExpiryEditor(b.dataset.addExpiry);
  });

  let expiryAutosaveTimer=null;
  const queueExpiryAutosave=()=>{clearTimeout(expiryAutosaveTimer);expiryAutosaveTimer=setTimeout(()=>saveExpiryFromForm({silent:true}),180)};
  $('expiryForm').addEventListener('submit',e=>{e.preventDefault();saveExpiryFromForm({silent:true})});
  $('expiryForm').addEventListener('input',queueExpiryAutosave);
  $('expiryForm').addEventListener('change',queueExpiryAutosave);
  $('expiryEnglishLevel').addEventListener('change',()=>{
    $('expiryExpiry').required=$('expiryEnglishLevel').value!=='6';
    applyEnglishExpiryDefault();
  });
  $('expiryCourseDate').addEventListener('change',applyEnglishExpiryDefault);
  $('expiryAuthority').addEventListener('change',applyEnglishExpiryDefault);

  $('expiryPhotoInput').addEventListener('change',async e=>{
    const file=e.target.files?.[0];if(!file)return;
    try{
      expiryPendingPhotoData=await expiryPhotoDataFromFile(file);
      updateExpiryPhotoControls();
      queueExpiryAutosave();
    }catch(err){alert(err.message||'PHOTO COULD NOT BE ADDED.')}
    finally{e.target.value=''}
  });
  $('expiryFormPhotoViewBtn').addEventListener('click',()=>showExpiryPhoto(expiryPendingPhotoData));
  $('expiryFormPhotoRemoveBtn').addEventListener('click',()=>{
    if(!confirm('REMOVE THE ATTACHED PHOTO?'))return;
    expiryPendingPhotoData='';
    updateExpiryPhotoControls();
    queueExpiryAutosave();
  });
  $('expiryPhotoCloseBtn').addEventListener('click',hideExpiryPhoto);
  $('expiryPhotoViewer').addEventListener('click',e=>{if(e.target===$('expiryPhotoViewer'))hideExpiryPhoto()});

  document.addEventListener('click',e=>{
    const photo=e.target.closest('[data-view-expiry-photo]');
    if(photo){
      const x=load(EXPIRY_KEY).find(v=>v.id===photo.dataset.viewExpiryPhoto);
      if(x?.photoData)showExpiryPhoto(x.photoData);
      return;
    }
    const lock=e.target.closest('#expiryToggleLockBtn');
    if(lock){
      const rows=load(EXPIRY_KEY),x=rows.find(v=>v.id===$('expiryEditId').value);
      if(!x)return;
      const next=!x.locked;
      x.locked=next;x._updatedAt=new Date().toISOString();x._syncRev=Number(x._syncRev||0)+1;
      save(EXPIRY_KEY,rows);markCloudEdited('expiry',x,next?'lock':'unlock');renderExpiry();scheduleAutoSync(next?'expiry-lock':'expiry-unlock');setExpiryEditorLocked(next);
      return;
    }
    const edit=e.target.closest('[data-edit-expiry]');
    if(edit){
      const x=load(EXPIRY_KEY).find(v=>v.id===edit.dataset.editExpiry);
      if(x)openExpiryEditor(x.category,x);
      return;
    }
    const del=e.target.closest('#expiryDeleteBtn');
    if(del){
      const x=load(EXPIRY_KEY).find(v=>v.id===$('expiryEditId').value);
      if(!x)return;
      if(!confirm('DELETE THIS EXPIRY RECORD?'))return;
      markCloudDeleted('expiry',x,{manual:true});
      save(EXPIRY_KEY,load(EXPIRY_KEY).filter(v=>v.id!==x.id));
      renderExpiry();
      $('expiryEditorWrap').classList.add('hidden');resetExpiryEditor();
      scheduleAutoSync('expiry-delete');
    }
  });
}
/* Cloud — v8.2 local-first verified generations */
const SUPABASE_URL='https://ytlfygmojojipdjeppic.supabase.co';
const SUPABASE_PUBLISHABLE_KEY='sb_publishable_a3P-hh1BBqsQ0zRiY1uquA_YgiFcIg0';
const SUPABASE_TABLE='pilotlog_entries';
const CLOUD_SESSION_KEY='pilotlog7_cloud_session_v1';

const SYNC_SINGLETONS=[[PAY_SETTINGS_KEY,'paySettings'],[PAY_MONTH_KEY,'payMonth'],[FX_KEY,'fx'],[APP_SETTINGS_KEY,'appSettings']];
function cloudUserPrefix(userId){return `${SYNC_PROTOCOL}:u:${stableHash128(String(userId||'' )).slice(0,16)}`}
function cloudActiveMetaId(userId){return `${cloudUserPrefix(userId)}:meta:active`}
function cloudGenerationPrefix(userId,generation){return `${cloudUserPrefix(userId)}:g:${generation}:`}
const SNAPSHOT_CHUNK_ITEMS=40;
const AUTO_SYNC_KEY='pilotlog7_auto_sync_v1';
const WEEKLY_BACKUP_INTERVAL_MS=7*24*60*60*1000;
const BACKUP_DB_NAME='pilotlog7-weekly-backup';
const BACKUP_STORE='snapshots';
let autoSyncTimer=null,periodicSyncTimer=null,retrySyncTimer=null,cloudSyncInProgress=false,syncDatabaseReady=false;
const SYNC_LOCAL_REV='pilotlog7_sync_local_revision_16',SYNC_BLOCK='pilotlog7_sync_block_16',SYNC_LOG='pilotlog7_sync_log_16';
const SYNC_OWNER='pilotlog7_sync_owner_16',SYNC_RECEIPT='pilotlog7_sync_activation_16';
function bootDataSignature(){
  const db=operationalDatabase;
  const values=Object.fromEntries(Object.entries(db.values).filter(([,v])=>v!=null&&(typeof v!=='object'||Object.keys(v).length)));
  return sectionDigest([db.records,values]);
}
function localRevision(){return Number(localStorage.getItem(SYNC_LOCAL_REV)||0)}
function markLocalRevision(){
  if(bootKnown&&!operationalDatabase.replica162&&!operationalDatabase.preSync162)operationalDatabase.preSync162={known:bootKnown,legacyDirty};
  localStorage.setItem(SYNC_LOCAL_REV,String(localRevision()+1));
  syncDirty();
}
function clearSyncTimers(){
  clearTimeout(autoSyncTimer);clearTimeout(periodicSyncTimer);clearTimeout(retrySyncTimer);
  autoSyncTimer=periodicSyncTimer=retrySyncTimer=null;
}
function armPeriodicSync(){
  clearTimeout(periodicSyncTimer);
  if(!autoSyncEnabled()||!syncDatabaseReady||localStorage.getItem(SYNC_BLOCK)||retrySyncTimer)return;
  periodicSyncTimer=setTimeout(()=>{periodicSyncTimer=null;runAutomaticSync('periodic')},600000);
}
function runAutomaticSync(reason){
  if(!autoSyncEnabled()||!cloudStoredSession()||localStorage.getItem(SYNC_BLOCK)){armPeriodicSync();return}
  syncSupabase({silent:true,reason});
}
function scheduleAutoSync(reason,eventTime){
  // Local changes already go through touchOperationalDatabase. Only these events arm an early sync.
  if(!['startup','sign-in'].includes(reason)||cloudSyncInProgress||!autoSyncEnabled()||!syncDatabaseReady||localStorage.getItem(SYNC_BLOCK)||retrySyncTimer)return;
  clearTimeout(autoSyncTimer);clearTimeout(periodicSyncTimer);
  autoSyncTimer=setTimeout(()=>{autoSyncTimer=null;runAutomaticSync(reason)},eventTime==null?5000:Math.max(0,eventTime+5000-Date.now()));
}


// Fresh installs/reset devices default to manual Sync now. Auto Sync can still be enabled explicitly.
function autoSyncEnabled(){return localStorage.getItem(AUTO_SYNC_KEY)==='yes'}
function setAutoSyncEnabled(v){localStorage.setItem(AUTO_SYNC_KEY,v?'yes':'no');if(!v)clearSyncTimers()}
function markLocalOnlyImport(){syncDirty()}
function backupDue(createdAt){
  if(!createdAt)return true;
  const t=Date.parse(createdAt);
  return !Number.isFinite(t)||Date.now()-t>=WEEKLY_BACKUP_INTERVAL_MS;
}
function openWeeklyBackupDb(){
  return new Promise((resolve,reject)=>{
    const req=indexedDB.open(BACKUP_DB_NAME,1);
    req.onupgradeneeded=()=>{const db=req.result;if(!db.objectStoreNames.contains(BACKUP_STORE))db.createObjectStore(BACKUP_STORE)};
    req.onsuccess=()=>resolve(req.result);
    req.onerror=()=>reject(req.error||new Error('Backup database unavailable'));
  });
}
async function weeklyBackup(){
  const db=await openWeeklyBackupDb();
  try{return await new Promise((resolve,reject)=>{const tx=db.transaction(BACKUP_STORE,'readonly'),req=tx.objectStore(BACKUP_STORE).get('weekly');req.onsuccess=()=>resolve(req.result||null);req.onerror=()=>reject(req.error)})}
  finally{db.close()}
}
async function storeWeeklyBackup(snapshot){
  const db=await openWeeklyBackupDb();
  try{await new Promise((resolve,reject)=>{const tx=db.transaction(BACKUP_STORE,'readwrite');tx.objectStore(BACKUP_STORE).put(snapshot,'weekly');tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error||new Error('Backup write aborted'))})}
  finally{db.close()}
}
function backupPayload(){
  return{version:VERSION,createdAt:new Date().toISOString(),data:{database:normalizeOperationalDatabase(operationalDatabase)}};
}
async function createWeeklyBackup(force=false){const current=await weeklyBackup();if(!force&&!backupDue(current?.createdAt))return current;const snapshot=backupPayload();await storeWeeklyBackup(snapshot);await renderWeeklyBackupStatus();return snapshot}
async function renderWeeklyBackupStatus(){const el=$('weeklyBackupStatus');if(!el)return;try{const b=await weeklyBackup();el.value=b?.createdAt?`${displayDateTime(b.createdAt)} • ${b.version||'unknown version'}`:'No backup yet'}catch(e){el.value='Backup unavailable';console.warn('Backup status unavailable',e)}}
async function restoreWeeklyBackup(){
  const b=await weeklyBackup();if(!b?.data)return alert('No weekly backup available.');
  if(!confirm(`Restore weekly backup from ${displayDateTime(b.createdAt)}? Current local PilotLog data will be replaced.`))return false;
  await replaceOperationalDatabase(migrateSnapshotToOperationalDatabase(b.data));reconcileAllDuties();syncDirty();localStorage.setItem(CLOUD_PENDING_IMPORT_KEY,'Weekly backup restored locally');
  await render();renderTrips();renderPayroll();renderSettings();alert('Weekly backup restored locally. Review it before pressing Sync now.');return true;
}
async function ensureWeeklyBackupBeforeSync(){const current=await weeklyBackup();if(!current||backupDue(current.createdAt))await createWeeklyBackup(true)}
function cloudStoredSession(){return loadObject(CLOUD_SESSION_KEY,null)}
function saveCloudSession(s){if(!s){localStorage.removeItem(CLOUD_SESSION_KEY);return}const expiresAt=s.expires_at||Math.floor(Date.now()/1000)+(Number(s.expires_in)||3600);localStorage.setItem(CLOUD_SESSION_KEY,JSON.stringify({...s,expires_at:expiresAt}))}
function waitMs(ms){return new Promise(resolve=>setTimeout(resolve,ms))}
async function cloudFetch(path,{method='GET',body=null,token='',headers={},timeoutMs=30000,retries=1}={}){
  const attempts=Math.max(1,Number(retries||0)+1);let lastError=null;
  for(let attempt=1;attempt<=attempts;attempt++){
    const ctrl=new AbortController(),timer=setTimeout(()=>ctrl.abort(),Math.max(5000,Number(timeoutMs)||30000));
    try{
      const res=await fetch(`${SUPABASE_URL}${path}`,{method,signal:ctrl.signal,headers:{apikey:SUPABASE_PUBLISHABLE_KEY,Authorization:`Bearer ${token||SUPABASE_PUBLISHABLE_KEY}`,...(body?{'Content-Type':'application/json'}:{}),...headers},body:body?JSON.stringify(body):undefined});
      const text=await res.text();let data=null;if(text){try{data=JSON.parse(text)}catch{data=text}}
      if(!res.ok){const msg=(data&&typeof data==='object'&&(data.msg||data.message||data.error_description||data.error))||`HTTP ${res.status}`;const err=new Error(msg);err.status=res.status;err.code=data?.code;if((res.status===429||res.status>=500)&&attempt<attempts){lastError=err;await waitMs(500*attempt);continue}throw err}
      return data;
    }catch(e){let err=e;if(e?.name==='AbortError')err=new Error(`Cloud request timed out after ${Math.round((Number(timeoutMs)||30000)/1000)} seconds.`);else if(e instanceof TypeError)err=new Error('Cloud network request failed.');lastError=err;const retryable=e?.name==='AbortError'||e instanceof TypeError||Number(e?.status)===429||Number(e?.status)>=500;if(retryable&&attempt<attempts){await waitMs(500*attempt);continue}throw err}
    finally{clearTimeout(timer)}
  }
  throw lastError||new Error('Cloud request failed.');
}
async function refreshCloudSession(session){if(!session?.refresh_token)return null;const data=await cloudFetch('/auth/v1/token?grant_type=refresh_token',{method:'POST',body:{refresh_token:session.refresh_token},retries:0});if(cloudStoredSession()?.refresh_token!==session.refresh_token)return cloudStoredSession();saveCloudSession(data);return data}
window.PilotLogPayslipAuth={session:()=>cloudSession(),url:()=>SUPABASE_URL,key:()=>SUPABASE_PUBLISHABLE_KEY};
window.PilotLogPayrollCompare={
  forMonth(month){
    const saved=monthExtras(month).validated;
    const regime=saved?.regime||payrollRegime();
    const base=saved?.projection||PilotLogEngine.payroll(month);
    return {regime,projection:saved?.projection||(regime==='MAC'?calculateMacPayrollProjection(base):base)};
  }
};
let refreshInProgress;
async function cloudSession(){
  const session=cloudStoredSession();
  if(!session)return null;
  if(session.access_token&&Number(session.expires_at)>Date.now()/1000+60)return session;
  const token=session.refresh_token;
  if(refreshInProgress?.token===token)return refreshInProgress.promise;
  const promise=refreshCloudSession(session).catch(error=>{
    if(cloudStoredSession()?.refresh_token!==token)return cloudStoredSession();
    if([400,401].includes(error.status)&&/refresh.token|invalid.grant|session.not.found/i.test(error.message)){saveCloudSession(null);return null}
    throw error;
  }).finally(()=>{if(refreshInProgress?.promise===promise)refreshInProgress=null});
  refreshInProgress={token,promise};return promise;
}

async function fetchRowsByPrefix(token,prefix,select='id,data,updated_at'){
  const pageSize=500,all=[];
  for(let offset=0;;offset+=pageSize){
    const page=await cloudFetch(`/rest/v1/${SUPABASE_TABLE}?select=${encodeURIComponent(select)}&id=like.${encodeURIComponent(prefix+'*')}&order=id.asc&limit=${pageSize}&offset=${offset}`,{token,headers:{Accept:'application/json'},timeoutMs:60000,retries:0});
    const rows=Array.isArray(page)?page:[];all.push(...rows);if(rows.length<pageSize)break;if(offset>100000)throw new Error('Cloud dataset is unexpectedly large; sync stopped safely.');
  }
  return all;
}
async function syncStep(label,operation){
  try{return await operation()}catch(error){
    error.message=label+' · '+error.message;
    throw error;
  }
}
async function fetchActiveMeta(token,userId){
  const rows=await cloudFetch(`/rest/v1/${SUPABASE_TABLE}?select=id,data,updated_at&id=eq.${encodeURIComponent(cloudActiveMetaId(userId))}&limit=1`,{token,headers:{Accept:'application/json'},timeoutMs:30000,retries:0});
  const row=Array.isArray(rows)?rows[0]:null,d=row?.data||null;if(!row)return null;if(!d||d.protocol!==SYNC_PROTOCOL||d.kind!=='active-meta')throw Error('Incompatible cloud pointer');validateSyncMeta(d.payload);return d.payload;
}
function snapshotSections(){const db=operationalDatabase;return{databaseRecords:db.records.filter(r=>r.activityStatus!=='DRAFT'),databaseValues:Object.entries(db.values).filter(([k])=>![ENTRY_DRAFT_KEY,ENTRY_DRAFTS_KEY].includes(k)).map(([key,value])=>({key,value})),databaseMeta:{schema:db.schema,schemaVersion:db.schemaVersion,updatedAt:db.updatedAt}}}
function localHasMeaningfulData(){return recordMap(operationalDatabase).size>0}
const SNAPSHOT_DIGEST_FORMAT='canonical-json-v1';
function canonicalJsonValue(v){
  if(Array.isArray(v))return v.map(canonicalJsonValue);
  if(v&&typeof v==='object'){const out=Object.create(null);for(const k of Object.keys(v).sort())out[k]=canonicalJsonValue(v[k]);return out}
  return v;
}
function canonicalJsonStringify(v){return JSON.stringify(canonicalJsonValue(v??null))}
function sectionDigest(v){return stableHash128(canonicalJsonStringify(v))}
function legacySectionDigest(v){return stableHash128(JSON.stringify(v??null))}
function buildSnapshotGeneration(revision,userId){
  const generation=`r${revision}-${Date.now().toString(36)}-${Array.from(crypto.getRandomValues(new Uint32Array(3)),n=>n.toString(36)).join('-')}`,sections=freezeSnapshot(JSON.parse(JSON.stringify(snapshotSections()))),rows=[],counts={},digests={},chunkCounts={};
  Object.entries(sections).forEach(([section,value])=>{
    const items=Array.isArray(value)?value:[value];counts[section]=Array.isArray(value)?value.length:1;digests[section]=sectionDigest(value);
    const total=Math.max(1,Math.ceil(items.length/SNAPSHOT_CHUNK_ITEMS));chunkCounts[section]=total;
    for(let i=0;i<total;i++){
      const chunk=items.slice(i*SNAPSHOT_CHUNK_ITEMS,(i+1)*SNAPSHOT_CHUNK_ITEMS);
      rows.push({id:`${cloudGenerationPrefix(userId,generation)}${section}:${String(i).padStart(4,'0')}`,user_id:userId,updated_at:new Date().toISOString(),data:{protocol:SYNC_PROTOCOL,kind:'snapshot-chunk',generation,section,index:i,totalChunks:total,isArray:Array.isArray(value),items:chunk,checks:chunk.map(syncFieldChecks)}});
    }
  });
  const meta={revision,generation,version:VERSION,syncVersion:16,previousGeneration:null,digestFormat:SNAPSHOT_DIGEST_FORMAT,createdAt:new Date().toISOString(),counts,digests,chunkCounts,rowCount:rows.length};
  return{generation,rows,meta};
}
function uploadBatches(rows,maxBytes=350000,maxRows=3){
  const out=[];let batch=[],bytes=2;
  for(const row of rows){const n=JSON.stringify(row).length+1;if(batch.length&&(batch.length>=maxRows||bytes+n>maxBytes)){out.push(batch);batch=[];bytes=2}batch.push(row);bytes+=n}
  if(batch.length)out.push(batch);return out;
}
async function pushCloudRows(rows,token,silent=false){
  const batches=uploadBatches(rows),total=batches.length;
  for(let i=0;i<total;i++){
    if($('cloudStatus'))$('cloudStatus').textContent=`${silent?'Auto syncing':'Publishing verified snapshot'}… upload ${i+1}/${total}`;
    await syncStep('Upload '+(i+1)+'/'+total+' '+batches[i][0].data.section,()=>cloudFetch(`/rest/v1/${SUPABASE_TABLE}`,{method:'POST',token,body:batches[i],timeoutMs:90000,retries:0,headers:{Prefer:'return=minimal'}}));
  }
}
function freezeSnapshot(value){
  if(value&&typeof value==='object'){Object.values(value).forEach(freezeSnapshot);Object.freeze(value)}
  return value;
}
function validateSyncMeta(meta){
  if(!meta||typeof meta.version!=='string'||!/^\d+(?:\.\d+)*$/.test(meta.version)||!Number.isSafeInteger(meta.revision)||meta.revision<1||!meta.generation||
    (meta.syncVersion!=null&&meta.syncVersion!==16)||Number(String(meta.version).split('.')[0])>16||
    (meta.digestFormat&&meta.digestFormat!==SNAPSHOT_DIGEST_FORMAT))throw Error('Incompatible central database revision or format');
  const sections=['databaseRecords','databaseValues','databaseMeta'];
  if(Object.keys(meta.counts||{}).sort().join()!==sections.sort().join())throw Error('Invalid central database section manifest');
  for(const key of sections){
    if(!Number.isSafeInteger(meta.counts[key])||meta.counts[key]<0||!meta.digests?.[key]||
       !Number.isSafeInteger(meta.chunkCounts?.[key])||meta.chunkCounts[key]<1)throw Error('Invalid manifest: '+key);
  }
  if(meta.counts.databaseMeta!==1||Object.values(meta.chunkCounts).reduce((a,b)=>a+b,0)!==meta.rowCount)throw Error('Invalid manifest: rowCount');
}
function syncFieldChecks(item){
  const fields=Object.create(null);
  function visit(value,path){
    if(value&&typeof value==='object'){
      fields[path]=Array.isArray(value)?'a'+value.length:'o';
      for(const key of Object.keys(value))visit(value[key],path+'/'+key.replace(/~/g,'~0').replace(/\//g,'~1'));
    }else fields[path]=sectionDigest(value);
  }
  visit(item,'');return {id:item?.id||item?.key||'',fields};
}
function diagnoseChunkFields(section,chunks){
  for(const chunk of chunks){
    for(const [index,item] of chunk.items.entries()){
      const reference=chunk.checks?.[index],actual=syncFieldChecks(item);
      if(!reference)throw Error(section+' chunk '+chunk.index+' record '+index+': missing verification fields');
      for(const path of new Set([...Object.keys(reference.fields),...Object.keys(actual.fields)])){
        if(reference.fields[path]!==actual.fields[path])throw Error('Integrity: '+section+' ['+(reference.id||index)+'] '+(path||'/'));
      }
    }
  }
}
function checkAccount(session){
  if(cloudStoredSession()?.user?.id!==session.user.id)throw Error('Account changed during sync');
}
async function publishLocalSnapshot(session,remoteMeta,silent){
  const revision=(remoteMeta?.revision||0)+1,local=localRevision(),built=buildSnapshotGeneration(revision,session.user.id);
  built.meta.previousGeneration=remoteMeta?.generation||null;
  await pushCloudRows(built.rows,session.access_token,silent);
  await fetchCentralSnapshot(session,built.meta);
  checkAccount(session);
  const receipt={user:session.user.id,generation:built.generation,revision,local,meta:built.meta,verifiedAt:Date.now()};
  localStorage.setItem(SYNC_RECEIPT,JSON.stringify(receipt));
  await activateVerifiedGeneration(session,receipt);
}
async function activateVerifiedGeneration(session,receipt){
  await syncStep('Activate revision '+receipt.revision,()=>cloudFetch('/rest/v1/rpc/pilotlog_activate_v16',{
    method:'POST',token:session.access_token,retries:0,
    body:{pointer_id:cloudActiveMetaId(session.user.id),expected_generation:receipt.meta.previousGeneration,new_meta:receipt.meta}}));
  checkAccount(session);acceptActivation(receipt);
}
function acceptActivation(receipt){
  localStorage.setItem(CLOUD_BASE_REV_KEY,String(receipt.revision));
  localStorage.setItem(SYNC_OWNER,receipt.user);
  if(localRevision()===receipt.local){
    localStorage.setItem(CLOUD_DIRTY_KEY,'0');localStorage.removeItem(CLOUD_PENDING_IMPORT_KEY);
  }
  localStorage.setItem(SYNC_READY_KEY,'1');localStorage.removeItem(SYNC_RECEIPT);
}
function reconstructSnapshot(meta,rows){
  validateSyncMeta(meta);
  if(rows.length!==meta.rowCount)throw Error('Generation '+meta.generation+': chunk count mismatch');
  const groups=Object.create(null);
  for(const row of rows){const d=row?.data||{};if(d.protocol!==SYNC_PROTOCOL||d.kind!=='snapshot-chunk'||d.generation!==meta.generation)continue;(groups[d.section]||(groups[d.section]=[])).push(d)}
  const out={},canonicalFormat=meta?.digestFormat===SNAPSHOT_DIGEST_FORMAT,legacyFormat=!meta?.digestFormat;
  for(const section of Object.keys(meta.counts||{})){
    const chunks=(groups[section]||[]).sort((a,b)=>Number(a.index)-Number(b.index)),expectedChunks=Number(meta.chunkCounts?.[section]||0);
    if(chunks.length!==expectedChunks)throw new Error(`Central database is incomplete: ${section} has ${chunks.length}/${expectedChunks} chunks. Local data was not replaced.`);
    const seen=new Set();
    for(let i=0;i<chunks.length;i++){
      const c=chunks[i],idx=Number(c.index);
      if(!Number.isInteger(idx)||idx!==i||seen.has(idx)||Number(c.totalChunks)!==expectedChunks||c.section!==section||c.generation!==meta.generation||c.protocol!==SYNC_PROTOCOL||c.kind!=='snapshot-chunk')
        throw new Error(`Central database chunk structure mismatch in ${section}. Local data was not replaced.`);
      seen.add(idx);
    }
    for(const chunk of chunks){
      if(!Array.isArray(chunk.items))throw Error('Integrity: '+section+' chunk '+chunk.index+' /items');
      if(meta.syncVersion===16&&chunk.items.length!==chunk.checks?.length){
        const missing=chunk.checks?.find(check=>check.id&&!chunk.items.some(item=>(item?.id||item?.key)===check.id));
        throw Error('Integrity: '+section+' chunk '+chunk.index+' ['+(missing?.id||'manifest')+'] /items');
      }
    }
    const items=chunks.flatMap(c=>Array.isArray(c.items)?c.items:[]),isArray=chunks[0]?.isArray!==false,value=isArray?items:(items[0]||{});
    const count=isArray?items.length:1;if(count!==Number(meta.counts?.[section]||0))throw new Error(`Central database count mismatch in ${section}. Local data was not replaced.`);
    const expected=meta.digests?.[section];
    if(meta.syncVersion===16&&sectionDigest(value)!==expected)diagnoseChunkFields(section,chunks);
    if(canonicalFormat){
      if(sectionDigest(value)!==expected)throw new Error(`Central database integrity check failed in ${section}. Local data was not replaced.`);
    }else if(legacyFormat){
      // db8 snapshots created before v8.6 hashed ordinary JSON.stringify output.
      // PostgreSQL jsonb is allowed to reorder object keys, so that old digest can change
      // even when the stored values are identical. Accept the legacy generation only
      // after the strict generation/chunk/count checks above. New publications always
      // use canonical-json-v1 and return to full content-digest verification.
      if(legacySectionDigest(value)!==expected)throw Error('Unverified legacy hash: '+section);
    }else{
      throw new Error(`Central database uses an unsupported integrity format in ${section}. Local data was not replaced.`);
    }
    out[section]=value;
  }
  if(out.databaseMeta?.schema!=='PilotLog Operational Database'||out.databaseMeta.schemaVersion!==1)throw Error('Incompatible databaseMeta.schemaVersion');
  if(!Array.isArray(out.databaseRecords)||!Array.isArray(out.databaseValues))throw Error('Invalid records or values section');
  const seen=new Set();
  for(const row of out.databaseRecords){
    if(!row?.id||seen.has(row.id))throw Error('databaseRecords ['+(row?.id||'missing')+'] /id');
    seen.add(row.id);
  }
  const keys=new Set();
  for(const entry of out.databaseValues){
    if(typeof entry?.key!=='string'||keys.has(entry.key))throw Error('databaseValues ['+(entry?.key||'missing')+'] /key');
    keys.add(entry.key);
  }
  return out;
}
async function fetchCentralSnapshot(session,meta){
  if(!meta?.generation)throw new Error('Central database metadata has no active generation.');
  validateSyncMeta(meta);
  const rows=[],prefix=cloudGenerationPrefix(session.user.id,meta.generation);
  for(const [section,total] of Object.entries(meta.chunkCounts)){
    for(let first=0;first<total;first+=3){
      const end=Math.min(first+3,total),ids=[];
      for(let i=first;i<end;i++)ids.push(prefix+section+':'+String(i).padStart(4,'0'));
      const filter=encodeURIComponent('in.('+ids.join(',')+')');
      const page=await syncStep('Read '+section+' '+(first+1)+'–'+end+'/'+total,()=>cloudFetch(
        `/rest/v1/${SUPABASE_TABLE}?select=id,data&id=${filter}&user_id=eq.${encodeURIComponent(session.user.id)}`,
        {token:session.access_token,headers:{Accept:'application/json'},timeoutMs:60000,retries:0}));
      if(!Array.isArray(page))throw Error('Invalid cloud response: '+section);
      const received=new Set(page.map(row=>row.id));
      for(const id of ids)if(!received.has(id))throw Error('Missing snapshot chunk '+id);
      rows.push(...page);
    }
  }
  return reconstructSnapshot(meta,rows);
}
function loadSyncLedger(){const rows=load(SYNC_LEDGER_KEY);return Array.isArray(rows)?rows:[]}
function saveSyncLedger(rows){save(SYNC_LEDGER_KEY,Array.isArray(rows)?rows:[])}
function ledgerTarget(kind,item,includeSource=true){
  const obj=item&&typeof item==='object'?item:null,recordId=obj?.id||String(item||''),sourceKey=includeSource&&obj?syncSourceKey(obj):'';return{collection:String(kind||''),recordId,sourceKey};
}
function appendSyncLedger(action,kind,item,options={}){
  if(!['delete','delete-collection'].includes(action)&&(!item||item==='*'||!effectiveCloudDeletion(kind,item)))return null;
  const target=ledgerTarget(kind,item,options.includeSource!==false),event={eventId:makeId(),action:['delete','delete-collection'].includes(action)?action:'restore162',...target,at:new Date().toISOString(),deviceId:syncDeviceId(),recordRev:item&&typeof item==='object'?recordRevision(item):0};
  if(options.note)event.note=String(options.note);if(options.count!=null)event.count=Number(options.count)||0;
  const rows=loadSyncLedger();rows.push(event);saveSyncLedger(rows);return event;
}
function markCloudEdited(kind,item,action='edit',options={}){return appendSyncLedger(action,kind,item,options)}
function markCloudCollectionEdited(kind,action='edit-collection',options={}){return appendSyncLedger(action,kind,'*',{...options,includeSource:false})}
function markCloudDeleted(kind,item,options={}){if(kind==='activities'&&item?.activityStatus==='DRAFT')return null;if(options.manual!==true)throw new Error('PilotLog safety block: deletion requires an explicit manual user action.');return appendSyncLedger('delete',kind,item,options)}
function markCloudCollectionDeleted(kind,options={}){return appendSyncLedger('delete-collection',kind,'*',{...options,includeSource:false})}
function loadCloudTombstones(){return loadSyncLedger().filter(x=>x?.action==='delete'||x?.action==='delete-collection')}
function saveCloudTombstones(rows){
  const existing=loadSyncLedger(),known=new Set(existing.map(x=>x.eventId));for(const row of (rows||[])){const e=row?.eventId?row:{eventId:makeId(),action:row?.action||'delete',collection:row?.collection||row?.kind||'',recordId:row?.recordId||row?.id||'',sourceKey:row?.sourceKey||'',at:row?.at||new Date().toISOString(),deviceId:row?.deviceId||syncDeviceId(),recordRev:Number(row?.recordRev)||0};if(!known.has(e.eventId)){existing.push(e);known.add(e.eventId)}}saveSyncLedger(existing);
}
function ledgerEventTime(e){return String(e?.at||'')}
function ledgerMatches(e,kind,item){
  if(!e||e.collection!==String(kind||''))return false;if(e.recordId==='*')return true;
  const target=ledgerTarget(kind,item,true);return (!!target.recordId&&e.recordId===target.recordId)||!!(target.sourceKey&&e.sourceKey&&e.sourceKey===target.sourceKey);
}
function effectiveCloudDeletion(kind,item,ledger=loadSyncLedger()){
  const events=(ledger||[]).filter(e=>ledgerMatches(e,kind,item)).sort((a,b)=>ledgerEventTime(a).localeCompare(ledgerEventTime(b)));if(!events.length)return null;
  let deleted=null,specific=false;
  for(const e of events){
    if(e.action==='delete'){deleted=e;specific=true;continue}
    if(e.action==='delete-collection'){deleted=e;specific=false;continue}
    // A delete is cleared only by a later action for this specific record/source.
    // Collection-wide maintenance events (for example Lock All) must never resurrect deleted rows.
    if(deleted&&e.recordId!=='*'&&ledgerEventTime(e)>ledgerEventTime(deleted)){deleted=null;specific=false}
  }
  if(deleted&&!specific&&item&&typeof item==='object'&&String(item._updatedAt||'')>ledgerEventTime(deleted))return null;return deleted;
}
function activityDeletionFromLedger(item,ledger=loadSyncLedger()){
  return effectiveCloudDeletion('activities',item,ledger)||
    (isEntryActivityRecord(item)&&effectiveCloudDeletion('flights',item,ledger))||
    (isFlight(item)&&effectiveCloudDeletion('roster',item,ledger))||
    (isDutyActivityRecord(item)&&effectiveCloudDeletion('duties',item,ledger));
}
function importSuppressedByDeletion(kind,item){return kind==='activities'?!!activityDeletionFromLedger(item):!!effectiveCloudDeletion(kind,item)}
function deletedByCloudState(kind,item){return kind==='activities'?!!activityDeletionFromLedger(item):!!effectiveCloudDeletion(kind,item)}
function mergeSyncLedgers(a,b){const out=[],seen=new Set();for(const e of [...(a||[]),...(b||[])]){if(!e||!e.eventId||seen.has(e.eventId))continue;seen.add(e.eventId);out.push(e)}return out.sort((x,y)=>ledgerEventTime(x).localeCompare(ledgerEventTime(y)))}
function syncRecordKey(kind,row){const sk=syncSourceKey(row);return sk?`${kind}|source|${sk}`:`${kind}|id|${row?.id||''}`}
function newerSyncRecord(a,b){
  if(!a)return b;if(!b)return a;const ta=String(a._updatedAt||''),tb=String(b._updatedAt||'');if(ta!==tb)return ta>tb?a:b;const ra=recordRevision(a),rb=recordRevision(b);return ra>=rb?a:b;
}
function mergeSyncCollection(kind,localRows,remoteRows,ledger){
  const map=new Map();for(const row of [...(remoteRows||[]),...(localRows||[])]){if(!row)continue;const k=syncRecordKey(kind,row);map.set(k,newerSyncRecord(map.get(k),row))}
  return [...map.values()].filter(row=>!effectiveCloudDeletion(kind,row,ledger));
}
function applyLedgerToUnifiedRecords(records,ledger){
  const out=[];
  for(const source of (records||[])){
    const row=normalizeCoreActivity(source,source?.activityCategory==='DUTY'?'duties':'');
    const deleted=effectiveCloudDeletion('activities',row,ledger)||
      (isEntryActivityRecord(row)&&effectiveCloudDeletion('flights',row,ledger))||
      (isFlight(row)&&effectiveCloudDeletion('roster',row,ledger))||
      (isDutyActivityRecord(row)&&effectiveCloudDeletion('duties',row,ledger));
    if(!deleted)out.push(row);
  }
  return out;
}
function mergeUnifiedDatabases(localDatabase,remoteDatabase){
  const local=normalizeOperationalDatabase(localDatabase),remote=normalizeOperationalDatabase(remoteDatabase),ledger=mergeSyncLedgers(local.values[SYNC_LEDGER_KEY]||[],remote.values[SYNC_LEDGER_KEY]||[]),map=new Map();
  for(const row of [...(remote.records||[]),...(local.records||[])]){
    if(!row)continue;const key=syncRecordKey('activities',row),prior=map.get(key),newer=newerSyncRecord(prior,row),older=newer===row?prior:row;
    map.set(key,older?mergeCoreActivities(older,newer):normalizeCoreActivity(newer,newer?.activityCategory==='DUTY'?'duties':''));
  }
  const values={...remote.values,...local.values,[SYNC_LEDGER_KEY]:ledger};
  // Keep validated months from either device; an explicit unlock has its own later revision.
  const localMonths=local.values[PAY_MONTH_KEY]||{},remoteMonths=remote.values[PAY_MONTH_KEY]||{},months={...remoteMonths,...localMonths};
  for(const month of new Set([...Object.keys(localMonths),...Object.keys(remoteMonths)])){
    if(!/^\d{4}-\d{2}$/.test(month))continue;
    const l=localMonths[month]||{},r=remoteMonths[month]||{};
    const winner=String(l.validationUpdatedAt||'')>=String(r.validationUpdatedAt||'')?l:r;
    months[month]={...r,...l,...(winner.validationUpdatedAt?{validated:winner.validated??null,validationUpdatedAt:winner.validationUpdatedAt}:r.validated?{validated:r.validated,validationUpdatedAt:r.validationUpdatedAt||r.validated.validatedAt}:{})};
  }
  values[PAY_MONTH_KEY]=months;
  const localSettings=local.values[APP_SETTINGS_KEY]||{},remoteSettings=remote.values[APP_SETTINGS_KEY]||{};
  const localPhotoRevision=String(localSettings.profilePhotoUpdatedAt||''),remotePhotoRevision=String(remoteSettings.profilePhotoUpdatedAt||'');
  const photoWinner=localPhotoRevision||remotePhotoRevision
    ?(localPhotoRevision>=remotePhotoRevision?localSettings:remoteSettings)
    :(remoteSettings.profilePhotoData&&!localSettings.profilePhotoData?remoteSettings:localSettings);
  values[APP_SETTINGS_KEY]={...remoteSettings,...localSettings,profilePhotoData:photoWinner.profilePhotoData||'',profilePhotoUpdatedAt:photoWinner.profilePhotoUpdatedAt||''};
  values[TRIPS_KEY]=mergeSyncCollection('trips',local.values[TRIPS_KEY]||[],remote.values[TRIPS_KEY]||[],ledger);
  values[EXPIRY_KEY]=mergeSyncCollection('expiry',local.values[EXPIRY_KEY]||[],remote.values[EXPIRY_KEY]||[],ledger);
  return normalizeOperationalDatabase({...local,records:applyLedgerToUnifiedRecords([...map.values()],ledger),values,updatedAt:new Date().toISOString()});
}
function mergeSnapshotsForSync(local,remote){
  const localDb=migrateSnapshotToOperationalDatabase(local),remoteDb=migrateSnapshotToOperationalDatabase(remote);
  return{database:mergeUnifiedDatabases(localDb,remoteDb)};
}
// The acknowledged hashes, record revisions and download cursor share the local
// database transaction with the Core. A server tombstone has a null value.
function copyJson(value){
  if(Array.isArray(value))return value.map(copyJson);
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,v])=>[key,copyJson(v)]));
  return value;
}
const storedHashes=new WeakMap();
// Only use this cache for immutable records held by the database.
function storedHash(row){
  if(row==null||typeof row!=='object')return contentHash(row);
  if(!storedHashes.has(row))storedHashes.set(row,contentHash(row));
  return storedHashes.get(row);
}
function contentHash(value){
  if(value==null)return null;
  if(typeof value!=='object'||Array.isArray(value))return sectionDigest(value);
  const {_updatedAt,_syncRev,...content}=value;
  if(content.locked===false)delete content.locked;
  for(const key of ['manualFields','timeOverrides','fieldSources','logtenImportConflicts'])if(content[key]&&Object.keys(content[key]).length===0)delete content[key];
  return sectionDigest(content);
}
function recordMap(db){
  const map=new Map(),put=(kind,id,value)=>{
    if(!id)throw Error('Missing record identity: '+kind);
    const key=JSON.stringify([kind,String(id)]);
    if(map.has(key))throw Error('Duplicate record identity: '+key);
    map.set(key,value);
  };
  for(const row of db.records)if(row.activityStatus!=='DRAFT')put('activity',row.id,row);
  for(const kind of [TRIPS_KEY,EXPIRY_KEY])for(const row of db.values[kind]||[])put(kind,row.id,row);
  for(const row of db.values[SYNC_LEDGER_KEY]||[])if(row.eventId&&['delete','delete-collection','restore162'].includes(row.action))put(SYNC_LEDGER_KEY,row.eventId,row);
  for(const kind of [FX_KEY,PAY_MONTH_KEY])for(const [id,value] of Object.entries(db.values[kind]||{}))if(id!=='_updatedAt')put(kind,id,value);
  for(const kind of [APP_SETTINGS_KEY,PAY_SETTINGS_KEY])if(db.values[kind])put(kind,'settings',db.values[kind]);
  return map;
}
function baseline(map){
  return Object.fromEntries([...map].map(([k,v])=>[k,{hash:storedHash(v),rev:0}]));
}
function changesSince(map,state){
  const keys=new Set([...map.keys(),...Object.keys(state.known)]);
  return [...keys].filter(k=>storedHash(map.get(k))!==(state.known[k]?.hash??null));
}
function setReceived(db,key,value){
  const [kind,id]=JSON.parse(key);
  if(['activity',TRIPS_KEY,EXPIRY_KEY,SYNC_LEDGER_KEY].includes(kind)){
    const rows=kind==='activity'?db.records:(db.values[kind]??=[]),field=kind===SYNC_LEDGER_KEY?'eventId':'id';
    if(value&&String(value[field])!==id)throw Error('Remote record identity mismatch: '+key);
    const i=rows.findIndex(r=>String(r[field])===id);
    if(i>=0){if(value==null)rows.splice(i,1);else rows[i]=value}
    else if(value!=null)rows.push(value);
  }else if([FX_KEY,PAY_MONTH_KEY].includes(kind)){
    const obj=db.values[kind]??={};if(value==null)delete obj[id];else obj[id]=value;
  }else if([APP_SETTINGS_KEY,PAY_SETTINGS_KEY].includes(kind)&&id==='settings'){
    if(value==null)delete db.values[kind];else db.values[kind]=value;
  }else throw Error('Unknown remote record: '+key);
}
let bootKnown=null,legacyDirty=false;
function syncDirty(){
  if(!operationalDatabase||!bootKnown||importStaging)return;
  const state=operationalDatabase.replica162;
  const dirty=(!state&&legacyDirty)||!!Object.keys(state?.sent||{}).length||!!Object.keys(state?.conflicts||{}).length||changesSince(recordMap(operationalDatabase),state||{known:bootKnown}).length>0;
  localStorage.setItem(CLOUD_DIRTY_KEY,dirty?'1':'0');
  if(syncDatabaseReady)drawSyncState();
}
async function persistReplica(){
  syncDirty();await queueFlightPersist();
}
async function attachReplica(session,meta){
  let db=operationalDatabase;const existing=db.replica162;
  if(existing){
    if(existing.user!==session.user.id)throw Error('Account conflict: local data retained');
    if(existing.generation===meta.generation)return existing;
    if(existing.cursor||meta.revision<=Number(localStorage.getItem(CLOUD_BASE_REV_KEY))||changesSince(recordMap(db),existing).length||Object.keys(existing.sent).length||Object.keys(existing.conflicts||{}).length)
      throw Error('Conflict: central baseline changed. Local data retained.');
    bootKnown=existing.known;legacyDirty=false;
  }
  const base=Number(localStorage.getItem(CLOUD_BASE_REV_KEY)||0);
  if(base>meta.revision)
    throw Error('Conflict: local data has no matching verified base. Both copies retained.');
  // A previously verified 16.1 copy can adopt the record protocol without downloading the same bytes again.
  const sections=snapshotSections();
  if(!existing&&base===meta.revision&&meta.digestFormat===SNAPSHOT_DIGEST_FORMAT&&
     sectionDigest(sections.databaseRecords)===meta.digests.databaseRecords&&sectionDigest(sections.databaseValues)===meta.digests.databaseValues){
    db.replica162={user:session.user.id,generation:meta.generation,known:baseline(recordMap(db)),cursor:0,sent:{}};
    delete db.preSync162;localStorage.setItem(SYNC_OWNER,session.user.id);await persistReplica();return db.replica162;
  }
  await ensureWeeklyBackupBeforeSync();
  const snapshot=await fetchCentralSnapshot(session,meta);
  checkAccount(session);
  const remote={...snapshot.databaseMeta,records:snapshot.databaseRecords,values:Object.fromEntries(snapshot.databaseValues.map(x=>[x.key,x.value]))};
  db=operationalDatabase;
  const remoteMap=recordMap(remote),localMap=recordMap(db),known=baseline(remoteMap);
  const changed=new Set(changesSince(localMap,{known:bootKnown||baseline(localMap)}));
  if(base!==meta.revision&&(legacyDirty||!base&&localHasMeaningfulData())){
    const ambiguous=[...localMap.keys()].find(k=>!changed.has(k)&&storedHash(localMap.get(k))!==storedHash(remoteMap.get(k)));
    if(ambiguous)throw Error('Conflict: unverified legacy change '+ambiguous+'. Both copies retained.');
  }
  if(legacyDirty){
    for(const k of changesSince(localMap,{known})){
      // Old snapshots had only one dirty bit. Missing rows without explicit
      // deletion evidence must never be interpreted as an instruction to delete.
      if(localMap.has(k))changed.add(k);
      else {const [kind]=JSON.parse(k),row=remoteMap.get(k);
        if(row&&effectiveCloudDeletion(kind==='activity'?'activities':kind===TRIPS_KEY?'trips':'expiry',row))changed.add(k);}
    }
  }
  for(const key of changed){
    if(base!==meta.revision&&contentHash(remoteMap.get(key))!==(bootKnown?.[key]?.hash??null)&&contentHash(localMap.get(key))!==contentHash(remoteMap.get(key)))
      throw Error('Conflict during upgrade: '+key);
  }
  const incoming=[...new Set([...localMap.keys(),...remoteMap.keys()])].filter(key=>!changed.has(key)&&storedHash(localMap.get(key))!==storedHash(remoteMap.get(key)));
  for(const key of incoming){
    const [kind,id]=JSON.parse(key);
    if(kind==='activity'&&db.localForms?.[id])throw Error('Conflict: local entry draft '+id+' retained during upgrade.');
  }
  const openId=$('editId')?.value;
  for(const key of incoming)setReceived(db,key,remoteMap.get(key)??null);
  if(openId&&incoming.includes(JSON.stringify(['activity',openId]))){
    const row=db.records.find(r=>r.id===openId);
    if(row)loadEntryToForm(row,{rosterActivity:entryReturnContext?.view==='rosterView'});else resetEntry(false);
  }
  if(!base){
    const ids=new Set(db.records.map(row=>row.id));
    for(const row of remote.records)if(row.activityStatus==='DRAFT'&&!ids.has(row.id)){db.records.push(row);ids.add(row.id)}
  }
  const history=remote.values[SYNC_LEDGER_KEY];
  if(Array.isArray(history))db.values[SYNC_LEDGER_KEY]=mergeSyncLedgers(db.values[SYNC_LEDGER_KEY],history);
  // Legacy non-replicated archives/settings can seed an empty device once.
  for(const [k,v] of Object.entries(remote.values))if(db.values[k]===undefined&&![ENTRY_DRAFT_KEY,ENTRY_DRAFTS_KEY].includes(k))db.values[k]=copyJson(v);
  delete db.preSync162;db.replica162={user:session.user.id,generation:meta.generation,known,cursor:0,sent:{}};
  localStorage.setItem(CLOUD_BASE_REV_KEY,String(meta.revision));localStorage.setItem(SYNC_OWNER,session.user.id);
  await persistReplica();return db.replica162;
}
async function syncRecordChanges(session,meta,manual){
  const state=await attachReplica(session,meta);
  state.conflicts??={};
  const priorConflicts=new Set(Object.keys(state.conflicts));
  let backedUp=false;
  const backup=async()=>{if(!backedUp){await ensureWeeklyBackupBeforeSync();backedUp=true}};
  const request=async(changes)=>{
    const response=await cloudFetch('/rest/v1/rpc/pilotlog_delta162',{method:'POST',token:session.access_token,retries:0,
      body:{pointer_id:cloudActiveMetaId(session.user.id),generation_id:meta.generation,after_seq:state.cursor,changes}});
    checkAccount(session);
    if(state!==operationalDatabase.replica162||!Array.isArray(response?.page)||!Array.isArray(response?.acks)||response.acks.length!==changes.length)throw Error('Invalid sync acknowledgement');
    let previousSeq=state.cursor;
    for(const item of response.page){
      const pair=JSON.parse(item.key);
      if(!Array.isArray(pair)||pair.length!==2||typeof pair[1]!=='string'||!pair[1]||
         !['activity',TRIPS_KEY,EXPIRY_KEY,SYNC_LEDGER_KEY,FX_KEY,PAY_MONTH_KEY,APP_SETTINGS_KEY,PAY_SETTINGS_KEY].includes(pair[0])||
         !Number.isSafeInteger(item.seq)||item.seq<=previousSeq||!Number.isSafeInteger(item.rev)||item.rev<1||
         !Object.prototype.hasOwnProperty.call(item,'value')||item.hash!==contentHash(item.value))throw Error('Integrity: invalid delta '+item.key);
      if(item.value&&['activity',TRIPS_KEY,EXPIRY_KEY,SYNC_LEDGER_KEY].includes(pair[0])&&String(item.value[pair[0]===SYNC_LEDGER_KEY?'eventId':'id'])!==pair[1])throw Error('Integrity: record ID '+item.key);
      previousSeq=item.seq;
    }
    if(response.page.length)await backup();
    checkAccount(session);
    const local=recordMap(operationalDatabase);
    for(const [i,ack] of response.acks.entries()){
      const sent=changes[i];
      if(ack.key!==sent.key||ack.hash!==sent.hash||!Number.isSafeInteger(ack.rev)||ack.rev<sent.base)throw Error('Invalid record revision');
      state.known[ack.key]={hash:contentHash(sent.value),rev:ack.rev};delete state.sent[ack.key];
    }

    let refreshOpen=false;
    for(const item of response.page){
      const {key,value,rev,seq}=item,prev=state.known[key],hash=contentHash(value),current=contentHash(local.get(key));
      if(!Number.isSafeInteger(seq)||seq<=state.cursor||!Number.isSafeInteger(rev)||rev<1)throw Error('Invalid delta sequence');
      if(rev>(prev?.rev||0)){
        const [recordKind,recordId]=JSON.parse(key);
        if(recordKind===FX_KEY&&recordId.startsWith('payroll:')&&value?.rate>0&&local.get(key)?.rate>0){
          if(value.locked||!local.get(key).locked)setReceived(operationalDatabase,key,value);
          state.known[key]={hash,rev};delete state.sent[key];delete state.conflicts[key];state.cursor=seq;continue;
        }
        const own=state.sent[key]&&state.sent[key].hash===hash&&state.sent[key].base+1===rev;
        if(recordKind==='activity'&&operationalDatabase.localForms?.[recordId]&&!own&&current!==hash){
          state.conflicts[key]=item;state.cursor=seq;continue;
        }
        if(!own&&current!==hash&&(state.sent[key]||state.conflicts[key]||current!==(prev?.hash??null))){
          state.conflicts[key]=item;state.cursor=seq;continue;
        }
        delete state.conflicts[key];
        if(!own&&current!==hash){
          const [kind,id]=JSON.parse(key);
          if(kind==='activity'&&$('editId')?.value===id&&!$('addView')?.classList.contains('hidden')){
            refreshOpen=true;
          }
          setReceived(operationalDatabase,key,value);
        }
        state.known[key]={hash,rev};delete state.sent[key];
      }
      state.cursor=seq;
    }
    if(refreshOpen){
      const active=operationalDatabase.records.find(r=>r.id===$('editId').value);
      if(active)loadEntryToForm(active,{rosterActivity:entryReturnContext?.view==='rosterView'});
      else resetEntry(false);
    }
    if(changes.length||response.page.length)await persistReplica();

    return response.page.length===40;
  };
  while(await request([])){}
  for(const [key,item] of Object.entries(state.conflicts)){
    const hash=contentHash(item.value),current=storedHash(recordMap(operationalDatabase).get(key));
    if(current!==hash){
      if(!manual||!priorConflicts.has(key))continue;
      const label=JSON.parse(key).join(' / ');
      if(!confirm('Conflict: '+label+'\nKeep and upload the local version?')){
        if(!confirm('Replace the local version of '+label+' with the central version? Cancel keeps both pending.'))continue;
        setReceived(operationalDatabase,key,item.value);
        const [kind,id]=JSON.parse(key);
        if(kind==='activity'){
          if(operationalDatabase.localForms)delete operationalDatabase.localForms[id];
          if($('editId')?.value===id){if(item.value)loadEntryToForm(item.value);else resetEntry(false)}
        }
      }
    }
    state.known[key]={hash,rev:item.rev};delete state.sent[key];delete state.conflicts[key];await persistReplica();
  }
  const local=recordMap(operationalDatabase),keys=[...new Set([...changesSince(local,state),...Object.keys(state.sent)])].filter(k=>!state.conflicts[k]);
  for(let i=0;i<keys.length;i+=40){
    const now=recordMap(operationalDatabase),batch=[];
    for(const key of keys.slice(i,i+40)){
      const value=now.get(key)??null,hash=contentHash(value),base=state.known[key]?.rev||0;
      if(!state.sent[key]&&hash===(state.known[key]?.hash??null))continue;
      batch.push({key,value,base,hash});state.sent[key]={hash,base};
    }
    if(batch.length){await backup();await persistReplica();let more=await request(copyJson(batch));while(more)more=await request([])}
  }
  syncDirty();
  const unresolved=Object.keys(state.conflicts);
  if(unresolved.length)throw Object.assign(Error('Conflict: '+unresolved.join(', ')+' — both copies retained. Use Sync Now to review.'),{recordConflict:true});
  return keys.length;
}

function drawSyncState(){
  const signed=cloudStoredSession(),last=localStorage.getItem('pilotlog7_last_cloud_sync');
  const dirty=localStorage.getItem(CLOUD_DIRTY_KEY)==='1',failed=localStorage.getItem('pilotlog7_sync_failure_16');
  const unverified=legacyDirty&&!operationalDatabase?.replica162&&bootKnown&&!changesSince(recordMap(operationalDatabase),{known:bootKnown}).length;
  const state=failed?'error':dirty&&!unverified?'pending':'ok';
  const title=failed?'Sync failed — Local data protected':unverified?'Checking previous sync':dirty?'Local changes pending':last?'Database synchronized':'Awaiting first sync';
  for(const id of ['cloudStatus','syncState']){
    const el=$(id);if(!el)continue;
    el.dataset.state=state;el.textContent=[signed?'':'Not signed in',title,failed,last?'Last successful sync '+new Date(last).toLocaleString():''].filter(Boolean).join(' · ');
  }
  const log=$('syncLog');
  if(log)log.textContent=syncHistory().slice().reverse().map(entry=>new Date(entry.date).toLocaleString()+' · '+entry.reason+' · '+(entry.ms/1000).toFixed(1)+' s · '+(entry.error||entry.result)).join('\n');
}
function syncHistory(){try{return JSON.parse(localStorage.getItem(SYNC_LOG)||'[]')}catch{return []}}
function logSync(start,reason,result,error){
  localStorage.setItem(SYNC_LOG,JSON.stringify([...syncHistory().slice(-99),{date:new Date(start).toISOString(),ms:Date.now()-start,reason,result,error}]));
}
async function updateCloudStatus(){
  const s=cloudStoredSession(),btn=$('cloudSignInBtn');
  if(btn){btn.classList.toggle('signed-in',!!s);btn.textContent=s?'Signed in':'Sign in'}
  if(s?.user?.email&&$('cloudEmail'))$('cloudEmail').value=s.user.email;
  drawSyncState();
}
async function cloudSignUp(){const email=$('cloudEmail').value.trim(),password=$('cloudPassword').value;if(!email||password.length<6)return alert('Enter an email and a password of at least 6 characters.');localStorage.setItem(LAST_EMAIL_KEY,email);$('cloudStatus').textContent='Creating account…';const data=await cloudFetch('/auth/v1/signup',{method:'POST',body:{email,password},headers:{'x-client-info':'pilotlog-web'}});if(data?.access_token){saveCloudSession(data);window.dispatchEvent(new Event('pilotlog-auth-change'));$('cloudPassword').value='';await updateCloudStatus();scheduleAutoSync('sign-in');alert('Account created and signed in.')}else{$('cloudStatus').textContent='Account created. Check your email to confirm it, then sign in.';alert('Account created. Check your email to confirm it, then sign in.')}}
async function cloudSignIn(){const email=$('cloudEmail').value.trim(),password=$('cloudPassword').value;if(!email||!password)return alert('Enter email and password.');localStorage.setItem(LAST_EMAIL_KEY,email);$('cloudStatus').textContent='Signing in…';const data=await cloudFetch('/auth/v1/token?grant_type=password',{method:'POST',body:{email,password},headers:{'x-client-info':'pilotlog-web'}});saveCloudSession(data);window.dispatchEvent(new Event('pilotlog-auth-change'));$('cloudPassword').value='';await updateCloudStatus();scheduleAutoSync('sign-in');alert('Signed in.')}
async function cloudSignOut(){const email=$('cloudEmail').value.trim()||localStorage.getItem(LAST_EMAIL_KEY)||'';if(email)localStorage.setItem(LAST_EMAIL_KEY,email);const s=await cloudSession();if(s?.access_token){try{await cloudFetch('/auth/v1/logout',{method:'POST',token:s.access_token})}catch{}}clearSyncTimers();saveCloudSession(null);window.dispatchEvent(new Event('pilotlog-auth-change'));$('cloudEmail').value=localStorage.getItem(LAST_EMAIL_KEY)||'';$('cloudPassword').value='';await updateCloudStatus()}
async function syncSupabase(options={}){
  if(navigator.locks?.request&&!options.hasDeviceLock){
    return navigator.locks.request('pilotlog-central-sync',{ifAvailable:true},lock=>{
      if(lock)return syncSupabase({...options,hasDeviceLock:true});
      if(!cloudSyncInProgress)armPeriodicSync();
      return false;
    });
  }
  const reason=options.reason||'manual',manual=reason==='manual';
  if(cloudSyncInProgress)return false;
  if(!syncDatabaseReady||!flightStoreReady){if(manual)alert('Local database is not ready for cloud sync');return false}
  if(!manual&&localStorage.getItem(SYNC_BLOCK))return false;
  clearSyncTimers();cloudSyncInProgress=true;
  const start=Date.now(),btn=$('syncCloudBtn');if(btn)btn.disabled=true;
  let result='Skipped',errorText='',retry=false;
  try{
    if(!cloudStoredSession()){if(manual)throw Error('Sign in first');return false}
    if(manual)localStorage.removeItem(SYNC_BLOCK);
    const session=await cloudSession();if(!session)throw Error('Sign in first');
    const owner=localStorage.getItem(SYNC_OWNER);
    if(owner&&owner!==session.user.id)throw Error('Account conflict: local data belongs to a different account');
    await flushFlightStore();
    const meta=await syncStep('Read revision',()=>fetchActiveMeta(session.access_token,session.user.id));
    checkAccount(session);
    const receipt=JSON.parse(localStorage.getItem(SYNC_RECEIPT)||'null');
    if(receipt?.user===session.user.id&&receipt.generation===meta?.generation)acceptActivation(receipt);
    let active=meta;
    if(!operationalDatabase.replica162&&(active||manual&&localHasMeaningfulData())){
      // Check that the server supports this client before installing or publishing its initial baseline.
      const probe=await cloudFetch('/rest/v1/rpc/pilotlog_delta162',{method:'POST',token:session.access_token,retries:0,body:{
        pointer_id:cloudActiveMetaId(session.user.id),generation_id:active?.generation||null,after_seq:0,changes:[],probe:true}});
      if(probe?.protocol!==162)throw Error('Install the PilotLog v16.2 Supabase migration before sync.');
    }
    if(!active&&receipt?.user===session.user.id&&receipt.meta?.previousGeneration===null&&receipt.revision===1){
      await activateVerifiedGeneration(session,receipt);active=await fetchActiveMeta(session.access_token,session.user.id);
    }
    if(!active){
      if(operationalDatabase.replica162||Number(localStorage.getItem(CLOUD_BASE_REV_KEY)))throw Error('Conflict: central baseline is missing');
      if(!manual||!localHasMeaningfulData()||!confirm('Publish the first central copy?'))return false;
      await ensureWeeklyBackupBeforeSync();await publishLocalSnapshot(session,null,false);
      active=await fetchActiveMeta(session.access_token,session.user.id);
      if(!active)throw Error('Missing initial server confirmation');
    }
    if(navigator.onLine!==false)recoverPendingFx();
    const count=await syncRecordChanges(session,active,manual);
    result=count+' changed record(s); central baseline '+active.revision;
    await render();
    for(const [id,draw] of [['tripsView',renderTrips],['rosterView',renderRoster],['flightsView',renderEntriesSafe],['settingsView',renderSettings],['expiryView',renderExpiry],['payrollView',renderPayroll]])if($(id)&&!$(id).classList.contains('hidden'))await draw();
    localStorage.setItem(SYNC_OWNER,session.user.id);
    localStorage.setItem('pilotlog7_last_cloud_sync',new Date().toISOString());
    localStorage.removeItem('pilotlog7_sync_failure_16');localStorage.removeItem(SYNC_BLOCK);return true;
  }catch(error){
    errorText=error.message||String(error);localStorage.setItem('pilotlog7_sync_failure_16',errorText);
    const temporary=error.code==='57014'&&/statement timeout/i.test(errorText)||[408,429,502,503,504].includes(error.status)||/network request failed|request timed out|failed to fetch|canceling statement due to statement timeout/i.test(errorText);
    retry=temporary;
    if(!retry&&!error.recordConflict&&!(error.status===409&&/Conflict:/.test(errorText)))localStorage.setItem(SYNC_BLOCK,errorText);
    if(manual)alert('Sync failed — Local data protected. '+errorText);
    return false;
  }finally{
    cloudSyncInProgress=false;if(btn)btn.disabled=false;
    logSync(start,reason,result,errorText);drawSyncState();
    armPeriodicSync();
  }
}

function deleteIndexedDb(name){return new Promise(resolve=>{try{const r=indexedDB.deleteDatabase(name);r.onsuccess=()=>resolve(true);r.onerror=()=>resolve(false);r.onblocked=()=>resolve(false)}catch{resolve(false)}})}
async function wipeCurrentUserCloud(session){
  if(!session?.access_token||!session?.user?.id)throw new Error('Sign in first so PilotLog can identify which cloud data to erase.');
  await cloudFetch(`/rest/v1/${SUPABASE_TABLE}?user_id=eq.${encodeURIComponent(session.user.id)}`,{method:'DELETE',token:session.access_token,timeoutMs:60000,retries:2,headers:{Prefer:'return=minimal'}});
}
async function clearPilotLogLocalStorage(){
  try{await flushFlightStore()}catch{}
  if(window.PilotLogNative?.isNative)await window.PilotLogNative.clearDatabase();
  operationalDatabase=emptyOperationalDatabase();flightStoreReady=false;
  for(const k of Object.keys(localStorage))if(k.startsWith('pilotlog_')||k.startsWith('pilotlog7_')||k.startsWith('pilotlog8_')||k.startsWith('pilotlog9_'))localStorage.removeItem(k);
  for(const k of Object.keys(sessionStorage))if(k.startsWith('pilotlog_')||k.startsWith('pilotlog7_')||k.startsWith('pilotlog8_')||k.startsWith('pilotlog9_'))sessionStorage.removeItem(k);
  await Promise.all([FLIGHT_DB_NAME,LEGACY_FLIGHT_DB_NAME,LOGTEN_ARCHIVE_DB,BACKUP_DB_NAME,'pilotlog7-airports','pilotlog-core-data','pilotlog-logten-archive','pilotlog-weekly-backup','pilotlog-airports'].map(deleteIndexedDb));
  if(globalThis.caches){for(const k of await caches.keys())if(k.startsWith('pilotlog-'))await caches.delete(k)}
  localStorage.setItem(SYNC_READY_KEY,'1');
  localStorage.setItem(CLOUD_BASE_REV_KEY,'0');
  localStorage.setItem(CLOUD_DIRTY_KEY,'0');
}
async function deepFactoryResetPilotLog(){
  const session=await cloudSession();
  if(!session?.access_token)return alert('Sign in to Cloud Sync first. Deep Reset erases both your Supabase PilotLog data and this device.');
  if(!confirm('ERASE OLD DATABASE: permanently delete ALL existing PilotLog records from Supabase and format ALL PilotLog data on this device?\n\nThis is intentionally irreversible. Export/keep your LogTen migration package first.'))return false;
  const phrase=prompt('Type RESET PILOTLOG to erase the old database and initialize PilotLog:');
  if(phrase!=='RESET PILOTLOG')return alert('Deep Reset cancelled. Nothing was erased.');
  const btn=$('deepResetBtn');if(btn)btn.disabled=true;
  try{
    if($('cloudStatus'))$('cloudStatus').textContent='Deep Reset… erasing cloud database';
    await wipeCurrentUserCloud(session);
    if($('cloudStatus'))$('cloudStatus').textContent='Deep Reset… formatting this device';
    await clearPilotLogLocalStorage();
    alert('Old PilotLog database erased completely. The PilotLog central database is initialized blank and this device has been deeply formatted.');
    location.reload();return true;
  }catch(e){
    console.error('Deep Reset failed',e);alert('Database erase stopped safely: '+e.message+'\n\nPilotLog did not format the device because the Supabase erase did not complete.');return false;
  }finally{if(btn)btn.disabled=false}
}


async function deepLocalResetPilotLog(){
  if(!confirm('Reset ALL PilotLog data on THIS DEVICE only? Cloud data will not be touched.'))return false;
  const phrase=prompt('Type RESET DEVICE to confirm:');if(phrase!=='RESET DEVICE')return alert('Device reset cancelled. Nothing was erased.');
  try{await clearPilotLogLocalStorage();alert('This device is now blank and ready for PilotLog v8.3.0. Import locally or download a verified central snapshot.');location.reload();return true}catch(e){alert('Device reset failed: '+e.message);return false}
}

/* UI render */
function logbookEntryStartDate(f){
  if(!f?.date)return null;
  let t='';
  if(isFlight(f)){
    t=f.onDuty||shiftTime(f.schedOut||f.out||f.off,-60)||f.schedOut||f.out||f.off||'00:00';
  }else{
    t=f.onDuty||f.schedOut||f.out||'00:00';
  }
  const d=zuluDate(f.date,t);
  return d&&Number.isFinite(d.getTime())?d:null;
}
function nextLogbookEntryId(fs,now=new Date()){
  const future=(fs||[])
    .map(f=>({f,start:logbookEntryStartDate(f)}))
    .filter(x=>x.start&&x.start>=now)
    .sort((a,b)=>a.start-b.start);
  if(future.length)return future[0].f.id;

  const past=(fs||[])
    .map(f=>({f,start:logbookEntryStartDate(f)}))
    .filter(x=>x.start&&x.start<now)
    .sort((a,b)=>b.start-a.start);
  return past[0]?.f?.id||null;
}
function scrollEntriesToNextDuty(){
  const anchor=$('entriesNextDutyAnchor');
  if(!anchor){scrollTo(0,0);return}
  requestAnimationFrame(()=>requestAnimationFrame(()=>{
    const top=anchor.getBoundingClientRect().top+window.scrollY-18;
    window.scrollTo({top:Math.max(0,top),left:0,behavior:'auto'});
  }));
}
function scrollLogbookToEntry(entryId){
  if(!entryId){scrollEntriesToNextDuty();return}
  requestAnimationFrame(()=>requestAnimationFrame(()=>{
    const row=document.querySelector(`[data-logbook-entry-id="${CSS.escape(String(entryId))}"]`);
    if(!row){scrollEntriesToNextDuty();return}
    const top=row.getBoundingClientRect().top+window.scrollY-18;
    window.scrollTo({top:Math.max(0,top),left:0,behavior:'auto'});
  }));
}
function show(id,options={}){
  if(!$('addView').classList.contains('hidden')&&id!=='addView')saveEntryDraft();

  const dashboardMode=id==='dashboardView';
  document.querySelectorAll('main>section').forEach(s=>{
    const visible=s.id===id||(dashboardMode&&s.id==='totalsView');
    s.classList.toggle('hidden',!visible);
  });

  document.querySelectorAll('.nav button').forEach(b=>
    b.classList.toggle('active',b.dataset.view===id)
  );

  if(id==='dashboardView')renderTotals();
  if(id==='totalsView')renderTotals();
  if(id==='tripsView')renderTrips();
  if(id==='payrollView')renderPayroll();
  if(id==='expiryView')renderExpiry();
  if(id==='settingsView')renderSettings();

  if(id==='flightsView'){
    if(options.logbookEntryId){
      const ordered=entryActivityRows().filter(isCompletedLogbookEntry).filter(f=>logbookMatchesQuery(f,logbookSearchQuery)).sort(compareLogbookEntries);
      const idx=ordered.findIndex(f=>f.id===options.logbookEntryId);if(idx>=0)logbookRenderLimit=Math.max(logbookRenderLimit,idx+1);
    }
    renderEntriesSafe();
    if(options.logbookEntryId)scrollLogbookToEntry(options.logbookEntryId);
    else scrollEntriesToNextDuty();
    return;
  }

  if(id==='rosterView'){
    Promise.resolve(renderRoster()).then(()=>window.scrollTo({top:0,left:0,behavior:'auto'}));
    return;
  }

  scrollTo({top:0,left:0,behavior:'smooth'});
}
function monthLabel(date){
  if(!date)return'';
  const d=new Date(`${String(date).slice(0,7)}-01T00:00:00Z`);
  try{return new Intl.DateTimeFormat('en-GB',{month:'long',year:'numeric',timeZone:'UTC'}).format(d)}
  catch{return String(date).slice(0,7)}
}
function timeGroupAppliesToEntry(key,entry){
  if(isFlight(entry))return ['totalFlightTime','multiPilot','pic','picUS','sic','ifrTime','airTime','night','picNight','dualGiven','dualReceived','credit','totalDuty'].includes(key);
  if(isSim(entry))return ['simulatorTime','sfiSfe','credit','totalDuty'].includes(key);
  if(isGround(entry))return ['groundInstruction','credit','totalDuty'].includes(key);
  if(isStby(entry))return ['stbyTime','credit','totalDuty'].includes(key);
  if(isPositioning(entry))return ['credit','totalDuty'].includes(key);
  return false;
}
function entryTimeGroupCardHtml(entry,context=null){
  const settings=timeGroupSettings(),rows=TIME_GROUP_DEFS.filter(({key})=>settings[key].showCard===true&&timeGroupAppliesToEntry(key,entry));
  return rows.map(({key,label},i)=>`${i?'':'<b>'}${esc(label)} ${fmt(timeGroupFinalValue(entry,key,context))}${i?'':'</b>'}`).join('<br>');
}
function flightHtml(fs,full=false,creditContext=null){
  if(!fs.length)return'<div class="empty">No entries yet.</div>';
  let html='',lastMonth='',nextAnchorAdded=false;
  const nextId=full?nextLogbookEntryId(fs):null;

  fs.forEach(f=>{
    const entryDate=String(f.date||'').slice(0,10);
    const month=entryDate.slice(0,7);

    if(full&&!nextAnchorAdded&&nextId&&f.id===nextId){
      html+='<div id="entriesNextDutyAnchor" class="entries-next-duty-anchor" aria-hidden="true"></div>';
      nextAnchorAdded=true;
    }

    if(full&&month&&month!==lastMonth){
      html+=`<div class="month-separator"><span>${esc(monthLabel(f.date))}</span></div>`;
      lastMonth=month;
    }

    const d='';
    const startInfo=isGround(f)&&f.onDuty?`<div class="small">Start ${esc(f.onDuty)} Z</div>`:'';

    const openAttrs=full?` data-edit-flight="${esc(f.id)}" role="button" tabindex="0" aria-label="Open logbook entry ${esc(displayDate(f.date))} ${esc(f.flightNo||'')}"`:'';
    const rolePill=f.instruction?'<span class="pill green">INSTRUCTION</span>':f.simInstructor?'<span class="pill green">INSTRUCTOR</span>':f.simExaminer?'<span class="pill green">EXAMINER</span>':'';
    html+=`<div class="flight${full?' logbook-entry-clickable':''}${f.locked?' logbook-entry-locked':''}" data-logbook-entry-id="${esc(f.id)}"${openAttrs}><div><div class="route">${esc(isSim(f)?(f.dep||'Simulator'):(f.dep||f.dutyType||'Entry'))}${!isSim(f)&&f.arr?` → ${esc(f.arr)}`:''}</div>${isFlight(f)?`<div class="small logbook-actuals"><span>Aircraft Registration: ${esc(f.reg||'—')}</span><span>Actual OUT: ${esc(f.out||'—')}${f.out?' Z':''}</span><span>Actual IN: ${esc(f.in||'—')}${f.in?' Z':''}</span></div>`:''}<div class="small">${esc(displayDate(f.date))} ${esc(f.flightNo||'')}</div>${startInfo}${f.locked?'<span class="pill green">LOCKED</span>':''}${f.activityStatus==='DRAFT'?'<span class="pill">DRAFT</span>':''}${rolePill}${f.callFromDayOff?'<span class="pill warning">Day OFF call</span>':''}${d}</div><div class="meta">${entryTimeGroupCardHtml(f,creditContext?.get(String(f.date||''))||null)}${full&&!f.locked?`<div class="list-actions"><button class="danger" data-delete-flight="${esc(f.id)}">Delete</button></div>`:''}</div></div>`;
  });

  if(full&&!nextAnchorAdded)html+='<div id="entriesNextDutyAnchor" class="entries-next-duty-anchor" aria-hidden="true"></div>';
  return html;
}

function isoDateLocal(d){
  const y=d.getFullYear(),m=String(d.getMonth()+1).padStart(2,'0'),day=String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}
function calculateDashboardDutyRows(startOffset,count){
  const today=new Date(),duties=dutyActivityRows(),groups=rosterGroups();
  const rows=[];
  for(let i=startOffset;i<startOffset+count;i++){
    const d=new Date(today.getFullYear(),today.getMonth(),today.getDate()+i),date=isoDateLocal(d),month=date.slice(0,7);
    const dayLabel=i===0?'Today':new Intl.DateTimeFormat('en-GB',{weekday:'short'}).format(d);
    let type='OFF / No duty',report='',end='',sectors=0,route='',detail='';

    if(rosterMonthAvailable(month)){
      // If the month has a roster, the dashboard is a roster view: never infer OFF from an incomplete Logbook.
      const dayGroups=rosterDayGroups(groups,date);
      const flightGroups=dayGroups.filter(g=>g.kind==='flight');
      if(flightGroups.length){
        const firstG=flightGroups[0],lastG=flightGroups.at(-1),first=firstG.items[0]||{},home=upper(appSettings().homeBase||'CMN');
        type='Flight Duty';report=firstG.start||shiftTime(first.std,-60);end=dutyEndTime(lastG.items.at(-1),lastG.end||shiftTime(lastG.items.at(-1)?.sta,30));
        sectors=flightGroups.reduce((n,g)=>n+Number(g.sectors||0),0);
        const allItems=flightGroups.flatMap(g=>g.items||[]),dest=allItems.find(x=>upper(x.dep)===home&&upper(x.arr)!==home)?.arr||first.arr||'';
        route=`${first.dep||home}${dest?' → '+dest:''}`;
      }else if(dayGroups.length){
        const g=dayGroups[0],u=upper(g.activityType||'Duty');
        type=u==='OFF'||u==='DAY OFF'?'Day OFF':(g.activityType||'Duty');report=g.start||'';end=g.end||'';
        if(isPositioning({dutyType:g.activityType}))route=(g.dep||g.arr)?`${g.dep||'?'} → ${g.arr||'?'}`:'';
        else route=g.dep||'';
        detail=[g.flightNo,g.courseType,g.remarks].filter(Boolean).join(' • ');
      }
    }else{
      // Historical month without a roster: LogTen/Logbook is the fallback source.
      const source=operationalEntriesForMonth(month),actual=dayEntries(date,source).filter(isLogbookEntry),actualFlights=actual.filter(isFlight),ext=duties.find(x=>x.date===date&&!x.aerolineRosterPlanned);
      if(actual.length){
        if(actualFlights.length){
          const first=actualFlights[0],last=actualFlights.at(-1),home=upper(appSettings().homeBase||'CMN');
          type='Flight Duty';report=first.onDuty||ext?.report||shiftTime(first.schedOut||first.out,-60);end=dutyEndTime(last,ext?.end||shiftTime(last.schedIn||last.in,30));sectors=actualFlights.length;
          const dest=actualFlights.find(x=>upper(x.dep)===home&&upper(x.arr)!==home)?.arr||first.arr||'';route=`${first.dep||home}${dest?' → '+dest:''}`;
        }else{
          const first=actual[0],last=actual.at(-1);type='Simulator';report=first.onDuty||'';end=last.offDuty||'';route=first.dep||first.type||'';detail=first.remarks||'';
        }
      }else if(ext){type=ext.type||'Duty';report=ext.report||'';end=ext.end||'';detail=ext.notes||''}
    }

    rows.push({today:i===0,dayLabel,date,type,report,end,sectors,route,detail});
  }
  return rows;
}
function dashboardDutyRows(startOffset,count){
  return PilotLogEngine.dashboard(startOffset,count).map(row=>`<div class="duty-week-row${row.today?' today-duty-row':''}">
      <div class="duty-week-date"><b>${esc(row.dayLabel)}</b><span>${esc(displayDate(row.date))}</span></div>
      <div class="duty-week-main"><b>${esc(row.type)}</b>${row.route?`<div class="small">${esc(row.route)}</div>`:''}${row.detail?`<div class="small">${esc(row.detail)}</div>`:''}</div>
      <div class="duty-week-time">${row.report||row.end?`<b>${esc(row.report||'--:--')} – ${esc(row.end||'--:--')}</b><div class="small">${row.sectors?`${row.sectors} sector${row.sectors===1?'':'s'}`:'Duty'}</div>`:'<span class="small">—</span>'}</div>
    </div>`).join('');
}



const RECOVERY_SNAPSHOT_PREFIX='recovery:';
const RECOVERY_SNAPSHOT_LIMIT=3;
let recoverySnapshotQueue=Promise.resolve(),lastRecoverySignature='';
function recoveryHash(text){
  let h=2166136261;
  for(let i=0;i<text.length;i++){h^=text.charCodeAt(i);h=Math.imul(h,16777619)}
  return `${text.length}:${(h>>>0).toString(16)}`;
}
async function storeRecoveryFlightSnapshot(snapshot){
  const db=await openWeeklyBackupDb();
  try{
    await new Promise((resolve,reject)=>{
      const tx=db.transaction(BACKUP_STORE,'readwrite'),store=tx.objectStore(BACKUP_STORE),key=`${RECOVERY_SNAPSHOT_PREFIX}${snapshot.at}`;
      store.put(snapshot,key);
      const q=store.getAllKeys();
      q.onsuccess=()=>{
        const keys=(q.result||[]).map(String).filter(k=>k.startsWith(RECOVERY_SNAPSHOT_PREFIX)).sort().reverse();
        keys.slice(RECOVERY_SNAPSHOT_LIMIT).forEach(k=>store.delete(k));
      };
      q.onerror=()=>reject(q.error);
      tx.oncomplete=resolve;
      tx.onerror=()=>reject(tx.error);
      tx.onabort=()=>reject(tx.error||new Error('Recovery snapshot write aborted'));
    });
  }finally{db.close()}
}
function snapshotFlights(reason='safety'){
  try{
    const parsed=entryActivityRows();if(!Array.isArray(parsed)||!parsed.length)return;
    const current=JSON.stringify(parsed),signature=recoveryHash(current);if(signature===lastRecoverySignature)return;
    lastRecoverySignature=signature;
    const snapshot={at:new Date().toISOString(),reason,count:parsed.length,signature,data:parsed};
    recoverySnapshotQueue=recoverySnapshotQueue.then(()=>storeRecoveryFlightSnapshot(snapshot)).catch(e=>console.warn('PilotLog recovery snapshot skipped',e));
  }catch(e){console.warn('PilotLog recovery snapshot skipped',e)}
}
async function migrateLegacyFlightSnapshots(){
  const raw=localStorage.getItem(LEGACY_FLIGHT_BACKUP_KEY);if(!raw)return;
  try{
    const legacy=JSON.parse(raw);if(!Array.isArray(legacy)||!legacy.length){localStorage.removeItem(LEGACY_FLIGHT_BACKUP_KEY);return}
    const selected=legacy.filter(x=>Array.isArray(x?.data)&&x.data.length).slice(0,RECOVERY_SNAPSHOT_LIMIT).reverse();
    for(const x of selected){
      const at=x.at||new Date().toISOString(),text=JSON.stringify(x.data);
      await storeRecoveryFlightSnapshot({at,reason:`legacy-${x.reason||'snapshot'}`,count:x.data.length,signature:recoveryHash(text),data:x.data});
    }
    localStorage.removeItem(LEGACY_FLIGHT_BACKUP_KEY);
  }catch(e){console.warn('Legacy recovery snapshot migration deferred',e)}
}
let logbookSearchQuery='',logbookRenderLimit=400,logbookAutoLoadObserver=null;
const LOGBOOK_RENDER_STEP=400;
function observeLogbookAutoLoad(remaining){
  if(logbookAutoLoadObserver){logbookAutoLoadObserver.disconnect();logbookAutoLoadObserver=null}
  const sentinel=$('logbookAutoLoadSentinel');
  if(!remaining||!sentinel||!('IntersectionObserver' in window))return;
  logbookAutoLoadObserver=new IntersectionObserver(entries=>{
    if(!entries.some(entry=>entry.isIntersecting))return;
    logbookAutoLoadObserver.disconnect();logbookAutoLoadObserver=null;
    setTimeout(()=>{logbookRenderLimit+=LOGBOOK_RENDER_STEP;renderEntriesSafe()},0);
  },{root:null,rootMargin:'0px 0px 800px 0px',threshold:0});
  logbookAutoLoadObserver.observe(sentinel);
}
function logbookAirportText(code){
  const c=upper(code||''),a=airportIndex?.[c];return [c,a?.iata,a?.icao,a?.name,a?.city,a?.country].filter(Boolean).join(' ');
}
function logbookSearchText(f){
  return [f.flightNo,f.dep,f.arr,f.reg,f.type,f.location,f.picName,f.sicName,f.soName,f.instructorName,f.role,f.remarks,logbookAirportText(f.dep),logbookAirportText(f.arr),logbookAirportText(f.location)].filter(Boolean).join(' ').toUpperCase();
}
function logbookMatchesQuery(f,q){
  const terms=String(q||'').trim().toUpperCase().split(/\s+/).filter(Boolean);if(!terms.length)return true;const hay=logbookSearchText(f);return terms.every(t=>hay.includes(t));
}
function entriesByDate(rows){
  const map=new Map();
  for(const f of (rows||[])){
    const key=String(f?.date||'');
    if(!map.has(key))map.set(key,[]);
    map.get(key).push(f);
  }
  return map;
}
function calculateLogbookStats(rows,total){
  const all=rows||[],flights=all.filter(isFlight),sims=all.filter(isSim),byDate=entriesByDate(all);
  return{entries:all.length,total:Number(total)||0,sectors:flights.length,block:sum(flights,totalFlightMins),credit:sum(all,f=>creditMinsForEntry(f,byDate.get(String(f.date||''))||[])),simulators:sims.length};
}
function renderLogbookSearchStats(rows,total){
  const el=$('logbookSearchStats');if(!el)return;const stats=PilotLogEngine.logbookStats(rows,total);
  const q=String(logbookSearchQuery||'').trim();el.innerHTML=`<b>${stats.entries.toLocaleString('en-US')}</b> ${q?'matching':'logbook'} entr${stats.entries===1?'y':'ies'} • <b>${stats.sectors.toLocaleString('en-US')}</b> sectors • Block <b>${fmt(stats.block)}</b> • Credit <b>${fmt(stats.credit)}</b>${stats.simulators?` • Simulator <b>${stats.simulators}</b>`:''}${q?` • from ${stats.total.toLocaleString('en-US')} total`:''}`;
}
async function lockAllLogbookEntries(){
  const rows=entryActivityRows(),targets=rows.filter(f=>isCompletedLogbookEntry(f)&&!f.locked),count=targets.length;if(!count){alert('All Logbook entries are already locked.');return}
  if(!confirm(`Lock all Logbook entries?\n\n${count.toLocaleString('en-US')} unlocked entr${count===1?'y':'ies'} will be locked.\nOnly the lock state will change; flight data will not be modified.`))return;
  const ids=new Set(targets.map(x=>x.id)),now=new Date().toISOString(),updated=rows.map(x=>ids.has(x.id)?{...x,locked:true,_syncRev:recordRevision(x)+1,_updatedAt:now}:x);await replaceEntryActivitiesDurable(updated);await flushFlightStore();markCloudCollectionEdited('activities','lock-all',{count});scheduleAutoSync('lock-all-entries');renderEntriesSafe();alert(`${count.toLocaleString('en-US')} Logbook entr${count===1?'y':'ies'} locked.`);
}

function renderEntriesSafe(){
  try{
    const all=displayActivityRows().filter(isCompletedLogbookEntry),creditContext=entriesByDate(all);
    const fs=all.filter(f=>logbookMatchesQuery(f,logbookSearchQuery)).sort(compareLogbookEntries);
    const target=$('allFlights'),visible=fs.slice(0,Math.max(LOGBOOK_RENDER_STEP,logbookRenderLimit));
    if(target){
      const remaining=Math.max(0,fs.length-visible.length);
      target.innerHTML=flightHtml(visible,true,creditContext)+(remaining?'<div id="logbookAutoLoadSentinel" class="logbook-auto-load-sentinel" aria-hidden="true"></div>':'');
      observeLogbookAutoLoad(remaining);
    }
    renderLogbookSearchStats(fs,all.length);
    return fs;
  }catch(e){
    console.error('Entries render failed',e);
    const target=$('allFlights');
    if(target)target.innerHTML='<div class="empty">Entries could not be displayed. Stored data has not been deleted.</div>';
    try{return entryActivityRows()||[]}catch{return[]}
  }
}


function cleanupExistingDuplicatesOnce(){
  // SAFETY INVARIANT: never alter existing records automatically during startup/version migration.
  localStorage.setItem('pilotlog7_dedupe_semantic_v1','1');
  return 0;
}

let rosterCalendarMonth=monthNow(),rosterSelectedDate=today(),suppressRosterCalendarClickUntil=0;

async function render(){
  refreshEntrySuggestions();
  try{
    if($('dashboardTodayDuty'))$('dashboardTodayDuty').innerHTML=dashboardDutyRows(0,1);
    if($('dashboardNextDuties'))$('dashboardNextDuties').innerHTML=dashboardDutyRows(1,6);
  }catch(e){
    console.error('Dashboard 7-day view failed',e);
    if($('dashboardTodayDuty'))$('dashboardTodayDuty').innerHTML='<div class="empty">Today duty temporarily unavailable.</div>';
    if($('dashboardNextDuties'))$('dashboardNextDuties').innerHTML='<div class="empty">Next duties temporarily unavailable.</div>';
  }
  // FTL uses optional airport data; keep its lookup from blocking roster startup.
  renderFtl('dashboardFtl',true).catch(e=>console.error('FTL totals view failed',e));
}

function rosterShiftMonth(month,amount){
  const [year,number]=String(month||monthNow()).split('-').map(Number),d=new Date(year,Math.max(0,number-1)+Number(amount||0),1);
  return`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
}
function rosterCalendarDates(month){
  const [year,number]=String(month||monthNow()).split('-').map(Number),first=new Date(year,number-1,1),mondayOffset=(first.getDay()+6)%7,start=new Date(year,number-1,1-mondayOffset),out=[];
  for(let i=0;i<42;i++){const d=new Date(start);d.setDate(start.getDate()+i);out.push(isoDateLocal(d))}
  return out;
}
function rosterDayGroups(groups,date){
  return(groups||[]).filter(g=>g.date===date).sort((a,b)=>
    String(a.sortTime||a.start||'99:99').localeCompare(String(b.sortTime||b.start||'99:99'))
  );
}
function rosterCalendarYears(groups){
  const currentYear=new Date().getFullYear(),selectedYear=Number(String(rosterCalendarMonth).slice(0,4))||currentYear,years=(groups||[]).map(g=>Number(String(g.date||'').slice(0,4))).filter(Number.isFinite);
  const min=Math.min(currentYear,selectedYear,...years),max=Math.max(currentYear+3,selectedYear,...years),out=[];
  for(let y=min;y<=max;y++)out.push(y);
  return out;
}
function rosterCalendarEventType(group){
  const type=upper(group.kind==='flight'?'FLIGHT':canonicalDutyType(group.activityType||'Duty','Duty'));
  if(type==='DAY OFF'||type==='OFF')return'off';
  if(type==='STBY')return'stby';
  if(type==='SIMULATOR')return'sim';
  if(type==='DHD'||type==='DHP')return'positioning';
  if(type==='GROUND COURSE')return'ground';
  return type==='FLIGHT'?'flight':'duty';
}
function rosterCalendarEvents(dayGroups){
  const events=[];
  for(const g of dayGroups||[]){
    if(g.kind==='flight'){
      for(const r of g.items||[])events.push({type:'flight',locked:!!r.locked,label:[upper(r.dep||''),upper(r.arr||'')].filter(Boolean).join('-')||rosterFlightLabel(r.flightNo||'FLIGHT')});
      continue;
    }
    const type=canonicalDutyType(g.activityType||'Duty','Duty'),u=upper(type),label=u==='DAY OFF'?'OFF':u==='SIMULATOR'?'SIM':u==='GROUND COURSE'?'GROUND':u;
    events.push({type:rosterCalendarEventType(g),label});
  }
  return events;
}
function rosterCalendarCells(month,groups,todayKey){
  return rosterCalendarDates(month).map(date=>{
    const dayGroups=rosterDayGroups(groups,date),events=rosterCalendarEvents(dayGroups),outside=date.slice(0,7)!==month,selected=date===rosterSelectedDate,isToday=date===todayKey,visible=events.slice(0,2),extra=Math.max(0,events.length-visible.length);
    return`<button type="button" class="roster-calendar-day${outside?' outside':''}${selected?' selected':''}${isToday?' today':''}${events.length?' has-events':''}" data-roster-date="${date}" aria-pressed="${selected?'true':'false'}"><span class="roster-day-number">${Number(date.slice(8,10))}</span><span class="roster-day-events">${visible.map(ev=>`<span class="roster-event roster-event-${ev.type}${ev.locked?' roster-event-locked':''}">${esc(ev.label)}</span>`).join('')}${extra?`<span class="roster-event-more">+${extra}</span>`:''}</span></button>`;
  }).join('');
}

function rosterMonthLabel(date){
  if(!date)return'';
  const d=new Date(`${String(date).slice(0,7)}-01T00:00:00Z`);
  try{return new Intl.DateTimeFormat('en-GB',{month:'long',year:'numeric',timeZone:'UTC'}).format(d)}
  catch{return String(date).slice(0,7)}
}
function rosterMonthDutyMins(groups,rows){
  return (rows||[]).reduce((sum,row)=>sum+timeGroupFinalValue(row,'totalDuty',rows||[]),0);
}
function rosterMonthScheduledBlockMins(rows){
  return sum((rows||[]).filter(isFlight),entryScheduleTotalMins);
}
function rosterMonthActualBlockMins(rows){
  return sum((rows||[]).filter(isFlight),entryBlockMins);
}
let rosterRenderRevision=0;
async function renderRoster(){
  const revision=++rosterRenderRevision;
  const box=$('rosterList');if(!box)return;
  cleanupRosterDuplicates();
  if($('aerolineImportStatus'))$('aerolineImportStatus').textContent=aerolineImportStatusText();
  
  const todayKey=isoDateLocal(new Date()),groups=rosterGroups(),month=rosterCalendarMonth||monthNow();
  const panelMonths=[rosterShiftMonth(month,-1),month,rosterShiftMonth(month,1)],calendarPanels=panelMonths.map((panelMonth,i)=>`<div class="roster-calendar-panel${i===1?' current':''}" data-roster-panel-month="${panelMonth}"><div class="roster-calendar-grid">${rosterCalendarCells(panelMonth,groups,todayKey)}</div></div>`).join('');
  const monthRows=displayActivityRows().filter(f=>String(f.date||'').slice(0,7)===month&&canonicalDutyType(f.dutyType||'')!=='Day OFF'),monthGroups=groups.filter(g=>String(g.date||'').slice(0,7)===month),monthSchedule=rosterMonthScheduledBlockMins(monthRows),monthActual=rosterMonthActualBlockMins(monthRows),monthDuty=rosterMonthDutyMins(monthGroups,monthRows),monthCredit=sum(monthRows,f=>creditMinsForEntry(f,monthRows));
  const selectedDate=rosterSelectedDate,selectedGroups=selectedDate?rosterDayGroups(groups,selectedDate):[];
  box.innerHTML=`
    <div class="roster-calendar-shell">
      <div class="roster-month-row"><label class="sr-only" for="rosterMonthSelect">Roster month</label><input id="rosterMonthSelect" type="month" value="${month}"><button type="button" class="secondary" data-roster-today>Today</button></div>
      <div class="roster-weekdays">${['MON','TUE','WED','THU','FRI','SAT','SUN'].map(x=>`<span>${x}</span>`).join('')}</div>
      <div class="roster-calendar-viewport" data-roster-calendar-swipe><div class="roster-calendar-track">${calendarPanels}</div></div>
      <div class="roster-month-totals"><div><span>SCHEDULED BLOCK HOURS</span><b>${fmt(monthSchedule)}</b></div><div><span>ACTUAL BLOCK HOURS</span><b>${fmt(monthActual)}</b></div><div><span>TOT DUTY</span><b>${fmt(monthDuty)}</b></div><div><span>TOT CREDIT</span><b>${fmt(monthCredit)}</b></div></div>
    </div>
    <div id="rosterRestDetails"></div>
    <div class="roster-agenda" id="rosterAgenda">
      <div class="roster-agenda-title">${rosterSelectedDate?`<button type="button" class="secondary" data-add-roster-entry="${esc(rosterSelectedDate)}">+ Add Entry</button>`:'Roster details'}</div>
      <div class="roster-agenda-list" id="rosterAgendaDetails">Loading…</div>
    </div>`;
  await Promise.allSettled([
    (async()=>{try{const html=selectedDate?await rosterGroupHtml(selectedGroups,true):'<div class="empty">Select a day to view its roster details.</div>';if(revision===rosterRenderRevision)$('rosterAgendaDetails').innerHTML=html}catch(error){console.error('Roster agenda failed',error);if(revision===rosterRenderRevision)$('rosterAgendaDetails').textContent='Unable to load details for this day.'}})(),
    (async()=>{try{const html=await weeklyRestRosterHtml(displayActivityRows(),selectedDate);if(revision===rosterRenderRevision)$('rosterRestDetails').innerHTML=html}catch(error){console.error('Roster rest details failed',error);if(revision===rosterRenderRevision)$('rosterRestDetails').textContent='Rest details unavailable.'}})()
  ]);
}

// User-configured MAC rest check. Read-only Core projection; no duty values inferred.
const WEEKLY_REST_HOUR=3600000;
const weeklyRestFormatters=new Map();
function weeklyLocalParts(ms,zone){
  if(!zone)return null;
  try{
    if(!weeklyRestFormatters.has(zone))weeklyRestFormatters.set(zone,new Intl.DateTimeFormat('en-GB',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}));
    return Object.fromEntries(weeklyRestFormatters.get(zone).formatToParts(new Date(ms)).filter(p=>p.type!=='literal').map(p=>[p.type,Number(p.value)]));
  }catch{return null}
}
function weeklyLocalInstant(day,hour,zone){
  const d=new Date(day),wanted=Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),hour);
  let candidate=wanted;
  for(let i=0;i<4;i++){
    const p=weeklyLocalParts(candidate,zone);if(!p)return null;
    const represented=Date.UTC(p.year,p.month-1,p.day,p.hour,p.minute,p.second),delta=wanted-represented;
    if(!delta)return candidate;candidate+=delta;
  }
  return null;
}
function weeklyRestQualification(start,end,zone){
  if(!Number.isFinite(start)||!Number.isFinite(end)||end-start<36*WEEKLY_REST_HOUR)return null;
  const p=weeklyLocalParts(start,zone);if(!p)return null;
  const firstDay=Date.UTC(p.year,p.month-1,p.day),nights=[];
  for(let i=0;i<4&&nights.length<2;i++){
    const nightStart=weeklyLocalInstant(firstDay+i*24*WEEKLY_REST_HOUR,22,zone),nightEnd=weeklyLocalInstant(firstDay+(i+1)*24*WEEKLY_REST_HOUR,8,zone);
    if(nightStart===null||nightEnd===null)return null;
    if(nightStart>=start&&nightEnd<=end)nights.push({start:nightStart,end:nightEnd});
  }
  if(nights.length<2)return null;
  return {start,end,zone,nights,qualifiedAt:Math.max(start+36*WEEKLY_REST_HOUR,nights[1].end)};
}
function weeklyRestProjection(windows,asOf,ongoingRestConfirmed=false){
  const sorted=windows.filter(w=>w.start<=asOf).map(w=>({...w,end:Math.min(w.end,asOf)})).sort((a,b)=>a.start-b.start||a.end-b.end),merged=[];
  for(const w of sorted){
    const last=merged.length?merged[merged.length-1]:null;
    if(last&&w.start<=last.end){
      last.unknown=last.unknown||w.unknown;
      if(w.end>last.end){last.end=w.end;last.zone=w.zone;last.open=w.open}
      else if(w.end===last.end&&w.zone!==last.zone)last.zone=null;
    }else merged.push({...w});
  }
  let reset=null,lastRest=null,uncertain=false;const violations=[];
  const checkGap=(previous,end)=>{
    if(previous.unknown||previous.open)return;
    if(end-previous.end>=36*WEEKLY_REST_HOUR&&!previous.zone){uncertain=true;return}
    const rest=weeklyRestQualification(previous.end,end,previous.zone);
    if(!rest)return;
    if(reset!==null&&rest.qualifiedAt>reset+168*WEEKLY_REST_HOUR)violations.push({deadline:reset+168*WEEKLY_REST_HOUR,restQualifiedAt:rest.qualifiedAt});
    // Every recognized weekly rest restarts the counter, not every End of Duty.
    reset=rest.end;lastRest=rest;uncertain=false;
  };
  for(let i=0;i<merged.length;i++){
    if(i)checkGap(merged[i-1],merged[i].start);
    if(merged[i].unknown){reset=null;lastRest=null;uncertain=true}
  }
  const last=merged.length?merged[merged.length-1]:null;if(last&&ongoingRestConfirmed)checkGap(last,asOf);
  const deadline=reset===null?null:reset+168*WEEKLY_REST_HOUR;
  return {status:uncertain||deadline===null?'unknown':asOf>deadline?'overdue':'ok',reset,deadline,lastRest,violations,remaining:deadline===null?null:deadline-asOf,asOf};
}
function weeklyDutyWindows(rows,asOf,zones){
  // One dutyRootId is ONE duty window. Child entries can contain a manual
  // DUTY OFF only; they must not become separate, unknown duty intervals.
  // Preserve all member data and manual values: this is a read-only projection.
  const byId=new Map(rows.map(r=>[r.id,r])),groups=new Map(),windows=[];
  for(const row of rows){
    if(['Day OFF','Annual Leave','Sick Leave'].includes(canonicalDutyType(row.dutyType)))continue;
    if(!/^\d{4}-\d{2}-\d{2}$/.test(row.date||''))continue;
    const rootId=row.dutyRootId||row.id;
    if(!groups.has(rootId))groups.set(rootId,[]);
    groups.get(rootId).push(row);
  }
  const parseClock=(row,key)=>/^\d{2}:\d{2}$/.test(row[key]||'')?Date.parse(row.date+'T'+row[key]+':00Z'):null;
  for(const [rootId,members] of groups){
    const ordered=[...members].sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
    const root=byId.get(rootId)||ordered[0];
    const control=root?.dutyControl||ordered.find(r=>r.dutyControl)?.dutyControl;
    const day=Date.parse(ordered[0].date+'T00:00:00Z');
    let start=null,end=null,open=false;
    if(control){
      start=Date.parse(control.start);
      open=control.state==='OPEN';
      end=open?asOf:Date.parse(control.end);
    }else{
      // Historical duties without a dutyControl may store DUTY ON on the
      // first flight and DUTY OFF on the last. Reassemble the same root.
      const starts=ordered.map(r=>parseClock(r,'onDuty')).filter(Number.isFinite);
      const ends=ordered.map(r=>parseClock(r,'offDuty')).filter(Number.isFinite);
      if(starts.length)start=Math.min(...starts);
      if(ends.length){
        end=Math.max(...ends);
        if(Number.isFinite(start))while(end<start)end+=24*WEEKLY_REST_HOUR;
      }
    }
    let locationRow=ordered.find(r=>r.id===control?.endEntryId)||null;
    if(!locationRow)locationRow=[...ordered].reverse().find(r=>r.arr||r.location||r.dep)||ordered[ordered.length-1];
    const station=isFlight(locationRow)||isPositioning(locationRow)?locationRow.arr||locationRow.dep:locationRow.location||locationRow.arr||locationRow.dep;
    const zone=zones.get(upper(station||''))||null;
    if(!Number.isFinite(start)||!Number.isFinite(end)||end<start){
      windows.push({start:day,end:day+24*WEEKLY_REST_HOUR,unknown:true,zone:null,rootId});
    }else windows.push({start,end,open,zone,unknown:false,rootId});
  }
  return windows;
}
async function weeklyRestRosterHtml(rows,selectedDate){
  const now=Date.now(),todayUtc=new Date(now).toISOString().slice(0,10),asOf=selectedDate&&selectedDate!==todayUtc?Date.parse(selectedDate+'T23:59:59Z'):now;
  // Roster navigation must not wait for the optional online airport database.
  // Use the built-in/cached index now; ensureAirportDb is already refreshed in the background.
  if(!airportDbLoaded)ensureAirportDb(false).catch(()=>{});
  const zones=new Map(Object.entries(airportIndex||{}).filter(([,a])=>a?.tz).map(([code,a])=>[code,a.tz]));
  const windows=weeklyDutyWindows(rows,asOf,zones);
  const ongoingRestConfirmed=true; // Rest follows recorded duty intervals, independent of day labels.
  const result=PilotLogEngine.weeklyRest(windows,asOf,ongoingRestConfirmed);
  const utc=ms=>new Date(ms).toISOString().slice(0,16).replace('T',' ')+' Z';
  const label=result.status==='unknown'?'Not verifiable':result.status==='overdue'?`OVERDUE by ${fmt(Math.floor(-result.remaining/60000))}`:`${fmt(Math.floor(result.remaining/60000))} remaining`;
  return `<section class="weekly-rest-card ${result.status}" aria-label="Weekly rest"><b>Weekly rest · 168h</b><div>${esc(label)}</div>${Number.isFinite(result.reset)?`<div class="small">${utc(result.reset)}</div>`:''}</section>`;
}

let entryReturnContext=null;
async function addRosterOff(date){
  if(!/^\d{4}-\d{2}-\d{2}$/.test(date))return;
  const rows=coreActivityRows().filter(f=>f.date===date);
  const existing=rows.find(f=>canonicalDutyType(f.dutyType)==='Day OFF');
  if(existing){openRosterDutyModule(existing.id);return}
  if(rows.length&&!confirm(`There are already activities on ${displayDate(date)}. Add an OFF without changing those activities?`))return;
  const row=upsertCoreActivity(stamp({id:makeActivityId(),date,dutyType:'Day OFF',recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.COMPLETED,source:'manual',manualOverride:true,locked:false,remarks:'',completedAt:new Date().toISOString()}));
  markCloudEdited('activities',row,'create');await persistCoreActivities();
  await renderRoster();scheduleAutoSync('roster-add-off');
}
let entryRosterActivityContext=null;
let dutyRosterContext=null;
function setEntryReturnContext(view,entryId=''){
  entryReturnContext=view?{view,entryId}:null;
  const b=$('entryBackBtn');if(!b)return;
  b.classList.toggle('hidden',!entryReturnContext||view==='rosterView');
  if(entryReturnContext)b.textContent=view==='flightsView'?'← Back to Logbook':'← Back to Roster';
}
function clearEntryRosterActivityContext(){
  entryRosterActivityContext=null;
  if($('dutyTypeFlight'))$('dutyTypeFlight').disabled=false;
  if($('lockEntryBtn'))$('lockEntryBtn').classList.remove('hidden');
  if($('entryRosterDeleteBtn'))$('entryRosterDeleteBtn').classList.add('hidden');
  if($('returnFlight'))$('returnFlight').classList.remove('hidden');
  if($('clearForm'))$('clearForm').classList.remove('hidden');
}
function setRosterActivityEditorUI(type){
  if($('returnFlight'))$('returnFlight').classList.add('hidden');
  if($('clearForm'))$('clearForm').classList.add('hidden');
  if($('entryLockStatus')){
    $('entryLockStatus').textContent=`Roster ${type||'activity'} • editable directly from Roster.`;
    $('entryLockStatus').classList.remove('success');
  }
}
function openRosterEntryModule(id){
  const item=coreActivityRows().find(x=>x.id===id);if(!item)return false;
  const rule=activityRule(item);if(rule.editor!=='entry')return false;
  loadEntryToForm(item,{rosterActivity:true});setEntryReturnContext('rosterView',item.id);show('addView');return true;
}
function resetDutyEditor(){dutyRosterContext=null;}
function dutyEditorType(type){return canonicalDutyType(type||'Duty','Duty')}
function openRosterDutyModule(id){
  const item=coreActivityRows().find(x=>x.id===id);if(!item)return false;
  loadEntryToForm(item,{rosterActivity:true});setEntryReturnContext('rosterView',item.id);show('addView');return true;
}
async function deleteRosterDutyActivities(ids,options={}){
  const wanted=[...new Set((ids||[]).map(x=>String(x||'').trim()).filter(Boolean))];if(!wanted.length)return false;
  const idSet=new Set(wanted),targets=coreActivityRows().filter(x=>idSet.has(String(x.id||'')));if(!targets.length)return false;
  if(targets.some(x=>x.locked)){alert('Unlock the duty before deleting it.');return false}
  const completed=targets.filter(isCompletedLogbookEntry),flightCount=targets.filter(isFlight).length;
  const label=options.label||((flightCount>1||targets.length>1)?'this roster duty':'this roster activity');
  const warning=completed.length
    ?`Delete ${label}?\n\nPilotLog uses ONE Core database. ${completed.length} completed ${completed.length===1?'activity is':'activities are'} already part of the Logbook. Deleting ${label} will therefore also remove the same Core record${targets.length===1?'':'s'} from Logbook, Totals, Payroll and Trips.\n\nThe deletion is recorded as a tombstone so the removed duty does not return on sync. Continue?`
    :`Delete ${label}?\n\nPilotLog uses ONE Core database. This removes the same Core ${targets.length===1?'activity':'activities'} from the Roster and from Totals, Payroll and Trips where applicable. ${targets.some(isFlight)?'Any planned flight sectors in this duty will be removed together. ':''}There is no separate Logbook copy.\n\nThe deletion is recorded as a tombstone so the removed duty does not return on sync. Continue?`;
  if(!confirm(warning))return false;
  targets.forEach(x=>markCloudDeleted('activities',x,{manual:true}));removeCoreActivities(x=>idSet.has(String(x.id||'')),{manual:true});await persistCoreActivities();reconcileAllDuties();
  clearEntryRosterActivityContext();setEntryReturnContext(null);await render();await renderRoster();renderDuty();renderTrips();renderPayroll();show('rosterView');scheduleAutoSync('roster-delete');return true;
}
async function deleteRosterEntryActivity(){
  const ctx=entryRosterActivityContext;if(!ctx?.id)return false;
  return deleteRosterDutyActivities([ctx.id],{label:'this roster activity'});
}

async function clearImportedRosterData(){
  const activities=coreActivityRows(),removed=activities.filter(x=>x?.aerolineRosterPlanned&&!x.locked&&!Object.values(x.manualFields||{}).some(Boolean)&&!Object.values(x.timeOverrides||{}).some(Boolean)&&x.activityStatus!==ACTIVITY_STATUS.COMPLETED),removedFlights=removed.filter(isFlight).length,removedEntries=removed.filter(x=>!isFlight(x)&&!isDutyActivityRecord(x)).length,removedDuties=removed.filter(isDutyActivityRecord).length;
  removed.forEach(x=>{if(x.id)markCloudDeleted('activities',x,{includeSource:false,manual:true})});
  removeCoreActivities(x=>removed.some(r=>r.id===x.id),{manual:true});await persistCoreActivities();
  const currentSource=String($('entrySourceDisplay')?.dataset.source||'').toLowerCase();
  if(currentSource==='aeroline'||currentSource==='roster'){clearEntryDraft();resetEntry()}
  reconcileAllDuties();return{roster:removedFlights,entries:removedEntries,duties:removedDuties};
}



function renderDuty(){return true}
function aircraftIcaoType(entry){
  const raw=upper(entry?.type||entry?.aircraftType||'').replace(/A32O/g,'A320').trim(),compact=raw.replace(/[^A-Z0-9]/g,'');
  if(!raw)return'';
  if(/^(?:AIRBUS)?(?:A319|A320|A19N|A20N)/.test(compact))return'A320 Family';
  if(compact==='A36'||compact==='BE36')return'BE36';
  if(compact.startsWith('C172'))return'C172';
  if(compact.startsWith('P66'))return'OSCR';
  if(compact.startsWith('PA34'))return'PA34';
  if(compact.startsWith('PA42')||compact==='720'||compact==='PAY3')return'PAY3';
  if(compact.startsWith('R2160')||compact==='R200')return'R200';
  return compact.length<=4?compact:raw;
}
function buildAircraftTypeBreakdown(flying,simulators){
  const rows=new Map(),ensure=(code)=>{if(!rows.has(code))rows.set(code,{code,variants:new Set(),flight:0,pic:0,sic:0,instruction:0,simulator:0,simInstruction:0});return rows.get(code)};
  flying.filter(f=>isFlight(f)&&!isSim(f)&&!(/\bSIM\b|FSTD|FFS/i.test(String(f.type||'')))).forEach(f=>{const code=aircraftIcaoType(f);if(!code)return;const r=ensure(code),mins=totalFlightMins(f);r.variants.add(upper(f.type||f.aircraftType||code));r.flight+=mins;r.pic+=picMins(f);r.sic+=sicMins(f);r.instruction+=flightInstrMins(f)});
  return[...rows.values()].sort((a,b)=>b.flight-a.flight||a.code.localeCompare(b.code)).map(r=>({...r,variants:[...r.variants].filter(Boolean).sort()}));
}
const APPROACH_DISPLAY=[['CAT3','CAT III'],['ILS','CAT I'],['LOC','LOC'],['RNAV','RNAV'],['VOR','VOR'],['NDB','NDB'],['CRCL','CIRC'],['GOA','Go Around']];
function approachDisplayKey(type){
  const key=upper(type||'').trim();
  return ({'CAT III':'CAT3','CAT I':'ILS','CIRC':'CRCL','GO AROUND':'GOA','GO AROUNDS':'GOA','VISUAL':'VIS'})[key]||key;
}
function normalizedApproaches(entry){
  // Projection only: retain source records, including zero and null, untouched.
  let rows=Array.isArray(entry?.approaches)?entry.approaches.filter(a=>a?.quantity!==null&&a?.quantity!==''&&a?.quantity!==undefined&&Number.isFinite(Number(a.quantity))).map(a=>({type:approachDisplayKey(a.type),quantity:Number(a.quantity)})).filter(a=>a.type):[];
  if(!Array.isArray(entry?.approaches)&&entry?.source!=='logten'){const type=approachDisplayKey(entry?.approachType);if(type)rows=[{type,quantity:1}]}
  if(entry?.goArounds!==undefined&&entry.goArounds!==null&&entry.goArounds!==''&&Number.isFinite(Number(entry.goArounds))){rows=rows.filter(a=>a.type!=='GOA');const quantity=Number(entry.goArounds);if(quantity)rows.push({type:'GOA',quantity})}
  return rows;
}
function buildTotalsModel(entries,now=new Date()){
  const all=entries||[],flying=all.filter(f=>activityRule(f).totals==='flight'),simulators=all.filter(f=>activityRule(f).totals==='simulator');
  const a320Flights=flying.filter(isA320Entry),a320Sims=simulators.filter(isA320Entry);
  return{
    flying,simulators,a320Flights,a320Sims,aircraftTypes:buildAircraftTypeBreakdown(flying,simulators),
    totalFlight:sum(flying,totalFlightMins),totalSimulator:sum(simulators,f=>timeGroupFinalValue(f,'simulatorTime')),a320Flight:sum(a320Flights,totalFlightMins),a320Pic:sum(a320Flights,picMins),a320Sic:sum(a320Flights,sicMins),
    a320FlightInstruction:sum(a320Flights,flightInstrMins),a320SimInstruction:sum(a320Sims,simInstrMins),a320SimulatorTime:sum(a320Sims,f=>timeGroupFinalValue(f,'simulatorTime')),
    rightSeat:sum(flying,f=>f.rhs===true?totalFlightMins(f):0)
  };
}
function buildTotalsProjection(entries,now=new Date()){
  const all=entries||[],model=buildTotalsModel(all,now),flying=model.flying,y=now.getUTCFullYear(),m=now.getUTCMonth();
  const rolling=n=>{const start=startOfDays(n,now);return sum(flying,f=>{const d=dateOnly(f.date);return d>=start&&d<=now?totalFlightMins(f):0})};
  const sixMonthsAgo=new Date(now);sixMonthsAgo.setUTCMonth(sixMonthsAgo.getUTCMonth()-6);sixMonthsAgo.setUTCHours(0,0,0,0);
  const periods=[
    ['This Month — Flight',sum(flying,f=>{const d=dateOnly(f.date);return d.getUTCFullYear()===y&&d.getUTCMonth()===m?totalFlightMins(f):0})],
    ['This Year — Flight',sum(flying,f=>dateOnly(f.date).getUTCFullYear()===y?totalFlightMins(f):0)],
    ['Last 28 days — Flight',rolling(28)],['Last 90 days — Flight',rolling(90)],
    ['Last 6 months — Flight',sum(flying,f=>dateOnly(f.date)>=sixMonthsAgo?totalFlightMins(f):0)],['Last 365 days — Flight',rolling(365)]
  ];
  const standardApproachTypes=['CAT3','ILS','LOC','RNAV','RNAV AR','VOR','VOR/DME','NDB','CRCL','VIS','GOA'],approaches={};
  // LogTen stores approaches as their own rows and can associate them with
  // flight or simulator activity, so totals must inspect the complete log.
  all.forEach(f=>normalizedApproaches(f).forEach(a=>approaches[a.type]=(Number(approaches[a.type])||0)+a.quantity));
  const recorded=Object.keys(approaches),approachTypes=[...standardApproachTypes.filter(x=>recorded.includes(x)),...recorded.filter(x=>!standardApproachTypes.includes(x)).sort()];
  const approachCount=Object.values(approaches).reduce((n,v)=>n+(Number(v)||0),0),dayTakeoffs=sum(all,f=>Number(f.dayTakeoffs)||0),nightTakeoffs=sum(all,f=>Number(f.nightTakeoffs)||0),dayLandings=sum(all,f=>Number(f.dayLandings)||0),nightLandings=sum(all,f=>Number(f.nightLandings)||0);
  return{...model,periods,approachTypes,approaches,approachCount,dayTakeoffs,nightTakeoffs,dayLandings,nightLandings};
}
// The UI consumes projections from this single calculation service; renderers only format its results.
const PilotLogEngine=Object.freeze({
  weeklyRest:(windows,asOf,ongoingRestConfirmed=false)=>weeklyRestProjection(windows,asOf,ongoingRestConfirmed),
  activity:(entry,context=null)=>calculateActivityMetrics(entry,context),
  credit:(entry,context=null)=>creditMinsForEntry(entry,context),
  dutySessions:(entries=null)=>buildDutySessions(entries||entryActivityRows()),
  operationalMonth:month=>operationalEntriesForMonth(month),
  operationalTrips:()=>operationalEntriesForTrips(),
  trip:(start,end,cashAmount=0,entries=null)=>calculateTripMetrics(start,end,cashAmount,entries),
  savedTrip:(trip,entries=null)=>calculateSavedTripProjection(trip,entries),
  savedTrips:(trips=null,entries=null)=>calculateSavedTripProjections(trips,entries),
  payroll:month=>calculatePayrollProjection(month),
  totals:(entries,now=new Date())=>buildTotalsProjection(entries,now),
  logbookStats:(rows,total)=>calculateLogbookStats(rows,total),
  dashboard:(startOffset,count)=>calculateDashboardDutyRows(startOffset,count)
});
function annualLeaveByYear(rows,cutoff=isoDateLocal(new Date())){
  const dates=new Set((rows||[]).filter(r=>canonicalDutyType(r.dutyType)==='Annual Leave'&&!r.rosterHidden&&/^\d{4}-\d{2}-\d{2}$/.test(r.date||'')&&r.date<cutoff).map(r=>r.date));
  const totals=new Map();
  for(const date of dates){const year=date.slice(0,4);totals.set(year,(totals.get(year)||0)+1)}
  return [...totals].sort((a,b)=>b[0].localeCompare(a[0]));
}
async function renderTotals(){
  await ensureAirportDb(false);
  const coreRows=coreActivityRows();
  // Totals include completed Core activities plus every LogTen record, regardless of LogTen locked/open state.
  const totalRows=coreRows.filter(activityIncludedInResults);
  const model=PilotLogEngine.totals(totalRows);
  const leave=annualLeaveByYear(coreRows);
  $('annualLeaveTotals').innerHTML=leave.length?leave.map(([year,days])=>`<div class="stat-row"><span>${esc(year)}</span><b>${days} ${days===1?'day':'days'}</b></div>`).join(''):'<div class="empty">0 days</div>';
  // LogTen locked/open state never excludes its approaches from Totals.
  const approachRows=coreRows.filter(f=>normalizedApproaches(f).length&&activityIncludedInResults(f));
  const approaches={};
  approachRows.forEach(f=>normalizedApproaches(f).forEach(a=>approaches[a.type]=(Number(approaches[a.type])||0)+a.quantity));
  const standardApproachTypes=['CAT3','ILS','LOC','RNAV','RNAV AR','VOR','VOR/DME','NDB','CRCL','VIS','GOA'];
  const recorded=Object.keys(approaches);
  model.approaches=approaches;
  model.approachTypes=[...standardApproachTypes.filter(x=>recorded.includes(x)),...recorded.filter(x=>!standardApproachTypes.includes(x)).sort()];
  model.approachCount=Object.values(approaches).reduce((n,v)=>n+(Number(v)||0),0);
  const settings=timeGroupSettings(),selected=TIME_GROUP_DEFS.filter(({key})=>settings[key].showTotal===true),totalBox=$('timeGroupTotals');
  if(totalBox)totalBox.innerHTML=selected.length?selected.map(({key,label})=>`<div class="stat-row"><span>${esc(label)}</span><b>${fmt(key==='totalDuty'?totalDutyMinsForRows(totalDutyRowsForTotals(coreRows)):sum(totalRows,f=>timeGroupFinalValue(f,key,totalRows)))}</b></div>`).join(''):'<div class="empty">No Time Group selected for Total.</div>';
  if(totalBox)totalBox.innerHTML+=`<div class="stat-row"><span>Total Instrument</span><b>${fmt(sum(totalRows,f=>timeGroupFinalValue(f,'totalInstrument')))}</b></div>`;
  $('fuelBurnedTotal').innerHTML=`<div class="stat-row"><span>Fuel Burned</span><b>${sum(totalRows,f=>Number(f.fuelBurned)||0).toLocaleString('en-US')} kg</b></div>`;
  $('periodTotals').innerHTML=model.periods.map(([n,v])=>`<div class="stat-row"><span>${n}</span><b>${fmt(v)}</b></div>`).join('');
  $('approachTotals').innerHTML=[
    `<div class="stat-row"><span>Day takeoffs</span><b>${model.dayTakeoffs}</b></div>`,
    `<div class="stat-row"><span>Night takeoffs</span><b>${model.nightTakeoffs}</b></div>`,
    `<div class="stat-row"><span>Day landings</span><b>${model.dayLandings}</b></div>`,
    `<div class="stat-row"><span>Night landings</span><b>${model.nightLandings}</b></div>`,
    ...APPROACH_DISPLAY.map(([key,label])=>`<div class="stat-row"><span>${label}</span><b>${model.approaches[key]??0}</b></div>`)
  ].join('');
  $('aircraftBreakdown').innerHTML=model.aircraftTypes.length?model.aircraftTypes.map(r=>{
    const details=[`Total ${fmt(r.flight)}`];if(r.pic)details.push(`PIC ${fmt(r.pic)}`);if(r.sic)details.push(`SIC ${fmt(r.sic)}`);if(r.instruction)details.push(`Instruction ${fmt(r.instruction)}`);if(r.simulator)details.push(`Simulator ${fmt(r.simulator)} (separate)`);if(r.simInstruction)details.push(`Sim instruction ${fmt(r.simInstruction)}`);
    return`<div class="stat-row aircraft-breakdown-row"><span><b>${esc(r.code)}</b><div class="small">ICAO • ${r.variants.map(esc).join(', ')}</div></span><div class="aircraft-breakdown-values">${details.map((x,i)=>`<${i?'span':'b'}>${esc(x)}</${i?'span':'b'}>`).join('')}</div></div>`;
  }).join(''):'<div class="empty">No aircraft flight time recorded.</div>';
}


function buildTripEntryLookup(sourceEntries){
  const fs=sourceEntries||[],byId=new Map(),timed=[];
  for(const f of fs){
    if(f?.id)byId.set(f.id,f);
    const a=tripSequenceStart(f),b=tripSequenceEnd(f);
    if(a&&b)timed.push({entry:f,start:a.getTime(),end:b.getTime()});
  }
  timed.sort((a,b)=>a.start-b.start);
  return{byId,timed};
}
function tripIncludedEntries(t,sourceEntries=null,sourceLookup=null){
  const fs=sourceEntries||operationalEntriesForTrips(),lookup=sourceLookup||null;
  if(Array.isArray(t.includedEntryIds)&&t.includedEntryIds.length){
    const index=lookup?.byId||new Map(fs.map(f=>[f.id,f])),selected=t.includedEntryIds.map(id=>index.get(id)).filter(Boolean).sort((a,b)=>(tripSequenceStart(a)?.getTime()||0)-(tripSequenceStart(b)?.getTime()||0));
    if(selected.length)return selected;
  }
  const s=new Date(t.start),e=new Date(t.end),sm=s.getTime(),em=e.getTime();
  if(lookup?.timed)return lookup.timed.filter(x=>x.start<em&&x.end>sm).map(x=>x.entry);
  return fs.filter(f=>{
    const a=tripSequenceStart(f),b=tripSequenceEnd(f);
    return a&&b&&a<e&&b>s;
  }).sort((a,b)=>(tripSequenceStart(a)?.getTime()||0)-(tripSequenceStart(b)?.getTime()||0));
}
function renderTripInspector(tripId){
  const wrap=$('tripInspectorWrap'),box=$('tripInspector');
  const saved=load(TRIPS_KEY).find(x=>x.id===tripId),sourceEntries=PilotLogEngine.operationalTrips();
  const t=PilotLogEngine.savedTrip(saved,sourceEntries);
  if(!t){wrap.classList.add('hidden');box.innerHTML='';return}
  const entries=tripIncludedEntries(t,sourceEntries),s=new Date(t.start),e=new Date(t.end);
  const rows=entries.map(f=>{
    const a=tripSequenceStart(f),b=tripSequenceEnd(f);
    const dur=a&&b?Math.round((b-a)/60000):0;
    const charge=tripChargeLabel(f);
    return `<div class="rowitem">
      <div>
        <b>${esc(displayDate(f.date))} • ${esc(f.dutyType||'Flight')} ${esc(f.flightNo||'')}</b>
        <div class="small">${esc(f.dep||'')} ${f.arr?'→ '+esc(f.arr):''}</div>
        <div class="small">${charge}</div>
      </div>
      <div class="meta">
        <b>${a?esc(a.toISOString().slice(11,16)):'--:--'}–${b?esc(b.toISOString().slice(11,16)):'--:--'} Z</b>
        <br><span class="small">${fmt(dur)}</span>
      </div>
    </div>`;
  }).join('');
  box.innerHTML=
    `<div class="stat-row"><span>Trip start</span><b>${esc(displayDateTime(s.toISOString().slice(0,16)))} Z</b></div>
     <div class="stat-row"><span>Trip end</span><b>${esc(displayDateTime(e.toISOString().slice(0,16)))} Z</b></div>
     <div class="stat-row"><span>Total trip time</span><b>${fmt(t.trip)}</b></div>
     <div class="stat-row"><span>Duty subtracted</span><b>${fmt(t.duty)}</b></div>
     <div class="stat-row"><span>Layover time</span><b>${fmt(t.layover)}</b></div>
     <div class="stat-row"><span>Layover pay</span><b>${esc(tripDisplayMoney(t).allowanceText)}</b></div>
     <div class="section-title inner">Included entries</div>
     ${rows||'<div class="empty">No entries found for this trip.</div>'}`;
  wrap.classList.remove('hidden');
  wrap.scrollIntoView({behavior:'smooth',block:'start'});
}
function tripFirstDhdDestination(t,sourceEntries=null,sourceLookup=null){
  const entries=tripIncludedEntries(t,sourceEntries,sourceLookup);
  const base=upper(t.base||appSettings().homeBase||'CMN');
  const firstDhd=entries.find(f=>isPositioning(f)&&upper(f.dep)===base&&upper(f.arr)!==base&&upper(f.arr||''));
  if(firstDhd?.arr)return upper(firstDhd.arr);
  const stations=String(t.stations||'').split(',').map(s=>upper(s.trim())).filter(Boolean);
  if(stations.length)return stations[0];
  const firstAway=entries.find(f=>upper(f.arr||'')&&upper(f.arr)!==base);
  if(firstAway?.arr)return upper(firstAway.arr);
  return upper(base||'TRIP');
}
function tripReferenceLabel(t){
  const d=new Date(t.start);
  if(!Number.isFinite(d.getTime()))return '';
  return d.toLocaleDateString('en-GB',{month:'long',year:'numeric'}).toUpperCase();
}
function tripDisplayName(t,sourceEntries=null,sourceLookup=null){
  const dest=tripFirstDhdDestination(t,sourceEntries,sourceLookup);
  const ref=tripReferenceLabel(t);
  return [dest,ref].filter(Boolean).join(' ');
}
function tripDisplayMoney(t){
  const cur=appCurrency(),cash=Number(t.cashAmount||0),cashPart=(()=>{if(!(cash>0))return'';const base=`Cash ${money(cash)} ${upper(t.cashCurrency||'')}`;if(upper(t.cashFxTo||'')===cur&&Number(t.cashConverted)>=0&&Number(t.cashFxRate)>0)return `${base} ≈ ${moneyCurrency(t.cashConverted,cur)} • ${t.cashFxProvisional?'Provisional FX':'FX'} ${displayDate(t.cashFxDate||t.cashFxTripStartDate||'')}`;return `${base}${t.cashFxDate?` • FX frozen ${displayDate(t.cashFxDate)}`:''}`})();
  const allowance=cash>0?0:Number(t.allowance||0),allowanceText=cash>0?moneyCurrency(0,cur):moneyCurrency(allowance,'MAD');return{cashPart,allowanceText};
}
function renderTrips(){
  updateCurrencyUI();
  const saved=load(TRIPS_KEY).sort((a,b)=>String(b.start).localeCompare(String(a.start))),sourceEntries=saved.length?PilotLogEngine.operationalTrips():[],ts=PilotLogEngine.savedTrips(saved,sourceEntries),sourceLookup=buildTripEntryLookup(sourceEntries);
  $('tripList').innerHTML=ts.length?ts.map(t=>{const m=tripDisplayMoney(t);return `<div class="rowitem"><div><b>${esc(tripDisplayName(t,sourceEntries,sourceLookup)||'TRIP')}</b><div class="small">${esc(displayDateTime(t.start))} → ${esc(displayDateTime(t.end))}${t.cashEligible?' • Positioning cash eligible':''}${m.cashPart?` • ${esc(m.cashPart)}`:''}</div></div><div class="meta"><b>${fmt(t.layover)}</b><br><span class="small">${esc(m.allowanceText)} allowance${Number(t.cashAmount||0)>0?' • CASH RECEIVED: LAYOVER PAY 0':''}</span><div class="list-actions"><button class="secondary" data-view-trip="${t.id}">View duties</button><button class="secondary" data-edit-trip="${t.id}">Edit</button><button class="danger" data-delete-trip="${t.id}">Delete</button></div></div></div>`}).join(''):'<div class="empty">No trips yet.</div>';
}
function renderReconciliationReview(){
  const box=$('reconciliationReview');if(!box)return;
  const rows=load(RECONCILIATION_REVIEW_KEY);
  if(!rows.length){box.innerHTML='Da controllare: nessun collegamento volo ambiguo.';return}
  box.innerHTML=`<b>Da controllare — ${rows.length}</b>`+rows.map(row=>`<div>${esc(displayDate(row.date))} • ${esc(row.flightNo||'Flight')} • ${esc(row.dep||'?')} → ${esc(row.arr||'?')} • ${esc(row.schedOut||'--:--')}–${esc(row.schedIn||'--:--')} Z</div>`).join('');
}
function renderTimeGroupSettings(){
  const body=$('timeGroupSettingsBody');if(!body)return;const settings=timeGroupSettings();
  body.innerHTML=TIME_GROUP_DEFS.map(({key,label})=>{const v=settings[key];return`<tr><th scope="row">${esc(label)}</th><td><input type="checkbox" data-time-setting="${key}" data-time-option="showTotal"${v.showTotal?' checked':''} aria-label="Show ${esc(label)} on Total"></td><td><input type="checkbox" data-time-setting="${key}" data-time-option="showCard"${v.showCard?' checked':''} aria-label="Show ${esc(label)} on Card"></td><td><input type="checkbox" data-time-setting="${key}" data-time-option="autoSync"${v.autoSync?' checked':''} aria-label="Auto Sync ${esc(label)}"></td></tr>`}).join('');
}
function saveTimeGroupSettingsFromForm(){
  const values=emptyTimeGroupSettings();document.querySelectorAll('[data-time-setting][data-time-option]').forEach(el=>{const key=el.dataset.timeSetting,option=el.dataset.timeOption;if(values[key]&&option in values[key])values[key][option]=!!el.checked});
  if(contentHash(values)!==contentHash(timeGroupSettings()))saveAppSettings({timeGroups:values});setEntryTypeUI();calcEntry();renderTotals();renderRoster();return values;
}
function renderSettings(){const st=appSettings();$('setHomeBase').value=st.homeBase||'CMN';$('setFlightPrefix').value=st.flightPrefix||'MAC';$('setAircraftPrefix').value=st.aircraftPrefix||'CN-NM';$('setProfileName').value=st.profileName||'';$('setProfileRole').value=st.profileRole||'Captain';$('setNightStart').value=st.nightStart||'18:00';$('setNightEnd').value=st.nightEnd||'05:00';$('setNightPremiumPct').value=st.nightPremiumPct??50;if($('setCurrency'))$('setCurrency').value=appCurrency();if($('setPayrollRegime'))$('setPayrollRegime').value=payrollRegime();if($('setMacDependents'))$('setMacDependents').value=macDependents();$('macDependentsSetting')?.classList.toggle('hidden',payrollRegime()!=='MAC');$('autoSyncEnabled').value=autoSyncEnabled()?'yes':'no';$('cloudEmail').value=localStorage.getItem(LAST_EMAIL_KEY)||$('cloudEmail').value||'';if($('logTenArchiveStatus'))$('logTenArchiveStatus').textContent=logTenArchiveStatusText();updatePrefixUI();updateAppHeader();updateProfilePhotoSettings();updateCurrencyUI();renderTimeGroupSettings();fillPaySettings();renderWeeklyBackupStatus();updateCloudStatus();renderReconciliationReview();ensureAirportDb(false)}
function setEntryLockedUI(locked){$('flightForm').querySelectorAll('input:not(#editId),select,textarea').forEach(el=>el.disabled=!!locked);$('lockEntryBtn').classList.remove('hidden');$('lockEntryBtn').disabled=false;$('lockEntryBtn').textContent=locked?'🔓 Unlock':'🔒 Lock';$('entryLockStatus').textContent=locked?'LOCKED • All entry data are protected from editing.':'Inputs autosave in Core. Lock the entry when all data are final.';$('entryLockStatus').classList.toggle('success',!!locked);if($('coreIdDisplay'))$('coreIdDisplay').textContent=$('editId').value||'';for(const id of ['entryDeleteBtn','entryRosterDeleteBtn'])if($(id))$(id).disabled=!!locked;document.querySelectorAll('[data-counter-shortcut]').forEach(b=>b.disabled=!!locked)}
async function toggleEntryLock(){
  let existing=coreActivityRows().find(x=>x.id===$('editId').value);if(!existing)return;
  if(existing.locked){
    if(!confirm('Unlock this entry and allow editing?'))return;
    const saved=upsertCoreActivity(stamp({...existing,locked:false}));markCloudEdited('activities',saved,'unlock');
    await persistCoreActivities();loadEntryToForm(saved,{rosterActivity:entryReturnContext?.view==='rosterView'});scheduleAutoSync('unlock-entry');return;
  }
  if(!confirm('Lock this entry? Its data will be protected from accidental editing.'))return;
  const suggested=carryCrewToRosterFlight(carryAircraftToAerolineReturn(existing));
  if(contentHash(suggested)!==contentHash(existing)&&!formUserEdited)upsertCoreActivity(stamp(suggested));
  saveEntryDraft();existing=coreActivityRows().find(x=>x.id===existing.id);
  // Locking existing records changes protection, not their operational/source values.
  const saved=existing.activityStatus===ACTIVITY_STATUS.DRAFT?persistEntry(true):upsertCoreActivity(stamp({...existing,locked:true}));
  if(!saved)return;markCloudEdited('activities',saved,'lock');await persistCoreActivities();
  loadEntryToForm(saved,{rosterActivity:entryReturnContext?.view==='rosterView'});show('addView');scheduleAutoSync('lock-entry');
}
const ENTRY_DRAFT_FIELDS=['approachCategory','variant','editId','rosterLinkId','entryDraftId','dutyTypeFlight','date','flightNo','reg','type','courseType','trainerTypeInput','dep','arr','schedOut','schedIn','onDuty','offDuty','dutyStartDate','dutyEndDate','out','off','on','in','blockDisplay','totalFlightTimeDisplay','multiPilotDisplay','picDisplay','picUSDisplay','sicDisplay','ifrTimeDisplay','airTimeDisplay','night','picNightDisplay','simulatorTimeDisplay','sfiSfeDisplay','dualGivenDisplay','dualReceivedDisplay','groundInstructionDisplay','stbyTimeDisplay','creditDisplay','totalDutyDisplay','rolePicCheck','pfCheck','instructionCheck','examinerCheck','simInstructorCheck','simExaminerCheck','rhsCheck','callFromDayOff','picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name','approachType','goArounds','dayTakeoffs','nightTakeoffs','dayLandings','nightLandings','pax','fuelAboard','fuelBurned','fuelMinimum','remarks'];
let entryDraftTimer=null;
function entryDraftPool(){return []}
function clearEntryDraft(){clearTimeout(entryDraftTimer)}
function clearAllEntryDrafts(){clearEntryDraft()}
function renderEntryDraftShelf(){}
let savingCoreInput=false,lastCoreInputSignature='',formUserEdited=false;
function coreFormSignature(){return JSON.stringify(ENTRY_DRAFT_FIELDS.map(id=>{const el=$(id);return el?.type==='checkbox'?!!el.checked:el?.value||''}))}
function saveEntryDraft(){
  if(formLoading||savingCoreInput||!$('editId')?.value)return null;
  const existing=coreActivityRows().find(x=>x.id===$('editId').value);
  if(!existing)return existing;
  if(!formUserEdited||coreFormSignature()===lastCoreInputSignature)return existing;
  if(existing.locked){lastCoreInputSignature=coreFormSignature();formUserEdited=false;syncControlledDutyEdit();persistCoreActivities().catch(error=>console.error('Duty edit save failed',error));return coreActivityRows().find(r=>r.id===existing.id)}
  const signature=coreFormSignature();
  savingCoreInput=true;
  try{
    if(existing.activityStatus!=='DRAFT'&&!validEntryForSilentSave()){
      const forms=operationalDatabase.localForms??={};
      forms[existing.id]={input:Object.fromEntries(ENTRY_DRAFT_FIELDS.map(k=>[k,$(k)?.type==='checkbox'?$(k).checked:$(k)?.value])),time:{...formTimeOverrides},manual:{...formManualFields},sources:{...formFieldSources}};
      lastCoreInputSignature=signature;scheduleFlightPersist();return existing;
    }
    if(operationalDatabase.localForms)delete operationalDatabase.localForms[existing.id];
    const f=collectEntry();
    f.activityStatus=existing.activityStatus;f.completedAt=existing.completedAt||'';
    // Incomplete field text lives on the same activity, never in a second store.
    f.coreInput={};ENTRY_DRAFT_FIELDS.forEach(id=>{const el=$(id);if(el)f.coreInput[id]=el.type==='checkbox'?el.checked:el.value});
    const saved=upsertCoreActivity(f);lastCoreInputSignature=coreFormSignature();formUserEdited=false;markCloudEdited('activities',saved,'edit');
    persistCoreActivities().catch(()=>{$('entryLockStatus').textContent='Saving failed. Keep this page open and try again.'});
    scheduleAutoSync('activity-input');return saved;
  }catch(error){$('entryLockStatus').textContent='Saving failed. Keep this page open and try again.';console.error('Core autosave failed',error);return null}finally{savingCoreInput=false}
}
function trainingCreditSetting(row){
  if(row.dutyType==='Simulator'&&userIsSimTrainer(row))return 'simCredit';
  const role=upper(row.trainerType??row.aerolineTrainerType);
  if(row.dutyType==='Ground Course'&&(role==='GI'||role==='SOD'||upper(row.courseType)==='SOD'))return 'groundCredit';
  return null;
}
function initialTrainingCreditSource(row){
  if(!['Simulator','Ground Course'].includes(row.dutyType)||entryHasTimeValue(row,'credit')||row.creditImported||entryManualField(row,'credit')||entryFieldIsSource(row,'credit'))return row;
  return applyNewTrainingCredit({...row,fieldSources:{...row.fieldSources,credit:'awaiting-role'}});
}
function applyNewTrainingCredit(row){
  if(row.fieldSources?.credit!=='awaiting-role'||entryManualField(row,'credit'))return row;
  const setting=trainingCreditSetting(row);
  if(!setting)return row;
  return {...row,credit:Math.round(paySettings()[setting]*60),fieldSources:{...row.fieldSources,credit:'entry-default'},timeOverrides:{...row.timeOverrides,credit:true}};
}
function beginCoreActivity(type,crewSource=null){
  const id=makeActivityId();$('editId').value=id;$('dutyTypeFlight').value=type;
  const open=null;
  const previous=open?(open.dutyControl.memberIds||[open.id]).map(id=>coreActivityRows().find(f=>f.id===id)).filter(f=>f&&isFlight(f)).at(-1):null;
  const crew=type==='Flight'?crewCarryData(crewSource||previous):null;
  const row={id,recordKind:'activity',activityStatus:'DRAFT',source:'manual',dutyType:type,date:$('date').value||today(),...(type==='Flight'?{manualDutyMode:true,type:upper(crewSource?.type||''),variant:upper(crewSource?.variant||''),rolePic:appSettings().profileRole==='Captain',role:appSettings().profileRole==='Captain'?'PIC':'SIC'}:{}),...(crew?{...crew,crewCarryApplied:true}:{})};
  const initial=initialTrainingCreditSource(row);
  if(initial.fieldSources?.credit)formFieldSources.credit=initial.fieldSources.credit;
  upsertCoreActivity(open?controlledDutyFields(initial,open,open.dutyControl):initial);
  if(open){const root=stamp({...open,dutyControl:{...open.dutyControl,memberIds:[...new Set([...(open.dutyControl.memberIds||[]),open.id,id])]}});upsertCoreActivity(root);markCloudEdited('activities',root,'duty-member');$('onDuty').value=hhmm(new Date(open.dutyControl.start))}
  if(type==='Flight'){$('offDuty').value='';$('totalDutyDisplay').value=''}
  if(crew)applyCrewToForm(crew);
  $('flightForm').classList.remove('hidden');$('activityTypePicker')?.classList.add('hidden');
  setEntryTypeUI();setEntryLockedUI(false);if(crewSource)formUserEdited=true;saveEntryDraft();return id;
}
function showActivityTypePicker(){
  saveEntryDraft();resetEntry(false);show('addView');
  $('flightForm').classList.add('hidden');$('activityTypePicker').classList.remove('hidden');
}
function validEntryForSilentSave(){const dt=$('dutyTypeFlight').value;if(!$('date').value)return false;if((dt==='Flight'||dt==='DHD'||dt==='DHP')&&(!$('dep').value.trim()||!$('arr').value.trim()))return false;return true}
function silentAutosaveExisting(){if(formLoading)return;queueMicrotask(()=>saveEntryDraft())}
function scrollRosterAgendaIntoView(){
  requestAnimationFrame(()=>requestAnimationFrame(()=>{const agenda=$('rosterAgenda');if(agenda)window.scrollTo({top:Math.max(0,window.scrollY+agenda.getBoundingClientRect().top-window.innerHeight*.48),behavior:'smooth'})}));
}
function bindPilotLogSwipeGestures(){
  let addStart=null,calendarStart=null,expiryStart=null;
  document.addEventListener('touchstart',e=>{
    if(e.touches.length!==1)return;
    const p=e.touches[0],stamp={x:p.clientX,y:p.clientY,at:Date.now()};
    if(!$('addView')?.classList.contains('hidden'))addStart=stamp;
    else if(!$('expiryEditorWrap')?.classList.contains('hidden'))expiryStart=stamp;
    else{
      const viewport=e.target.closest('[data-roster-calendar-swipe]'),track=viewport?.querySelector('.roster-calendar-track');
      if(viewport&&track)calendarStart={...stamp,viewport,track,width:viewport.getBoundingClientRect().width,axis:null,lastX:p.clientX,lastAt:Date.now()};
    }
  },{passive:true});
  document.addEventListener('touchmove',e=>{
    if(!calendarStart||e.touches.length!==1)return;
    const p=e.touches[0],dx=p.clientX-calendarStart.x,dy=p.clientY-calendarStart.y;
    if(!calendarStart.axis&&Math.max(Math.abs(dx),Math.abs(dy))>6)calendarStart.axis=Math.abs(dx)>Math.abs(dy)*1.1?'x':'y';
    if(calendarStart.axis!=='x')return;
    e.preventDefault();
    const width=Math.max(1,calendarStart.width),drag=Math.max(-width,Math.min(width,dx));
    calendarStart.lastX=p.clientX;calendarStart.lastAt=Date.now();
    calendarStart.track.classList.add('dragging');
    calendarStart.track.style.transform=`translate3d(calc(-100% + ${drag}px),0,0)`;
  },{passive:false});
  document.addEventListener('touchend',e=>{
    const p=e.changedTouches?.[0];if(!p)return;
    if(addStart){
      const dx=p.clientX-addStart.x,dy=p.clientY-addStart.y,elapsed=Date.now()-addStart.at;addStart=null;
      if(dx>=70&&Math.abs(dx)>Math.abs(dy)*1.25&&elapsed<1200){saveEntryDraft();const ctx=entryReturnContext;setEntryReturnContext(null);if(ctx?.view==='rosterView')show('rosterView');else show('flightsView',{logbookEntryId:$('editId')?.value||''})}
      return;
    }
    if(expiryStart){
      const dx=p.clientX-expiryStart.x,dy=p.clientY-expiryStart.y,elapsed=Date.now()-expiryStart.at;expiryStart=null;
      if(dx>=70&&Math.abs(dx)>Math.abs(dy)*1.25&&elapsed<1200){
        // Form input autosaves continuously; this flushes a last keystroke before returning.
        if($('expiryEditId')?.value)saveExpiryFromForm({silent:true});
        $('expiryEditorWrap').classList.add('hidden');resetExpiryEditor();
      }
      return;
    }
    if(calendarStart){
      const swipe=calendarStart,dx=p.clientX-swipe.x,dy=p.clientY-swipe.y,elapsed=Date.now()-swipe.at;calendarStart=null;
      if(swipe.axis!=='x'||Math.abs(dx)<=Math.abs(dy)){swipe.track.classList.remove('dragging');swipe.track.style.transform='';return}
      e.preventDefault();
      const speed=Math.abs(dx)/Math.max(1,elapsed),commit=Math.abs(dx)>=Math.min(80,Math.max(1,swipe.width)*.22)||speed>.45,direction=dx<0?1:-1;
      suppressRosterCalendarClickUntil=Date.now()+450;
      swipe.track.classList.remove('dragging');
      swipe.track.style.transform=commit?(direction>0?'translate3d(-200%,0,0)':'translate3d(0,0,0)'):'translate3d(-100%,0,0)';
      if(commit)setTimeout(()=>{rosterCalendarMonth=rosterShiftMonth(rosterCalendarMonth,direction);rosterSelectedDate='';renderRoster()},240);
    }
  },{passive:false});
  document.addEventListener('touchcancel',()=>{
    expiryStart=null;
    if(!calendarStart)return;
    calendarStart.track.classList.remove('dragging');calendarStart.track.style.transform='translate3d(-100%,0,0)';calendarStart=null;
  },{passive:true});
}
function syncOffDutyFromScheduleIn(){const current=coreActivityRows().find(f=>f.id===$('editId')?.value)||{dutyType:$('dutyTypeFlight')?.value||'Flight',source:'manual'};if(controlledDuty(current))return;if(isManualFlight(current)){if(requiresManualDutyControl(current)){$('offDuty').value='';delete formFieldSources.offDuty}return}if(formTimeOverrides.offDuty||['aeroline','logten','calendar','screenshot','source','imported'].includes(String(formFieldSources.offDuty||'').toLowerCase()))return;const t=$('schedIn')?.value||'',positioning=isPositioning({dutyType:$('dutyTypeFlight')?.value||''});$('offDuty').value=t?(positioning?t:shiftTime(t,30)):'';formFieldSources.offDuty=t?'calculated':''}
function syncProfileNameWithRole(force=false){
  const st=appSettings(),name=upper(st.profileName||'');if(!name)return;
  const source=$('entrySourceDisplay')?.dataset.source||'manual';if(!force&&source!=='manual')return;
  if($('dutyTypeFlight')?.value!=='Flight')return;
  const target=$('rolePicCheck')?.checked?'picName':'sicName';
  ['picName','sicName'].forEach(id=>{if(id!==target&&aerolineSamePerson($(id)?.value||'',name))$(id).value=''});
  if($(target))$(target).value=name;
}
function profilePicDefault(entry){
  if(!isFlight(entry))return false;
  if(entry.rolePic===true||entry.role==='PIC')return true;
  if(entry.rolePic===false||entry.role==='SIC'||entry.locked||entryManualField(entry,'rolePic')||entryManualField(entry,'role'))return false;
  return appSettings().profileRole==='Captain';
}
function applyProfileDefaultsToEntry(){
  const st=appSettings(),name=upper(st.profileName||''),profileRole=st.profileRole||'Captain';
  if(profileRole==='Captain'){
    $('rolePicCheck').checked=true;$('role').value='PIC';
    if(name&&!$('picName').value)$('picName').value=name;
  }else if(profileRole==='First Officer'){
    $('rolePicCheck').checked=false;$('role').value='SIC';
    if(name&&!$('sicName').value)$('sicName').value=name;
  }
}
function applyAerolineFlightRoleRuleToForm(r){
  if(!r||r.source!=='aeroline')return false;
  const self=upper(appSettings().profileName||''),profileRole=appSettings().profileRole||'Captain',crew=(Array.isArray(r.aerolineCrew)?r.aerolineCrew:[]).map(upper).filter(Boolean);
  if(profileRole==='Captain'&&self){
    $('rolePicCheck').checked=true;$('role').value='PIC';$('picName').value=self;
    if(crew.length===1){$('sicName').value=crew[0];$('soName').value=''}
    else if(crew.length>1){$('sicName').value='';$('soName').value=''}
    if(aerolineSamePerson($('instructorName').value,self))$('instructorName').value='';
    if(r.aerolineLineTraining)$('instructionCheck').checked=true;
    return true;
  }
  if(profileRole==='First Officer'&&self){$('rolePicCheck').checked=false;$('role').value='SIC';$('sicName').value=self;return true}
  return false;
}
function applyRosterCarryToForm(r){
  const c=rosterCarryDefaultsForItem(r);
  if(!c)return false;
  $('reg').value=aircraftIdInput(c.reg||'');
  for(const key of ['type','variant'])if(!$(key).value&&!entryManualField(r,key))$(key).value=upper(c[key]||'');
  $('rolePicCheck').checked=(c.role||$('role').value)==='PIC';$('role').value=$('rolePicCheck').checked?'PIC':'SIC';
  applyCrewToForm(c);
  $('rhsCheck').checked=upper(c.seatPosition||'')==='RIGHT';
  $('pfCheck').checked=c.pf==='yes'||c.pf===true;
  return true;
}

function resetEntry(clearDraft=true){
  formUserEdited=false;lastCoreInputSignature='';clearTimeout(entryDraftTimer);aircraftLookupSeq++;
  $('flightForm').classList.remove('hidden');$('activityTypePicker')?.classList.add('hidden');
  formLoading=true;formTimeOverrides={};formManualFields={};formFieldSources={};
  if(clearDraft)clearEntryDraft();clearEntryRosterActivityContext();
  $('approachType').dataset.loadedValue='';
  const f=$('flightForm');f.reset();$('editId').value='';$('entryDraftId').value=makeId();if($('rosterLinkId'))$('rosterLinkId').value='';
  if($('entrySourceDisplay'))$('entrySourceDisplay').dataset.source='manual';setEntrySourceDisplay('manual');updatePrefixUI();
  $('date').value=today();$('dutyTypeFlight').value='Flight';$('role').value='PIC';$('seatPosition').value='';$('callFromDayOff').checked=false;$('approachType').value='';$('simLocation').value='';
  ['rolePicCheck','pfCheck','instructionCheck','examinerCheck','simInstructorCheck','simExaminerCheck','rhsCheck'].forEach(id=>{if($(id))$(id).checked=false});
  ['dayTakeoffs','nightTakeoffs','dayLandings','nightLandings'].forEach(id=>$(id).value=0);TIME_GROUP_DEFS.forEach(({key})=>{const el=$(TIME_GROUP_INPUT_IDS[key]);if(el)el.value=''});$('blockDisplay').value='';
  applyProfileDefaultsToEntry();$('entryTitle').textContent='Add Duty / Activity';setEntryLockedUI(false);setEntryTypeUI();calcEntry();updateAirportInfo();renderEntryDraftShelf();formLoading=false;
}
function loadEntryToForm(f,options={}){
  f=carryAircraftToAerolineReturn(f);
  f=carryCrewToRosterFlight(f);
  resetEntry(false);formLoading=true;formTimeOverrides={...(f.timeOverrides||{})};formManualFields={...(f.manualFields||{})};formFieldSources={...(f.fieldSources||{})};const rosterActivity=!!options.rosterActivity,canonicalType=canonicalDutyType(f.dutyType||'Flight');
  $('editId').value=f.id;$('entryDraftId').value='';if($('rosterLinkId'))$('rosterLinkId').value=rosterActivity?(f.id||''):'';
  if($('entrySourceDisplay'))$('entrySourceDisplay').dataset.source=f.source||'manual';setEntrySourceDisplay(f.source||'manual');$('callFromDayOff').checked=!!f.callFromDayOff;
  $('dutyTypeFlight').value=canonicalType;$('date').value=f.date||'';$('type').value=upper(f.type||'');$('variant').value=upper(f.variant||'');if($('trainerTypeInput'))$('trainerTypeInput').value=upper(f.trainerType??f.aerolineTrainerType??'');$('dep').value=upper(isSim(f)?(f.dep||f.location||''):(f.dep||''));$('arr').value=upper(f.arr||'');$('schedOut').value=f.schedOut||'';$('schedIn').value=f.schedIn||'';$('onDuty').value=f.onDuty||'';$('offDuty').value=f.offDuty||'';$('out').value=f.out||'';$('off').value=f.off||'';$('on').value=f.on||'';$('in').value=f.in||'';$('approachType').value=f.approachType||f.approaches?.[0]?.type||'';$('approachCategory').value=f.approachCategory||f.approaches?.[0]?.category||'';$('remarks').value=f.remarks||'';$('courseType').value=f.courseType||'';
  $('flightNo').value=flightNoInput(f.flightNo||'');$('reg').value=isSim(f)?upper(f.reg||''):aircraftIdInput(f.reg||'');$('simLocation').value='';setDurationPicker($('blockDisplay'),entryHasTimeValue(f,'block')?numericTime(f.block):null);
  const loadedCrew=crewCarryData(f);CREW_NAME_FIELDS.forEach(id=>{if($(id))$(id).value=upper(loadedCrew[id]||'')});
  if($('goArounds'))$('goArounds').value=f.goArounds??'';$('pax').value=f.pax??'';$('fuelAboard').value=f.fuelAboard??'';$('fuelBurned').value=f.fuelBurned??'';$('fuelMinimum').value=f.fuelMinimum??'';
  $('rolePicCheck').checked=profilePicDefault(f);$('pfCheck').checked=f.pf===true||f.pf==='yes';$('instructionCheck').checked=f.instruction===true;$('examinerCheck').checked=f.examiner===true;$('simInstructorCheck').checked=f.simInstructor===true;$('simExaminerCheck').checked=f.simExaminer===true;$('rhsCheck').checked=f.rhs===true||upper(f.seatPosition||'')==='RIGHT';
  ['dayTakeoffs','nightTakeoffs','dayLandings','nightLandings'].forEach(id=>$(id).value=Number(f[id]||0));
  TIME_GROUP_DEFS.forEach(({key})=>{const el=$(TIME_GROUP_INPUT_IDS[key]);if(el)setDurationPicker(el,entryHasTimeValue(f,key)?numericTime(f[key]):null)});
  if(isSim(f)&&!entryManualField(f,'credit')&&!entryFieldIsSource(f,'credit')&&f.creditImported!==true&&!userIsSimTrainer(f))$('creditDisplay').value='';
  const duty=controlledDuty(f);if(duty){const fields=controlledDutyFields(f,duty.root,duty.control);$('onDuty').value=fields.onDuty;$('offDuty').value=fields.offDuty}
  if(f.coreInput&&f.activityStatus==='DRAFT')ENTRY_DRAFT_FIELDS.forEach(id=>{if(['editId','entryDraftId','rosterLinkId'].includes(id)||duty&&['onDuty','offDuty','totalDutyDisplay'].includes(id))return;const el=$(id);if(el&&f.coreInput[id]!==undefined){if(el.type==='checkbox')el.checked=!!f.coreInput[id];else el.value=f.coreInput[id]}});
  if(requiresManualDutyControl(f)&&!duty){$('offDuty').value='';$('totalDutyDisplay').value=''}
  const approachKey=approachDisplayKey(f.approachType||f.approaches?.[0]?.type||'');
  $('approachType').value=APPROACH_DISPLAY.some(([key])=>key===approachKey)?approachKey:'';
  $('approachType').dataset.loadedValue=$('approachType').value;
  const draft=operationalDatabase.localForms?.[f.id];
  if(draft){for(const [id,value] of Object.entries(draft.input)){const el=$(id);if(el){if(el.type==='checkbox')el.checked=value;else el.value=value}}
    formTimeOverrides={...draft.time};formManualFields={...draft.manual};formFieldSources={...draft.sources};}
  setEntryTypeUI();calcEntry();formLoading=false;
  if(rosterActivity&&rosterCanEditLockedActivity({...f,dutyType:canonicalType})){
    entryRosterActivityContext={id:f.id,type:canonicalType,preserveLocked:!!f.locked};$('lockEntryBtn').classList.remove('hidden');$('entryRosterDeleteBtn').classList.remove('hidden');$('entryTitle').textContent=f.locked?'View locked activity':`Edit roster ${canonicalType}`;setRosterActivityEditorUI(canonicalType);setEntryLockedUI(!!f.locked);
  }else{$('entryTitle').textContent=f.locked?'View locked activity':'Edit activity';setEntryLockedUI(!!f.locked)}
  updateDutyControls();lastCoreInputSignature=coreFormSignature();updateAirportInfo();updateDelayUI();updateDutyControls();
  if(!f.locked&&isFlight(f)&&f.reg&&(!$('type').value.trim()||!$('variant').value.trim()))scheduleAircraftRegistrationLookup();
}


function confirmPossibleDayOffPaid(){
  const rid=$('rosterLinkId')?.value||'';if(!rid||$('callFromDayOff')?.checked)return true;
  const r=coreActivityRows().find(x=>x.id===rid);if(!r?.aerolinePossibleDayOffPaid||r.dayOffPaidPromptDismissed)return true;
  const reason=r.aerolineDayOffPaidSignal||'AeroLINE roster change';
  const yes=confirm(`Possible Day Off Paid detected for ${displayDate(r.date)}.\n\nSignal: ${reason}.\n\nMark “Called to work from a scheduled day off”?`);
  if(yes){$('callFromDayOff').checked=true;r.dayOffPaidPromptConfirmed=true;r._syncRev=recordRevision(r)+1;r._updatedAt=new Date().toISOString();upsertCoreActivity(r);markCloudEdited('activities',r,'edit');return true}
  r.dayOffPaidPromptDismissed=true;r._syncRev=recordRevision(r)+1;r._updatedAt=new Date().toISOString();upsertCoreActivity(r);markCloudEdited('activities',r,'edit');return true;
}
function collectEntry(lockedOverride=null){
  syncControlledDutyEdit();
  const c=calcEntry(),dutyType=canonicalDutyType($('dutyTypeFlight').value||'Flight'),id=$('editId').value||makeActivityId(),existing=entryActivityRows().find(x=>x.id===id),delayMinutes=dutyType==='Flight'?formDelayMinutes():0;
  const sim=dutyType==='Simulator',flight=dutyType==='Flight',ground=dutyType==='Ground Course',standby=dutyType==='STBY';
  const timeValues={};TIME_GROUP_DEFS.forEach(({key})=>{const value=optionalDuration($(TIME_GROUP_INPUT_IDS[key])?.value);if(value!==null)timeValues[key]=value});
  for(const key of Object.keys(LOGTEN_TIME_COLUMNS)){
    if(existing&&Object.prototype.hasOwnProperty.call(existing,key)&&!formManualFields[key]&&!formTimeOverrides[key]&&(entryFieldSource(existing,key)==='logten'||existing.source==='logten'))timeValues[key]=existing[key];
  }
  const blockValue=optionalDuration($('blockDisplay')?.value),timeOverrides={...(existing?.timeOverrides||{}),...formTimeOverrides},manualFields={...(existing?.manualFields||{}),...formManualFields},fieldSources={...(existing?.fieldSources||{}),...formFieldSources};
  Object.keys(manualFields).forEach(k=>{if(manualFields[k]===true)fieldSources[k]='manual'});Object.keys(timeOverrides).forEach(k=>{if(timeOverrides[k]===true&&fieldSources[k]!=='entry-default')fieldSources[k]='manual'});
  if(!timeOverrides.onDuty&&$('onDuty').value)fieldSources.onDuty=existing?.fieldSources?.onDuty||(($('schedOut').value&&$('onDuty').value===shiftTime($('schedOut').value,-60))?'calculated':fieldSources.onDuty||'source');
  if(!timeOverrides.offDuty&&$('offDuty').value)fieldSources.offDuty=existing?.fieldSources?.offDuty||(($('schedIn').value&&$('offDuty').value===shiftTime($('schedIn').value,30))?'calculated':fieldSources.offDuty||'source');
  return stamp(applyDutyToEntry({
    ...existing,...timeValues,
    id,dutyType,date:$('date').value,flightNo:flight?composeFlightNo($('flightNo').value):existing?.flightNo||'',
    reg:sim?upper($('reg').value):composeAircraftId($('reg').value),type:upper($('type').value),variant:upper($('variant')?.value||''),location:flight?'':existing?.location||'',
    dep:upper($('dep').value),arr:sim?existing?.arr||'':upper($('arr').value),schedOut:$('schedOut').value,schedIn:$('schedIn').value,schedBlock:c.schedBlock,
    dutyActivityEnd:!flight&&$('offDuty').value?$('offDuty').value:existing?.dutyActivityEnd,onDuty:$('onDuty').value,offDuty:$('offDuty').value,out:flight?$('out').value:existing?.out||'',off:flight?$('off').value:existing?.off||'',on:flight?$('on').value:existing?.on||'',in:flight?$('in').value:existing?.in||'',
    block:(flight||ground||standby)&&blockValue!==null?blockValue:flight?c.block:existing?.block??null,airTime:flight?c.airTime:existing?.airTime??null,credit:optionalDuration($('creditDisplay')?.value),totalDuty:optionalDuration($('totalDutyDisplay')?.value),timeOverrides,manualFields,fieldSources,
    rolePic:flight&&!!$('rolePicCheck')?.checked,role:flight?formRoleValue(existing||{}):'',rhs:flight&&!!$('rhsCheck')?.checked,seatPosition:flight&&$('rhsCheck')?.checked?'Right':flight?'Left':'',callFromDayOff:!!$('callFromDayOff')?.checked,
    instruction:flight&&!!$('instructionCheck')?.checked,examiner:flight&&!!$('examinerCheck')?.checked,simInstructor:sim&&!!$('simInstructorCheck')?.checked,simExaminer:sim&&!!$('simExaminerCheck')?.checked,
    sim:sim?'yes':'no',ifr:'',
    dayTakeoffs:sim?0:Number($('dayTakeoffs').value||0),nightTakeoffs:sim?0:Number($('nightTakeoffs').value||0),dayLandings:sim?0:Number($('dayLandings').value||0),nightLandings:sim?0:Number($('nightLandings').value||0),
    courseType:upper($('courseType')?.value||''),trainerType:ground?upper($('trainerTypeInput')?$('trainerTypeInput').value:existing?.trainerType||existing?.aerolineTrainerType||''):(existing?.trainerType||''),aerolineTrainerType:existing?.aerolineTrainerType||'',picName:upper($('picName')?.value||''),sicName:upper($('sicName')?.value||''),soName:upper($('soName')?.value||''),instructorName:upper($('instructorName')?.value||''),examinerName:upper($('examinerName')?$('examinerName').value:existing?.examinerName||''),
    purserName:upper($('purserName')?.value||''),attendant1Name:upper($('attendant1Name')?.value||''),attendant2Name:upper($('attendant2Name')?.value||''),attendant3Name:upper($('attendant3Name')?.value||''),cabinCrew:[$('attendant1Name')?.value,$('attendant2Name')?.value,$('attendant3Name')?.value].map(upper),observerCrew:Array.isArray(existing?.observerCrew)?existing.observerCrew:[],studentName:existing?.studentName||'',
    pax:$('pax')?.value===''?null:Number($('pax').value),fuel:existing?.fuel??null,fuelAboard:$('fuelAboard')?.value===''?null:Number($('fuelAboard').value),fuelBurned:$('fuelBurned')?.value===''?null:Number($('fuelBurned').value),fuelMinimum:$('fuelMinimum')?.value===''?null:Number($('fuelMinimum').value),customNote:existing?.customNote,goArounds:$('goArounds')?($('goArounds').value===''?null:Math.max(0,Math.trunc(Number($('goArounds').value)||0))):existing?.goArounds??null,logtenCustom:existing?.logtenCustom,creditImported:existing?.creditImported===true,
    pf:flight&&$('pfCheck')?.checked?'yes':'no',approachCategory:sim?'':$('approachCategory')?.value||'',approachType:existing&&$('approachType').value===$('approachType').dataset.loadedValue?existing.approachType:upper($('approachType').value),approaches:existing&&$('approachType').value===$('approachType').dataset.loadedValue?existing.approaches:($('approachType').value?[{type:upper($('approachType').value),quantity:1}]:[]),delayMinutes,remarks:$('remarks').value.trim(),
    locked:lockedOverride===null?!!existing?.locked:!!lockedOverride,source:existing?.source||$('entrySourceDisplay')?.dataset.source||'manual',manualOverride:true,
    recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.COMPLETED,
    completedAt:existing?.completedAt||new Date().toISOString()
  }));
}
function persistEntry(lockIt=false){
  const dutyType=canonicalDutyType($('dutyTypeFlight').value||'Flight');if(!$('date').value){alert('Please enter the date.');return false}
  if((dutyType==='Flight'||dutyType==='DHD'||dutyType==='DHP')&&(!$('dep').value.trim()||!$('arr').value.trim())){alert('Please enter From and To.');return false}
  const id=$('editId').value,fs=entryActivityRows(),existing=id?fs.find(x=>x.id===id):null,rosterEdit=!!(existing&&entryRosterActivityContext?.id===existing.id&&rosterCanEditLockedActivity(existing));
  if(existing?.locked&&!lockIt&&!rosterEdit){alert('This entry is locked. Unlock it before editing.');return false}
  if(existing&&existing.activityStatus!=='DRAFT'&&!lockIt&&!formUserEdited)return existing;
  const f=collectEntry(lockIt?true:(rosterEdit?null:false)),saved=upsertCoreActivity(f);$('editId').value=saved.id;markCloudEdited('activities',saved,lockIt?'lock':(existing?'edit':'create'));
  reconcileAllDuties();refreshEntrySuggestions();saveEntryDraft();scheduleAutoSync(lockIt?'lock-entry':'save-entry');return saved;
}



function runRosterLogbookIntegrityCheck(){
  const activities=coreActivityRows(),flights=activities.filter(isFlight),logbook=activities.filter(isCompletedLogbookEntry),ids=new Set(),duplicateIds=[],legacyFields=[],semanticDuplicates=[];
  for(const row of activities){
    if(ids.has(row.id))duplicateIds.push(row);else ids.add(row.id);
    const found=Object.values(LEGACY_ACTIVITY_FIELDS).filter(key=>Object.prototype.hasOwnProperty.call(row,key));if(found.length)legacyFields.push({row,fields:found});
  }
  const flightBuckets=new Map();
  for(const row of flights){
    const digits=rawFlightDigits(row.flightNo),key=digits&&row.date&&row.dep&&row.arr?`${row.date}|${digits}|${upper(row.dep)}|${upper(row.arr)}`:`${row.date||''}|${upper(row.dep)}|${upper(row.arr)}`;
    const bucket=flightBuckets.get(key)||[];
    for(const candidate of bucket)if(unifiedFlightMatch(candidate,row))semanticDuplicates.push([candidate,row]);
    bucket.push(row);flightBuckets.set(key,bucket);
  }
  const logbookIds=new Set(logbook.map(row=>row.id));
  const invalidLogbook=logbook.filter(x=>!activityIncludedInResults(x)&&x.activityStatus!=='DRAFT'),missingCompleted=flights.filter(x=>activityIncludedInResults(x)&&!logbookIds.has(x.id));
  const tripBuckets=new Map();
  for(const trip of load(TRIPS_KEY)||[]){
    if(!trip.start||!trip.end)continue;
    const key=JSON.stringify([trip.start,trip.end,upper(trip.stations||'').trim()]);
    if(!tripBuckets.has(key))tripBuckets.set(key,[]);
    tripBuckets.get(key).push(trip);
  }
  const duplicateTrips=[...tripBuckets.values()].filter(group=>group.length>1);
  const issues=duplicateTrips.length+duplicateIds.length+legacyFields.length+semanticDuplicates.length+invalidLogbook.length+missingCompleted.length;
  const result={checkedAt:new Date().toISOString(),activities:activities.length,flights:flights.length,logbook:logbook.length,duplicateTrips,duplicateIds,legacyFields,semanticDuplicates,invalidLogbook,missingCompleted};
  const status=$('integrityStatus'),out=$('integrityResults');
  if(status)status.textContent=`${activities.length} Core activities checked • ${flights.length} flight records • ${logbook.length} included Logbook records • ${issues} issue(s)`;
  if(out){
    if(!issues){out.innerHTML='<strong>PASS</strong> — Roster and Logbook are functional filters of the same Core activity records.'}
    else out.innerHTML=`<strong>REVIEW</strong> — duplicate IDs ${duplicateIds.length} • legacy view fields ${legacyFields.length} • duplicate flight records ${semanticDuplicates.length} • duplicate trip groups ${duplicateTrips.length} • invalid Logbook filters ${invalidLogbook.length+missingCompleted.length}`;
  }
  return result;
}

function showIntegrityRecord(row,trip){
  const list=$('integrityIssueList');list.replaceChildren();
  const heading=document.createElement('h4');heading.textContent=(trip?'Trip ':'Activity ')+row.id;
  const details=document.createElement('pre');details.textContent=JSON.stringify(row,null,2);
  list.append(heading,details);$('integrityDialog').showModal();
}
function reviewIntegrity(){
  const report=runRosterLogbookIntegrityCheck(),dialog=$('integrityDialog'),list=$('integrityIssueList');
  list.replaceChildren();
  function addGroup(title,rows,trip=false){
    if(!rows.length)return;
    const heading=document.createElement('h4');heading.textContent=title;list.append(heading);
    for(const row of rows){
      const button=document.createElement('button');
      button.type='button';button.className='secondary';
      button.textContent=trip?[row.start,row.end,row.stations].filter(Boolean).join(' · '):[row.date,row.flightNo||row.dutyType,row.dep,row.arr].filter(Boolean).join(' · ');
      button.addEventListener('click',()=>{
        dialog.close();
        showIntegrityRecord(row,trip);
      });list.append(button);
    }
  }
  report.semanticDuplicates.forEach(pair=>addGroup('Possible duplicate flight',pair));
  report.duplicateTrips.forEach(group=>addGroup('Possible duplicate trip',group,true));
  addGroup('Duplicate ID',report.duplicateIds);
  addGroup('Legacy fields',report.legacyFields.map(x=>x.row));
  addGroup('Logbook inclusion',[...report.invalidLogbook,...report.missingCompleted]);
  if(!list.childElementCount)list.textContent='No issues found.';
  dialog.showModal();
}

const CORE_105_RECOVERY_KEY='pilotlog10_5_recovery_bridge_v1';
async function repairCore105FromLegacySources(){
  if(localStorage.getItem(CORE_105_RECOVERY_KEY)==='done')return{changed:0,recovered:0,trips:0};
  let changed=0,recovered=0,tripRecovered=0;
  const current=coreActivityRows();
  const legacyEntries=[];
  try{
    const local=legacyLocalFlights();
    if(Array.isArray(local))legacyEntries.push(...local);
  }catch{}
  try{
    const idb=await readLegacyFlightStore();
    if(Array.isArray(idb))legacyEntries.push(...idb);
  }catch{}
  /* Every row in the legacy Entries store was a saved operational entry, not merely a roster plan. */
  const recoveredEntries=legacyEntries.filter(Boolean).map(raw=>{
    const r=normalizeCoreActivity({...raw,activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.COMPLETED},'entries');
    r.activityStatus=ACTIVITY_STATUS.COMPLETED;
    if(!r.completedAt)r.completedAt=r._updatedAt||new Date().toISOString();
    return r;
  });
  if(recoveredEntries.length){
    const before=JSON.stringify(normalizeCoreActivityRecords(current));
    const merged=normalizeCoreActivityRecords([...current,...recoveredEntries]);
    const after=JSON.stringify(merged);
    if(before!==after){commitCoreActivities(merged);changed++;recovered=recoveredEntries.length}
  }
  /* Repair already-migrated rows even when the legacy Entry snapshot is unavailable. */
  const repaired=coreActivityRows().map(row=>{
    const r={...row};
    if(r.activityStatus==='DRAFT')return r;
    const sourceName=String(r.source||'').toLowerCase();
    const type=rawCanonicalType(r.dutyType);
    const hasActualFlightTimes=type==='Flight'&&!!(r.out||r.off||r.on||r.in||Number(r.block)>0||Number(r.airTime)>0);
    const hasCompletedSim=type==='Simulator'&&(Number(r.simulatorTime)>0||r.out||r.in||r.onDuty||r.offDuty);
    const hasSavedNonFlight=['DHD','DHP','Ground Course','STBY'].includes(type)&&!!(r.manualOverride||r.completedAt||r.locked||r.onDuty||r.offDuty||r.dep||r.arr||r.location||r.courseType||Number(r.credit)>0||r.callFromDayOff);
    if(sourceName==='logten'){
      const wanted=r.locked?ACTIVITY_STATUS.COMPLETED:ACTIVITY_STATUS.OPEN;
      if(r.activityStatus!==wanted){r.activityStatus=wanted;changed++}
      if(!r.locked&&r.completedAt){delete r.completedAt;changed++}
      if(r.locked&&!r.completedAt)r.completedAt=r._updatedAt||new Date().toISOString();
      return r;
    }
    const savedOperational=!!r.manualOverride||!!r.completedAt||!!r.locked;
    if(r.activityStatus!==ACTIVITY_STATUS.COMPLETED&&(savedOperational||hasActualFlightTimes||hasCompletedSim||hasSavedNonFlight)){
      r.activityStatus=ACTIVITY_STATUS.COMPLETED;
      if(!r.completedAt)r.completedAt=r._updatedAt||new Date().toISOString();
      changed++;
    }
    return r;
  });
  if(changed||recovered)commitCoreActivities(repaired);
  /* Trips were stored separately in 10.0. If the new DB has none/fewer, merge the legacy values back in. */
  try{
    const legacyTrips=legacyLocalJson(TRIPS_KEY,[]),currentTrips=databaseValue(TRIPS_KEY,[]);
    if(Array.isArray(legacyTrips)&&legacyTrips.length){
      const byId=new Map((Array.isArray(currentTrips)?currentTrips:[]).filter(Boolean).map(t=>[t.id||`${t.start||''}|${t.end||''}|${t.stations||''}`,t]));
      for(const t of legacyTrips.filter(Boolean)){
        const k=t.id||`${t.start||''}|${t.end||''}|${t.stations||''}`;
        const prior=byId.get(k);
        if(!prior){byId.set(k,t);tripRecovered++}
        else byId.set(k,{...t,...prior});
      }
      if(tripRecovered){operationalDatabase.values[TRIPS_KEY]=[...byId.values()];changed++}
    }
  }catch{}
  if(changed)await persistCoreActivities();
  localStorage.setItem(CORE_105_RECOVERY_KEY,'done');
  return{changed,recovered,tripRecovered};
}


const CORE_107_SIM_CLASS_REPAIR_KEY='pilotlog10_7_sim_instruction_repair_v1';
async function repairLogTenSimulatorInstruction107(){
  localStorage.setItem(CORE_107_SIM_CLASS_REPAIR_KEY,'done');
  return{changed:0};
}



const CORE_107_SUPPLEMENTAL_REPAIR_KEY='pilotlog10_7_supplemental_repair_v1';
async function repairCore107SupplementalFromLegacy(){
  if(localStorage.getItem(CORE_107_SUPPLEMENTAL_REPAIR_KEY)==='done')return{changed:0};
  const legacy=[];
  try{const a=legacyLocalFlights();if(Array.isArray(a))legacy.push(...a)}catch{}
  try{const a=await readLegacyFlightStore();if(Array.isArray(a))legacy.push(...a)}catch{}
  if(!legacy.length){localStorage.setItem(CORE_107_SUPPLEMENTAL_REPAIR_KEY,'done');return{changed:0}}
  const supplemental=['approaches','approachType','pax','fuelAboard','fuelBurned','fuelMinimum','pic','sic','purserName','cabinCrew','observerCrew','studentName','customNote','goArounds','logtenCustom','sfiSfe','dualGiven','trainerType','aerolineTrainerType','courseType','location','delayMinutes'];
  const keyOf=r=>[String(r?.date||''),rawCanonicalType(r?.dutyType||'Flight'),rawFlightDigits(r?.flightNo||''),upper(r?.dep||''),upper(r?.arr||''),String(r?.schedOut||''),String(r?.schedIn||'')].join('|');
  const byUid=new Map(),byId=new Map(),byKey=new Map();
  for(const raw of legacy.filter(Boolean)){
    if(raw.logtenUniqueId)byUid.set(String(raw.logtenUniqueId),raw);
    if(raw.id)byId.set(String(raw.id),raw);
    const k=keyOf(raw);if(k&&!byKey.has(k))byKey.set(k,raw);
  }
  let changed=0;
  const repaired=coreActivityRows().map(row=>{
    const r={...row};
    const legacyRow=(r.logtenUniqueId&&byUid.get(String(r.logtenUniqueId)))||byId.get(String(r.id||''))||byKey.get(keyOf(r));
    if(!legacyRow)return r;
    for(const field of supplemental){
      const hasCurrent=Object.prototype.hasOwnProperty.call(r,field);
      const current=r[field],incoming=legacyRow[field];
      const incomingUseful=Array.isArray(incoming)?incoming.length>0:incoming!==undefined&&incoming!==null&&incoming!=='';
      if(!incomingUseful)continue;
      if((r.locked||entryManualField(r,field))&&hasCurrent)continue;
      const currentBlank=Array.isArray(current)?current.length===0:current===undefined||current===null||current==='';
      if(!hasCurrent||currentBlank){r[field]=incoming;changed++}
    }
    if(Array.isArray(r.approaches)&&r.approaches.length&&!r.approachType)r.approachType=upper(r.approaches[0]?.type||'');
    return r;
  });
  if(changed){commitCoreActivities(repaired);await persistCoreActivities()}
  localStorage.setItem(CORE_107_SUPPLEMENTAL_REPAIR_KEY,'done');
  return{changed};
}

document.addEventListener('DOMContentLoaded',async()=>{
  const startupTime=Date.now();
  try{await initFlightStore()}catch(error){
    document.body.textContent='PilotLog: impossibile aprire i dati salvati. Puoi riprovare riaprendo l’app oppure ripristinare un backup JSON. Nessun dato è stato sostituito.';
    const input=document.createElement('input');input.type='file';input.accept='.json,application/json';input.setAttribute('aria-label','Ripristina backup PilotLog');document.body.appendChild(input);
    input.addEventListener('change',async()=>{const file=input.files?.[0];if(!file)return;try{const restored=validateFullBackupPayload(JSON.parse(await file.text()));if(!confirm('Sostituire i dati locali con questo backup?'))return;await writeFlightStore(normalizeOperationalDatabase(restored.database));location.reload()}catch(e){alert('Ripristino non completato: '+e.message)}});
    return;
  }
  if(!localStorage.getItem(SYNC_OWNER)&&Number(localStorage.getItem(CLOUD_BASE_REV_KEY))>0&&cloudStoredSession()?.user?.id)localStorage.setItem(SYNC_OWNER,cloudStoredSession().user.id);
  if(localStorage.getItem('pilotlog7_sync_client_protocol')!=='162'){
    localStorage.removeItem(SYNC_BLOCK);localStorage.removeItem('pilotlog7_sync_failure_16');
    localStorage.setItem('pilotlog7_sync_client_protocol','162');
  }
  legacyDirty=localStorage.getItem(CLOUD_DIRTY_KEY)==='1';
  try{bootKnown=operationalDatabase.preSync162?.known||baseline(recordMap(operationalDatabase))}
  catch(error){localStorage.setItem(SYNC_BLOCK,error.message);localStorage.setItem('pilotlog7_sync_failure_16',error.message)}
  if(operationalDatabase.preSync162)legacyDirty=operationalDatabase.preSync162.legacyDirty;
  syncDirty();
  for(const group of document.querySelectorAll('details.settings-group')){
    group.addEventListener('toggle',()=>{
      if(group.open)for(const other of document.querySelectorAll('details.settings-group[open]')){
        if(other!==group)other.open=false;
      }
    });
  }
  bindPilotLogSwipeGestures();
  document.querySelector('.nav').addEventListener('click',e=>{const b=e.target.closest('button[data-view]');if(b){e.preventDefault();if(b.dataset.view==='addView')showActivityTypePicker();else show(b.dataset.view)}});
  document.addEventListener('change',e=>{
    if(e.target.id==='rosterMonthSelect'){
      if(/^\d{4}-\d{2}$/.test(e.target.value)){rosterCalendarMonth=e.target.value;rosterSelectedDate='';renderRoster()}return;
    }
    const el=e.target.closest('.pay-dayoff-remark');
    if(!el)return;
    const month=$('payrollMonth').value||monthNow();
    if(monthExtras(month).validated)return;
    saveDayOffRemark(month,el.dataset.dayoffDate,el.value);
    scheduleAutoSync('payroll-dayoff-remark');
  });
  $('runIntegrityCheckBtn')?.addEventListener('click',reviewIntegrity);
  $('integrityClose').addEventListener('click',()=>$('integrityDialog').close());
  $('runIntegrityCheckBtn').closest('details').addEventListener('toggle',event=>{if(event.target.open)runRosterLogbookIntegrityCheck()});
  $('deepResetBtn')?.addEventListener('click',deepFactoryResetPilotLog);
  $('deepLocalResetBtn')?.addEventListener('click',deepLocalResetPilotLog);
  $('logbookAddBtn').addEventListener('click',()=>{setEntryReturnContext(null);showActivityTypePicker()});
  $('activityTypePicker').addEventListener('click',e=>{const b=e.target.closest('[data-new-activity]');if(b)beginCoreActivity(b.dataset.newActivity)});
  $('entryDeleteBtn').addEventListener('click',async()=>{const id=$('editId').value,f=coreActivityRows().find(x=>x.id===id);if(!f||f.locked)return;if(!confirm('Delete this activity?'))return;markCloudDeleted('activities',f,{manual:true});removeCoreActivities(x=>x.id===id,{manual:true});$('editId').value='';await persistCoreActivities();show('flightsView');scheduleAutoSync('delete-activity')});
  ['dutyStartDate','dutyEndDate'].forEach(id=>$(id).addEventListener('change',()=>{formTimeOverrides[id==='dutyStartDate'?'onDuty':'offDuty']=true;saveEntryDraft();calcEntry();updateDutyControls()}));$('dutyOnBtn').addEventListener('click',dutyOn);$('dutyOffBtn').addEventListener('click',dutyOff);
  document.addEventListener('keydown',e=>{if((e.key!=='Enter'&&e.key!==' ')||e.target.closest('button,input,select,textarea'))return;const row=e.target.closest('[data-edit-flight]');if(!row)return;e.preventDefault();row.click()});
  $('logbookSearch')?.addEventListener('input',e=>{logbookSearchQuery=e.target.value||'';logbookRenderLimit=LOGBOOK_RENDER_STEP;renderEntriesSafe();if(logbookSearchQuery.trim().length>=2&&!airportDbLoaded)ensureAirportDb(false).then(()=>renderEntriesSafe()).catch(()=>{})});
  $('clearLogbookSearch')?.addEventListener('click',()=>{logbookSearchQuery='';logbookRenderLimit=LOGBOOK_RENDER_STEP;if($('logbookSearch'))$('logbookSearch').value='';renderEntriesSafe();$('logbookSearch')?.focus()});
  $('lockAllEntriesBtn')?.addEventListener('click',()=>lockAllLogbookEntries().catch(err=>alert('Lock all entries failed: '+err.message)));
  $('entryBackBtn').addEventListener('click',()=>{
    saveEntryDraft();
    const ctx=entryReturnContext;setEntryReturnContext(null);
    if(ctx?.view==='flightsView')show('flightsView',{logbookEntryId:ctx.entryId||$('editId').value});
    else if(ctx?.view==='rosterView')show('rosterView');
    else show('flightsView');
  });
  $('entryRosterDeleteBtn')?.addEventListener('click',deleteRosterEntryActivity);
  bindExpiryEvents();
  document.querySelectorAll('[data-upper]').forEach(el=>el.addEventListener('input',()=>{const p=el.selectionStart;el.value=CREW_NAME_FIELDS.includes(el.id)||el.id==='expiryEndorsedBy'?String(el.value||'').toUpperCase():upper(el.value);try{el.setSelectionRange(p,p)}catch{}}));
  $('flightForm').addEventListener('click',e=>{const button=e.target.closest('[data-counter-shortcut]');if(!button)return;const input=$(button.dataset.counterShortcut);if(!input||input.disabled)return;input.value=Number(input.value||0)>0?'0':'1';input.dispatchEvent(new Event('input',{bubbles:true}));calcEntry();});
  for(const event of ['input','change'])$('flightForm').addEventListener(event,()=>{if(!formLoading)formUserEdited=true},true);
  $('flightForm').addEventListener('input',silentAutosaveExisting);
  $('flightForm').addEventListener('change',silentAutosaveExisting);
  ['dep','arr'].forEach(id=>$(id).addEventListener('change',async()=>{await updateAirportInfo();calcEntry();await calcNightForForm();silentAutosaveExisting()}));
  $('date').addEventListener('change',async()=>{calcEntry();await calcNightForForm();silentAutosaveExisting()});
  const manualFieldMap={date:'date',flightNo:'flightNo',reg:'reg',type:'type',variant:'variant',courseType:'courseType',trainerTypeInput:'trainerType',dep:'dep',arr:'arr',schedOut:'schedOut',schedIn:'schedIn',onDuty:'onDuty',offDuty:'offDuty',out:'out',off:'off',on:'on',in:'in',approachType:'approachType',approachCategory:'approachCategory',remarks:'remarks',picName:'picName',sicName:'sicName',soName:'soName',instructorName:'instructorName',examinerName:'examinerName',purserName:'purserName',attendant1Name:'attendant1Name',attendant2Name:'attendant2Name',attendant3Name:'attendant3Name',pax:'pax',goArounds:'goArounds',fuelAboard:'fuelAboard',fuelBurned:'fuelBurned',fuelMinimum:'fuelMinimum',dayTakeoffs:'dayTakeoffs',nightTakeoffs:'nightTakeoffs',dayLandings:'dayLandings',nightLandings:'nightLandings'};
  $('flightForm')?.addEventListener('input',e=>{if(formLoading)return;const key=manualFieldMap[e.target?.id];if(key){formManualFields[key]=true;formFieldSources[key]='manual'}});
  $('flightForm')?.addEventListener('change',e=>{if(formLoading)return;const map={dutyTypeFlight:'dutyType',rolePicCheck:'rolePic',pfCheck:'pf',instructionCheck:'instruction',examinerCheck:'examiner',simInstructorCheck:'simInstructor',simExaminerCheck:'simExaminer',rhsCheck:'rhs',callFromDayOff:'callFromDayOff',...manualFieldMap},key=map[e.target?.id];if(key){formManualFields[key]=true;formFieldSources[key]='manual'}});
  ['out','off','on','in','schedOut','schedIn','onDuty','offDuty','approachType'].forEach(id=>$(id).addEventListener('input',async()=>{if(!formLoading&&(id==='onDuty'||id==='offDuty'))formTimeOverrides[id]=true;if(id==='schedOut')syncOnDutyFromScheduleOut();if(id==='schedIn')syncOffDutyFromScheduleIn();calcEntry();if(['out','off','on','in'].includes(id))await calcNightForForm();silentAutosaveExisting()}));
  $('out').addEventListener('change',()=>{silentAutosaveExisting()});
  $('reg')?.addEventListener('input',scheduleAircraftRegistrationLookup);
  $('reg')?.addEventListener('change',()=>applyAircraftRegistrationLookup());
  ['rolePicCheck','instructionCheck','examinerCheck','simInstructorCheck','simExaminerCheck','rhsCheck'].forEach(id=>$(id)?.addEventListener('change',()=>{if(id==='rolePicCheck')syncProfileNameWithRole(true);calcEntry();silentAutosaveExisting()}));
  $('simTrainerCheck')?.addEventListener('change',updateSimulatorTrainerFromForm);
  $('pfCheck')?.addEventListener('change',async()=>{calcEntry();await calcNightForForm();silentAutosaveExisting()});
  $('dutyTypeFlight').addEventListener('change',()=>{setEntryTypeUI();if(entryRosterActivityContext?.typeOnly){setEntryLockedUI(true);$('dutyTypeFlight').disabled=false;}if(canonicalDutyType($('dutyTypeFlight').value)==='Flight')syncProfileNameWithRole();calcEntry();silentAutosaveExisting()});
  $('trainerTypeInput')?.addEventListener('change',()=>{calcEntry();silentAutosaveExisting()});
  $('courseType').addEventListener('input',()=>calcEntry());
  $('blockDisplay')?.addEventListener('input',()=>{if(!formLoading)formTimeOverrides.block=true;calcEntry();silentAutosaveExisting()});
  $('blockDisplay')?.addEventListener('change',()=>{const value=optionalDuration($('blockDisplay').value);$('blockDisplay').value=value===null?'':fmt(value);calcEntry();silentAutosaveExisting()});
  TIME_GROUP_DEFS.forEach(({key})=>{const el=$(TIME_GROUP_INPUT_IDS[key]);if(!el)return;el.addEventListener('input',()=>{if(!formLoading){formTimeOverrides[key]=true;formManualFields[key]=true;formFieldSources[key]='manual'}calcEntry();silentAutosaveExisting()});el.addEventListener('change',()=>{const value=optionalDuration(el.value);setDurationPicker(el,value);calcEntry();silentAutosaveExisting()})});
  $('flightForm').addEventListener('submit',e=>e.preventDefault());
  $('returnFlight').addEventListener('click',createReturnFlight);
  $('clearForm')?.addEventListener('click',()=>{setEntryReturnContext(null);showActivityTypePicker()});
  $('lockEntryBtn').addEventListener('click',toggleEntryLock);
  $('tripCashConverted').addEventListener('change',()=>{const raw=$('tripCashConverted').value.replace(/,/g,'').replace(/[^0-9.]/g,'');tripFormOverrides.cashConverted=raw===''?null:{value:Number(raw),amount:Number($('tripCashAmount').value||0),from:upper($('tripCashCurrency').value),to:load(TRIPS_KEY).find(t=>t.id===$('tripEditId').value&&t.cashCurrency===upper($('tripCashCurrency').value))?.cashFxTo||appCurrency(),date:tripStartDateFromValue($('tripStart').value)};});
  $('tripLayover').addEventListener('change',()=>{tripFormOverrides.layover=optionalDuration($('tripLayover').value);tripCalc()});
  $('tripAllowance').addEventListener('change',()=>{const raw=$('tripAllowance').value.replace(/,/g,'').replace(/[^0-9.]/g,'');tripFormOverrides.allowance=raw===''?null:Number(raw);tripCalc()});
  ['tripStart','tripEnd'].forEach(id=>$(id).addEventListener('input',tripCalc));$('tripCashAmount')?.addEventListener('input',()=>{tripCalc();if(Number($('tripCashAmount').value||0)<=0)$('tripCashConverted').value=''});['tripCashAmount','tripCashCurrency','tripStart'].forEach(id=>$(id)?.addEventListener('change',()=>tripMoneyConversion()));$('calcTrip').addEventListener('click',()=>tripMoneyConversion());$('clearTrip').addEventListener('click',resetTrip);
  $('tripForm').addEventListener('submit',async e=>{e.preventDefault();if($('tripEditId').value&&tripBaseline===tripContent()){resetTrip();return}const c=tripCalc();if(!c)return alert('Enter a valid Trip Start and Trip End.');const fx=await tripMoneyConversion();if(fx?.error)return alert(fx.error);const ts=load(TRIPS_KEY),id=$('tripEditId').value||makeId(),existing=ts.find(x=>x.id===id),cashAmount=Number($('tripCashAmount')?.value||0),cashCurrency=cashAmount>0?upper($('tripCashCurrency')?.value||''):'',cashRates=cashAmount>0?{...(existing?.cashFxRates||{})}:{};if(cashAmount>0&&fx?.cashRate>0)cashRates[fx.cashTo]={from:cashCurrency,to:fx.cashTo,rate:fx.cashRate,date:fx.cashFxDate,tripStartDate:fx.tripStartDate,provisional:fx.cashFxProvisional,manual:fx.cashFxManual};const t=stamp({...existing,id,...(existing?{}:{createdDate:today()}),base:upper(appSettings().homeBase||'CMN'),stations:upper($('tripStations').value),start:$('tripStart').value,end:$('tripEnd').value,trip:c.trip,duty:c.duty,layover:c.layover,allowance:c.allowance,allowanceCurrency:'MAD',tripOverrides:{...tripFormOverrides},cashEligible:existing?.cashEligible||false,cashAmount,cashCurrency,cashConverted:cashAmount>0?fx?.cashConverted??null:0,cashFxPending:!!fx?.cashFxPending,cashFxProvisional:!!fx?.cashFxProvisional,cashFxManual:!!fx?.cashFxManual,cashEur:cashAmount>0&&fx?.cashTo==='EUR'?Number(fx?.cashConverted||0):0,cashFxFrom:cashAmount>0?cashCurrency:'',cashFxTo:cashAmount>0?fx.cashTo:'',cashFxRate:cashAmount>0?Number(fx?.cashRate||0):0,cashFxDate:cashAmount>0?String(fx?.cashFxDate||''):'',cashFxTripStartDate:cashAmount>0?String(fx?.tripStartDate||''):'',cashFxRates:cashRates,tripFxTo:'MAD',tripFxMadRate:1,tripFxDate:'',tripFxTripStartDate:String(fx?.tripStartDate||''),manualOverride:true,remarks:$('tripRemarks').value.trim()}),i=ts.findIndex(x=>x.id===id);if(i>=0)ts[i]=t;else ts.push(t);save(TRIPS_KEY,ts);markCloudEdited('trips',t,existing?'edit':'create');resetTrip();renderTrips();renderPayroll();scheduleAutoSync('trip-save');if(t.cashFxPending)refreshPendingTripFx();alert('Trip saved.')});
  $('screenshotFile').addEventListener('change',async e=>{
    const file=e.target.files[0];if(!file)return;
    clearScreenshotReview();
    $('screenshotOcrWrap').classList.add('hidden');
    if(screenshotObjectUrl)URL.revokeObjectURL(screenshotObjectUrl);
    screenshotObjectUrl=URL.createObjectURL(file);
    $('screenshotPreview').src=screenshotObjectUrl;
    $('screenshotPreviewWrap').classList.remove('hidden');
    try{
      const text=await runScreenshotOcr(file);
      $('screenshotOcrText').value=text;
      $('screenshotOcrWrap').classList.remove('hidden');
      screenshotReviewRows=parseScreenshotText(text);
      renderScreenshotReview();
      const crewGrid=/INDIVIDUAL\s+CREW\s+SCHEDULE\s+REPORT/i.test(text),smart=screenshotSmartCells.length;
      $('screenshotImportStatus').textContent=screenshotReviewRows.length
        ?`${smart?'Smart Cell Scan':'OCR'} complete${crewGrid?' — Crew Schedule detected':''}. ${screenshotReviewRows.length} review row(s) created${smart?` from ${smart} populated day(s)`:''}. Complete yellow fields before importing.`
        :'OCR complete, but no structured rows were detected. Review the OCR text or try a sharper/full-page screenshot.';
    }catch(err){
      console.error('Screenshot OCR failed',err);
      $('screenshotOcrWrap').classList.remove('hidden');
      $('screenshotImportStatus').textContent=`OCR unavailable: ${err.message} You can paste or type recognized text manually and press Parse / Re-parse.`;
    }
  });
  $('parseScreenshotText').addEventListener('click',()=>{
    screenshotReviewRows=parseScreenshotText($('screenshotOcrText').value);
    renderScreenshotReview();
    $('screenshotImportStatus').textContent=screenshotReviewRows.length?`${screenshotReviewRows.length} review row(s) detected.`:'No structured rows detected. Adjust the OCR text or choose the correct screenshot type.';
  });
  $('screenshotReviewBody').addEventListener('input',e=>updateScreenshotReviewFromControl(e.target));
  $('screenshotReviewBody').addEventListener('change',e=>{updateScreenshotReviewFromControl(e.target);renderScreenshotReview()});
  $('importScreenshotReviewed').addEventListener('click',()=>importReviewedScreenshot().then(()=>scheduleAutoSync('screenshot-import')).catch(err=>{console.error(err);alert('Screenshot import failed: '+err.message)}));
  $('clearScreenshotReview').addEventListener('click',()=>{clearScreenshotReview();$('screenshotImportStatus').textContent='Review cleared. The OCR text is still available.'});
  $('exportEasaPdf').addEventListener('click',exportEasaStylePdf);
  $('exportExperiencePdf').addEventListener('click',exportExperiencePdf);
  $('exportBackupJson').addEventListener('click',exportFullBackupJson);
  $('restoreBackupJsonFile').addEventListener('change',async e=>{const file=e.target.files?.[0];if(!file)return;try{await restoreFullBackupJsonFile(file)}catch(err){console.error('Full backup restore failed',err);alert('Restore failed: '+err.message)}finally{e.target.value=''}});
  $('exportCsv').addEventListener('click',()=>{const d=coreActivityRows().filter(isCompletedLogbookEntry);if(!d.length)return alert('No entries to export');const cols=['dutyType','date','flightNo','reg','type','variant','dep','arr','schedOut','schedIn','schedBlock','onDuty','offDuty','totalDuty','out','off','on','in','block','totalFlightTime','multiPilot','pic','picUS','sic','ifrTime','airTime','night','picNight','simulatorTime','sfiSfe','dualGiven','dualReceived','groundInstruction','stbyTime','credit','rolePic','pf','instruction','examiner','simInstructor','simExaminer','callFromDayOff','rhs','picName','sicName','soName','instructorName','examinerName','purserName','attendant1Name','attendant2Name','attendant3Name','dayTakeoffs','nightTakeoffs','dayLandings','nightLandings','goArounds','pax','fuelAboard','fuelBurned','fuelMinimum','approachType','remarks','locked'];download('pilotlog_logbook.csv',[cols.join(','),...d.map(r=>cols.map(c=>csv(r[c])).join(','))].join('\n'),'text/csv')});
  $('exportLogTen').addEventListener('click',logTenExport);
  const updateEasaRangeUI=()=>{const period=$('easaExportMode').value==='period';$('easaFrom').disabled=!period;$('easaTo').disabled=!period};
  $('easaExportMode').addEventListener('change',updateEasaRangeUI);updateEasaRangeUI();
  $('aerolineRosterFile').addEventListener('change',async e=>{
    const file=e.target.files?.[0];if(!file)return;
    
    try{
      const text=await file.text(),payload=JSON.parse(String(text||'').replace(/^\uFEFF/,''));
      const r=importAerolineRosterObject(payload);e.target.value='';
      await render();await renderRoster();renderDuty();markLocalOnlyImport('AeroLINE JSON import');
      const imported=r.flightsAdded+r.activitiesAdded+r.offAdded,updated=r.flightsUpdated+r.activitiesUpdated+r.offUpdated,expiryText=r.expiryImport?` • Expiry ${r.expiryImport.added} added / ${r.expiryImport.updated} updated`:'';
      const extras=r.unsupported.length?` • Unsupported codes: ${r.unsupported.join(', ')}`:'';
      $('aerolineImportStatus').textContent=`${r.monthLabel||'AeroLINE'} • ${r.flights.length} flights • ${r.activities.length} activities • ${r.daysOff.length} OFF • ${imported} added • ${updated} updated${r.staleRemoved?` • ${r.staleRemoved} stale removed`:''}${expiryText}${extras}`;
      alert(`AeroLINE roster imported: ${r.monthLabel||''}\n${r.flights.length} flights • ${r.activities.length} activities • ${r.daysOff.length} OFF\n${imported} added • ${updated} updated${r.staleRemoved?` • ${r.staleRemoved} stale removed`:''}${expiryText}`);
    }catch(err){e.target.value='';console.error('AeroLINE import failed',err);alert('AeroLINE import failed: '+err.message)}
  });
  $('logTenFullFile').addEventListener('change',async e=>{
    const file=e.target.files?.[0];if(!file)return;
    showImportProgress('Importing LogTen Pro');
    try{
      $('logTenImportStatus').textContent='Importing complete LogTen migration… progress is shown on screen.';
      const r=await importLogTenMigrationPackage(e.target.files);e.target.value='';
      setImportProgress(99,'Import complete • refreshing PilotLog…');await render();renderDuty();setImportProgress(100,'Complete');await yieldToUi();
      markLocalOnlyImport(`Complete LogTen migration — ${r.total.toLocaleString('en-US')} rows ready`);
      if($('logTenArchiveStatus'))$('logTenArchiveStatus').textContent=logTenArchiveStatusText();
      const span=r.manifest?.dateFrom&&r.manifest?.dateTo?`\n${displayDate(r.manifest.dateFrom)} – ${displayDate(r.manifest.dateTo)}`:'';
      $('logTenImportStatus').textContent=`Complete LogTen migration • ${r.total.toLocaleString('en-US')} logbook rows • ${r.imported.toLocaleString('en-US')} added • ${r.updated.toLocaleString('en-US')} matched/updated • ${(r.expiryImport?.total||0)} licences/validities • ${(r.tripImport?.total||0)} trips. Credit H is calculated from Scheduled Block Hours. Original SQLite database archived locally • verified local rows ${r.verified.toLocaleString('en-US')}. Nothing has been sent to cloud.`;
      const ex=r.expiryImport||{imported:0,updated:0,total:0},tr=r.tripImport||{imported:0,updated:0,total:0};
      alert(`Complete LogTen migration imported.${span}\nLogbook: ${r.imported.toLocaleString('en-US')} new • ${r.updated.toLocaleString('en-US')} matched/updated\nLicences/validities: ${ex.imported} new • ${ex.updated} updated\nTrips: ${tr.imported} new • ${tr.updated} updated\nCredit H: calculated from Scheduled Block Hours.\nOriginal LogTen database archived byte-for-byte on this device.\n\nLOCAL VERIFY: ${r.verified.toLocaleString('en-US')} LogTen IDs present.\n${r.conflicts?`\nManual values preserved: ${r.conflicts} conflicting fields across ${r.protectedMerged} records.\n${r.conflictDetails.slice(0,20).join("\n")}`:""}\nNothing has been sent to cloud.`);
    }catch(err){e.target.value='';console.error('Complete LogTen import failed',err);if($('logTenArchiveStatus'))$('logTenArchiveStatus').textContent=logTenArchiveStatusText();$('logTenImportStatus').textContent='Complete LogTen migration failed: '+err.message;alert('Complete LogTen migration failed: '+err.message)}
    finally{hideImportProgress()}
  });
  $('exportLogTenArchive').addEventListener('click',()=>exportArchivedLogTenDatabase().catch(err=>alert('LogTen archive export failed: '+err.message)));
  $('logTenFile').addEventListener('change',async e=>{const file=e.target.files[0];if(!file)return;try{const r=logTenImport(await file.text());e.target.value='';await render();markLocalOnlyImport('LogTen Tab import');$('logTenImportStatus').textContent=`Imported ${r.imported} new, repaired/updated ${r.updated}, simulators ${r.sims}, other duties ${r.other}${r.duplicatesRemoved?`, duplicates merged ${r.duplicatesRemoved}`:''}.`;alert(`LogTen import complete.${r.duplicatesRemoved?` ${r.duplicatesRemoved} duplicate${r.duplicatesRemoved===1?'':'s'} merged.`:''}`)}catch(err){alert('LogTen import failed: '+err.message)}});
  $('calendarFile').addEventListener('change',async e=>{const file=e.target.files[0];if(!file)return;try{const events=parseIcs(await file.text());if(!events.length)throw new Error('No calendar events found.');const r=importCalendar(events);e.target.value='';await render();markLocalOnlyImport('Calendar import');$('calendarImportStatus').textContent=`Imported ${r.sectors} flight sectors, ${r.duties} duties and ${r.other} other entries. ${r.skipped} skipped.`;alert(`Imported: ${r.sectors} flights • ${r.duties} duties • ${r.other} other entries`)}catch(err){alert('Calendar import failed: '+err.message)}});
  $('rosterFile').addEventListener('change',async e=>{const file=e.target.files[0];if(!file)return;const rows=parseCsv(await file.text());if(rows.length<2)return alert('CSV contains no data');const norm=s=>String(s||'').toLowerCase().replace(/[^a-z0-9]/g,''),aliases={date:['date','day'],flightNo:['flightno','flightnumber','flight','flt'],dep:['dep','departure','from','origin'],arr:['arr','arrival','to','destination'],std:['std','departuretime','scheduleddeparture','offblock'],sta:['sta','arrivaltime','scheduledarrival','onblock']},fieldFor=h=>Object.keys(aliases).find(k=>aliases[k].includes(norm(h)))||null,map=rows[0].map(fieldFor),imp=rows.slice(1).map(r=>{const o={recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:'Flight',source:'roster'};map.forEach((k,i)=>{if(k)o[k]=r[i]||''});o.date=normalDate(o.date);o.schedOut=o.std||'';o.schedIn=o.sta||'';delete o.std;delete o.sta;['dep','arr','flightNo'].forEach(k=>o[k]=upper(o[k]));if(o.flightNo)o.flightNo=rosterFlightLabel(o.flightNo);return o}).filter(x=>x.date&&(x.dep||x.arr||x.flightNo));const csvKey=r=>JSON.stringify([r.date,r.flightNo,r.dep,r.arr,r.schedOut,r.schedIn]);const seen=new Set(coreActivityRows().filter(r=>r.source==='roster'||r.sourceRowKey?.startsWith('csv:')).flatMap(r=>[r.sourceRowKey,'csv:'+csvKey(r)]));
    const fresh=[];
    for(const row of imp){
      row.sourceRowKey='csv:'+csvKey(row);
      if(seen.has(row.sourceRowKey))continue;
      row.id=importCoreId(syncSourceKey(row));
      seen.add(row.sourceRowKey);
      fresh.push(row);
    }
    if(fresh.length)commitCoreActivities([...coreActivityRows(),...fresh]);e.target.value='';await render();markLocalOnlyImport('Roster CSV import');alert(`${imp.length} roster sectors imported`)});
  $('detectTripsBtn').addEventListener('click',()=>autoDetectTrips(true));
  $('profilePhotoInput').addEventListener('change',async e=>{
    const file=e.target.files?.[0];if(!file)return;
    try{const profilePhotoData=await profilePhotoDataFromFile(file);saveAppSettings({profilePhotoData,profilePhotoUpdatedAt:new Date().toISOString()});updateAppHeader();updateProfilePhotoSettings();scheduleAutoSync('profile-photo')}
    catch(err){alert(err.message||'PHOTO COULD NOT BE ADDED.')}
    finally{e.target.value=''}
  });
  $('removeProfilePhoto').addEventListener('click',async()=>{
    if(!confirm('REMOVE THE PERSONAL PHOTO AND RESTORE THE PILOTLOG LOGO?'))return;
    saveAppSettings({profilePhotoData:'',profilePhotoUpdatedAt:new Date().toISOString()});updateAppHeader();updateProfilePhotoSettings();scheduleAutoSync('profile-photo-remove');
  });
  $('setPayrollRegime')?.addEventListener('change',()=>{$('macDependentsSetting')?.classList.toggle('hidden',upper($('setPayrollRegime').value)!=='MAC')});
  $('appSettingsForm').addEventListener('submit',async e=>{e.preventDefault();saveAppSettings({flightPrefix:cleanPrefix($('setFlightPrefix').value)||'MAC',aircraftPrefix:cleanPrefix($('setAircraftPrefix').value)||'CN-NM',profileName:$('setProfileName').value.trim(),profileRole:$('setProfileRole').value||'Captain'});renderSettings();renderTrips();renderPayroll();if(!$('editId').value)applyProfileDefaultsToEntry();refreshEntrySuggestions();scheduleAutoSync('settings');alert('Settings saved.')});
  $('saveTimeGroupSettings')?.addEventListener('click',()=>{saveTimeGroupSettingsFromForm();scheduleAutoSync('time-group-settings');alert('Time Group settings saved. Past activities now use the selected Auto Sync rules.')});
  $('refreshAirportsBtn').addEventListener('click',()=>ensureAirportDb(true));
  $('cloudEmail').addEventListener('change',()=>localStorage.setItem(LAST_EMAIL_KEY,$('cloudEmail').value.trim()));
  $('cloudSignUpBtn').addEventListener('click',async()=>{try{await cloudSignUp()}catch(e){alert('Create account failed: '+e.message)}});
  $('cloudSignInBtn').addEventListener('click',async()=>{try{await cloudSignIn()}catch(e){alert('Sign in failed: '+e.message)}});
  $('cloudSignOutBtn').addEventListener('click',async()=>{try{await cloudSignOut()}catch(e){alert('Sign out failed: '+e.message)}});
  $('syncCloudBtn').addEventListener('click',()=>syncSupabase({silent:false,reason:'manual'}));
  $('backupNowBtn').addEventListener('click',async()=>{try{await createWeeklyBackup(true);alert('Weekly backup updated.')}catch(e){alert('Backup failed: '+e.message)}});
  $('restoreWeeklyBackupBtn').addEventListener('click',()=>restoreWeeklyBackup().catch(e=>alert('Restore failed: '+e.message)));
  $('autoSyncEnabled').addEventListener('change',()=>{setAutoSyncEnabled($('autoSyncEnabled').value==='yes');armPeriodicSync()});

  $('payrollMonth').addEventListener('change',renderPayroll);$('recalcPayroll').addEventListener('click',()=>{const month=$('payrollMonth').value||monthNow(),ex=monthExtras(month);if(ex.validated)return;const arrears=Number($('payArrears').value||0);if(arrears!==Number(ex.arrears||0))saveMonthExtras(month,{...ex,dayOffCount:0,arrears,regime:payrollRegime()});scheduleAutoSync('payroll-month');renderPayroll()});
  $('validatePayroll').addEventListener('click',async()=>{
    const month=$('payrollMonth').value||monthNow();if(monthExtras(month).validated)return;
    const result=await renderPayroll();if(!result||$('payrollMonth').value!==month)return alert('Complete the payroll calculation before validation.');
    const validatedAt=new Date().toISOString(),snapshot=JSON.parse(JSON.stringify({...result,projection:payrollSnapshotProjection(result.projection),validatedAt}));
    saveMonthExtras(month,{...monthExtras(month),validated:snapshot,validationUpdatedAt:validatedAt});
    await persistCoreActivities();scheduleAutoSync('payroll-validated');await renderPayroll();
  });
  $('unlockPayroll').addEventListener('click',async()=>{
    const month=$('payrollMonth').value||monthNow(),ex=monthExtras(month);
    if(!ex.validated||!confirm(`Unlock the validated payslip for ${month}? Its values will be calculated again from current entries and settings.`))return;
    saveMonthExtras(month,{...ex,validated:null,validationUpdatedAt:new Date().toISOString()});
    await persistCoreActivities();scheduleAutoSync('payroll-unlocked');await renderPayroll();
  });
  $('paySettingsForm').addEventListener('submit',e=>{e.preventDefault();if($('setNightStart').value===$('setNightEnd').value)return alert('Night start and end must differ.');if(!$('paySettingsForm').checkValidity())return $('paySettingsForm').reportValidity();const nextCurrency=upper($('setCurrency')?.value||'MAD');const fields={homeBase:upper($('setHomeBase').value)||'CMN',currency:ISO_CURRENCIES.includes(nextCurrency)?nextCurrency:'MAD',payrollRegime:upper($('setPayrollRegime')?.value||'IBC')==='MAC'?'MAC':'IBC',macDependents:Math.max(0,Math.min(6,Math.trunc(Number($('setMacDependents')?.value)||0)))};saveAppSettings({...fields,nightStart:$('setNightStart').value,nightEnd:$('setNightEnd').value,nightPremiumPct:Number($('setNightPremiumPct').value)});const rates=readPaySettings();if(Object.entries(rates).some(([k,v])=>paySettings()[k]!==v))saveObject(PAY_SETTINGS_KEY,{...paySettings(),...rates,_updatedAt:new Date().toISOString()});renderTrips();setEntryTypeUI();renderPayroll();scheduleAutoSync('payroll-settings');alert('Payroll settings saved.')});
  $('paySettingsForm').addEventListener('input',()=>showFixedSalary(readPaySettings()));
  $('resetPaySettings').addEventListener('click',()=>{if(confirm('Restore default rates?')){if(Object.entries(PAY_DEFAULTS).some(([k,v])=>paySettings()[k]!==v))saveObject(PAY_SETTINGS_KEY,PAY_DEFAULTS);fillPaySettings();renderPayroll()}});
  $('deleteAll').addEventListener('click',async()=>{if(confirm('Delete ALL saved log entries? This cannot be undone.')){const targets=coreActivityRows().filter(isCompletedLogbookEntry),ids=new Set(targets.map(x=>x.id));targets.forEach(x=>markCloudDeleted('activities',x,{includeSource:false,manual:true}));removeCoreActivities(x=>ids.has(x.id),{manual:true});await persistCoreActivities();clearAllEntryDrafts();await render();scheduleAutoSync('delete-all-entries')}});
  $('clearRoster').addEventListener('click',async()=>{if(confirm('Clear the complete imported roster? Flights, OFF, DHD/DHP, standby and training roster activities will be removed. Locked Logbook entries are preserved.')){const r=await clearImportedRosterData();await render();renderDuty();renderTrips();scheduleAutoSync('clear-roster');alert(`Roster cleared: ${r.roster} sectors • ${r.entries} activities • ${r.duties} duties removed.`)}});
  $('exportRoster').addEventListener('click',()=>{const d=rosterFlightRows();if(!d.length)return alert('No roster to export');const cols=['date','flightNo','dep','arr','std','sta','status'];download('pilotlog_roster.csv',[cols.join(','),...d.map(r=>cols.map(c=>csv(r[c])).join(','))].join('\n'),'text/csv')});
  document.addEventListener('click',async e=>{let b=e.target.closest('[data-roster-today]');if(b){rosterSelectedDate=today();rosterCalendarMonth=rosterSelectedDate.slice(0,7);await renderRoster();scrollRosterAgendaIntoView();return}
    b=e.target.closest('[data-add-roster-entry]');if(b){showActivityTypePicker();$('date').value=b.dataset.addRosterEntry;setEntryReturnContext('rosterView');return}
    b=e.target.closest('[data-roster-date]');if(b){if(Date.now()<suppressRosterCalendarClickUntil)return;const date=b.dataset.rosterDate;rosterSelectedDate=rosterSelectedDate===date?'':date;if(rosterSelectedDate)rosterCalendarMonth=date.slice(0,7);await renderRoster();if(rosterSelectedDate)scrollRosterAgendaIntoView();return}
    b=e.target.closest('[data-delete-flight]');if(b){if(!confirm('Confirm to delete this log entry?'))return;const victim=coreActivityRows().find(f=>f.id===b.dataset.deleteFlight);markCloudDeleted('activities',victim||b.dataset.deleteFlight,{manual:true});removeCoreActivities(f=>f.id===b.dataset.deleteFlight,{manual:true});reconcileAllDuties();await render();scheduleAutoSync('delete-flight');return}
    b=e.target.closest('[data-edit-flight]');if(b){const f=coreActivityRows().find(x=>x.id===b.dataset.editFlight);if(f){loadEntryToForm(f);setEntryReturnContext('flightsView',f.id);show('addView')}return}
    b=e.target.closest('[data-delete-roster-duty]');if(b){e.preventDefault();e.stopPropagation();const ids=String(b.dataset.deleteRosterDuty||'').split(',').filter(Boolean);await deleteRosterDutyActivities(ids,{label:ids.length>1?'this roster duty':'this roster activity'});return}
    b=e.target.closest('[data-edit-roster-entry]');if(b){openRosterEntryModule(b.dataset.editRosterEntry);return}
    b=e.target.closest('[data-edit-roster-duty]');if(b){openRosterDutyModule(b.dataset.editRosterDuty);return}
    b=e.target.closest('[data-delete-duty]');if(b){if(!confirm('Confirm to delete this duty?'))return;const victim=coreActivityRows().find(d=>d.id===b.dataset.deleteDuty);markCloudDeleted('activities',victim||b.dataset.deleteDuty,{manual:true});removeCoreActivities(d=>d.id===b.dataset.deleteDuty,{manual:true});reconcileAllDuties();renderDuty();await render();scheduleAutoSync('delete-duty');return}
    b=e.target.closest('[data-view-trip]');if(b){renderTripInspector(b.dataset.viewTrip);return}
    b=e.target.closest('[data-edit-trip]');if(b){const saved=load(TRIPS_KEY).find(x=>x.id===b.dataset.editTrip),t=PilotLogEngine.savedTrip(saved);if(!t)return;tripFormOverrides={...saved.tripOverrides};$('tripEditId').value=t.id;$('tripStations').value=t.stations||'';$('tripStart').value=t.start||'';$('tripEnd').value=t.end||'';$('tripLayover').value=fmt(t.layover||0);$('tripAllowance').value=tripDisplayMoney(t).allowanceText;$('tripCashAmount').value=Number(t.cashAmount||0)||'';$('tripCashCurrency').value=upper(t.cashCurrency||'USD');$('tripCashConverted').value=upper(t.cashFxTo||'')===appCurrency()&&Number(t.cashFxRate)>0?moneyCurrency(Number(t.cashConverted||0),appCurrency()):'';$('tripRemarks').value=t.remarks||'';tripMoneyConversion();tripBaseline=tripContent();$('tripForm').scrollIntoView({behavior:'smooth',block:'start'});return}
    b=e.target.closest('[data-delete-trip]');if(b){if(!confirm('Confirm to delete this trip?'))return;const rows=load(TRIPS_KEY),victim=rows.find(t=>t.id===b.dataset.deleteTrip);markCloudDeleted('trips',victim||b.dataset.deleteTrip,{manual:true});save(TRIPS_KEY,rows.filter(t=>t.id!==b.dataset.deleteTrip));renderTrips();$('tripInspectorWrap')?.classList.add('hidden');scheduleAutoSync('delete-trip');return}    b=e.target.closest('[data-roster-action]');if(b){
      const r=rosterFlightRows().find(x=>x.id===b.dataset.rosterAction);if(!r)return;
      loadEntryToForm(coreActivityRows().find(x=>x.id===r.id)||r,{rosterActivity:true});setEntryReturnContext('rosterView',r.id);
      show('addView');
      return;
    }
  });
  document.addEventListener('keydown',e=>{if(e.target.closest('button,input,select,textarea,a'))return;const row=e.target.closest('.roster-agenda-clickable');if(row&&(e.key==='Enter'||e.key===' ')){e.preventDefault();row.click()}});

  window.addEventListener('beforeunload',()=>{try{if(!$('addView')?.classList.contains('hidden'))saveEntryDraft()}catch{}});
  window.addEventListener('pagehide',()=>{try{saveEntryDraft()}catch{};flushFlightStore().catch(()=>{})});
  document.addEventListener('visibilitychange',()=>{if(document.visibilityState!=='visible'){try{saveEntryDraft()}catch{};flushFlightStore().catch(()=>{})}});

  populateCurrencySelect($('setCurrency'),['MAD','EUR','USD','TRY','AED','GBP']);populateCurrencySelect($('tripCashCurrency'),QUICK_CASH_CURRENCIES);updateAppHeader();resetEntry(false);refreshEntrySuggestions();[...CREW_NAME_FIELDS,'expiryEndorsedBy'].forEach(id=>setupSmartAutocomplete(id,()=>entrySuggestionDb.crew));setupSmartAutocomplete('type',()=>entrySuggestionDb.types);resetTrip();resetDutyEditor();$('payrollMonth').value=monthNow();$('cloudEmail').value=localStorage.getItem(LAST_EMAIL_KEY)||'';fillPaySettings();renderDuty();renderSettings();await render();show('rosterView');syncDatabaseReady=true;drawSyncState();scheduleAutoSync('startup',startupTime);
  recoverPendingFx();
  window.addEventListener('online',recoverPendingFx);
  window.addEventListener('pilotlog-native-pause',()=>{try{saveEntryDraft()}catch{};flushFlightStore().catch(e=>console.error('Save on pause failed',e))});

  if(!window.PilotLogNative?.isNative&&'serviceWorker' in navigator){navigator.serviceWorker.register('./sw-16.3.js').catch(err=>console.warn('Offline cache unavailable',err));}
  console.log('PilotLog v'+VERSION+' loaded');
});
/* Screenshot acquisition: ephemeral images/OCR, one Core write after review. */
const ROSTER_SHOT_CREW=['picName','sicName','purserName','attendant1Name','attendant2Name','attendant3Name'];
const ROSTER_SHOT_FIELDS={picName:'Captain',sicName:'First Officer',purserName:'Purser',attendant1Name:'Cabin crew 1',attendant2Name:'Cabin crew 2',attendant3Name:'Cabin crew 3',screenshotDutyStart:'Duty START',screenshotArrivalDate:'Data arrivo',onDuty:'Duty On',date:'Data',flightNo:'Volo',dep:'Da',arr:'A',schedOut:'Schedule OUT',schedIn:'Schedule IN',reg:'Registrazione',type:'Aircraft',schedBlock:'Flying Hrs (schedule)',instruction:'Instruction',remarks:'Training Program'};
let rosterShotFiles=[],rosterShotRows=[],rosterShotCrew=[],rosterShotPlan=[],rosterShotBusy=false;
function rosterShotDate(value){
 const m=String(value||'').match(/(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}:[0-5]\d)/);if(!m)return '';
 const iso=`${m[3]}-${m[2]}-${m[1]}T${m[4]}:00Z`,d=new Date(iso);
 return Number.isFinite(+d)&&d.toISOString()===iso.replace(':00Z',':00.000Z')?iso:'';
}
function rosterShotFlightNo(value){return upper(value).replace(/^30(?=\d{2,4}$)/,'3O')}
function rosterShotParseFlight(text){
 const header=/\b([A-Z]{3})[ \t]*(?:[>→➜›»—–\-]+[ \t]*)?([A-Z]{3})[ \t]*[.·,:]?\s*((?:3[O0]|[A-Z]{2})\s*\d{2,4})\b/g;
 const hits=[...upper(text).matchAll(header)],rows=[];
 hits.forEach((h,i)=>{
  const chunk=text.slice(h.index,hits[i+1]?.index??text.length);
  const dates=tag=>{const part=chunk.split(new RegExp(tag,'i'))[1]||'';return rosterShotDate(part)};
  const start=dates('START'),depart=dates('DEPARTURE'),arrival=dates('ARRIVAL');
  const reg=upper(chunk.match(/\bCN\s*[-–]\s*[A-Z]{3}\b/i)?.[0]||'').replace(/\s/g,'').replace('–','-');
  const type=upper(chunk.match(/\bA(?:319|320|321)\b/i)?.[0]||'');
  const hours=(chunk.split(/Flying\s+Hrs\.?/i)[1]||'').match(/\b(\d{2}:[0-5]\d)\b/)?.[1]||'';
  const training=chunk.match(/Training\s+Program\s*\n\s*([^\n]+)/i)?.[1]?.trim()||'';
  rows.push({date:depart.slice(0,10),flightNo:rosterShotFlightNo(h[3].replace(/\s/g,'')),dep:h[1],arr:h[2],schedOut:depart.slice(11,16),schedIn:arrival.slice(11,16),screenshotArrivalDate:arrival.slice(0,10),screenshotDutyStart:start,reg,type,schedBlock:hours?timeSpanMins('00:00',hours):null,...(training?{remarks:training}:{}),...(/LINE\s+TRAINING/i.test(training)?{instruction:true}:{})});
 });return rows;
}
function rosterShotParseCrew(text){
 const lines=text.split(/\n/).map(s=>s.trim()).filter(Boolean),names=[];
 for(let i=0;i<lines.length;i++){
  if(!/\b\d{5,7}\b/.test(lines[i])&&!/Nationality/i.test(lines[i]))continue;
  let name=lines[i-1]||'';
  name=name.replace(/^(?:CP|FO|CS|CC)\s+/,'').replace(/\s+(?:Ra|[e©&])$/,'').trim();
  names.push(name&&!/\d|Nationality|FLIGHT INFO|Message all crew|^(TRI|TNE|CP|FO|CS|CC)$/i.test(name)&&name.split(/\s+/).length>=2?name:'');
 }return names.slice(0,ROSTER_SHOT_CREW.length);
}
function rosterShotIdentity(row){return `${row.date}|${rawFlightDigits(rosterShotFlightNo(row.flightNo))}|${upper(row.dep)}|${upper(row.arr)}`}
function rosterShotMerge(rows){
 const result=[];
 for(const row of rows){
  const previous=result.find(r=>rosterShotIdentity(r)===rosterShotIdentity(row));
  if(!previous){result.push({...row,_conflicts:{...(row._conflicts||{})}});continue}
  for(const [key,value] of Object.entries(row)){
   if(key==='_conflicts'){for(const [field,choices] of Object.entries(value||{}))previous._conflicts[field]=[...new Set([...(previous._conflicts[field]||[]),...choices])];continue}
   if(value===undefined||value===null||value==='')continue;
   if(previous[key]===undefined||previous[key]===null||previous[key]==='')previous[key]=value;
   else if(previous[key]!==value)previous._conflicts[key]=[...new Set([...(previous._conflicts[key]||[]),previous[key],value])];
  }
 }return result;
}
function rosterShotCandidates(row,records=coreActivityRows()){
 return records.filter(r=>isFlight(r)&&r.date===row.date&&((rawFlightDigits(r.flightNo)&&rawFlightDigits(r.flightNo)===rawFlightDigits(row.flightNo))||(upper(r.dep)===upper(row.dep)&&upper(r.arr)===upper(row.arr)&&(!r.schedOut||!row.schedOut||r.schedOut===row.schedOut))));
}
function rosterShotDiscard(){rosterShotFiles=[];rosterShotRows=[];rosterShotCrew=[];rosterShotPlan=[];$('rosterScreenshotFiles').value='';$('rosterScreenshotReview').replaceChildren();$('rosterScreenshotConfirm').classList.add('hidden')}
function rosterShotOpen(){if(rosterShotBusy)return;rosterShotDiscard();$('rosterScreenshotStatus').textContent='Aggiungi uno o più screenshot dello stesso duty.';$('rosterScreenshotDialog').showModal()}
function rosterShotInput(container,label,value,change,type='text'){
 const wrap=document.createElement('label');wrap.textContent=label;wrap.style.display='block';
 const input=document.createElement('input');input.type=type;input.value=value??'';input.addEventListener('change',()=>change(input.value));wrap.append(input);container.append(wrap);return input;
}
function rosterShotResolveView(){
 const host=$('rosterScreenshotReview');host.replaceChildren();$('rosterScreenshotConfirm').classList.add('hidden');
 const title=document.createElement('p');title.textContent='Verifica i dati letti. I nomi troncati devono essere completati. Nessun dato è ancora salvato.';host.append(title);
 for(const row of rosterShotRows){
  const card=document.createElement('div');card.className='card spaced';host.append(card);
  for(const key of ['date','flightNo','dep','arr','schedOut','schedIn','reg','type'])rosterShotInput(card,ROSTER_SHOT_FIELDS[key],row[key],v=>{row[key]=['flightNo','dep','arr','reg','type'].includes(key)?upper(v):v;delete row._conflicts[key]},key==='date'?'date':['schedOut','schedIn'].includes(key)?'time':'text');
  rosterShotInput(card,'Duty START (Zulu)',row.screenshotDutyStart.slice(0,16),v=>{row.screenshotDutyStart=v?`${v}:00Z`:'';delete row._conflicts.screenshotDutyStart},'datetime-local');
  for(const key of Object.keys(row._conflicts||{})){
   const label=document.createElement('label');label.textContent=`Screenshot discordanti: ${ROSTER_SHOT_FIELDS[key]||key}`;
   const select=document.createElement('select');select.append(new Option('Scegli il valore',''));
   row._conflicts[key].forEach((v,i)=>select.append(new Option(String(v),String(i))));
   select.onchange=()=>{if(select.value!==''){row[key]=row._conflicts[key][Number(select.value)];delete row._conflicts[key]}};label.append(select);card.append(label);
  }
 }
 const starts=[...new Set(rosterShotRows.map(r=>r.screenshotDutyStart).filter(Boolean))];
 for(const crew of rosterShotCrew){
  const card=document.createElement('div');card.className='card spaced';host.append(card);
  const label=document.createElement('label');label.textContent='Crew per duty START';const select=document.createElement('select');select.append(new Option('Scegli duty',''));
  starts.forEach(v=>select.append(new Option(v.slice(0,16).replace('T',' '),v)));if(starts.length===1)crew.start=starts[0];select.value=crew.start||'';select.onchange=()=>crew.start=select.value;label.append(select);card.append(label);
  crew.names.forEach((name,i)=>rosterShotInput(card,ROSTER_SHOT_FIELDS[ROSTER_SHOT_CREW[i]],name,v=>crew.names[i]=v));
 }
 const b=document.createElement('button');b.className='secondary';b.textContent='Prepara anteprima';b.onclick=()=>rosterShotPrepare();host.append(b);
}
function rosterShotSame(a,b){return String(a??'')===String(b??'')}
function rosterShotPrepare(){
 try{
  for(const row of rosterShotRows){
   if(!/^\d{4}-\d{2}-\d{2}$/.test(row.date)||!Number.isFinite(Date.parse(`${row.date}T00:00:00Z`))||!rawFlightDigits(row.flightNo)||!/^([A-Z]{3}|[A-Z]{4})$/.test(row.dep)||!/^([A-Z]{3}|[A-Z]{4})$/.test(row.arr)||!row.schedOut||!row.schedIn||!rosterShotValidStart(row.screenshotDutyStart))throw new Error('Completa data, volo, tratta, orari e Duty START.');
   if(Object.keys(row._conflicts||{}).length)throw new Error('Scegli i valori per gli screenshot discordanti.');
  }
  for(const crew of rosterShotCrew)if(!crew.start||crew.names.some(n=>!n.trim()||/[.…]/.test(n)))throw new Error('Seleziona il duty del crew e completa i nomi troncati.');
  const starts=[...new Set(rosterShotRows.map(r=>r.screenshotDutyStart))];
  if(new Set(rosterShotCrew.map(c=>c.start)).size!==rosterShotCrew.length)throw new Error('Più schermate Crew per lo stesso duty: verifica i nomi e usa una sola schermata Crew.');
  const rows=rosterShotMerge(rosterShotRows);if(rows.some(r=>Object.keys(r._conflicts).length))throw new Error('I dati modificati contengono valori discordanti per lo stesso volo.');
  const plans=rows.map(row=>({row,candidates:rosterShotCandidates(row),fields:[]}));
  // Reuse explicit imported duty groups; never infer membership from a date alone.
  for(const start of starts){
   const linked=plans.filter(p=>p.row.screenshotDutyStart===start).flatMap(p=>p.candidates);
   const blockIds=new Set(linked.map(r=>r.aerolineBlockId).filter(Boolean)),rootIds=new Set(linked.map(r=>r.dutyRootId||r.dutyControl&&r.id).filter(Boolean));
   const members=coreActivityRows().filter(r=>isFlight(r)&&(r.screenshotDutyStart===start||blockIds.has(r.aerolineBlockId)||rootIds.has(r.dutyRootId||r.dutyControl&&r.id)));
   for(const member of members)if(!plans.some(p=>p.candidates.some(c=>c.id===member.id)))plans.push({row:{date:member.date,flightNo:member.flightNo,dep:member.dep,arr:member.arr,screenshotDutyStart:start},candidates:[member],fields:[],crewOnly:true});
  }
  rosterShotPlan=plans;
  for(const p of plans){
   p.targetId=p.candidates.length===1?p.candidates[0].id:p.candidates.length?'':'NEW';
   const crew=rosterShotCrew.find(c=>c.start===p.row.screenshotDutyStart);
   const proposed={...p.row};delete proposed._conflicts;
   if(crew)crew.names.forEach((n,i)=>proposed[ROSTER_SHOT_CREW[i]]=n.trim());
   if(!p.crewOnly){
    const first=rows.filter(r=>r.screenshotDutyStart===p.row.screenshotDutyStart).sort((a,b)=>`${a.date}T${a.schedOut}`.localeCompare(`${b.date}T${b.schedOut}`))[0];
    if(first===p.row||rosterShotIdentity(first)===rosterShotIdentity(p.row))proposed.onDuty=p.row.screenshotDutyStart.slice(11,16);
   }
   p.proposed=proposed;rosterShotPlanFields(p);
  }
  rosterShotReviewPlan();
 }catch(error){$('rosterScreenshotStatus').textContent=error.message}
}
function rosterShotValidStart(v){return /^\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):[0-5]\d:00Z$/.test(v||'')&&Number.isFinite(Date.parse(v))}
function rosterShotPlanFields(p){
 const target=coreActivityRows().find(r=>r.id===p.targetId);p.baseline=target?JSON.stringify(target):'';
 p.fields=Object.entries(p.proposed).filter(([k,v])=>k!=='_conflicts'&&v!==undefined&&v!==null&&v!=='').map(([key,value])=>{
  const before=target?.[key],manual=target&&(entryManualField(target,key)||entryTimeOverride(target,key)||entryFieldSource(target,key)==='manual'||target.manualOverride===true&&before!==undefined&&before!==null&&before!=='');
  return {key,value,before,manual:!!manual,apply:!target||!manual&&(before===undefined||before===null||before===''),same:rosterShotSame(before,value)};
 });
}
function rosterShotReviewPlan(){
 const host=$('rosterScreenshotReview');host.replaceChildren();
 for(const p of rosterShotPlan){
  const card=document.createElement('div');card.className='card spaced';host.append(card);
  const h=document.createElement('h3');h.textContent=`${p.row.date} · ${p.row.flightNo} · ${p.row.dep} → ${p.row.arr}`;card.append(h);
  const select=document.createElement('select');
  if(p.candidates.length>1)select.append(new Option('Abbinamento dubbio: scegli il volo esistente',''));
  if(!p.candidates.length)select.append(new Option('Crea nel Roster','NEW'));
  p.candidates.forEach(r=>select.append(new Option(`${r.date} ${r.flightNo} ${r.dep}–${r.arr} ${r.schedOut||''}${r.locked?' · LOCKED':''}`,r.id)));
  select.value=p.targetId;select.onchange=()=>{p.targetId=select.value;rosterShotPlanFields(p);rosterShotReviewPlan()};card.append(select);
  const target=coreActivityRows().find(r=>r.id===p.targetId);
  if(target?.locked){const warning=document.createElement('p');warning.textContent='LOCKED: questa attività verrà mantenuta senza modifiche.';card.append(warning)}
  for(const field of p.fields){
   if(field.same)continue;
   const label=document.createElement('label');label.style.display='block';const check=document.createElement('input');check.type='checkbox';check.style.width='auto';check.checked=field.apply;check.disabled=field.manual||!!target?.locked||!p.targetId;check.onchange=()=>field.apply=check.checked;
   label.append(check,document.createTextNode(` ${ROSTER_SHOT_FIELDS[field.key]||field.key}: ${field.before??'—'} → ${field.value}${field.manual?' · manuale: mantenuto':''}`));card.append(label);
  }
 }
 const edit=document.createElement('button');edit.className='secondary';edit.textContent='Rivedi dati letti';edit.onclick=()=>rosterShotResolveView();host.append(edit);
 $('rosterScreenshotStatus').textContent='Lo START identifica il duty; la fine provvisoria usa l’ultimo IN disponibile +30 minuti. Campi diversi non selezionati: mantieni il valore salvato oppure scegli esplicitamente quello letto. Override manuali mantenuti.';
 $('rosterScreenshotConfirm').classList.remove('hidden');
}
async function rosterShotConfirm(){
 if(rosterShotBusy)return;
 if(!rosterShotPlan.length||rosterShotPlan.some(p=>!p.targetId))return alert('Risolvi tutti gli abbinamenti prima di confermare.');
 const targetIds=rosterShotPlan.filter(p=>p.targetId!=='NEW').map(p=>p.targetId);
 if(new Set(targetIds).size!==targetIds.length)return alert('Due voli letti sono abbinati alla stessa attività: rivedi i dati.');
 rosterShotBusy=true;rosterShotButtons();const before=coreActivityRows().map(r=>({...r})),changed=[];
 try{
  // Validate the entire batch before the first mutation.
  for(const p of rosterShotPlan){
   const target=before.find(r=>r.id===p.targetId);
   if(p.targetId==='NEW'&&rosterShotCandidates(p.row,before).length)throw new Error('Il volo ora esiste: riapri l’anteprima per aggiornare l’abbinamento.');
   if(p.targetId!=='NEW'&&(!target||JSON.stringify(target)!==p.baseline))throw new Error('I dati sono cambiati durante l’anteprima. Prepara una nuova anteprima.');
   if(target?.locked)continue;
   const row=target?{...target,fieldSources:{...target.fieldSources}}:{id:makeActivityId(),recordKind:'activity',activityCategory:'OPERATIONAL',activityStatus:ACTIVITY_STATUS.PLANNED,dutyType:'Flight',source:'screenshot',locked:false,totalDuty:null,fieldSources:{}};
   let touched=false;
   for(const field of p.fields)if(field.apply&&!field.manual&&!field.same){row[field.key]=field.value;row.fieldSources[field.key]='screenshot';touched=true}
   if(touched)changed.push(stamp(row));
  }
  const after=before.map(r=>changed.find(c=>c.id===r.id)||r).concat(changed.filter(c=>!before.some(r=>r.id===c.id)));
  const starts=[...new Set(rosterShotPlan.map(p=>p.row.screenshotDutyStart))],newDuties=[];
  for(const start of starts){
   const members=after.filter(r=>isFlight(r)&&r.screenshotDutyStart===start).sort((a,b)=>entryChrono(a).localeCompare(entryChrono(b)));
   if(!members.length)continue;
   const roots=members.filter(r=>r.dutyControl||r.dutyRootId);
   if(roots.length){
    const rootIds=[...new Set(roots.map(r=>r.dutyRootId||r.id))];
    if(rootIds.length!==1)throw new Error('Gli screenshot risultano associati a duty diversi: verifica gli abbinamenti.');
    const root=after.find(r=>r.id===rootIds[0]);
    if(root?.dutyControl?.provisional===true){
      const control=provisionalDutyControl(root,members,root.dutyControl);
      for(const member of members){const updated=stamp(controlledDutyFields(member,root,control));after[after.findIndex(r=>r.id===member.id)]=updated;const pos=changed.findIndex(r=>r.id===member.id);if(pos<0)changed.push(updated);else changed[pos]=updated}
    }
    continue;
   }
   if(members.some(r=>r.locked))throw new Error('Il duty include un volo LOCKED: sbloccalo prima di aprire il duty.');
   newDuties.push({start,members});
  }
  for(const {start,members} of newDuties){
   // A saved historical release/duration is an existing duty; never migrate it here.
   if(members.some(r=>r.offDuty||entryHasTimeValue(r,'totalDuty')&&numericTime(r.totalDuty)>0))continue;
   const root=members[0],resolvedStart=root.onDuty?dutyClockOnDate(root.onDuty,new Date(start))?.toISOString()||start:start;
   const manualStart=entryManualField(root,'onDuty')||entryFieldSource(root,'onDuty')==='manual'||root.manualOverride===true&&!!root.onDuty;
   const control=provisionalDutyControl(root,members,{id:makeActivityId(),state:'OPEN',provisional:true,start:resolvedStart,startBasis:manualStart?'manual':'screenshot',end:'',memberIds:members.map(r=>r.id),totalDutyAtLast:true});
   for(const member of members){const updated=stamp(controlledDutyFields(member,root,control));after[after.findIndex(r=>r.id===member.id)]=updated;const pos=changed.findIndex(r=>r.id===member.id);if(pos<0)changed.push(updated);else changed[pos]=updated}
  }
  commitCoreActivities(after);
  try{await persistCoreActivities()}catch(error){commitCoreActivities(before);await persistCoreActivities();throw error}
  changed.forEach(row=>markCloudEdited('activities',row,'screenshot-import'));await persistCoreActivities();
  rosterShotDiscard();$('rosterScreenshotDialog').close();
  if(entryReturnContext?.view==='rosterView'){
   const row=coreActivityRows().find(r=>r.id===$('editId')?.value);if(row)loadEntryToForm(row,{rosterActivity:true});
  }
  await render();scheduleAutoSync('screenshot-import');
  alert(`${changed.length} attività create o completate nel Roster.`);
 }catch(error){$('rosterScreenshotStatus').textContent=`Importazione non completata: ${error.message}`}
 finally{rosterShotBusy=false;rosterShotButtons()}
}
function rosterShotButtons(){for(const id of ['rosterScreenshotAnalyze','rosterScreenshotConfirm','rosterScreenshotCancel','rosterScreenshotFiles'])$(id).disabled=rosterShotBusy}
function rosterShotCardHeaders(blocks){
 const lines=screenshotWordLines(screenshotBlockWords(blocks)),headers=[];
 for(const line of lines){
  if(!/\b(?:3[O0]|[A-Z]{2})\s*\d{2,4}\b/i.test(line.text))continue;
  const route=rosterShotParseFlight(line.text);
  // Both airport codes and the flight number must be visible; never infer a route.
  if(route.length&&(!headers.length||line.y0-headers.at(-1).y0>40))headers.push(line);
 }
 return headers;
}
async function rosterShotReadCards(worker,file,result){
 const headers=rosterShotCardHeaders(result.data.blocks||[]);
 if(!headers.length)return {rows:[],count:0};
 const bitmap=await createImageBitmap(file),rows=[];
 try{
  for(let i=0;i<headers.length;i++){
   const header=headers[i],margin=Math.max(12,(header.y1-header.y0)*.5),top=Math.max(0,Math.floor(header.y0-margin));
   const bottom=i+1<headers.length?Math.floor(headers[i+1].y0-margin):bitmap.height;
   if(bottom<=top)continue;
   const height=bottom-top,scale=Math.min(2,Math.sqrt(12000000/(bitmap.width*height)));
   const canvas=document.createElement('canvas');canvas.width=Math.round(bitmap.width*scale);canvas.height=Math.round(height*scale);
   const context=canvas.getContext('2d',{alpha:false});context.fillStyle='#fff';context.fillRect(0,0,canvas.width,canvas.height);
   context.drawImage(bitmap,0,top,bitmap.width,height,0,0,canvas.width,canvas.height);
   $('rosterScreenshotStatus').textContent=`Lettura scheda volo ${i+1}/${headers.length}…`;
   try{const scan=await worker.recognize(canvas);rows.push(...rosterShotParseFlight(scan.data.text||''))}finally{canvas.width=0;canvas.height=0}
  }
 }finally{bitmap.close()}
 return {rows,count:headers.length};
}
async function rosterShotAnalyze(){
 if(rosterShotBusy||!rosterShotFiles.length)return;
 rosterShotBusy=true;rosterShotButtons();let worker;
 try{
  const T=await loadTesseractBrowser();worker=await T.createWorker('eng',1,{logger:m=>{if(m.status)$('rosterScreenshotStatus').textContent=`${m.status} ${Math.round((m.progress||0)*100)}%`}});
  rosterShotRows=[];rosterShotCrew=[];rosterShotPlan=[];$('rosterScreenshotConfirm').classList.add('hidden');
  if(T.PSM?.AUTO)await worker.setParameters({tessedit_pageseg_mode:T.PSM.AUTO});
  for(const file of rosterShotFiles){
   const result=await worker.recognize(file,{}, {blocks:true});const text=result.data.text||'';
   const wholeRows=rosterShotParseFlight(text),cardScan=await rosterShotReadCards(worker,file,result);
   const rows=rosterShotMerge([...wholeRows,...cardScan.rows]);
   const visibleDepartures=(text.match(/\bDEPARTURE\b/gi)||[]).length,expected=Math.max(cardScan.count,visibleDepartures);
   if(rows.length&&expected>rows.length)throw new Error(`Sono visibili almeno ${expected} voli, ma ne ho letto ${rows.length}. Importazione fermata: aggiungi screenshot separati delle schede mancanti.`);
   rosterShotRows.push(...rows);
   if(!rows.length&&/Nationality/i.test(text)){
    // Crop to the name column: photos, roles and contact icons are not imported.
    const bitmap=await createImageBitmap(file);const canvas=document.createElement('canvas');canvas.width=Math.round(bitmap.width*.55);canvas.height=bitmap.height;
    canvas.getContext('2d').drawImage(bitmap,bitmap.width*.30,0,bitmap.width*.55,bitmap.height,0,0,canvas.width,canvas.height);bitmap.close();
    const namesResult=await worker.recognize(canvas);const names=rosterShotParseCrew(namesResult.data.text||'');canvas.width=0;canvas.height=0;
    if(names.length)rosterShotCrew.push({names,start:''});else throw new Error('Nomi Crew non leggibili: usa uno screenshot completo e più nitido.');
   }else if(!rows.length)throw new Error('Screenshot non riconosciuto: usa Flight Info o Crew.');
  }
  rosterShotRows=rosterShotMerge(rosterShotRows);rosterShotFiles=[];$('rosterScreenshotFiles').value='';
  if(!rosterShotRows.length&&rosterShotCrew.length&&entryReturnContext?.view==='rosterView'){
   const current=coreActivityRows().find(r=>r.id===$('editId')?.value),duty=controlledDuty(current),start=current?.screenshotDutyStart||duty?.control.start;
   if(current&&isFlight(current)&&start){
    const members=duty?dutySequenceFor(current):coreActivityRows().filter(r=>r.screenshotDutyStart===start&&isFlight(r));
    rosterShotRows=members.map(r=>({date:r.date,flightNo:r.flightNo,dep:r.dep,arr:r.arr,schedOut:r.schedOut,schedIn:r.schedIn,screenshotDutyStart:start,_conflicts:{}}));
   }
  }
  if(!rosterShotRows.length)throw new Error('Aggiungi anche Flight Info per identificare il duty START e i voli.');
  rosterShotResolveView();$('rosterScreenshotStatus').textContent=`${rosterShotRows.length} voli letti. Verifica prima di preparare l’anteprima.`;
 }catch(error){$('rosterScreenshotStatus').textContent=`Lettura non completata: ${error.message}`}
 finally{if(worker)await worker.terminate();rosterShotBusy=false;rosterShotButtons()}
}
document.addEventListener('DOMContentLoaded',()=>{
 $('completeRosterScreenshot').onclick=rosterShotOpen;$('completeEntryScreenshot').onclick=rosterShotOpen;
 $('rosterScreenshotFiles').onchange=e=>{rosterShotFiles.push(...e.target.files);e.target.value='';rosterShotRows=[];rosterShotCrew=[];rosterShotPlan=[];$('rosterScreenshotReview').replaceChildren();$('rosterScreenshotConfirm').classList.add('hidden');$('rosterScreenshotStatus').textContent=`${rosterShotFiles.length} screenshot selezionati.`};
 $('rosterScreenshotAnalyze').onclick=rosterShotAnalyze;$('rosterScreenshotConfirm').onclick=rosterShotConfirm;
 $('rosterScreenshotCancel').onclick=()=>{if(!rosterShotBusy){rosterShotDiscard();$('rosterScreenshotDialog').close()}};
 $('rosterScreenshotDialog').addEventListener('cancel',e=>{if(rosterShotBusy)e.preventDefault();else rosterShotDiscard()});
 // Show the same acquisition command when editing an activity reached from Roster.
 const original=setEntryReturnContext;setEntryReturnContext=function(view,id=''){original(view,id);$('completeEntryScreenshotWrap').classList.toggle('hidden',view!=='rosterView')};
});

})();
