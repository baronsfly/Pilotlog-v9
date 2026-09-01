const CACHE='pilotlog9-9.0-core-v1';
const ASSETS=['./','./index.html','./app.css','./src/app.js','./src/db.js','./src/rules.js','./src/importers.js','./src/cloud.js','./manifest.webmanifest'];
self.addEventListener('install',e=>e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)).then(()=>self.skipWaiting())));
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k.startsWith('pilotlog9-')&&k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',e=>{
  if(e.request.method!=='GET')return;
  const url=new URL(e.request.url);
  // Never intercept AeroLINE, Supabase, WX, FX or CDN requests. PilotLog 9's
  // offline shell is isolated to its own origin and cannot affect PilotLog 8 caches.
  if(url.origin!==self.location.origin)return;
  e.respondWith(fetch(e.request).then(r=>{
    const cp=r.clone();caches.open(CACHE).then(c=>c.put(e.request,cp)).catch(()=>{});return r;
  }).catch(async()=>{
    const cached=await caches.match(e.request);if(cached)return cached;
    if(e.request.mode==='navigate')return caches.match('./index.html');
    return Response.error();
  }));
});
