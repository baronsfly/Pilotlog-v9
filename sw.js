// PilotLog v9.1 development mode: do not cache application files.
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',event=>event.waitUntil((async()=>{
  const keys=await caches.keys();
  await Promise.all(keys.filter(k=>k.startsWith('pilotlog9-')).map(k=>caches.delete(k)));
  await self.clients.claim();
})()));
self.addEventListener('fetch',()=>{});
