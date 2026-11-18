const $=id=>document.getElementById(id);
const validMonth=value=>/^\d{4}-(0[1-9]|1[0-2])$/.test(value);
const bytesLabel=n=>n<1e6?`${(n/1000).toFixed(1)} KB`:`${(n/1e6).toFixed(2)} MB`;
const base=new URL('.',import.meta.url);
let pdfLibrary;
export async function loadPdf(){
  if(!pdfLibrary){
    pdfLibrary=await import('./vendor/pdfjs/pdf.min.mjs');
    pdfLibrary.GlobalWorkerOptions.workerSrc=new URL('./vendor/pdfjs/pdf.worker.min.mjs',base).href;
    // WKWebView's custom app scheme cannot reliably be imported by a module worker.
    // Native PDF parsing uses PDF.js's supported in-process worker handler instead.
    if(globalThis.window?.PilotLogNative?.isNative)globalThis.pdfjsWorker=await import('./vendor/pdfjs/pdf.worker.min.mjs');
  }
  return pdfLibrary;
}
export async function openPdf(blob){
  const pdf=await loadPdf();
  return pdf.getDocument({data:new Uint8Array(await blob.arrayBuffer()),isEvalSupported:false,useWorkerFetch:false,
    cMapUrl:new URL('./vendor/pdfjs/cmaps/',base).href,cMapPacked:true,
    standardFontDataUrl:new URL('./vendor/pdfjs/standard_fonts/',base).href,
    wasmUrl:new URL('./vendor/pdfjs/wasm/',base).href}).promise;
}
export function detectedMonth(text){
  const found=String(text).match(/For\s+Period\s*:\s*(0[1-9]|1[0-2])\s*\/\s*(\d{4})/i);
  return found?`${found[2]}-${found[1]}`:'';
}
export async function base64(blob){
  return new Promise((resolve,reject)=>{const r=new FileReader();r.onerror=()=>reject(r.error);r.onload=()=>resolve(String(r.result).split(',')[1]);r.readAsDataURL(blob)});
}
export function recordBlob(record){
  const binary=atob(record.data),bytes=new Uint8Array(binary.length);
  for(let i=0;i<bytes.length;i++)bytes[i]=binary.charCodeAt(i);
  return new Blob([bytes],{type:record.type});
}
async function fingerprint(blob){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer()))].map(x=>x.toString(16).padStart(2,'0')).join('')}
async function imageElement(blob){
  const url=URL.createObjectURL(blob),img=new Image();
  try{await new Promise((resolve,reject)=>{img.onload=resolve;img.onerror=()=>reject(Error('Image format not supported. Choose a JPEG or PNG.'));img.src=url});return img}finally{URL.revokeObjectURL(url)}
}
export async function compactImage(blob){
  const img=await imageElement(blob),ratio=Math.min(1,2400/Math.max(img.naturalWidth,img.naturalHeight));
  const canvas=document.createElement('canvas');canvas.width=Math.max(1,Math.round(img.naturalWidth*ratio));canvas.height=Math.max(1,Math.round(img.naturalHeight*ratio));
  const ctx=canvas.getContext('2d');ctx.fillStyle='#fff';ctx.fillRect(0,0,canvas.width,canvas.height);ctx.drawImage(img,0,0,canvas.width,canvas.height);
  const compressed=await new Promise(resolve=>canvas.toBlob(resolve,'image/jpeg',0.9));canvas.width=canvas.height=0;
  return compressed&&compressed.size<blob.size?compressed:blob;
}
export function ocrWorkerSource(core,worker,languageData,languageUrl){
  return core+`\nself.fetch=async function(url){if(String(url)!==${JSON.stringify(languageUrl)})throw new Error('Unexpected OCR resource');return new Response(Uint8Array.from(atob(${JSON.stringify(languageData)}),c=>c.charCodeAt(0)),{status:200});};\n`+worker;
}
async function newOcr(status){
  const {default:{createWorker}}=await import('./vendor/ocr/tesseract.esm.min.js');
  const resources=await Promise.all(['tesseract-core-lstm.wasm.js','worker.min.js','eng.traineddata'].map(async name=>{
    const response=await fetch(new URL('./vendor/ocr/'+name,base));
    if(!response.ok)throw Error('Local OCR resource unavailable: '+name);
    return name.endsWith('.traineddata')?new Uint8Array(await response.arrayBuffer()):response.text();
  }));
  // Preload both core and language in a Blob worker: no custom-scheme imports or
  // network requests are needed inside a WebView worker.
  const languageUrl=new URL('./vendor/ocr/eng.traineddata',base).href;
  const languageData=await base64(new Blob([resources[2]]));
  const url=URL.createObjectURL(new Blob([ocrWorkerSource(resources[0],resources[1],languageData,languageUrl)],{type:'application/javascript'}));
  try{
    const worker=await createWorker('eng',1,{workerPath:url,workerBlobURL:false,langPath:new URL('./vendor/ocr/',base).href,gzip:false,cacheMethod:'none',
      logger:m=>{if(m.status==='recognizing text')status(`Reading text… ${Math.round(100*m.progress)}%`)}});
    const terminate=worker.terminate;
    worker.terminate=async()=>{try{return await terminate()}finally{URL.revokeObjectURL(url)}};
    return worker;
  }catch(e){URL.revokeObjectURL(url);throw e}
}
function pageText(items){
  const lines=[];
  for(const item of items){
    if(typeof item.str!=='string')continue;
    const y=item.transform?.[5]||0,x=item.transform?.[4]||0;
    let line=lines.find(r=>Math.abs(r.y-y)<2);
    if(!line){line={y,items:[]};lines.push(line)}line.items.push({x,text:item.str});
  }
  return lines.sort((a,b)=>b.y-a.y).map(line=>line.items.sort((a,b)=>a.x-b.x).map(x=>x.text).join(' ')).join('\n');
}
export async function extractDocument(blob,status=()=>{}){
  let worker,pdf;const texts=[],methods=[];
  const recognize=async input=>{worker ||= await newOcr(status);return (await worker.recognize(input)).data.text||''};
  try{
    if(blob.type==='application/pdf'){
      pdf=await openPdf(blob);
      for(let n=1;n<=pdf.numPages;n++){
        status(`Reading page ${n} / ${pdf.numPages}…`);
        const page=await pdf.getPage(n),content=await page.getTextContent();
        let text=pageText(content.items),method='text';
        if(text.replace(/[^\p{L}\p{N}]/gu,'').length<20){
          const original=page.getViewport({scale:1}),scale=Math.min(2.5,Math.sqrt(8e6/(original.width*original.height))),view=page.getViewport({scale});
          const canvas=document.createElement('canvas');canvas.width=Math.ceil(view.width);canvas.height=Math.ceil(view.height);
          try{await page.render({canvasContext:canvas.getContext('2d'),viewport:view}).promise;text=await recognize(canvas);method='OCR'}finally{canvas.width=canvas.height=0}
        }
        texts.push(text);methods.push(method);page.cleanup();
      }
    }else{texts.push(await recognize(blob));methods.push('OCR')}
    const text=texts.join('\n\n');
    return {text,method:[...new Set(methods)].join(' + '),pages:texts.length,month:detectedMonth(text)};
  }finally{if(worker)await worker.terminate();if(pdf)await pdf.destroy()}
}
// Browser fallback has its own archive, outside the operational database and its backups.
function browserStore(){
  async function open(){return new Promise((resolve,reject)=>{
    const q=indexedDB.open('pilotlog-local-payslips',1);
    q.onupgradeneeded=()=>q.result.createObjectStore('documents',{keyPath:'id'});
    q.onsuccess=()=>resolve(q.result);q.onerror=()=>reject(q.error);
  })}
  async function action(mode,fn){const db=await open();try{return await new Promise((resolve,reject)=>{
    const tx=db.transaction('documents',mode);let result;fn(tx.objectStore('documents'),x=>{result=x});
    tx.oncomplete=()=>resolve(result);tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error||Error('Save interrupted'));
  })}finally{db.close()}}
  return {list:()=>action('readonly',(s,done)=>{s.getAll().onsuccess=e=>done(e.target.result.map(({data,...meta})=>meta))}),
    read:id=>action('readonly',(s,done)=>{s.get(id).onsuccess=e=>done(e.target.result)}),
    save:r=>action('readwrite',(s,done)=>{s.get(r.id).onsuccess=e=>{const old=e.target.result;if(!old)s.add(r);done({duplicate:!!old,record:old||r})}}),
    remove:id=>action('readwrite',s=>s.delete(id))};
}
export function initializePayslips({extract=extractDocument,compact=compactImage}={}){
  const store=window.PilotLogNative?.isNative?window.PilotLogNative.payslips:browserStore();
  let draft=null,busy=false,viewerPdf=null,viewerPage=1,viewerBlob=null,viewerName='',previewUrl='',previewRevision=0,opener=null;
  const status=text=>{$('payslipStatus').textContent=text};
  const setBusy=value=>{busy=value;for(const id of ['payslipFile','payslipCamera','payslipSave','payslipCancel','payslipKeepOriginal'])$(id).disabled=value};
  function clearDraft(){draft=null;$('payslipReview').classList.add('hidden');$('payslipText').value='';$('payslipFile').value='';$('payslipCamera').value=''}
  function button(label,fn){const b=document.createElement('button');b.type='button';b.className='secondary';b.textContent=label;b.addEventListener('click',()=>Promise.resolve().then(fn).catch(e=>status(e.message)));return b}
  async function render(){
    const container=$('payslipArchive');
    if(!store){container.textContent='Local archive unavailable.';return}
    const records=(await store.list()).sort((a,b)=>b.month.localeCompare(a.month)||b.createdAt.localeCompare(a.createdAt));
    container.replaceChildren();
    if(!records.length){container.textContent='No payslips saved.';return}
    let year='';
    for(const row of records){
      if(row.month.slice(0,4)!==year){year=row.month.slice(0,4);const h=document.createElement('h3');h.textContent=year;container.append(h)}
      const item=document.createElement('div');item.className='payslip-row';
      const name=document.createElement('b');name.textContent=`${row.month.slice(5)}/${row.month.slice(0,4)} · ${row.name}`;
      const size=document.createElement('div');size.className='small';size.textContent=`${bytesLabel(row.size)} · ${row.method||'Text not read'}`;
      const actions=document.createElement('div');actions.className='actions';
      actions.append(button('Open',async()=>{const doc=await store.read(row.id);await preview(recordBlob(doc),doc.name,doc.text)}),
        button('Export',async()=>{const doc=await store.read(row.id);await exportBlob(recordBlob(doc),doc.name)}),
        button('Delete',async()=>{if(busy)return;if(confirm(`Delete ${row.name} from this device?`)){await store.remove(row.id);await render();status('Deleted from this device.')}}));
      item.append(name,size,actions);container.append(item);
    }
  }
  async function exportBlob(blob,name){
    if(window.PilotLogNative?.isNative){await window.PilotLogNative.exportFile(name,blob,blob.type);return}
    const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),60000);
  }
  async function closePreview(){
    previewRevision++;$('payslipViewer').classList.add('hidden');
    if(viewerPdf){const previous=viewerPdf;viewerPdf=null;await previous.destroy()}
    if(previewUrl){URL.revokeObjectURL(previewUrl);previewUrl=''}
    $('payslipPreviewContent').replaceChildren();viewerBlob=null;opener?.focus();
  }
  async function renderPage(){
    const revision=++previewRevision,pdf=viewerPdf;if(!pdf)return;
    $('payslipPrev').disabled=true;$('payslipNext').disabled=true;
    const page=await pdf.getPage(viewerPage);if(revision!==previewRevision)return;
    const original=page.getViewport({scale:1}),scale=Math.min(2,1600/original.width),view=page.getViewport({scale});
    const canvas=document.createElement('canvas');canvas.width=Math.ceil(view.width);canvas.height=Math.ceil(view.height);
    await page.render({canvasContext:canvas.getContext('2d'),viewport:view}).promise;
    if(revision!==previewRevision)return;
    $('payslipPreviewContent').replaceChildren(canvas);$('payslipPage').textContent=`${viewerPage} / ${pdf.numPages}`;
    $('payslipPrev').disabled=viewerPage<=1;$('payslipNext').disabled=viewerPage>=pdf.numPages;
    page.cleanup();
  }
  async function preview(blob,name,text=''){
    await closePreview();opener=document.activeElement;viewerBlob=blob;viewerName=name;
    $('payslipViewer').classList.remove('hidden');$('payslipViewerTitle').textContent=name;
    $('payslipViewerText').textContent=text||'No text available.';
    $('payslipPreviewContent').textContent='Opening…';$('payslipPageControls').classList.toggle('hidden',blob.type!=='application/pdf');
    $('payslipViewerClose').focus();
    try{
      if(blob.type==='application/pdf'){const revision=previewRevision,pdf=await openPdf(blob);if(revision!==previewRevision){await pdf.destroy();return}viewerPdf=pdf;viewerPage=1;await renderPage()}
      else{previewUrl=URL.createObjectURL(blob);const img=new Image();img.alt='Payslip';img.src=previewUrl;$('payslipPreviewContent').replaceChildren(img)}
    }catch(e){$('payslipPreviewContent').textContent=`Preview unavailable: ${e.message}. Use Export to open the original.`}
  }
  async function acquire(file){
    if(busy||!file)return;
    clearDraft();setBusy(true);status('Reading document…');
    try{
      let type=file.type;
      if(/\.pdf$/i.test(file.name))type='application/pdf';
      if(!['application/pdf','image/jpeg','image/png','image/webp'].includes(type))throw Error('Choose a PDF, JPEG, PNG or WebP file.');
      if(!file.size)throw Error('The selected file is empty.');
      const original=file.slice(0,file.size,type),id=await fingerprint(original);
      const duplicate=(await store.list()).find(x=>x.id===id);
      if(duplicate){status(`Already saved: ${duplicate.month}.`);return}
      let result={text:'',method:'',pages:0,month:''},readError='';
      try{result=await extract(original,status)}catch(e){readError=`Text could not be read: ${e.message}. The original can still be saved.`}
      let blob=original;
      if(type!=='application/pdf'){try{blob=await compact(original)}catch{}}
      const name=blob.type!==type?file.name.replace(/\.[^.]+$/,'')+'.jpg':file.name;
      draft={id,original,blob,originalName:file.name,name,...result};
      $('payslipDocumentMonth').value=result.month||$('payrollMonth').value||new Date().toISOString().slice(0,7);
      $('payslipText').value=result.text;$('payslipFileSummary').textContent=`${name} · ${bytesLabel(blob.size)}${blob.size<original.size?` (original ${bytesLabel(original.size)})`:''}`;
      $('payslipKeepOriginal').checked=false;$('payslipOriginalChoice').classList.toggle('hidden',blob===original);
      $('payslipReview').classList.remove('hidden');status(readError||(result.text.trim()?'Ready to save.':'No text detected. You can save the document.'));
    }catch(e){status(e.message)}finally{setBusy(false)}
  }
  $('payslipFile').addEventListener('change',e=>acquire(e.target.files[0]));
  $('payslipCamera').addEventListener('change',e=>acquire(e.target.files[0]));
  $('payslipCancel').addEventListener('click',()=>{if(!busy){clearDraft();status('')}});
  $('payslipKeepOriginal').addEventListener('change',()=>{if(draft){const original=$('payslipKeepOriginal').checked;$('payslipFileSummary').textContent=`${original?draft.originalName:draft.name} · ${bytesLabel((original?draft.original:draft.blob).size)}`}});
  $('payslipPreviewDraft').addEventListener('click',()=>{if(draft){const original=$('payslipKeepOriginal').checked;preview(original?draft.original:draft.blob,original?draft.originalName:draft.name,draft.text)}});
  $('payslipSave').addEventListener('click',async()=>{
    if(busy||!draft)return;
    const month=$('payslipDocumentMonth').value;if(!validMonth(month)){status('Choose the payslip month.');return}
    setBusy(true);status('Saving…');
    try{
      const original=$('payslipKeepOriginal').checked,blob=original?draft.original:draft.blob;
      const record={id:draft.id,month,name:original?draft.originalName:draft.name,type:blob.type,size:blob.size,originalSize:draft.original.size,text:draft.text,method:draft.method,pages:draft.pages,createdAt:new Date().toISOString(),data:await base64(blob)};
      const result=await store.save(record);clearDraft();await render();status(result.duplicate?'Already saved on this device.':'Saved on this device.');
    }catch(e){status(`Save failed: ${e.message}`)}finally{setBusy(false)}
  });
  $('payslipViewerClose').addEventListener('click',closePreview);
  $('payslipViewerExport').addEventListener('click',()=>{if(viewerBlob)exportBlob(viewerBlob,viewerName).catch(e=>status(e.message))});
  $('payslipPrev').addEventListener('click',()=>{if(viewerPdf&&viewerPage>1){viewerPage--;renderPage().catch(e=>status(e.message))}});
  $('payslipNext').addEventListener('click',()=>{if(viewerPdf&&viewerPage<viewerPdf.numPages){viewerPage++;renderPage().catch(e=>status(e.message))}});
  $('payslipViewer').addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();closePreview()}else if(event.key==='Tab'){
    const controls=[...$('payslipViewer').querySelectorAll('button:not(:disabled),summary')].filter(x=>x.getClientRects().length),first=controls[0],last=controls.at(-1);
    if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus()}else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus()}
  }});
  window.addEventListener('pilotlog-close-payslip',closePreview);
  render().catch(e=>status(`Archive unavailable: ${e.message}`));
}
