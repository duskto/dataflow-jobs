const $ = id => document.getElementById(id);
const labels = {QUEUED:'排队中',RUNNING:'处理中',RETRY_WAIT:'等待重试',SUCCEEDED:'已完成',FAILED:'失败',CANCELLED:'已取消'};
let selected = null, nextOffset = null, items = [], busy = false;
$('token').value = sessionStorage.getItem('df-token') || '';
$('token').addEventListener('change', () => {sessionStorage.setItem('df-token', $('token').value); refresh();});
function notify(message, error=false) { $('notice').hidden=false; $('notice').textContent=message; $('notice').className=error?'error':''; }
async function api(path, options={}) {
  const headers = new Headers(options.headers || {});
  if ($('token').value) headers.set('Authorization',`Bearer ${$('token').value}`);
  const response = await fetch(`/api/v1${path}`, {...options, headers});
  if (!response.ok) { const body=await response.json().catch(()=>({message:'服务暂时不可用'})); throw new Error(body.message); }
  return response;
}
function el(tag, text, className) {const node=document.createElement(tag); if(text!=null) node.textContent=text; if(className) node.className=className; return node;}
$('files').addEventListener('change',()=>{$('file-list').replaceChildren(...Array.from($('files').files).map((f,i)=>el('li',`${i+1}. ${f.name} · ${(f.size/1024).toFixed(1)} KB`)));});
$('pipeline').addEventListener('change',()=>{const v=$('pipeline').value;$('custom').hidden=v!=='custom';$('columns').hidden=v==='custom'||v==='merge';$('keys').parentElement.hidden=!['all','merge-dedup'].includes(v);$('groups').parentElement.parentElement.hidden=!['all','merge-agg'].includes(v);});
const columns=id=>$(id).value.split(',').map(s=>s.trim()).filter(Boolean);
$('job-form').addEventListener('submit',async event=>{
  event.preventDefault(); $('submit').disabled=true;
  try {
    const type=$('pipeline').value; let steps=[{type:'merge',schema_policy:'strict'}];
    if(type==='custom') steps=JSON.parse($('custom').value);
    else {if(['all','merge-dedup'].includes(type)) steps.push({type:'deduplicate',keys:columns('keys'),keep:'first'}); if(['all','merge-agg'].includes(type)) steps.push({type:'aggregate',group_by:columns('groups'),sum:columns('sums')});}
    const files=Array.from($('files').files); if(!files.length||files.length>20) throw new Error('请选择 1 至 20 个 CSV 文件');
    const ids=[];
    for(const [i,file] of files.entries()) {notify(`正在上传 ${i+1}/${files.length}：${file.name}`);const body=new FormData();body.append('file',file);ids.push((await (await api('/files',{method:'POST',body})).json()).file_id);}
    const body={input_file_ids:ids,steps,csv_options:{encoding:$('encoding').value,delimiter:$('delimiter').value==='tab'?'\t':$('delimiter').value}};
    const job=await(await api('/jobs',{method:'POST',headers:{'Content-Type':'application/json','Idempotency-Key':crypto.randomUUID()},body:JSON.stringify(body)})).json();
    selected=job.id;notify('任务已提交，可以继续添加其他任务。'); await refresh();
  } catch(error){notify(error.message,true);} finally {$('submit').disabled=false;}
});
function drawJobs(){
  if(!items.length){$('jobs').replaceChildren(el('p','还没有任务。上传数据，开始第一次处理。','empty'));return;}
  $('jobs').replaceChildren(...items.map(job=>{
    const button=el('button',null,`job ${selected===job.id?'active':''}`);button.type='button';
    const left=el('span'); left.append(el('span',`任务 ${job.id.slice(0,8)}`,'job-name'),el('span',`${new Date(job.created_at).toLocaleString()} · ${job.steps.length} 个步骤`,'job-date'));
    button.append(left,el('span',job.cancel_requested&&job.status==='RUNNING'?'取消已请求':labels[job.status],`badge ${job.status}`));
    button.onclick=()=>{selected=job.id;drawJobs();loadDetail().catch(e=>notify(e.message,true));};return button;
  }));
}
async function loadDetail(){
  if(!selected) return; const id=selected;
  const [job,events]=await Promise.all([api(`/jobs/${id}`).then(r=>r.json()),api(`/jobs/${id}/events?limit=100`).then(r=>r.json())]);
  if(selected!==id)return;
  const grid=el('div',null,'detail-grid');
  [['当前阶段',job.cancel_requested&&job.status==='RUNNING'?'取消已请求':job.phase],['本次处理行数',job.processed_rows.toLocaleString()],['执行次数',`${job.attempt_count} / ${job.max_attempts}`]].forEach(([title,value])=>{const n=el('div');n.append(el('small',title),el('strong',value));grid.append(n);});
  const actions=el('div',null,'actions');
  function action(text,fn){const b=el('button',text);b.onclick=async()=>{b.disabled=true;try{await fn();await refresh();}catch(e){notify(e.message,true);}finally{b.disabled=false;}};actions.append(b);}
  if(job.status==='SUCCEEDED')action('下载结果 ↓',async()=>{const blob=await(await api(`/jobs/${id}/result`)).blob();const url=URL.createObjectURL(blob);const a=el('a');a.href=url;a.download=`${id}.csv`;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  if(['QUEUED','RUNNING','RETRY_WAIT'].includes(job.status)&&!job.cancel_requested)action('请求取消',()=>api(`/jobs/${id}/cancel`,{method:'POST'}));
  if(job.status==='FAILED')action('创建重试任务',async()=>{selected=(await(await api(`/jobs/${id}/retry`,{method:'POST',headers:{'Idempotency-Key':crypto.randomUUID()}})).json()).id;});
  if(events.archive)action(`下载归档日志（${events.archive.event_count} 条）`,async()=>{const blob=await(await api(`/jobs/${id}/events/archive`)).blob();const url=URL.createObjectURL(blob);const a=el('a');a.href=url;a.download=`${id}-events.jsonl.gz`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
  const logs=el('div',events.items.map(e=>`${new Date(e.created_at).toLocaleTimeString()}  ${e.event_type}\n${e.detail}`).join('\n\n')||(events.archive?'较早日志已归档，可下载查看。':'等待执行事件…'),'logs');
  $('detail').replaceChildren(grid,actions,logs);
  if(job.error_message)$('detail').append(el('small',`${job.error_code}: ${job.error_message}`));
}
async function loadOperations(){
  const data=await(await api('/ops')).json();
  $('ops-mode').textContent=data.mode==='celery'?'CELERY / POSTGRESQL':'LOCAL WORKERS';
  $('ops-stats').replaceChildren(...[
    ['在线 Worker',data.components_online.worker],['排队任务',data.jobs.QUEUED],
    ['最久等待',`${Math.round(data.queue_oldest_seconds)} 秒`],
    ['近 1 小时失败率',`${(data.recent_failure_rate*100).toFixed(1)}%`]
  ].map(([title,value])=>{const card=el('div',null,'ops-stat');card.append(el('small',title),el('strong',String(value)));return card;}));
  $('ops-alerts').replaceChildren(...(data.alerts.length?data.alerts.map(a=>el('span',a.message,'ops-alert')):[el('div','● 服务运行正常','ops-ok')]));
  const m=data.maintenance;
  $('ops-retention').textContent=`已归档 ${data.inputs_archived} 个输入、${data.events_archived} 条日志。最近成功巡检：${m&&m.last_succeeded_at?new Date(m.last_succeeded_at).toLocaleString():'尚未执行'}。归档输入仍可直接用于新任务，归档日志可在任务详情下载。`;
}
async function refresh(append=false){
  if(busy)return;busy=true;
  try{const offset=append?(nextOffset||0):0;const limit=append?20:Math.min(100,Math.max(20,items.length));const page=await(await api(`/jobs?limit=${limit}&offset=${offset}&status=${encodeURIComponent($('status-filter').value)}`)).json();items=append?[...items,...page.items]:page.items;nextOffset=page.next_offset;$('more').hidden=nextOffset===null;drawJobs();await Promise.all([loadDetail(),loadOperations()]);}
  catch(error){notify(error.message,true);}finally{busy=false;}
}
$('refresh').onclick=()=>refresh();$('more').onclick=()=>refresh(true);$('status-filter').onchange=()=>{items=[];refresh();};
refresh();setInterval(()=>{if(!document.hidden)refresh();},3000);
