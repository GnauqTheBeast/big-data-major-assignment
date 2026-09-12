import { $, node, bytes, basename, query, downloadURL, jobURL, api, notify } from './common.js';
const state = { directory: '/', files: [], selected: null, blocks: [], services: [], jobs: [], jobId: null, selectionVersion: 0, directoryVersion: 0, uploading: false, controls: [], pendingControls: new Map(), hdfsBusy: false };
const parent = path => path.slice(0, path.lastIndexOf('/')) || '/';

function action(button, work) {
  button.addEventListener('click', async () => {
    button.disabled = true;
    try { await work(); } catch (error) { notify(error.message); }
    finally { button.disabled = false; updateButtons(); }
  });
}
function updateButtons() {
  const busy = state.jobs.some(job => job.status === 'running');
  const controls = new Map([...state.controls.map(item => [item.service, item.action]), ...state.pendingControls]);
  const maintenance = state.jobs.some(job => job.status === 'running' && job.kind !== 'sort');
  const hdfsChanging = [...controls.keys()].some(name => name === 'namenode' || name.startsWith('datanode')) || maintenance;
  const hdfsBusy = state.hdfsBusy || state.uploading || state.jobs.some(job => job.kind === 'sort' && job.status === 'running');
  $('sort').disabled = busy || !state.selected || hdfsChanging;
  $('start-cluster').disabled = busy || controls.size > 0;
  $('file-upload').disabled = state.uploading || hdfsChanging;
  for (const button of document.querySelectorAll('button.service-control')) {
    const service = button.dataset.service;
    const pending = controls.get(service);
    const protectedHdfs = (service === 'namenode' || service.startsWith('datanode')) && hdfsBusy;
    button.disabled = Boolean(pending) || protectedHdfs || maintenance || button.dataset.state === 'unknown';
    button.textContent = pending ? (pending === 'stop' ? 'Stopping…' : 'Starting…') : button.dataset.action === 'stop' ? 'Stop' : 'Start';
    button.title = protectedHdfs ? 'Wait for the active upload, download, or sort to finish.' : `${button.dataset.action === 'stop' ? 'Stop' : 'Start'} only ${service}`;
  }
}

function renderArchitecture() {
  const groups = $('service-groups'); groups.replaceChildren();
  for (const [key, title] of [['hdfs', 'HDFS STORAGE'], ['yarn', 'YARN'], ['spark', 'SPARK']]) {
    const group = node('div', 'service-group');
    group.append(node('div', 'group-title', title));
    state.services.filter(service => service.group === key).forEach((service, index) => {
      if (index) group.append(node('div', 'connector', '↕'));
      const holdsBlocks = state.blocks.some(block => block.locations.some(location => service.addresses.some(address => location === address + ':9866') || location.startsWith(service.name + ':')));
      const card = node('div', 'service-card' + (holdsBlocks ? ' holds-blocks' : ''));
      const name = node('div', 'service-name');
      const label = node(service.port ? 'a' : 'span', '', service.label);
      if (service.port) { label.href = `http://localhost:${service.port}`; label.target = '_blank'; label.rel = 'noopener noreferrer'; label.title = `Open ${service.label} native UI`; }
      const dot = node('span', 'status-dot ' + (service.state === 'running' ? 'running' : ''));
      dot.title = service.state;
      name.append(label, dot);
      card.append(name, node('div', 'service-role', service.role), node('div', 'service-state ' + service.state, service.state + (service.health ? ` · ${service.health}` : '')));
      if (service.addresses.length) card.append(node('div', 'service-state', service.addresses.join(', ')));
      if (holdsBlocks) card.append(node('div', 'service-state running', '● Selected file blocks'));
      if (service.optional) {
        const manage = node('a', 'service-control', 'Manage node');
        manage.href = '/datanodes';
        card.append(manage);
        group.append(card);
        return;
      }
      const control = node('button', 'service-control');
      control.dataset.service = service.name;
      control.dataset.state = service.state;
      control.dataset.action = ['running', 'restarting', 'paused'].includes(service.state) ? 'stop' : 'start';
      control.setAttribute('aria-label', `${control.dataset.action === 'stop' ? 'Stop' : 'Start'} ${service.label}`);
      control.addEventListener('click', () => controlService(service.name, control.dataset.action));
      card.append(control);
      group.append(card);
    });
    groups.append(group);
  }
  updateButtons();
}

async function controlService(service, operation) {
  if (state.pendingControls.has(service)) return;
  state.pendingControls.set(service, operation); updateButtons();
  try {
    await api(`/api/services/${encodeURIComponent(service)}/${operation}`, { method: 'POST' });
    notify(`${service} ${operation === 'stop' ? 'stopped' : 'started'}.`, true);
  } catch (error) { notify(error.message); }
  finally { state.pendingControls.delete(service); await refreshStatus(); updateButtons(); }
}
let statusRefreshing = false;
async function refreshStatus() {
  if (statusRefreshing) return;
  statusRefreshing = true;
  try {
    const data = await api('/api/status');
    state.services = data.services;
    state.controls = data.controls || [];
    state.hdfsBusy = data.hdfsBusy || false;
    $('cluster-count').textContent = `${data.services.filter(service => service.state === 'running').length} / ${data.services.length} running`;
    $('cluster-error').hidden = !data.error;
    $('cluster-error').textContent = data.error ? `Container status unavailable: ${data.error}` : '';
    renderArchitecture();
  } catch (error) {
    $('cluster-count').textContent = 'Unavailable';
    $('cluster-error').hidden = false; $('cluster-error').textContent = error.message;
    state.services = state.services.map(service => ({ ...service, state: 'unknown', health: '', addresses: [] }));
    renderArchitecture();
  } finally { statusRefreshing = false; }
}

function renderFiles() {
  $('file-list').replaceChildren();
  for (const file of state.files) {
    const row = node('tr', state.selected?.path === file.path ? 'selected' : '');
    const name = node('td');
    const button = node('button', 'file-button'); button.title = file.path;
    button.append(node('span', 'file-symbol', file.directory ? '▱' : '▤'), node('span', 'filename', file.name));
    button.addEventListener('click', () => file.directory ? browse(file.path) : selectFile(file));
    name.append(button); row.append(name, node('td', '', file.directory ? '—' : bytes(file.size)), node('td', '', file.replication ?? '—'));
    $('file-list').append(row);
  }
  $('file-count').textContent = `${state.files.length} item${state.files.length === 1 ? '' : 's'}`;
}
async function browse(directory) {
  const version = ++state.directoryVersion;
  state.directory = directory;
  $('hdfs-path').value = directory; $('parent').disabled = directory === '/';
  $('file-message').textContent = 'Loading HDFS files…'; $('file-message').hidden = false;
  state.files = []; renderFiles();
  try {
    const data = await api('/api/files' + query(directory));
    if (version !== state.directoryVersion) return;
    state.files = data.files.sort((a, b) => Number(b.directory) - Number(a.directory) || a.name.localeCompare(b.name));
    renderFiles();
    $('file-message').hidden = state.files.length > 0;
    $('file-message').textContent = 'This directory is empty. Upload a file to get started.';
  } catch (error) {
    if (version !== state.directoryVersion) return;
    $('file-message').textContent = `Cannot read HDFS. ${error.message}`;
  }
}
async function selectFile(file) {
  const version = ++state.selectionVersion;
  state.selected = file; state.blocks = [];
  $('selection-empty').hidden = true; $('selection-details').hidden = false;
  $('selected-name').textContent = file.name; $('selected-path').textContent = file.path;
  $('download-input').href = downloadURL(file.path);
  $('preview').textContent = 'Loading preview…'; $('blocks').textContent = 'Locating blocks…'; $('block-report').textContent = '';
  renderFiles(); renderArchitecture(); updateButtons();
  await Promise.allSettled([
    api('/api/preview' + query(file.path)).then(data => { if (version === state.selectionVersion) $('preview').textContent = data.text || '(Empty file)'; }).catch(error => { if (version === state.selectionVersion) $('preview').textContent = error.message; }),
    refreshBlocks(version),
  ]);
}
async function refreshBlocks(version = state.selectionVersion) {
  if (!state.selected) return;
  try {
    const data = await api('/api/blocks' + query(state.selected.path));
    if (version !== state.selectionVersion) return;
    state.blocks = data.blocks;
    $('block-report').textContent = data.report;
    $('blocks').replaceChildren();
    if (!data.blocks.length) $('blocks').textContent = 'This empty file has no data blocks.';
    for (const block of data.blocks) {
      const row = node('div', 'block-row');
      row.append(node('strong', '', block.id), node('p', '', `${bytes(block.size)} · ${block.replicas} live replica${block.replicas === 1 ? '' : 's'}`));
      for (const location of block.locations) {
        const service = state.services.find(service => service.addresses.some(address => location === address + ':9866') || location.startsWith(service.name + ':'));
        row.append(node('p', '', `↳ ${service ? service.label + ' · ' : ''}${location}`));
      }
      if (!block.locations.length) row.append(node('p', '', 'No replica location reported. See raw HDFS report.'));
      $('blocks').append(row);
    }
    renderArchitecture();
  } catch (error) {
    if (version !== state.selectionVersion) return;
    state.blocks = []; renderArchitecture(); $('blocks').textContent = 'Block locations unavailable: ' + error.message;
  }
}

async function upload(file) {
  if (!file || state.uploading) return;
  if (!file.size) return notify('Choose a non-empty file.');
  state.uploading = true; updateButtons(); notify('');
  $('upload-state').hidden = false; $('upload-progress').value = 0;
  $('upload-text').textContent = `Uploading ${file.name}…`;
  try {
    const data = await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/upload?name=' + encodeURIComponent(file.name));
      xhr.timeout = 0;
      xhr.upload.onprogress = event => {
        if (event.lengthComputable) $('upload-progress').value = event.loaded / event.total * 100;
        $('upload-text').textContent = `Uploading ${bytes(event.loaded)} / ${bytes(file.size)} to HDFS…`;
        if (event.loaded === event.total) $('upload-text').textContent = 'Saving file to HDFS…';
      };
      xhr.onload = () => { try { const result = JSON.parse(xhr.responseText); if (xhr.status >= 400) reject(new Error(result.error)); else resolve(result); } catch { reject(new Error('The server returned an invalid upload response.')); } };
      xhr.onerror = () => reject(new Error('Upload failed. Check that the Node.js server is running.'));
      xhr.ontimeout = () => reject(new Error('Upload timed out. Check cluster health before retrying.'));
      xhr.send(file);
    });
    notify('File uploaded to HDFS. Select Run sort when you’re ready.', true);
    await browse(parent(data.path));
    await selectFile({ path: data.path, name: basename(data.path), size: file.size, directory: false });
  } catch (error) { notify(error.message); }
  finally { state.uploading = false; $('upload-state').hidden = true; $('file-upload').value = ''; updateButtons(); }
}

let jobsRefreshing = false;
async function refreshJobs() {
  if (jobsRefreshing) return;
  jobsRefreshing = true;
  try {
    const data = await api('/api/jobs');
    const completed = data.jobs.filter(job => job.status !== 'running' && state.jobs.some(previous => previous.id === job.id && previous.status === 'running'));
    state.jobs = data.jobs;
    if (!data.jobs.some(job => job.id === state.jobId)) state.jobId = null;
    updateButtons();
    for (const job of completed) {
      notify(job.status === 'succeeded' ? (job.kind === 'cluster' ? 'Containers started. HDFS may need a moment to become ready; click Refresh to reconnect.' : 'Operation complete. Open Job activity for the logs and any results.') : 'Operation failed. Open Job activity for the error log.', job.status === 'succeeded');
      if (job.kind === 'cluster') { await refreshStatus(); await browse(state.directory); }
    }
  } catch (error) { notify('Cannot refresh jobs: ' + error.message); }
  finally { jobsRefreshing = false; }
}

$('file-upload').addEventListener('change', event => upload(event.target.files[0]));
$('dropzone').addEventListener('dragover', event => { event.preventDefault(); $('dropzone').classList.add('dragging'); });
$('dropzone').addEventListener('dragleave', () => $('dropzone').classList.remove('dragging'));
$('dropzone').addEventListener('drop', event => { event.preventDefault(); $('dropzone').classList.remove('dragging'); upload(event.dataTransfer.files[0]); });
$('path-form').addEventListener('submit', event => { event.preventDefault(); browse($('hdfs-path').value); });
$('parent').addEventListener('click', () => browse(parent(state.directory)));
$('browse-root').addEventListener('click', () => browse('/'));
action($('refresh-blocks'), () => refreshBlocks());
action($('refresh'), () => Promise.allSettled([refreshStatus(), browse(state.directory), refreshJobs(), refreshBlocks()]));
action($('sort'), async () => {
  const job = await api('/api/jobs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ input: state.selected.path }) });
  location.href = jobURL(job.id);
});
action($('start-cluster'), async () => {
  const job = await api('/api/cluster/start', { method: 'POST' });
  location.href = jobURL(job.id);
});
const initialFile = new URLSearchParams(location.search).get('file');
await Promise.allSettled([refreshStatus(), browse(initialFile ? parent(initialFile) : '/'), refreshJobs()]);
if (initialFile) await selectFile({ path: initialFile, name: basename(initialFile), directory: false });
setInterval(() => { if (!document.hidden) refreshStatus(); }, 10000);
setInterval(() => { if (!document.hidden) refreshJobs(); }, 2000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) { refreshStatus(); refreshJobs(); } });
