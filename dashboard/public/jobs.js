import { $, node, api, notify, jobTitle, downloadURL, jobURL } from './common.js';
import { createFlow } from './flow.js';

let jobs = [];
let selectedId = new URLSearchParams(location.search).get('job');
let refreshing = false;
let renderedId = null;
const flow = createFlow($('flow'));

function renderRows(tbody, rows) {
  tbody.replaceChildren();
  if (!rows || !rows.length) {
    const cell = node('td', '', 'No rows returned.');
    cell.colSpan = 3;
    tbody.append(node('tr', '', cell));
    return;
  }
  rows.forEach((row, index) => {
    tbody.append(node('tr', '', node('td', '', `#${index + 1}`), node('td', '', row.item), node('td', '', row.count ?? '—')));
  });
}

function resultTable(rows) {
  const table = node('table', 'topk-table');
  table.append(node('thead', '', node('tr', '', node('th', '', 'Rank'), node('th', '', 'Item'), node('th', '', 'Count'))));
  const tbody = node('tbody');
  renderRows(tbody, rows);
  table.append(tbody);
  return table;
}

function render() {
  $('job-count').textContent = jobs.length;
  $('jobs-empty').hidden = jobs.length > 0;
  $('job-workspace').hidden = jobs.length === 0;
  $('job-list').replaceChildren();
  for (const job of jobs) {
    const button = node('button', 'job-item' + (selectedId === job.id ? ' active' : ''));
    button.append(node('strong', '', jobTitle(job)), node('small', '', `${job.status} · ${new Date(job.started).toLocaleTimeString()}`));
    button.addEventListener('click', () => {
      selectedId = job.id;
      history.replaceState(null, '', jobURL(job.id));
      render();
    });
    $('job-list').append(button);
  }
  const job = jobs.find(item => item.id === selectedId);
  if (!job) { flow.setJob(null); return; }
  $('job-meta').replaceChildren(node('strong', '', jobTitle(job)), node('span', 'job-status ' + job.status, job.status));
  $('job-paths').replaceChildren();
  if (job.kind === 'sort') {
    $('job-paths').append(node('div', '', `Input: ${job.input}`), node('div', '', `Output: ${job.output}`));
  } else if (job.kind === 'topk-hadoop') {
    $('job-paths').append(node('div', '', `Input: ${job.input} · K=${job.k ?? '—'}`), node('div', '', `Output: ${job.output}`));
  }
  const log = $('job-log');
  const follow = renderedId !== job.id || log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  if (log.textContent !== job.log) log.textContent = job.log || 'Starting…';
  if (follow) log.scrollTop = log.scrollHeight;
  renderedId = job.id;
  $('job-result').replaceChildren();
  const topk = job.kind === 'topk-hadoop';
  if (topk && job.status === 'succeeded' && job.rows) {
    $('job-result').append(resultTable(job.rows));
    const inspect = node('a', 'button secondary', 'Inspect result');
    inspect.href = '/topk?file=' + encodeURIComponent(job.result) + '#files';
    const download = node('a', 'text-button', '↓ Download result');
    download.href = downloadURL(job.result);
    $('job-result').append(inspect, download);
  } else if (job.result) {
    const inspect = node('a', 'button secondary', 'Inspect sorted output');
    inspect.href = '/?file=' + encodeURIComponent(job.result) + '#files';
    const download = node('a', 'text-button', '↓ Download result');
    download.href = downloadURL(job.result);
    $('job-result').append(inspect, download);
  } else if (job.status === 'failed') {
    $('job-result').textContent = 'The operation failed. Review its system log above before retrying.';
  }
  flow.setJob(job);
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try {
    jobs = (await api('/api/jobs')).jobs;
    if (selectedId && !jobs.some(job => job.id === selectedId)) notify('That job is no longer in this server session. Showing the latest available job.');
    if (!jobs.some(job => job.id === selectedId)) selectedId = jobs[0]?.id || null;
    render();
  } catch (error) { notify(error.message); }
  finally { refreshing = false; }
}

$('refresh').addEventListener('click', refresh);
await refresh();
setInterval(() => { if (!document.hidden) refresh(); }, 2000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
