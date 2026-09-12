import { $, node, api, notify, jobTitle, downloadURL, jobURL } from './common.js';
import { createFlow } from './flow.js';

let jobs = [];
let selectedId = new URLSearchParams(location.search).get('job');
let refreshing = false;
let renderedId = null;
const flow = createFlow($('flow'));

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
  }
  const log = $('job-log');
  const follow = renderedId !== job.id || log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  if (log.textContent !== job.log) log.textContent = job.log || 'Starting…';
  if (follow) log.scrollTop = log.scrollHeight;
  renderedId = job.id;
  $('job-result').replaceChildren();
  if (job.result) {
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
