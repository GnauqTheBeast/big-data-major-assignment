export const $ = id => document.getElementById(id);

export function node(tag, className, ...children) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  for (const child of children) {
    if (child == null) continue;
    if (typeof child === 'string' || typeof child === 'number') element.append(String(child));
    else element.append(child);
  }
  return element;
}

export function bytes(size) {
  if (size == null || !Number.isFinite(size)) return '—';
  for (const [unit, factor] of [['TB', 1024 ** 4], ['GB', 1024 ** 3], ['MB', 1024 ** 2], ['KB', 1024]]) {
    if (size >= factor) return `${(size / factor).toFixed(1)} ${unit}`;
  }
  return `${size} B`;
}

export async function api(url, options = {}) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(90000) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

export function notify(message, success = false) {
  $('notice').hidden = !message;
  $('notice').textContent = message;
  $('notice').className = 'notice' + (success ? ' success' : '');
}

export const basename = path => path?.split('/').filter(Boolean).at(-1) || '/';
export const query = path => '?path=' + encodeURIComponent(path);
export const downloadURL = path => '/api/download' + query(path);
export const jobURL = id => '/jobs?job=' + encodeURIComponent(id);

export function jobTitle(job) {
  if (job.kind === 'sort') return basename(job.input);
  if (job.kind === 'topk-hadoop') return `Top-K K=${job.k ?? '?'} · ${basename(job.input)}`;
  if (job.kind === 'cluster') return 'Start cluster';
  if (job.kind === 'add-node') return `Add ${job.input}`;
  if (job.kind === 'remove-node') return `Remove ${job.input} safely`;
  if (job.kind === 'rebalance') return 'Rebalance HDFS';
  return job.kind;
}
