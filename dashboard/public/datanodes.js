import { $, node, bytes, api, notify, jobURL } from './common.js';

let refreshing = false;
let submitting = false;
let latest;

async function submit(route) {
  if (submitting) return;
  submitting = true;
  render();
  try {
    const job = await api(route, { method: 'POST' });
    location.href = jobURL(job.id);
  } catch (error) { notify(error.message); }
  finally { submitting = false; render(); }
}

function render() {
  if (!latest) return;
  const busy = submitting || latest.busy;
  $('node-count').textContent = `${latest.nodes.filter(item => item.live).length} / ${latest.maximum} live`;
  $('rebalance').disabled = busy || Boolean(latest.error) || latest.nodes.filter(item => item.live && !item.excluded).length < 2;
  $('storage-message').hidden = !latest.error && !busy;
  $('storage-message').textContent = latest.error || 'A transfer or cluster operation is active. Storage controls will be available when it finishes.';
  $('node-cards').replaceChildren();
  for (const item of latest.nodes) {
    const card = node('article', 'storage-node');
    const heading = node('div', 'storage-node-heading');
    heading.append(node('h3', '', item.label), node('span', item.live ? 'badge' : 'badge neutral', item.live ? 'Live' : item.state));
    card.append(heading, node('p', 'storage-address', item.addresses.join(', ') || 'No active address'));
    const metrics = node('dl', 'storage-metrics');
    for (const [label, value] of [['HDFS state', item.adminState], ['Blocks', item.blocks ?? '—'], ['HDFS used', bytes(item.used)], ['Available', bytes(item.remaining)], ['Reported capacity', bytes(item.capacity)]]) {
      metrics.append(node('dt', '', label), node('dd', '', value));
    }
    card.append(metrics);
    const actions = node('div', 'storage-actions');
    if (item.primary) {
      const link = node('a', 'text-button', 'Primary node · view container →');
      link.href = '/#architecture';
      actions.append(link);
    } else {
      if (!item.enabled || !item.live || item.excluded) {
        const add = node('button', 'button primary', item.excluded ? 'Rejoin cluster' : item.enabled ? 'Start / rejoin' : 'Add DataNode');
        add.disabled = busy;
        add.addEventListener('click', () => submit(`/api/datanodes/${item.name}/add`));
        actions.append(add);
      }
      if (item.live) {
        const remove = node('button', 'button secondary', item.excluded ? 'Resume safe removal' : 'Remove safely');
        remove.disabled = busy || Boolean(latest.error);
        remove.addEventListener('click', () => submit(`/api/datanodes/${item.name}/remove`));
        actions.append(remove);
      }
    }
    card.append(actions);
    if (item.excluded) card.append(node('p', 'storage-note', 'Excluded from new writes. Rejoin to cancel decommissioning, or resume safe removal.'));
    $('node-cards').append(card);
  }
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  try { latest = await api('/api/datanodes'); render(); }
  catch (error) { notify(error.message); }
  finally { refreshing = false; }
}
$('refresh').addEventListener('click', refresh);
$('rebalance').addEventListener('click', () => submit('/api/datanodes/rebalance'));
await refresh();
setInterval(() => { if (!document.hidden) refresh(); }, 10000);
