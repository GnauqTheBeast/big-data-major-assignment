import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hadoop, validatePath, validateSortInput, validateServiceAction, validateTopK } from './lib/hadoop.js';
import { Jobs } from './lib/jobs.js';
import { DataNodes, DATA_NODES, isDataNode } from './lib/datanodes.js';

const assets = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/topk', ['topk.html', 'text/html; charset=utf-8']],
  ['/jobs', ['jobs.html', 'text/html; charset=utf-8']],
  ['/datanodes', ['datanodes.html', 'text/html; charset=utf-8']],
  ['/app.js', ['app.js', 'text/javascript; charset=utf-8']],
  ['/common.js', ['common.js', 'text/javascript; charset=utf-8']],
  ['/jobs.js', ['jobs.js', 'text/javascript; charset=utf-8']],
  ['/topk.js', ['topk.js', 'text/javascript; charset=utf-8']],
  ['/flow.js', ['flow.js', 'text/javascript; charset=utf-8']],
  ['/datanodes.js', ['datanodes.js', 'text/javascript; charset=utf-8']],
  ['/pages.css', ['pages.css', 'text/css; charset=utf-8']],
  ['/style.css', ['style.css', 'text/css; charset=utf-8']],
]);
const problem = (message, status = 400) => Object.assign(new Error(message), { status });

function body(req, limit) {
  return new Promise((resolve, reject) => {
    let size = 0, chunks = [], exceeded = false;
    req.on('data', chunk => {
      size += chunk.length;
      if (size > limit) { exceeded = true; chunks = []; reject(problem(`Request exceeds the ${limit} byte limit.`, 413)); }
      if (!exceeded) chunks.push(chunk);
    });
    req.on('end', () => { if (!exceeded) resolve(Buffer.concat(chunks)); });
    req.on('error', reject);
    req.on('aborted', () => reject(problem('Upload interrupted.')));
  });
}

export function createApp({ backend = new Hadoop() } = {}) {
  const datanodes = new DataNodes(backend);
  const TOPK_KINDS = new Set(['topk-hadoop', 'topk-spark', 'topk-compare']);
  const topkOutputPaths = output => ({ hadoop: `${output}/hadoop`, spark: `${output}/spark` });
  const jobs = new Jobs(async (input, output, log, kind, extra = {}, record) => {
    if (kind === 'cluster') return datanodes.startCluster(log);
    if (kind === 'add-node') return datanodes.add(input, log);
    if (kind === 'remove-node') return datanodes.remove(input, log);
    if (kind === 'rebalance') return datanodes.rebalance(log);
    if (TOPK_KINDS.has(kind)) {
      const k = validateTopK(extra?.k);
      const paths = topkOutputPaths(output);
      if (kind === 'topk-hadoop') {
        await backend.topkHadoop(input, paths.hadoop, k, log);
        return backend.topkResult(paths.hadoop, 'hadoop');
      }
      if (kind === 'topk-spark') {
        await backend.topkSpark(input, paths.spark, k, log);
        return backend.topkResult(paths.spark, 'spark');
      }
      await backend.topkHadoop(input, paths.hadoop, k, log);
      const hadoop = await backend.topkResult(paths.hadoop, 'hadoop');
      await backend.topkSpark(input, paths.spark, k, log);
      const spark = await backend.topkResult(paths.spark, 'spark');
      const normalize = rows => (rows || []).map(row => `${row.item}\t${row.count}`).join('\n');
      const match = normalize(hadoop.rows) === normalize(spark.rows);
      log(match ? '\nBoth engines agree on the top-K answer.\n' : '\nThe engines disagree — inspect both outputs below.\n');
      record.outputs = paths;
      return { rows: hadoop.rows, comparison: { match, hadoop: hadoop.rows, spark: spark.rows } };
    }
    return backend.sort(input, output, log);
  });
  let uploading = false;
  let downloads = 0;
  const controls = new Map();
  const maintenance = () => jobs.list().some(job => job.status === 'running' && job.kind !== 'sort');
  const hdfsChanging = () => controls.has('namenode') || DATA_NODES.some(name => controls.has(name)) || maintenance();
  const hdfsBusy = () => uploading || downloads > 0 || jobs.list().some(job => job.status === 'running' && job.kind === 'sort');
  const requireHdfsStable = () => { if (hdfsChanging()) throw problem('An HDFS container is starting or stopping. Wait for it to finish.', 409); };
  const server = http.createServer(async (req, res) => {
    const json = (value, status = 200) => {
      res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(value));
    };
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Content-Security-Policy', "default-src 'self'; style-src 'self'; script-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
    try {
      const host = req.headers.host || '';
      if (!/^(localhost|127\.0\.0\.1)(:\d+)?$/.test(host)) throw problem('Use localhost or 127.0.0.1 to access this dashboard.', 403);
      if (req.headers.origin && req.headers.origin !== `http://${host}`) throw problem('Cross-origin requests are not allowed.', 403);
      if (req.headers['sec-fetch-site'] === 'cross-site') throw problem('Cross-site requests are not allowed.', 403);
      const url = new URL(req.url, `http://${host}`);
      const route = url.pathname;
      const inputPath = () => validatePath(url.searchParams.get('path'));
      if (req.method === 'GET' && assets.has(route)) {
        const [filename, mime] = assets.get(route);
        const content = await readFile(new URL('./public/' + filename, import.meta.url));
        res.writeHead(200, { 'Content-Type': mime }); res.end(content); return;
      }
      if (req.method === 'GET' && route === '/api/status') return json({ ...await backend.status(), controls: [...controls].map(([service, action]) => ({ service, action })), hdfsBusy: hdfsBusy() });
      if (req.method === 'GET' && route === '/api/datanodes') return json({ ...await datanodes.status(), busy: hdfsBusy() || maintenance() || controls.size > 0 });
      if (req.method === 'POST' && route.startsWith('/api/datanodes/')) {
        if (hdfsBusy() || maintenance() || controls.size) throw problem('Wait for current transfers, jobs, and container operations before changing storage.', 409);
        const parts = route.split('/');
        if (route === '/api/datanodes/rebalance') return json(jobs.start(null, 'rebalance'), 202);
        const service = parts[3], action = parts[4];
        if (parts.length !== 5 || !DATA_NODES.slice(1).includes(service) || !['add', 'remove'].includes(action)) throw problem('Choose Add or Remove for DataNode 2 or 3.');
        return json(jobs.start(service, action === 'add' ? 'add-node' : 'remove-node'), 202);
      }
      if (req.method === 'POST' && route.startsWith('/api/services/')) {
        const parts = route.split('/');
        const service = parts[3], action = parts[4];
        if (parts.length !== 5) throw problem('Invalid service control route.');
        validateServiceAction(service, action);
        if (controls.has(service)) throw problem('This container already has an operation in progress.', 409);
        if (maintenance()) throw problem('Cluster maintenance is in progress. See Job activity.', 409);
        if (['datanode-2', 'datanode-3'].includes(service)) throw problem('Use the DataNodes page to add, rejoin, or safely remove this node.');
        if ((service === 'namenode' || isDataNode(service)) && hdfsBusy()) throw problem('HDFS is busy with an upload, download, or sort. Wait before changing its containers.', 409);
        controls.set(service, action);
        try { await backend.controlService(service, action); return json({ service, action }); }
        finally { controls.delete(service); }
      }
      if (req.method === 'GET' && route === '/api/files') return json({ files: await backend.list(inputPath()) });
      if (req.method === 'GET' && route === '/api/preview') return json(await backend.preview(inputPath()));
      if (req.method === 'GET' && route === '/api/blocks') return json(await backend.blocks(inputPath()));
      if (req.method === 'GET' && route === '/api/jobs') return json({ jobs: jobs.list() });
      if (req.method === 'GET' && route.startsWith('/api/jobs/')) {
        const job = jobs.get(route.slice('/api/jobs/'.length));
        if (!job) throw problem('Job not found. History resets when the server restarts.', 404);
        return json(job);
      }
      if (req.method === 'POST' && route === '/api/cluster/start') {
        if (controls.size || hdfsBusy()) throw problem('Wait for the current transfer or container operation to finish.', 409);
        return json(jobs.start(null, 'cluster'), 202);
      }
      if (req.method === 'POST' && route === '/api/jobs') {
        let data;
        try { data = JSON.parse((await body(req, 8192)).toString()); }
        catch (error) { if (error.status) throw error; throw problem('Invalid JSON request.'); }
        const kind = data?.kind || 'sort';
        if (!['sort', 'topk-hadoop', 'topk-spark', 'topk-compare'].includes(kind)) throw problem('Unknown job kind.');
        const input = validateSortInput(data?.input);
        const k = kind === 'sort' ? null : validateTopK(data?.k);
        requireHdfsStable();
        await backend.requireFile(input);
        requireHdfsStable();
        return json(jobs.start(input, kind, { k }), 202);
      }
      if (req.method === 'POST' && route === '/api/upload') {
        requireHdfsStable();
        if (uploading) throw problem('Another upload is in progress.', 409);
        uploading = true;
        try {
          const filename = url.searchParams.get('name');
          if (!filename || filename.length > 255 || /[/\\\x00-\x1f]/.test(filename)) throw problem('Provide a filename without directory separators.');
          const scope = url.searchParams.get('scope') || 'integer-sort';
          if (!['integer-sort', 'top-k'].includes(scope)) throw problem('Choose a valid upload scope.');
          if (req.headers['content-length'] === '0') throw problem('Choose a non-empty file.');
          // Handle disconnects even while the backend is preparing the HDFS directory.
          req.on('error', () => {});
          req.setTimeout(5 * 60 * 1000, () => req.destroy(new Error('Upload stalled for five minutes.')));
          return json({ path: await backend.upload(req, filename, scope) }, 201);
        } finally { uploading = false; req.setTimeout(0); req.resume(); }
      }
      if (req.method === 'GET' && route === '/api/download') {
        requireHdfsStable();
        const file = inputPath();
        await backend.requireFile(file);
        if (res.destroyed || req.aborted) return;
        requireHdfsStable();
        downloads++;
        res.once('close', () => { downloads--; });
        const child = backend.download(file);
        let stderr = '';
        const timer = setTimeout(() => { child.kill(); res.destroy(new Error('Download timed out.')); }, 5 * 60 * 1000);
        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(path.posix.basename(file)).replace(/'/g, '%27')}`);
        child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-8192); });
        child.stdout.pipe(res, { end: false });
        child.on('error', error => { clearTimeout(timer); if (!res.headersSent) json({ error: error.message }, 502); else res.destroy(error); });
        child.on('close', code => {
          clearTimeout(timer);
          if (res.destroyed || res.writableEnded) return;
          if (code === 0) res.end();
          else if (!res.headersSent) json({ error: stderr || 'Download failed.' }, 502);
          else res.destroy(new Error('Download interrupted.'));
        });
        res.on('close', () => { clearTimeout(timer); child.kill(); });
        return;
      }
      throw problem('Not found.', 404);
    } catch (error) {
      if (!res.headersSent) json({ error: error.message }, error.status || 502);
      else res.destroy(error);
    }
  });
  // Large uploads may take hours. Uploads use an idle timeout, not a total limit.
  server.requestTimeout = 0;
  return server;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 3000);
  const server = createApp();
  server.on('error', error => { console.error(`Dashboard could not start: ${error.message}`); process.exitCode = 1; });
  server.listen(port, '127.0.0.1', () => console.log(`Hadoop Lab → http://localhost:${port}`));
}
