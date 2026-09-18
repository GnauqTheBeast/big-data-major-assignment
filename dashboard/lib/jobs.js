import { randomUUID } from 'node:crypto';

export class Jobs {
  constructor(execute) { this.execute = execute; this.jobs = new Map(); }
  get(id) { return this.jobs.get(id); }
  list() { return [...this.jobs.values()].reverse(); }
  start(input, kind = 'sort', extra = {}) {
    if (this.list().some(job => job.status === 'running')) throw Object.assign(new Error('An operation is already running. Wait for it to finish.'), { status: 409 });
    const id = randomUUID();
    const topk = kind === 'topk-hadoop';
    const root = topk ? '/training/top-k/runs' : '/training/integer-sort/runs';
    const output = topk || kind === 'sort' ? `${root}/${id}` : null;
    const job = { id, kind, input, k: topk ? extra.k : undefined, output, status: 'running', log: '', result: null, rows: null, started: new Date().toISOString(), finished: null };
    this.jobs.set(id, job);
    if (this.jobs.size > 50) this.jobs.delete(this.jobs.keys().next().value);
    const log = text => { job.log = (job.log + text).slice(-128 * 1024); };
    Promise.resolve().then(() => this.execute(input, job.output, log, kind, extra, job)).then(result => {
      job.status = 'succeeded';
      if (kind === 'sort') job.result = `${job.output}/part-r-00000`;
      else if (kind === 'topk-hadoop') { job.result = `${job.output}/part-r-00000`; job.rows = result?.rows || null; }
      log('\nCompleted.\n');
    }).catch(error => { job.status = 'failed'; log('\n' + error.message + '\n'); }).finally(() => { job.finished = new Date().toISOString(); });
    return job;
  }
}
