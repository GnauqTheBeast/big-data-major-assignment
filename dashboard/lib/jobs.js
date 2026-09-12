import { randomUUID } from 'node:crypto';

export class Jobs {
  constructor(execute) { this.execute = execute; this.jobs = new Map(); }
  get(id) { return this.jobs.get(id); }
  list() { return [...this.jobs.values()].reverse(); }
  start(input, kind = 'sort') {
    if (this.list().some(job => job.status === 'running')) throw Object.assign(new Error('An operation is already running. Wait for it to finish.'), { status: 409 });
    const id = randomUUID();
    const job = { id, kind, input, output: `/training/integer-sort/runs/${id}`, status: 'running', log: '', result: null, started: new Date().toISOString(), finished: null };
    this.jobs.set(id, job);
    if (this.jobs.size > 50) this.jobs.delete(this.jobs.keys().next().value);
    const log = text => { job.log = (job.log + text).slice(-128 * 1024); };
    Promise.resolve().then(() => this.execute(input, job.output, log, kind)).then(() => {
      job.status = 'succeeded';
      job.result = kind === 'sort' ? `${job.output}/part-r-00000` : null;
      log('\nCompleted.\n');
    }).catch(error => { job.status = 'failed'; log('\n' + error.message + '\n'); }).finally(() => { job.finished = new Date().toISOString(); });
    return job;
  }
}
