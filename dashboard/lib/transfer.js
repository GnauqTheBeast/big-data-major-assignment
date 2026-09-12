import { PassThrough } from 'node:stream';
import { pipeline } from 'node:stream/promises';

// Keep the HTTP request outside pipeline: a failed HDFS write must still allow
// the server to return an error response instead of destroying its socket.
export async function streamToProcess(input, child, { finalizationTimeout = 5 * 60 * 1000 } = {}) {
  const bridge = new PassThrough();
  let bytes = 0, stderr = '', sourceError, timer, finalizationError, finalizing = false;
  const activity = () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      finalizationError = new Error(finalizing ? 'HDFS upload finalization timed out. Check cluster health before retrying.' : 'HDFS upload stalled. Check cluster health before retrying.');
      bridge.destroy(finalizationError);
      child.kill('SIGKILL');
    }, finalizationTimeout);
  };
  const onEnd = () => { finalizing = true; activity(); };
  input.once('end', onEnd);
  bridge.on('data', chunk => { bytes += chunk.length; activity(); });
  child.stdout.resume();
  child.stderr.on('data', chunk => { stderr = (stderr + chunk.toString()).slice(-8192); });
  const completed = new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', code => code === 0 ? resolve() : reject(new Error(stderr.trim() || `HDFS upload process exited with code ${code}`)));
  });
  const onError = error => { sourceError = error; bridge.destroy(error); child.kill(); };
  input.on('error', onError);
  const transferred = pipeline(bridge, child.stdin).catch(error => { child.kill(); throw error; });
  activity();
  if (input.destroyed) onError(new Error('Upload interrupted.'));
  else input.pipe(bridge);
  const results = await Promise.allSettled([transferred, completed]);
  clearTimeout(timer);
  input.removeListener('end', onEnd);
  input.unpipe(bridge);
  input.removeListener('error', onError);
  if (sourceError) throw sourceError;
  if (finalizationError) throw finalizationError;
  if (results[1].status === 'rejected') throw results[1].reason;
  if (results[0].status === 'rejected') throw results[0].reason;
  return bytes;
}
