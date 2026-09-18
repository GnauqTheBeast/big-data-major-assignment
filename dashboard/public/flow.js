import { api, node, query } from './common.js';

const idleStages = [{
  title: 'Select a job', active: ['browser'],
  description: 'Choose a job above to build this walkthrough from its actual HDFS input and execution engine.',
  input: 'No job selected', output: 'Waiting for a job',
  detail: 'Only the first 1 KB is read to demonstrate intermediate transformations; the complete input is never rendered here.',
}];

const previewLines = text => text ? text.split(/\r?\n/).filter((line, index, rows) => line || index < rows.length - 1) : [];
const clipped = (values, limit = 8) => values.slice(0, limit).join('\n') + (values.length > limit ? `\n… ${values.length - limit} more in preview` : '');
const sampleLabel = (lines, available = true) => available
  ? `${lines.length} line${lines.length === 1 ? '' : 's'} sampled from the first 1 KB`
  : 'Input preview unavailable';
const outputState = job => job.status === 'succeeded' ? 'Completed output' : job.status === 'failed' ? 'No output produced' : 'Output pending';

function inputStages(job, lines, available) {
  return [
    {
      title: 'Selected HDFS input', active: ['browser', 'namenode'],
      description: 'The walkthrough reads a bounded preview from the selected job’s real HDFS input. It summarizes the sample without displaying the raw file.',
      input: job.input, output: sampleLabel(lines, available),
      detail: 'The preview is for explaining transformations only. Hadoop processes the complete file.',
    },
    {
      title: 'Read HDFS blocks', active: ['namenode', 'storage'],
      description: 'NameNode resolves the path to block locations, then the selected engine reads the file bytes from DataNodes.',
      input: 'HDFS path metadata', output: 'Input splits / DataFrame partitions',
      detail: 'The visualization uses a 1 KB sample; the real job reads every input split and does not load the whole file into the dashboard.',
    },
  ];
}

function sortStages(job, lines, resultText, available) {
  const parsed = lines.map(line => line.trim());
  const valid = parsed.filter(line => /^[+-]?\d+$/.test(line)).map(Number).filter(value => Number.isInteger(value) && value >= -2147483648 && value <= 2147483647);
  const skipped = parsed.length - valid.length;
  const sorted = [...valid].sort((a, b) => a - b);
  const actual = previewLines(resultText).filter(Boolean);
  return [
    ...inputStages(job, lines, available),
    {
      title: 'Map integers', active: ['storage', 'map'],
      description: 'The mapper trims and parses each sampled line, emits valid IntWritable keys, and skips empty, malformed, or out-of-range values.',
      input: sampleLabel(lines, available), output: available ? `${valid.length} valid keys\n${skipped} skipped lines` : 'Waiting for readable input',
      detail: clipped(valid.map(value => `emit (${value}, ∅)`)) || 'No valid integer keys in the preview.',
    },
    {
      title: 'Numeric shuffle', active: ['map', 'shuffle'],
      description: 'Hadoop groups duplicate integer keys and orders groups using numeric IntWritable comparison.',
      input: `${valid.length} mapped keys`, output: clipped([...new Map(sorted.map(value => [value, sorted.filter(item => item === value).length]))].map(([value, count]) => `${value} × ${count}`)) || 'No groups in preview',
      detail: 'These groups come from the selected input preview; the real shuffle includes all mapped records.',
    },
    {
      title: 'Reduce in order', active: ['shuffle', 'reduce'],
      description: 'One reducer emits each numeric key once per grouped value, preserving duplicates in global ascending order.',
      input: `${new Set(valid).size} preview groups`, output: clipped(sorted.map(String)) || 'No preview values',
      detail: 'This is a sample-derived demonstration, not a replacement for the job output.',
    },
    {
      title: 'Write actual result', active: ['reduce', 'output', 'storage'],
      description: 'The reducer writes part-r-00000 to the job’s HDFS output directory.',
      input: outputState(job), output: actual.length ? clipped(actual) : outputState(job),
      detail: actual.length ? 'These values are read from the selected job’s actual output file.' : `Job status: ${job.status}.`,
    },
  ];
}

function countItems(lines) {
  const counts = new Map();
  for (const raw of lines) {
    const item = raw.trim();
    if (item) counts.set(item, (counts.get(item) || 0) + 1);
  }
  return [...counts].map(([item, count]) => ({ item, count }))
    .sort((left, right) => right.count - left.count || (left.item < right.item ? -1 : left.item > right.item ? 1 : 0));
}

const formatRows = rows => clipped((rows || []).map(row => `${row.item} → ${row.count}`)) || 'No rows available';

function topKStages(job, lines, available) {
  const counts = countItems(lines);
  const k = job.k || 1;
  const candidates = counts.slice(0, k);
  const actual = job.rows;
  const base = inputStages(job, lines, available);
  const stages = [
    ...base,
    {
      title: 'Map item counts', active: ['storage', 'map'],
      description: 'The counting mapper trims each line, skips empty rows, and emits one count for every item occurrence.',
      input: sampleLabel(lines, available), output: available ? `${lines.filter(line => line.trim()).length} sampled (item, 1) pairs` : 'Waiting for readable input',
      detail: 'The raw input stays hidden; this count comes from the selected input preview.',
    },
    {
      title: 'Combine and reduce', active: ['map', 'shuffle', 'reduce'],
      description: 'Hadoop groups equal items and sums their occurrences into one count per distinct item.',
      input: `${lines.filter(line => line.trim()).length} sampled pairs`, output: formatRows(counts),
      detail: 'These intermediate counts describe only the preview sample.',
    },
    {
      title: `Keep local Top-${k}`, active: ['reduce', 'map'],
      description: 'Each second-stage mapper keeps a bounded min-heap so only its strongest K candidates continue.',
      input: `${counts.length} sampled item counts`, output: formatRows(candidates),
      detail: 'The heap limits memory and network traffic. Sample candidates may differ from the whole-file result.',
    },
    {
      title: `Select global Top-${k}`, active: ['map', 'shuffle', 'reduce'],
      description: 'One reducer merges mapper candidates, orders by count descending with item-name tie-breaking, and emits the final K.',
      input: 'Local Top-K candidates', output: actual?.length ? formatRows(actual) : outputState(job),
      detail: actual?.length ? 'These rows are the selected Hadoop job’s actual full-input result.' : `Job status: ${job.status}.`,
    },
  ];
  stages.push({
    title: 'Write actual result', active: ['reduce', 'output', 'storage'],
    description: 'Hadoop writes the final rows to part-r-00000 in the selected job’s output directory.',
    input: actual?.length ? formatRows(actual) : outputState(job), output: job.result || outputState(job),
    detail: actual?.length ? 'The displayed rows come from the selected job’s actual full-input result.' : `Job status: ${job.status}.`,
  });
  return stages;
}

const machines = [
  { id: 'browser', label: 'Selected job', x: -300, z: -100, color: '#92b7a6', height: 45 },
  { id: 'server', label: 'Dashboard', x: -150, z: -100, color: '#489478', height: 65 },
  { id: 'namenode', label: 'NameNode', x: 0, z: -100, color: '#4f87a5', height: 95 },
  { id: 'storage', label: 'DataNode(s)', x: 190, z: -100, color: '#c89c57', height: 75 },
  { id: 'map', label: 'Map / transform', x: -150, z: 100, color: '#6c86bc', height: 48 },
  { id: 'shuffle', label: 'Shuffle / group', x: 0, z: 100, color: '#8e79b7', height: 62 },
  { id: 'reduce', label: 'Reduce / limit', x: 150, z: 100, color: '#599ca0', height: 48 },
  { id: 'output', label: 'HDFS output', x: 300, z: 100, color: '#489478', height: 45 },
];

// Small software-rendered 3D scene. Geometry stays in 3D coordinates; the
// canvas projects it into the current camera view. No external asset/library.
function createScene(canvas, currentStage) {
  const context = canvas.getContext('2d');
  if (!context) return { redraw() {} };
  let angle = -0.3;
  let zoom = 1;
  let drag = null;
  let frame;
  let moving = false;
  const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;

  function draw(time = 0) {
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    if (!width || !height) return;
    const ratio = Math.min(devicePixelRatio || 1, 2);
    if (canvas.width !== width * ratio || canvas.height !== height * ratio) {
      canvas.width = width * ratio;
      canvas.height = height * ratio;
    }
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    const scale = Math.min(width / 900, height / 450) * zoom;
    function project(x, y, z) {
      const rx = x * Math.cos(angle) - z * Math.sin(angle);
      const rz = x * Math.sin(angle) + z * Math.cos(angle);
      // Positive Z recedes into the scene. Farther points move upward and
      // shrink, while positive Y always rises from the ground plane. Keeping
      // those directions consistent makes this read as a floor, not a ceiling.
      const depth = rz * 0.8 - y * 0.4;
      const perspective = 1100 / (1100 + depth);
      return { x: width / 2 + rx * scale * perspective, y: height * 0.67 - (rz * 0.5 + y * 0.9) * scale * perspective, depth };
    }
    function line(a, b, color = '#dce7e2', lineWidth = 1) {
      context.beginPath(); context.moveTo(a.x, a.y); context.lineTo(b.x, b.y);
      context.strokeStyle = color; context.lineWidth = lineWidth; context.stroke();
    }
    for (let x = -440; x <= 440; x += 40) line(project(x, 0, -240), project(x, 0, 240));
    for (let z = -240; z <= 240; z += 40) line(project(-440, 0, z), project(440, 0, z));
    const stage = currentStage();
    const active = stage.active.map(id => machines.find(machine => machine.id === id));
    for (let index = 1; index < active.length; index++) {
      const a = active[index - 1], b = active[index];
      line(project(a.x, 5, a.z), project(b.x, 5, b.z), '#74b297', 3);
      const t = moving && !reducedMotion ? (time / 1600) % 1 : 0.5;
      const point = project(a.x + (b.x - a.x) * t, 12, a.z + (b.z - a.z) * t);
      context.fillStyle = '#237a61';
      context.fillRect(point.x - 5, point.y - 5, 10, 10);
    }
    const faces = [];
    for (const machine of machines) {
      const x = machine.x, z = machine.z, h = machine.height, w = 42, d = 30;
      const vertices = [
        [x - w, 0, z - d], [x + w, 0, z - d], [x + w, 0, z + d], [x - w, 0, z + d],
        [x - w, h, z - d], [x + w, h, z - d], [x + w, h, z + d], [x - w, h, z + d],
      ].map(point => project(...point));
      for (const [indices, shade] of [[[0, 1, 5, 4], -25], [[1, 2, 6, 5], -10], [[2, 3, 7, 6], 0], [[3, 0, 4, 7], -15], [[4, 5, 6, 7], 25]]) {
        const points = indices.map(index => vertices[index]);
        faces.push({ points, depth: points.reduce((sum, point) => sum + point.depth, 0) / 4, machine, shade });
      }
    }
    faces.sort((a, b) => b.depth - a.depth);
    for (const face of faces) {
      const isActive = stage.active.includes(face.machine.id);
      const hex = face.machine.color.slice(1);
      const rgb = [0, 2, 4].map(offset => Math.max(0, Math.min(255, parseInt(hex.slice(offset, offset + 2), 16) + face.shade)));
      context.fillStyle = `rgba(${rgb.join(',')},${isActive ? 1 : 0.25})`;
      context.beginPath();
      face.points.forEach((point, index) => index ? context.lineTo(point.x, point.y) : context.moveTo(point.x, point.y));
      context.closePath(); context.fill();
      context.strokeStyle = isActive ? '#ffffff80' : '#ffffff50'; context.stroke();
    }
    context.textAlign = 'center';
    for (const machine of machines) {
      const point = project(machine.x, machine.height + 20, machine.z);
      context.font = `${stage.active.includes(machine.id) ? '600' : '400'} ${width < 500 ? 10 : 12}px system-ui`;
      context.fillStyle = stage.active.includes(machine.id) ? '#254c40' : '#7c8e85';
      context.fillText(machine.label, point.x, point.y);
    }
    if (moving && !reducedMotion && !document.hidden) frame = requestAnimationFrame(draw);
  }
  canvas.addEventListener('pointerdown', event => { drag = event.clientX; canvas.setPointerCapture(event.pointerId); });
  canvas.addEventListener('pointermove', event => {
    if (drag === null) return;
    angle += (event.clientX - drag) * 0.005; drag = event.clientX;
    if (!moving) draw();
  });
  canvas.addEventListener('pointerup', () => { drag = null; });
  canvas.addEventListener('pointercancel', () => { drag = null; });
  canvas.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
    event.preventDefault(); angle += event.key === 'ArrowLeft' ? -0.1 : 0.1; if (!moving) draw();
  });
  new ResizeObserver(() => { if (!moving) draw(); }).observe(canvas);
  document.addEventListener('visibilitychange', () => { if (moving && !document.hidden) { cancelAnimationFrame(frame); draw(); } });
  return {
    redraw: () => { if (!moving || reducedMotion) draw(); },
    play: value => { moving = value; cancelAnimationFrame(frame); draw(); },
    zoom: value => { zoom = value; if (!moving) draw(); },
    reset: () => { angle = -0.3; if (!moving) draw(); },
  };
}

export function createFlow(root) {
  let step = 0, timer = null, stages = idleStages, requestVersion = 0, signature = '';
  const previewCache = new Map();
  const context = node('p', 'flow-context', 'Select a job to demonstrate its real data flow.');
  const steps = node('div', 'flow-steps');
  steps.setAttribute('aria-label', 'Data-flow stages');
  const canvas = node('canvas', 'flow-scene');
  canvas.tabIndex = 0;
  canvas.setAttribute('aria-label', 'Illustrative 3D flow. Drag or use left and right arrows to rotate. The text below explains every stage.');
  const caption = node('div', 'scene-caption', '3D illustration · drag to rotate · intermediate values use at most the first 1 KB of the selected input');
  const controls = node('div', 'flow-controls');
  const previous = node('button', 'button secondary', '← Previous');
  const play = node('button', 'button primary', '▶ Play');
  const next = node('button', 'button secondary', 'Next →');
  const reset = node('button', 'text-button', 'Reset view');
  const zoomLabel = node('label', 'flow-zoom', 'Zoom ');
  const zoom = node('input'); zoom.type = 'range'; zoom.min = '0.7'; zoom.max = '1.3'; zoom.step = '0.05'; zoom.value = '1';
  zoomLabel.append(zoom);
  controls.append(previous, play, next, reset, zoomLabel);
  const detail = node('div', 'flow-detail');
  const heading = node('h3'); const description = node('p');
  const values = node('div', 'flow-values');
  const before = node('div'), after = node('div');
  const input = node('pre'), output = node('pre');
  before.append(node('span', 'detail-label', 'BEFORE'), input);
  after.append(node('span', 'detail-label', 'AFTER'), output);
  values.append(before, after);
  const note = node('p', 'flow-note');
  detail.append(heading, description, values, note);
  root.append(context, steps, canvas, caption, controls, detail);
  const scene = createScene(canvas, () => stages[step]);

  function render() {
    steps.replaceChildren();
    stages.forEach((stage, index) => {
      const button = node('button', 'flow-step' + (index === step ? ' active' : ''), `${index + 1}. ${stage.title}`);
      button.setAttribute('aria-pressed', String(index === step));
      button.addEventListener('click', () => { stop(); step = index; render(); });
      steps.append(button);
    });
    const stage = stages[step];
    heading.textContent = `${step + 1} / ${stages.length} · ${stage.title}`;
    description.textContent = stage.description;
    input.textContent = stage.input; output.textContent = stage.output; note.textContent = stage.detail;
    previous.disabled = step === 0; next.disabled = step === stages.length - 1;
    scene.redraw();
  }
  function stop() { clearInterval(timer); timer = null; play.textContent = '▶ Play'; scene.play?.(false); }
  previous.addEventListener('click', () => { stop(); step = Math.max(0, step - 1); render(); });
  next.addEventListener('click', () => { stop(); step = Math.min(stages.length - 1, step + 1); render(); });
  play.addEventListener('click', () => {
    if (timer) return stop();
    if (step === stages.length - 1) step = 0;
    play.textContent = 'Ⅱ Pause'; render(); scene.play?.(true);
    timer = setInterval(() => { if (step < stages.length - 1) { step++; render(); } else stop(); }, 5000);
  });
  reset.addEventListener('click', () => scene.reset?.());
  zoom.addEventListener('input', () => scene.zoom?.(Number(zoom.value)));
  render();
  async function preview(path) {
    if (!path) return '';
    if (!previewCache.has(path)) previewCache.set(path, api('/api/preview' + query(path)).then(data => data.text || '').catch(() => null));
    return previewCache.get(path);
  }
  return {
    async setJob(job) {
      const nextSignature = job ? `${job.id}:${job.status}:${JSON.stringify(job.rows)}` : 'none';
      if (nextSignature === signature) return;
      signature = nextSignature;
      const version = ++requestVersion;
      stop(); step = 0;
      if (!job || !['sort', 'topk-hadoop'].includes(job.kind)) {
        stages = idleStages;
        context.textContent = job ? `The selected ${job.kind} operation has no record-processing walkthrough.` : 'Select a job to demonstrate its real data flow.';
        render(); return;
      }
      context.textContent = `Loading a bounded preview from ${job.input}…`;
      stages = [{ ...idleStages[0], title: 'Load selected input', input: job.input, output: 'Reading first 1 KB…' }];
      render();
      const [inputText, resultText] = await Promise.all([
        preview(job.input),
        job.kind === 'sort' && job.status === 'succeeded' ? preview(job.result) : Promise.resolve(''),
      ]);
      if (version !== requestVersion) return;
      const available = inputText !== null;
      const lines = previewLines(inputText || '');
      stages = job.kind === 'sort' ? sortStages(job, lines, resultText, available) : topKStages(job, lines, available);
      context.textContent = job.kind === 'sort'
        ? `Selected integer-sort job · ${job.status} · ${sampleLabel(lines, available)} for intermediate stages.`
        : `Selected Hadoop Top-K job · ${job.status} · K=${job.k ?? '—'} · ${sampleLabel(lines, available)} for intermediate stages.`;
      render();
    },
  };
}
