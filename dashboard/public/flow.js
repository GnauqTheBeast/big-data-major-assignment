import { node } from './common.js';

const stages = [
  {
    title: 'User input', active: ['browser'],
    description: 'The user chooses a text file, with one signed 32-bit integer per line. This small example includes a duplicate and an invalid line.',
    input: '7\n-2\n7\nhello\n3', output: 'A file ready to upload',
    detail: 'The original file is kept in HDFS. Validation happens when the mapper reads each line.',
  },
  {
    title: 'Stream upload', active: ['browser', 'server', 'namenode'],
    description: 'The browser sends file bytes to Node.js. Node streams them into the Hadoop client running in the NameNode container, with backpressure.',
    input: 'Browser file bytes', output: 'Node.js → Docker stdin → HDFS client',
    detail: 'The file is not loaded completely into Node memory. The Hadoop client asks NameNode where to write the blocks.',
  },
  {
    title: 'Store in HDFS', active: ['namenode', 'storage'],
    description: 'NameNode records the file path and block metadata. The HDFS client writes the bytes to DataNodes, whose persistent volumes hold the blocks.',
    input: 'File → HDFS block(s)', output: 'NameNode: metadata\nDataNode: file bytes',
    detail: 'This tiny example fits in one block. The current default replication is 1: one copy of a block, not one copy on every DataNode. Adding nodes does not automatically increase replication.',
  },
  {
    title: 'Map', active: ['storage', 'map'],
    description: 'When Run sort is clicked, the Java job reads the HDFS input. The mapper parses each line and emits an integer key with an empty value.',
    input: '7, -2, 7, hello, 3', output: '(7, ∅)  (-2, ∅)  (7, ∅)  (3, ∅)',
    detail: '∅ represents NullWritable. “hello” increments MALFORMED_LINES and is skipped. Empty lines have their own counter; integers outside the 32-bit range are also skipped.',
  },
  {
    title: 'Shuffle & sort', active: ['map', 'shuffle'],
    description: 'Hadoop orders the mapper output by numeric key and groups identical keys before calling the reducer.',
    input: '(7, ∅)  (-2, ∅)  (7, ∅)  (3, ∅)', output: '-2 → [∅]\n 3 → [∅]\n 7 → [∅, ∅]',
    detail: 'The ordering comes from Hadoop’s IntWritable comparison. This demo uses LocalJobRunner: these compute stages are inside one container, not separate network workers.',
  },
  {
    title: 'Reduce', active: ['shuffle', 'reduce'],
    description: 'The reducer receives keys in ascending order. For each key, it emits that key once for every value in the group, preserving duplicates.',
    input: '-2 × 1, 3 × 1, 7 × 2', output: '-2\n3\n7\n7',
    detail: 'There is one reducer, producing a globally sorted result. Adding DataNodes expands storage topology but does not add reducers or distribute this computation.',
  },
  {
    title: 'Write output', active: ['reduce', 'output', 'storage'],
    description: 'The job writes the sorted result into a new HDFS output directory. Its part-r-00000 file can be inspected or downloaded through the UI.',
    input: 'Sorted integers', output: 'part-r-00000\n_SUCCESS',
    detail: 'HDFS stores output blocks on DataNodes too. The input stays intact, and every dashboard run gets its own output directory.',
  },
];

const machines = [
  { id: 'browser', label: 'User file', x: -300, z: -100, color: '#92b7a6', height: 45 },
  { id: 'server', label: 'Node.js', x: -150, z: -100, color: '#489478', height: 65 },
  { id: 'namenode', label: 'NameNode', x: 0, z: -100, color: '#4f87a5', height: 95 },
  { id: 'storage', label: 'DataNode(s)', x: 190, z: -100, color: '#c89c57', height: 75 },
  { id: 'map', label: 'Map', x: -150, z: 100, color: '#6c86bc', height: 48 },
  { id: 'shuffle', label: 'Shuffle / sort', x: 0, z: 100, color: '#8e79b7', height: 62 },
  { id: 'reduce', label: 'Reduce', x: 150, z: 100, color: '#599ca0', height: 48 },
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
    const stage = stages[currentStage()];
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
  let step = 0, timer = null;
  const context = node('p', 'flow-context', 'Example data: 7, -2, 7, hello, 3. This illustration does not replay individual records from the selected job.');
  const steps = node('div', 'flow-steps');
  steps.setAttribute('aria-label', 'Data-flow stages');
  const canvas = node('canvas', 'flow-scene');
  canvas.tabIndex = 0;
  canvas.setAttribute('aria-label', 'Illustrative 3D flow. Drag or use left and right arrows to rotate. The text below explains every stage.');
  const caption = node('div', 'scene-caption', '3D illustration · drag to rotate · Map / Shuffle / Reduce are stages inside LocalJobRunner, not separate containers');
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
  const scene = createScene(canvas, () => step);

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
  return {
    setJob(job) {
      context.textContent = job?.kind === 'sort'
        ? `Selected job: ${job.status} · ${job.input}. The scene uses example data (7, -2, 7, hello, 3), not a live record trace. Use the logs above and the file inspector for actual results and block locations.`
        : 'Example data: 7, -2, 7, hello, 3. This is an illustrative sort walkthrough; cluster maintenance operations have their actual logs above.';
    },
  };
}
