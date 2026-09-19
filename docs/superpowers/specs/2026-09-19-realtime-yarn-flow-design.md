# Real-time YARN data-flow visualization

## Goal

Replace the sample-derived "Illustrative walkthrough" with a live execution
view driven by the selected job's real Hadoop/YARN client log. The view must
make the Map, Shuffle, local-disk spill, Sort/Merge, Reduce, and HDFS-output
phases explicit without inventing values that Hadoop has not reported.

This applies to dashboard-submitted Integer Sort and Hadoop Top-K jobs. Integer
Sort receives the complete Map-to-HDFS phase view. Top-K reuses the same runtime
telemetry and presents its multiple MapReduce jobs as successive attempts when
the log exposes them.

## Runtime truth

The dashboard must stop labeling jobs as `LocalJobRunner` before Hadoop reports
their runtime. The configured submission command is runtime-neutral. The parser
classifies execution as YARN when it sees ResourceManager connection, YARN
staging, application IDs, or non-local Hadoop job IDs. It classifies local mode
only from explicit local-runner evidence such as `job_local` or
`local.LocalJobRunner`.

Raw log text remains the source of truth and remains visible unchanged. Parsed
telemetry is a derived projection. Missing evidence is displayed as pending or
unavailable, never replaced with sample or hard-coded numbers.

## Backend design

Add `dashboard/lib/job-log-parser.js`, an incremental, stateful parser. It
accepts arbitrary stdout/stderr chunks, buffers incomplete lines, and exposes a
serializable progress model. Parsing must tolerate timestamp/logger prefixes,
unknown lines, repeated progress lines, and counters that arrive only at job
completion.

The progress model contains:

- detected runtime, application ID, and Hadoop job ID;
- current phase and terminal status;
- Map and Reduce percentages;
- reported Map/Reduce task counts when available;
- Hadoop counters keyed by their printed names, including Map input/output
  records, Reduce input groups/records/output records, Reduce shuffle bytes,
  Shuffled Maps, Failed Shuffles, Merged Map outputs, and Spilled Records;
- evidence flags for shuffle, spill, merge, reduce, and HDFS output;
- the known HDFS output path supplied by the dashboard job record.

`dashboard/lib/jobs.js` owns one parser per job. Every log chunk continues to be
appended to the existing bounded `job.log` and is also passed to the parser.
The current `/api/jobs` responses include `job.progress`; no new endpoint or
streaming protocol is required. The browser's existing two-second polling is
the real-time delivery interval.

`dashboard/lib/hadoop.js` replaces the false `Running LocalJobRunner` message
with a runtime-neutral submission message. Runtime labels returned by status
and rendered in the existing pages must no longer contradict parsed YARN
evidence.

## Visualization design

`dashboard/public/flow.js` builds stages from `job.progress`, not the first 1 KB
input preview. The Integer Sort view uses these stages:

1. Submit to YARN and allocate the application.
2. Map tasks process HDFS input; show real Map percentage and counters.
3. Map output becomes available for shuffle; show actual task/partition counts
   only when derivable from reported counters.
4. Shuffle transfers map partitions to the reducer; show shuffle bytes,
   shuffled maps, and failures when reported.
5. Spill to local disk; visually emphasize RAM-to-local-disk movement. Before
   evidence appears, label it `Awaiting spill evidence`. Once `Spilled Records`
   is greater than zero or an explicit spill line is observed, label it
   `Confirmed from YARN log` and show the actual count. A zero counter is shown
   as `No spill reported`, not as a spill.
6. Sort and merge; show `Merged Map outputs` and related evidence.
7. Reduce consumes sorted groups sequentially; show real Reduce percentage,
   input groups, and input/output records.
8. Write the known output path to HDFS and mark completion only when the job
   succeeds.

Each stage has a state of pending, active, completed, failed, or unavailable.
The scene highlights the active stage automatically while a job runs. Users may
still select completed stages to inspect their counters. Labels clearly
distinguish facts observed in logs from explanatory text about Hadoop behavior.

The existing raw-log panel remains above the flow so users can verify every
derived value. The heading changes from `Illustrative walkthrough` to
`Live YARN execution` when runtime evidence is available, with an explicit
fallback label when telemetry is incomplete.

## Failure and compatibility behavior

- Unknown Hadoop log formats never fail a job or hide its raw log.
- Partial lines are buffered until completed; remaining buffered text is parsed
  when the process closes.
- Failed jobs keep all last-known progress and mark the active phase failed.
- Older in-memory jobs without `progress` render a telemetry-unavailable state.
- Maintenance jobs retain the existing no-record-processing message.
- The parser has no dependency on ResourceManager history or log aggregation.

## Verification

Use Node's built-in test runner for parser fixtures covering:

- YARN detection versus genuine LocalJobRunner detection;
- split chunks and timestamp/logger prefixes;
- Map/Reduce progress updates;
- counter parsing with commas and plain integers;
- positive, zero, and missing spill evidence;
- shuffle/merge counters and failed-job preservation;
- multiple Hadoop job IDs in a Top-K log.

Add focused DOM-independent tests for mapping progress into visualization
stages where practical. Run syntax checks for changed browser modules and a
dashboard smoke test that confirms `/api/jobs` serializes progress without
breaking the existing raw log.
