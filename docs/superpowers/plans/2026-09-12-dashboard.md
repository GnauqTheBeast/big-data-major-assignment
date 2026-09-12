# Integer-sort Dashboard Implementation Plan

Historical implementation record. The user subsequently requested a lightweight demo without dashboard tests; the Node tests and smoke scripts below were removed, along with their npm commands. Container cards now provide individual Start/Stop controls.

**Goal:** Operate the existing integer-sort lab from a browser and inspect actual container and HDFS block placement.

**Architecture:** A localhost Node HTTP server serves static UI files and an API. A Docker adapter owns fixed Compose commands, HDFS commands, and Java compilation. Asynchronous jobs retain bounded logs in memory.

**Tech stack:** Node.js 22 built-in HTTP, child_process, filesystem and test modules; browser JavaScript/CSS; existing Hadoop 3.2.1 Java source.

**Spec:** `docs/superpowers/specs/2026-09-12-dashboard-design.md`

## Constraints

No npm dependencies. Loopback only. No user-supplied shell commands. Preserve existing HDFS inputs and outputs. Report real status; distinguish unavailable observations from stopped containers.

## Tasks

- [x] Backend (`dashboard/lib/hadoop.js`, `dashboard/lib/jobs.js`): implement the adapter and bounded asynchronous job state. The initial tests were subsequently removed at the user's request.
- [x] HTTP application (`dashboard/server.js`): implement origin/host checks, streaming uploads, static routes, API error propagation and streaming downloads. The initial tests were subsequently removed at the user's request.
- [x] UI (`dashboard/public/index.html`, `style.css`, `app.js`): implement architecture cards and connections, file browser with parent/root navigation, upload progress, selection/block details, job status/logs/results and cluster start. Use DOM text nodes for data and accessible controls.
- [x] Integration/docs (`dashboard/README.md`, root `package.json`, `integer-sort/README.md`): add `npm start` / `npm test`, document startup and limits. Start the app, check actual HTTP responses and perform an isolated HDFS upload-sort-download with the sample file. Verify source syntax, review diff, and record any environment limitation.

## Verification record

- Ten Node tests pass, including the HTTP workflow and hidden Hadoop filename regression.
- Headless Chrome successfully exercised actual file upload, all six running services, selected-file DataNode highlighting, Java sort, exact output preview, and downloaded bytes.
- Browser reported no JavaScript errors. Desktop (1440 px) and mobile (390 px) screenshots were inspected; mobile has no horizontal overflow.
- Independent code review identified the Hadoop hidden-file filter; uploads now normalize these filenames and existing hidden sort inputs return a clear error.
- Server and cluster were left running for use. No Compose configuration or Java sorting behavior changed.
