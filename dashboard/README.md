# Hadoop Lab dashboard

A local Node.js UI for the existing Hadoop integer-sort exercise. Upload and sort from your browser, inspect the actual containers, and locate a file's HDFS blocks.

## Open the dashboard

Requirements: Node.js 22 or newer, Docker Desktop running, and Docker Compose available. No npm packages or host Maven installation are needed.

From the repository root, start the UI server once:

```bash
npm start
```

Open **http://localhost:3000**. To use another port: `PORT=3001 npm start`.

All exercise operations are now available in the browser:

1. Click **Start cluster** if the containers are not running. Startup opens the separate **Job activity** page with its system logs. Initial image downloads may take several minutes. Once services are healthy, return to Files and click **Refresh** to read HDFS.
2. Click **Choose file**, or drop a file into the upload area. Large files are supported with no fixed application upload-size cap; use one signed 32-bit integer per line. Alternatively, browse HDFS folders and select an existing file.
3. Inspect its first 1 KB, block IDs, replica counts, and DataNode addresses. The architecture highlights the service holding the selected blocks. Service names link to Hadoop/Spark's native UIs.
4. Click **Run sort**. The server compiles the existing Java source inside the NameNode container, builds a JAR, and executes Hadoop. You are taken to `/jobs?job=<id>`, where status and the original system log, including Hadoop counters, remain visible.
5. Click **Inspect sorted output** or **Download result**. Each run has its own HDFS directory; previous files are preserved.

## What the architecture shows

Core container cards have a **Start** or **Stop** button with a pending state while Docker works. These controls affect only that service; **Start cluster** starts core services and the extra DataNodes currently enabled through the admin page. Extra DataNode cards link to their management page for safe removal. Stopping containers preserves their HDFS volumes. Spark Master/Worker can be stopped when using this Hadoop integer-sort exercise. Stopping Spark interrupts any separate Spark applications using those services. Likewise, YARN services are not used by the current LocalJobRunner sort.

The server prevents changing NameNode/DataNode containers during dashboard uploads, downloads, or sorts, including requests from other tabs. Outside those operations you may stop them, but HDFS access then becomes unavailable until both run again. Individual Start buttons do not automatically start dependencies. The dashboard tracks only its own jobs; it cannot protect jobs started separately in a terminal or another application.

- Container state, health, and IP addresses come from this repository's Docker Compose project and Docker inspect. Status refreshes every 10 seconds while the page is visible.
- HDFS file metadata comes from `hdfs dfs -ls`. Block IDs and replica addresses come from `hdfs fsck <file> -files -blocks -locations`. Select a file or click its block refresh button to refresh placement. Empty files have no blocks. Unavailable reports are displayed as errors, not guessed locations.
- The cluster starts with one DataNode and replication factor 1. Two optional DataNodes have separate volumes and host UI ports (9865 and 9867). The NameNode stores namespace metadata; DataNodes store the bytes. Blocks show all replica addresses returned by HDFS; the architecture highlights their matching containers.
- Integer sort currently runs with **LocalJobRunner inside the NameNode container** and one reducer, reading and writing HDFS. YARN and Spark appear as separate available services; neither executes this job. This UI does not change the cluster's execution mode.

## Job activity and the data-flow walkthrough

`/jobs` is a separate screen with selectable jobs and their system logs at the top. Immediately below is an illustrative 3D walkthrough, with Play/Pause, Previous/Next, zoom, and drag-to-rotate controls. The same explanations are available as text; arrow keys rotate the focused scene.

The walkthrough adapts to the selected integer-sort, Hadoop Top-K, Spark Top-K, or comparison job. It reads at most the first 1 KB of the job's actual HDFS input to demonstrate intermediate transformations without rendering the raw file; sample-derived values are labeled and are not presented as whole-file results. When a job succeeds, the final stage uses its actual output preview or Top-K rows. The scene uses native canvas with projected 3D geometry and adds no external package or model asset.

## DataNode management

`/datanodes` provides local admin controls for 1–3 explicitly configured DataNodes. No user-account system is added; access remains localhost-only. NameNode JMX supplies liveness, administrative state, capacity, HDFS usage, and block counts. All containers share the host's physical storage, so their reported capacities must not be added together as if they were independent disks.

- **Add DataNode:** starts or rejoins an extra node and waits for NameNode registration. On first use, the server may recreate NameNode to apply the exclude-file configuration, preserving its existing volume. Joining a node does not move old blocks automatically or increase replication.
- **Rebalance blocks:** runs the HDFS balancer with a 10% utilization threshold. On a small demo with similarly filled nodes, it may have nothing to move.
- **Remove safely:** marks an extra node for decommissioning, waits for NameNode to confirm that the required replicas exist elsewhere, then stops it. The node's volume is retained. The original primary DataNode cannot be removed through this page.
- **Rejoin cluster:** removes a node from the exclude list and starts it again. If safe removal fails or times out, the node is not forcibly stopped. After the operation finishes, use Rejoin or Resume safe removal as appropriate.

Operations appear with their logs on Job activity. Dashboard uploads, downloads, sorts, and other cluster controls are blocked during maintenance. Removal can take up to 30 minutes; insufficient remaining capacity or replica requirements can prevent completion. The dashboard cannot coordinate work started outside it.

Desired nodes are saved in `dashboard/state/datanodes.json`. The NameNode reads `dashboard/state/excluded-datanodes` through a directory bind mount. Keep these files along with the Docker volumes when restarting the demo. `docker compose up` alone starts the core services; the dashboard's Start cluster also restores enabled extras. Existing HDFS data and the `hadoop_datanode` volume are preserved.

## Files and limits

Integer-sort uploads use `/training/integer-sort/uploads/<unique-id>/<filename>`, while Top-K uploads use `/training/top-k/uploads/<unique-id>/<filename>`. Output uses the corresponding exercise's `/training/.../runs` area. Unsafe filename characters are replaced with `_`; names starting with `.` or `_` are prefixed with `file-` because Hadoop ignores hidden input files. Existing hidden HDFS files remain downloadable, but sorting them returns an explanation.

Sorting preserves duplicate integers and skips blank, malformed, and out-of-range lines, following `IntegerSortJob.java`. The UI runs one sort/startup operation at a time and accepts one upload at a time. Uploads stream with backpressure from the browser through Node and Docker stdin directly into HDFS. They do not require a whole-file memory buffer or host/container temporary copy. HDFS writes to a hidden staging filename and renames it after success; failed transfers attempt to remove their unique staging directory. Available HDFS capacity, quotas, and transfer speed determine practical upload limits. The browser has no total upload deadline; a connection idle for five minutes is closed. Uploads are not resumable after interruption.

The latest 50 jobs and up to 128 KB of logs per job are kept in memory. Restarting the UI clears job history but leaves HDFS files intact. Large directory/block reports are capped at 4 MB; browse narrower directories if needed. The existing sort still uses LocalJobRunner and a single reducer, so removing the upload cap does not make its computation distributed across YARN workers.

The server binds to `127.0.0.1` and rejects foreign hosts and cross-origin requests. It invokes fixed Docker command argument arrays; it does not expose a terminal or arbitrary command endpoint. Run it on the same machine as Docker, as a local lab application. Container startup can download the images specified in `docker-compose.yml`.

Closing the browser does not stop a running job. Keep the Node server running until it finishes. Sort commands have a one-hour timeout and downloads a five-minute timeout. A disconnected/timed-out Docker client does not guarantee cancellation of an already launched Java process; inspect the container before retrying such a job. HDFS uploads and completed/partial outputs persist for later inspection.

Transfers also have a five-minute inactivity/finalization watchdog around the Docker subprocess, so a stalled write cannot indefinitely occupy the upload slot. This timer resets while bytes flow and is independent of total file size.

Implementation: `server.js` owns HTTP; `lib/hadoop.js` owns Docker/HDFS; `lib/datanodes.js` owns storage administration; `lib/jobs.js` owns job lifecycle; `lib/transfer.js` streams uploads. In `public/`, `app.js` handles the workspace, `jobs.js` the log page, `flow.js` the illustrative scene, and `datanodes.js` the admin page. All JavaScript uses `import`/`export`, enabled by `"type": "module"` in the root `package.json`. No automated test suite is included, as requested for this POC.
