# Job page and DataNode management

User-approved layout: Job activity is a separate page, retains system logs, and places the data-flow visualization underneath. Keep `.js`, built-in Node modules, and no automated tests for this demo.

- [x] Split workspace and job activity into `/` and `/jobs`. Preserve job selection in the URL, log polling, result downloads, and links back to the file inspector.
- [x] Add a clearly labeled, interactive illustrative input → upload → HDFS → map → shuffle → reduce → output walkthrough below logs. Keep metadata vs block storage and LocalJobRunner vs YARN distinctions accurate. Following the user's 3D suggestion, use projected 3D geometry with drag/keyboard rotation, zoom, and stage playback; no external asset is assumed.
- [x] Add `/datanodes` for 1–3 explicit DataNodes, each with a separate volume. Preserve the existing primary DataNode volume. Show NameNode-reported capacity, block counts, liveness, and decommission state.
- [x] Implement asynchronous add, rebalance, and safe removal with logs in Job activity. Removal decommissions first, waits for NameNode confirmation, then stops the container without deleting its volume. Keep the primary DataNode; block conflicting dashboard transfers and cluster changes during maintenance.
- [x] Update navigation/docs. Use syntax and Compose configuration checks only; do not add or run test suites.

Static review addressed optional-node links in button handling and starting missing nodes before waiting for HDFS safe mode. Live scale-up/decommission actions have not been exercised during implementation, per the request to keep verification lightweight.
