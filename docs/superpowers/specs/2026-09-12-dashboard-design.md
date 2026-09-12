# Integer-sort dashboard

Approved in chat on 2026-09-12. Build a localhost Node.js application for this repository's Docker Compose Hadoop lab.

- Browser UI: upload a local file to HDFS, browse HDFS directories, select an existing file, preview/download it, and run the existing integer-sort Java job.
- Jobs: asynchronous execution, bounded logs, success/failure states, distinct output directories under `/training/integer-sort/runs`, and downloadable `part-r-00000`. Build the Java source using the container's Hadoop classpath so Maven is not needed on the host.
- Architecture: all six Compose services, observed container status and network addresses, service connections, and selected-file block locations from HDFS. Report missing/unavailable information explicitly. Show LocalJobRunner execution separately from the available YARN/Spark services.
- Server: Node.js 22+, built-in modules only, loopback binding, argument-array Docker subprocesses, bounded memory for streaming uploads and metadata output, same-origin mutation protection. No arbitrary command endpoint. A Start cluster button invokes this repository's Compose configuration.
- Storage: unique upload folders preserve earlier files; browser uploads stream directly to HDFS through Docker stdin with backpressure, no fixed file-size cap, and no host temporary file. A hidden staging file is published only after successful completion. Uploads have a five-minute idle timeout, with no total transfer deadline. Job history is in memory and resets on server restart; HDFS files persist.
- Demo scope: per user request, do not maintain dashboard automated tests or smoke-test scripts. Existing Java exercise tests are outside this change.
