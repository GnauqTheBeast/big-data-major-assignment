# Bài 17 — Top-K Popular Items (Hadoop MapReduce + Spark)

This exercise finds the **K items that occur most often** in the input (for
example, the K most popular search keywords). Each non-empty input line is
counted as one occurrence of that item.

Like the sort lab, the same problem is solved **two ways** so the Hadoop
MapReduce and Spark implementations can be compared directly:

| Engine | Location | Class | Build |
|---|---|---|---|
| Hadoop MapReduce | `top-k/` | `com.example.hadoop.TopKItemsJob` | Maven |
| Spark SQL | `spark-top-k/` | `com.example.spark.SparkTopKDF` | Maven |

Both read the **same HDFS keyword file** and produce the **same meaning**: the
top K `(item, count)` pairs ordered by descending count, with ties broken by
item name ascending so repeated runs are deterministic. The sample input has
6× `laptop`, 5× `phone`, 4× `tablet`, and four single-occurrence items, so with
K = 3 the correct answer is:

```text
laptop    6
phone     5
tablet    4
```

---

# Part A — Hadoop MapReduce (`top-k/`)

## How it works

The job runs in **two MapReduce stages**:

**Stage 1 — Counting (word count).** Each Mapper emits `(item, 1)`.
A Combiner and Reducer sum the occurrences into `(item, count)` pairs.
The Combiner keeps shuffle traffic low by pre-aggregating inside each Mapper.

**Stage 2 — Top-K selection.** Each Mapper reads the `(item, count)` pairs,
keeps only its local Top K in a min-heap of size K, and emits at most K
candidates. One Reducer merges all candidates with a global min-heap of
size K and writes the final K pairs. Each task heap holds at most K entries,
so memory usage is proportional to K, not to the input size.

## Project structure

```text
top-k/
├── pom.xml
├── input/keywords.txt
└── src/
    ├── main/java/com/example/hadoop/TopKItemsJob.java
    └── test/java/com/example/hadoop/TopKItemsJobSmokeTest.java
```

## Build with Maven

```bash
cd top-k
mvn clean test
mvn clean package
```

The generated JAR is `target/top-k-1.0-SNAPSHOT.jar`.

## Start Hadoop services

```bash
docker compose up -d namenode datanode resourcemanager nodemanager
```

## Upload the shared keyword input to HDFS

Both engines read exactly this file:

```bash
docker cp top-k/input/keywords.txt namenode:/tmp/keywords.txt

docker exec namenode hdfs dfs -mkdir -p /training/top-k/input
docker exec namenode hdfs dfs -put -f \
  /tmp/keywords.txt /training/top-k/input/keywords.txt

docker exec namenode hdfs dfs -cat /training/top-k/input/keywords.txt
```

## Run the Hadoop job

```bash
docker cp top-k/target/top-k-1.0-SNAPSHOT.jar namenode:/tmp/top-k.jar

docker exec namenode hdfs dfs -rm -r -f /training/top-k/hadoop-output

docker exec namenode hadoop jar /tmp/top-k.jar \
  com.example.hadoop.TopKItemsJob \
  /training/top-k/input \
  /training/top-k/hadoop-output \
  3
```

Read the result:

```bash
docker exec namenode hdfs dfs -cat /training/top-k/hadoop-output/part-r-00000
```

Expected result (tab separated):

```text
laptop	6
phone	5
tablet	4
```

Invalid K (`<= 0`, non-numeric, or missing) is rejected with exit code `2`.

---

# Part B — Spark SQL (`spark-top-k/`)

## How it works

`SparkTopKDF` expresses the same two steps as DataFrame operators:

```java
cleaned   = raw.withColumn("item", trim(col("value"))).filter(item != "")
counted   = cleaned.groupBy("item").count()
topK      = counted.orderBy(count desc, item asc).limit(k)
```

`groupBy(...).count()` is Spark's distributed word count (a shuffle + partial
aggregation), and `orderBy(...).limit(k)` selects the K largest counts. Spark
handles the heap/top-K internally, so there is no explicit per-task heap like
in the MapReduce version.

## Build with Maven

```bash
cd spark-top-k
mvn clean package
```

The generated JAR is `target/spark-top-k-1.0-SNAPSHOT.jar`. If Maven is not on
the host, build with Docker instead:

```bash
docker run --rm \
  -v "$PWD/spark-top-k:/work" \
  -v spark-top-k-m2:/root/.m2 \
  -w /work \
  maven:3.9.9-eclipse-temurin-17 \
  mvn clean package
```

## Start Spark services and grant HDFS write access

```bash
docker compose up -d namenode datanode spark-master spark-worker

docker exec namenode hdfs dfs -mkdir -p /training/top-k
docker exec namenode hdfs dfs -chown spark:supergroup /training/top-k
```

(Re-uploading `keywords.txt` is not needed if Part A already did it.)

## Submit the Spark job

```bash
docker cp spark-top-k/target/spark-top-k-1.0-SNAPSHOT.jar spark-master:/tmp/spark-top-k.jar

docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  --class com.example.spark.SparkTopKDF \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /tmp/spark-top-k.jar \
  hdfs://namenode:9000/training/top-k/input/keywords.txt \
  hdfs://namenode:9000/training/top-k/spark-output \
  3
```

## Inspect the Spark result

```bash
docker exec namenode hdfs dfs -cat \
  '/training/top-k/spark-output/part-*.csv'
```

Expected result (comma separated):

```text
laptop,6
phone,5
tablet,4
```

The output mode is `overwrite`, so rerunning replaces only the Spark output
directory. Spark may write multiple part files plus a `_SUCCESS` marker.

---

# Compare the Hadoop and Spark results

Both jobs read the same keyword file, so their top-K answers must agree:

```bash
mkdir -p comparison-output

docker exec namenode hdfs dfs -cat \
  /training/top-k/hadoop-output/part-r-00000 \
  > comparison-output/hadoop.txt

docker exec namenode hdfs dfs -cat \
  '/training/top-k/spark-output/part-*.csv' \
  | tr ',' '\t' \
  > comparison-output/spark.txt

diff -u comparison-output/hadoop.txt comparison-output/spark.txt
```

No `diff` output means both engines produced the same top-K items and counts.

### What is being compared?

| Topic | Hadoop MapReduce | Spark SQL |
|---|---|---|
| Input | HDFS text (`keywords.txt`) | Same HDFS text |
| Counting | Mapper `(item,1)` + Combiner + Reducer | `groupBy("item").count()` |
| Top-K selection | Explicit per-task min-heap size K, one reducer | `orderBy(...).limit(k)` |
| Invalid/blank lines | Skipped, counted via `EMPTY_LINES`/`MALFORMED_LINES` | Filtered out in the DataFrame |
| Compute service | Current lab uses LocalJobRunner | Spark standalone master/worker |
| Output | One `part-r-00000` (item TAB count) | One or more `part-*.csv` (item,count) |
| Best fit | Durable batch pipeline, teaches MapReduce internals | Concise relational API, iterative/multi-stage work |

The MapReduce version makes the distributed top-K algorithm explicit (mapper
candidate heap, single merging reducer, bounded per-task memory). The Spark
version reaches the same answer with far less code by delegating the shuffle
and top-K to the query engine. This is a correctness and architecture
comparison, not a strict performance benchmark.

---

# Run the Hadoop smoke test inside the NameNode container

Uses `javac` and the Hadoop libraries already in the container (no host Maven
needed):

```bash
docker cp top-k namenode:/tmp/top-k

docker exec namenode sh -c '
  mkdir -p /tmp/top-k/classes &&
  javac -cp "$(hadoop classpath)" \
    -d /tmp/top-k/classes \
    /tmp/top-k/src/main/java/com/example/hadoop/TopKItemsJob.java \
    /tmp/top-k/src/test/java/com/example/hadoop/TopKItemsJobSmokeTest.java &&
  java -cp "/tmp/top-k/classes:$(hadoop classpath)" \
    com.example.hadoop.TopKItemsJobSmokeTest
'
```

The final line must be:

```text
PASS: all TopKItemsJob smoke tests
```

# Run the Spark smoke test

```bash
docker run --rm \
  -v "$PWD/spark-top-k:/work" \
  -v spark-top-k-m2:/root/.m2 \
  -w /work \
  maven:3.9.9-eclipse-temurin-17 \
  mvn test-compile

docker run --rm \
  -v "$PWD/spark-top-k:/work" \
  -w /work \
  maven:3.9.9-eclipse-temurin-17 \
  jar --create \
  --file target/spark-top-k-smoke-tests.jar \
  -C target/classes . \
  -C target/test-classes .

docker run --rm \
  -v "$PWD/spark-top-k:/work" \
  -w /work \
  bitnamilegacy/spark:3.5.0 \
  /opt/bitnami/spark/bin/spark-submit \
  --master 'local[2]' \
  --class com.example.spark.SparkTopKDFSmokeTest \
  /work/target/spark-top-k-smoke-tests.jar
```

The final line must be:

```text
PASS: all SparkTopKDF smoke tests
```

## Current execution mode

The repository's `hadoop.env` does not set `mapreduce.framework.name=yarn`, so
the Hadoop job above uses LocalJobRunner while reading and writing HDFS. The
Spark job runs on Spark Standalone. Neither requires configuring MapReduce on
YARN, and Spark is not required for the Hadoop job.
