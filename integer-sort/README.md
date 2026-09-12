# Hadoop Integer Sort

This MapReduce job sorts one signed 32-bit integer per input line. It preserves duplicates, skips invalid records, and reports invalid input through Hadoop counters.

## Browser UI

Run `npm start` from the repository root and open **http://localhost:3000**. The dashboard can start the cluster, upload or browse HDFS files, run this job, display logs, and download results. It also shows container status and the DataNodes holding the selected file's blocks. No manual JAR build or Docker/HDFS commands are needed for this workflow. See [dashboard instructions](../dashboard/README.md).

## How sorting works

The Mapper parses each line and emits:

```text
(integer, NullWritable)
```

Hadoop's shuffle/sort phase orders mapper output by `IntWritable`. The Reducer receives keys in ascending numeric order and emits every value so duplicates are preserved.

The job deliberately uses one reducer:

```java
job.setNumReduceTasks(1);
```

This produces one globally sorted `part-r-00000`. With multiple reducers, every part file would be internally sorted, but concatenating part files would not necessarily produce a global order without a total-order partitioner.

## Project structure

```text
integer-sort/
├── pom.xml
├── input/numbers.txt
└── src/
    ├── main/java/com/example/hadoop/IntegerSortJob.java
    └── test/java/com/example/hadoop/
        ├── IntegerSortJobTest.java
        └── IntegerSortJobSmokeTest.java
```

## Build with Maven

The project targets Java 8 bytecode to match the Hadoop 3.2.1 Java 8 container:

```bash
cd integer-sort
mvn clean test
mvn clean package
```

The generated JAR is:

```text
target/integer-sort-1.0-SNAPSHOT.jar
```

## Upload input to HDFS

Run these commands from the repository root:

```bash
docker cp integer-sort/input/numbers.txt namenode:/tmp/numbers.txt

docker exec namenode hdfs dfs -mkdir -p /training/integer-sort/input
docker exec namenode hdfs dfs -put -f \
  /tmp/numbers.txt /training/integer-sort/input/numbers.txt

docker exec namenode hdfs dfs -cat \
  /training/integer-sort/input/numbers.txt
```

## Run the JAR

Copy the packaged JAR into the NameNode container:

```bash
docker cp \
  integer-sort/target/integer-sort-1.0-SNAPSHOT.jar \
  namenode:/tmp/integer-sort.jar
```

Hadoop requires the output directory not to exist before a job starts. Remove only the previous lab output before rerunning:

```bash
docker exec namenode hdfs dfs -rm -r -f \
  /training/integer-sort/output
```

Run the job:

```bash
docker exec namenode hadoop jar /tmp/integer-sort.jar \
  /training/integer-sort/input \
  /training/integer-sort/output
```

Read the globally sorted result:

```bash
docker exec namenode hdfs dfs -cat \
  /training/integer-sort/output/part-r-00000
```

Expected result:

```text
-100
-2
0
3
7
9
9
15
42
```

The job log also contains these custom counters:

```text
com.example.hadoop.IntegerSortJob$InputCounters
    EMPTY_LINES=1
    MALFORMED_LINES=1
```

## Run the lightweight test inside the NameNode container

This option uses `javac` and Hadoop libraries already available in the container, so Maven is not required on the host:

```bash
docker cp integer-sort namenode:/tmp/integer-sort

docker exec namenode sh -c '
  mkdir -p /tmp/integer-sort/classes &&
  javac -cp "$(hadoop classpath)" \
    -d /tmp/integer-sort/classes \
    /tmp/integer-sort/src/main/java/com/example/hadoop/IntegerSortJob.java \
    /tmp/integer-sort/src/test/java/com/example/hadoop/IntegerSortJobSmokeTest.java &&
  java -cp "/tmp/integer-sort/classes:$(hadoop classpath)" \
    com.example.hadoop.IntegerSortJobSmokeTest
'
```

The final line must be:

```text
PASS: all IntegerSortJob smoke tests
```

## Current execution mode

The repository's `hadoop.env` configures HDFS and YARN services but does not set `mapreduce.framework.name=yarn`. Consequently, the command above uses Hadoop's LocalJobRunner while reading input from and writing output to HDFS.

To run MapReduce tasks in YARN containers, first configure MapReduce-on-YARN consistently across the cluster. Spark is not required for either execution mode.
