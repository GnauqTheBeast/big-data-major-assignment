package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

public final class SparkTopKDF {
    private SparkTopKDF() {
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: SparkTopKDF <input-path> <output-path> <k>");
            System.exit(2);
        }

        int k;
        try {
            k = Integer.parseInt(args[2]);
        } catch (NumberFormatException exception) {
            System.err.println("Invalid K (must be a positive integer): " + args[2]);
            System.exit(2);
            return;
        }
        if (k <= 0) {
            System.err.println("Invalid K (must be a positive integer): " + args[2]);
            System.exit(2);
        }

        SparkSession spark = SparkSession.builder()
                .appName("TopKItemsDF")
                .getOrCreate();

        try {
            Dataset<Row> raw = spark.read().text(args[0]);
            Dataset<Row> topK = countAndTakeTopK(raw, k);

            topK.explain(true);
            topK.write()
                    .mode("overwrite")
                    .csv(args[1]);
        } finally {
            spark.stop();
        }
    }

    /**
     * Counts occurrences of each non-empty trimmed line, then returns the K
     * most frequent items ordered by descending count (ties broken by item
     * name ascending for determinism).
     */
    static Dataset<Row> countAndTakeTopK(Dataset<Row> raw, int k) {
        Dataset<Row> cleaned = raw
                .withColumn("item", trim(col("value")))
                .filter(col("item").notEqual(""))
                .select(col("item"));

        Dataset<Row> counted = cleaned
                .groupBy(col("item"))
                .count();

        return counted
                .orderBy(col("count").desc(), col("item").asc())
                .limit(k);
    }
}
