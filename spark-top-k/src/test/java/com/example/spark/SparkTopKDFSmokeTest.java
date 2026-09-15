package com.example.spark;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SparkTopKDFSmokeTest {
    private SparkTopKDFSmokeTest() {
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkTopKDFSmokeTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        try {
            returnsTopKByDescendingCount(spark);
            tieOnCountPrefersSmallerItemName(spark);
            skipsBlankLines(spark);
            System.out.println("PASS: all SparkTopKDF smoke tests");
        } finally {
            spark.stop();
        }
    }

    private static void returnsTopKByDescendingCount(SparkSession spark) {
        Dataset<Row> raw = textRows(
                spark, "phone", "laptop", "phone", "tablet", "laptop", "phone", "laptop", "tablet");

        assertEquals(
                Arrays.asList("laptop:3", "phone:3"),
                pairs(SparkTopKDF.countAndTakeTopK(raw, 2)),
                "top 2 must be laptop and phone with count 3");
    }

    private static void tieOnCountPrefersSmallerItemName(SparkSession spark) {
        Dataset<Row> raw = textRows(spark, "beta", "alpha", "beta", "alpha");

        assertEquals(
                Arrays.asList("alpha:2"),
                pairs(SparkTopKDF.countAndTakeTopK(raw, 1)),
                "tied counts with k=1 must prefer the smaller item name");
    }

    private static void skipsBlankLines(SparkSession spark) {
        Dataset<Row> raw = textRows(spark, "a", "", "  ", "a", "b");

        assertEquals(
                Arrays.asList("a:2", "b:1"),
                pairs(SparkTopKDF.countAndTakeTopK(raw, 10)),
                "blank lines must be ignored and all items returned when k exceeds distinct count");
    }

    private static Dataset<Row> textRows(SparkSession spark, String... lines) {
        return spark.createDataset(Arrays.asList(lines), Encoders.STRING()).toDF("value");
    }

    private static List<String> pairs(Dataset<Row> rows) {
        return rows.collectAsList().stream()
                .map(row -> row.getString(0) + ":" + row.getLong(1))
                .collect(Collectors.toList());
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }
}
