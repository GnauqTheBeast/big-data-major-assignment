package com.example.spark;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class SparkSortDFSmokeTest {
    private SparkSortDFSmokeTest() {
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSortDFSmokeTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        try {
            sortsSignedIntegersAndPreservesDuplicates(spark);
            skipsBlankAndMalformedRecords(spark);
            System.out.println("PASS: all SparkSortDF smoke tests");
        } finally {
            spark.stop();
        }
    }

    private static void sortsSignedIntegersAndPreservesDuplicates(SparkSession spark) {
        Dataset<Row> raw = textRows(spark, "9", "-2", "9", "0", "3");

        assertEquals(
                Arrays.asList(-2, 0, 3, 9, 9),
                values(SparkSortDF.parseAndSort(raw)),
                "numbers must be globally sorted and duplicates must be preserved");
    }

    private static void skipsBlankAndMalformedRecords(SparkSession spark) {
        Dataset<Row> raw = textRows(spark, "10", "not-a-number", "", " 7 ", "2.5");

        assertEquals(
                Arrays.asList(7, 10),
                values(SparkSortDF.parseAndSort(raw)),
                "only valid 32-bit integers must be emitted");
    }

    private static Dataset<Row> textRows(SparkSession spark, String... lines) {
        return spark.createDataset(Arrays.asList(lines), Encoders.STRING()).toDF("value");
    }

    private static List<Integer> values(Dataset<Row> rows) {
        return rows.collectAsList().stream()
                .map(row -> row.getInt(0))
                .collect(Collectors.toList());
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }
}
