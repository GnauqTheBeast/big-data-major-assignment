package com.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

public final class SparkSortDF {
    private SparkSortDF() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: SparkSortDF <input-path> <output-path>");
            System.exit(2);
        }

        SparkSession spark = SparkSession.builder()
                .appName("IntegerSortDF")
                .getOrCreate();

        try {
            Dataset<Row> raw = spark.read().text(args[0]);
            Dataset<Row> sorted = parseAndSort(raw);

            sorted.explain(true);
            sorted.write()
                    .mode("overwrite")
                    .csv(args[1]);
        } finally {
            spark.stop();
        }
    }

    static Dataset<Row> parseAndSort(Dataset<Row> raw) {
        return raw
                .withColumn("num", expr("try_cast(trim(value) AS INT)"))
                .filter(col("num").isNotNull())
                .select(col("num"))
                .orderBy(col("num").asc());
    }
}
