package com.example.hadoop;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public final class IntegerSortJobSmokeTest {
    private IntegerSortJobSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        sortsSignedIntegersAndPreservesDuplicates();
        skipsInvalidRecordsAndReportsCounters();
        rejectsMissingInputAndOutputArguments();
        System.out.println("PASS: all IntegerSortJob smoke tests");
    }

    private static void sortsSignedIntegersAndPreservesDuplicates() throws Exception {
        Path directory = Files.createTempDirectory("integer-sort-order-");
        Path input = writeInput(directory, "9", "-2", "9", "0", "3");
        Path output = directory.resolve("output");
        Job job = IntegerSortJob.createJob(localConfiguration(), input.toString(), output.toString());

        assertTrue(job.waitForCompletion(true), "sort job must succeed");
        assertEquals(
                Arrays.asList("-2", "0", "3", "9", "9"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8),
                "numbers must be globally sorted and duplicates must be preserved");
    }

    private static void skipsInvalidRecordsAndReportsCounters() throws Exception {
        Path directory = Files.createTempDirectory("integer-sort-validation-");
        Path input = writeInput(directory, "10", "not-a-number", "", " 7 ", "2.5");
        Path output = directory.resolve("output");
        Job job = IntegerSortJob.createJob(localConfiguration(), input.toString(), output.toString());

        assertTrue(job.waitForCompletion(true), "validation job must succeed");
        assertEquals(
                Arrays.asList("7", "10"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8),
                "only valid integers must be emitted");
        assertEquals(
                2L,
                job.getCounters()
                        .findCounter(IntegerSortJob.InputCounters.MALFORMED_LINES)
                        .getValue(),
                "malformed-line counter");
        assertEquals(
                1L,
                job.getCounters()
                        .findCounter(IntegerSortJob.InputCounters.EMPTY_LINES)
                        .getValue(),
                "empty-line counter");
    }

    private static void rejectsMissingInputAndOutputArguments() throws Exception {
        IntegerSortJob tool = new IntegerSortJob();
        tool.setConf(localConfiguration());
        assertEquals(2, tool.run(new String[0]), "missing arguments must return usage error code 2");
    }

    private static Path writeInput(Path directory, String... lines) throws Exception {
        Path input = directory.resolve("input.txt");
        Files.write(input, Arrays.asList(lines), StandardCharsets.UTF_8);
        return input;
    }

    private static Configuration localConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("mapreduce.framework.name", "local");
        return configuration;
    }

    private static void assertTrue(boolean actual, String message) {
        if (!actual) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }
}
