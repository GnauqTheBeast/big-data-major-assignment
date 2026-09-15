package com.example.hadoop;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public final class TopKItemsJobSmokeTest {
    private TopKItemsJobSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        returnsTopKByDescendingCount();
        tieOnCountPrefersSmallerItemName();
        kLargerThanDistinctItemsReturnsEverything();
        skipsEmptyLinesAndRejectsBadK();
        System.out.println("PASS: all TopKItemsJob smoke tests");
    }

    private static void returnsTopKByDescendingCount() throws Exception {
        Path directory = Files.createTempDirectory("top-k-order-");
        Path input = writeInput(
                directory,
                "phone",
                "laptop",
                "phone",
                "tablet",
                "laptop",
                "phone",
                "laptop",
                "tablet");
        Path output = directory.resolve("output");
        Job countJob = TopKItemsJob.createCountJob(
                localConfiguration(), input.toString(), directory.resolve("counts").toString());
        assertTrue(countJob.waitForCompletion(true), "counting job must succeed");

        Job topKJob = TopKItemsJob.createTopKJob(
                localConfiguration(),
                directory.resolve("counts").toString(),
                output.toString(),
                2);
        assertTrue(topKJob.waitForCompletion(true), "selection job must succeed");
        assertEquals(
                Arrays.asList("laptop\t3", "phone\t3"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8),
                "top 2 must be laptop and phone with count 3");
    }

    private static void tieOnCountPrefersSmallerItemName() throws Exception {
        Path directory = Files.createTempDirectory("top-k-tie-");
        Path input = writeInput(directory, "beta", "alpha", "beta", "alpha");
        Path output = directory.resolve("output");

        TopKItemsJob tool = new TopKItemsJob();
        tool.setConf(localConfiguration());
        assertEquals(
                0, tool.run(new String[] {input.toString(), output.toString(), "1"}),
                "k=1 must succeed");
        assertEquals(
                Arrays.asList("alpha\t2"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8),
                "tied counts must prefer the smaller item name");
    }

    private static void kLargerThanDistinctItemsReturnsEverything() throws Exception {
        Path directory = Files.createTempDirectory("top-k-overflow-");
        Path input = writeInput(directory, "x", "y", "x");
        Path output = directory.resolve("output");

        TopKItemsJob tool = new TopKItemsJob();
        tool.setConf(localConfiguration());
        assertEquals(
                0, tool.run(new String[] {input.toString(), output.toString(), "10"}),
                "k larger than distinct count must succeed");
        List<String> lines =
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8);
        assertEquals(Arrays.asList("x\t2", "y\t1"), lines, "all items must be returned");
    }

    private static void skipsEmptyLinesAndRejectsBadK() throws Exception {
        Path directory = Files.createTempDirectory("top-k-validation-");
        Path input = writeInput(directory, "a", "", "  ", "a", "b");
        Path output = directory.resolve("output");

        TopKItemsJob tool = new TopKItemsJob();
        tool.setConf(localConfiguration());
        assertEquals(
                0, tool.run(new String[] {input.toString(), output.toString(), "2"}),
                "validation run must succeed");

        Job countJob = TopKItemsJob.createCountJob(
                localConfiguration(), input.toString(), directory.resolve("counts").toString());
        assertTrue(countJob.waitForCompletion(true), "count job must succeed");
        assertEquals(
                2L,
                countJob
                        .getCounters()
                        .findCounter(TopKItemsJob.InputCounters.EMPTY_LINES)
                        .getValue(),
                "empty-line counter");

        assertEquals(
                2, tool.run(new String[] {input.toString(), output.toString(), "0"}),
                "k=0 must return usage error code 2");
        assertEquals(
                2, tool.run(new String[] {input.toString(), output.toString()}),
                "missing k must return usage error code 2");
        assertEquals(
                2, tool.run(new String[] {input.toString(), output.toString(), "abc"}),
                "non-numeric k must return usage error code 2");
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
