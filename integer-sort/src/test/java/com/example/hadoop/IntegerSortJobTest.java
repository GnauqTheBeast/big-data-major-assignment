package com.example.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegerSortJobTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void sortsSignedIntegersAndPreservesDuplicates() throws Exception {
        Path input = writeInput("9", "-2", "9", "0", "3");
        Path output = temporaryDirectory.resolve("sorted-output");

        Job job = IntegerSortJob.createJob(localConfiguration(), input.toString(), output.toString());

        assertTrue(job.waitForCompletion(true));
        assertEquals(
                Arrays.asList("-2", "0", "3", "9", "9"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8));
    }

    @Test
    void skipsInvalidRecordsAndReportsCounters() throws Exception {
        Path input = writeInput("10", "not-a-number", "", " 7 ", "2.5");
        Path output = temporaryDirectory.resolve("validated-output");

        Job job = IntegerSortJob.createJob(localConfiguration(), input.toString(), output.toString());

        assertTrue(job.waitForCompletion(true));
        assertEquals(
                Arrays.asList("7", "10"),
                Files.readAllLines(output.resolve("part-r-00000"), StandardCharsets.UTF_8));
        assertEquals(
                2,
                job.getCounters()
                        .findCounter(IntegerSortJob.InputCounters.MALFORMED_LINES)
                        .getValue());
        assertEquals(
                1,
                job.getCounters()
                        .findCounter(IntegerSortJob.InputCounters.EMPTY_LINES)
                        .getValue());
    }

    @Test
    void rejectsMissingInputAndOutputArguments() throws Exception {
        IntegerSortJob tool = new IntegerSortJob();
        tool.setConf(localConfiguration());

        assertEquals(2, tool.run(new String[0]));
    }

    private Path writeInput(String... lines) throws Exception {
        Path input = temporaryDirectory.resolve("input.txt");
        List<String> content = Arrays.asList(lines);
        Files.write(input, content, StandardCharsets.UTF_8);
        return input;
    }

    private Configuration localConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("mapreduce.framework.name", "local");
        return configuration;
    }
}
