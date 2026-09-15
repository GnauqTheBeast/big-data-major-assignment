package com.example.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Exercise 17 — Top-K Popular Items.
 *
 * <p>Finds the K items that occur most often in the input (for example, the K most
 * popular search keywords). Each non-empty input line is one item occurrence.
 *
 * <p>The job runs in two MapReduce stages:
 *
 * <ol>
 *   <li><b>Counting stage:</b> each Mapper emits {@code (item, 1)}; a Combiner and
 *       Reducer sum the occurrences into {@code (item, count)} pairs.</li>
 *   <li><b>Selection stage:</b> each Mapper keeps a min-heap of at most K
 *       {@code (count, item)} pairs and emits only those candidates; one Reducer
 *       merges the candidates with a global min-heap of size K and writes the
 *       final K pairs ordered by descending count.</li>
 * </ol>
 *
 * <p>Usage: {@code TopKItemsJob <input> <output> <k>}
 */
public final class TopKItemsJob extends Configured implements Tool {
    public enum InputCounters {
        EMPTY_LINES,
        MALFORMED_LINES
    }

    static final String TOP_K_CONF_KEY = "topk.k";

    // Order inside heaps: smallest count first; on ties the larger item name
    // is considered "smaller" so it gets evicted first, keeping results
    // deterministic (survivors prefer the smaller item name).
    static final Comparator<ItemCount> HEAP_ORDER = (left, right) -> {
        int byCount = Long.compare(left.count, right.count);
        if (byCount != 0) {
            return byCount;
        }
        return right.item.compareTo(left.item);
    };

    // Final output order: largest count first, ties broken by item name ascending.
    static final Comparator<ItemCount> OUTPUT_ORDER = (left, right) -> {
        int byCount = Long.compare(right.count, left.count);
        if (byCount != 0) {
            return byCount;
        }
        return left.item.compareTo(right.item);
    };

    static final class ItemCount {
        final String item;
        final long count;

        ItemCount(String item, long count) {
            this.item = item;
            this.count = count;
        }
    }

    // ================= Stage 1: counting =================

    public static final class CountMapper
            extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                context.getCounter(InputCounters.EMPTY_LINES).increment(1);
                return;
            }
            outputKey.set(line);
            context.write(outputKey, ONE);
        }
    }

    public static final class CountReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable outputValue = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    // ================= Stage 2: top-K selection =================

    /** Parses one counting-stage output line ("item\tcount"). */
    static ItemCount parseCountLine(String line) {
        int separator = line.lastIndexOf('\t');
        if (separator < 0) {
            return null;
        }
        String item = line.substring(0, separator);
        if (item.isEmpty()) {
            return null;
        }
        try {
            long count = Long.parseLong(line.substring(separator + 1).trim());
            if (count < 0) {
                return null;
            }
            return new ItemCount(item, count);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    static void offerBounded(PriorityQueue<ItemCount> heap, ItemCount candidate, int k) {
        if (heap.size() < k) {
            heap.offer(candidate);
        } else if (HEAP_ORDER.compare(candidate, heap.peek()) > 0) {
            heap.poll();
            heap.offer(candidate);
        }
    }

    public static final class TopKMapper
            extends Mapper<LongWritable, Text, LongWritable, Text> {
        private PriorityQueue<ItemCount> heap;
        private int k;
        private final LongWritable outputKey = new LongWritable();
        private final Text outputValue = new Text();

        @Override
        protected void setup(Context context) {
            k = context.getConfiguration().getInt(TOP_K_CONF_KEY, 10);
            heap = new PriorityQueue<>(k, HEAP_ORDER);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ItemCount parsed = parseCountLine(value.toString());
            if (parsed == null) {
                context.getCounter(InputCounters.MALFORMED_LINES).increment(1);
                return;
            }
            offerBounded(heap, parsed, k);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ItemCount candidate : heap) {
                outputKey.set(candidate.count);
                outputValue.set(candidate.item);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static final class TopKReducer
            extends Reducer<LongWritable, Text, Text, LongWritable> {
        private PriorityQueue<ItemCount> heap;
        private int k;
        private final Text outputKey = new Text();
        private final LongWritable outputValue = new LongWritable();

        @Override
        protected void setup(Context context) {
            k = context.getConfiguration().getInt(TOP_K_CONF_KEY, 10);
            heap = new PriorityQueue<>(k, HEAP_ORDER);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) {
            long count = key.get();
            for (Text value : values) {
                offerBounded(heap, new ItemCount(value.toString(), count), k);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<ItemCount> ordered = new ArrayList<>(heap);
            Collections.sort(ordered, OUTPUT_ORDER);
            for (ItemCount entry : ordered) {
                outputKey.set(entry.item);
                outputValue.set(entry.count);
                context.write(outputKey, outputValue);
            }
        }
    }

    // ================= Job wiring =================

    public static Job createCountJob(Configuration configuration, String input, String output)
            throws IOException {
        Job job = Job.getInstance(configuration, "top-k-counting");
        job.setJarByClass(TopKItemsJob.class);
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    public static Job createTopKJob(
            Configuration configuration, String input, String output, int k) throws IOException {
        Configuration configurationWithK = new Configuration(configuration);
        configurationWithK.setInt(TOP_K_CONF_KEY, k);
        Job job = Job.getInstance(configurationWithK, "top-k-selection");
        job.setJarByClass(TopKItemsJob.class);
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // One reducer merges every mapper candidate into the global Top K.
        job.setNumReduceTasks(1);
        return job;
    }

    /** Runs both stages; shared by the CLI entry point and the smoke test. */
    static boolean runTopK(Configuration configuration, String input, String output, int k)
            throws Exception {
        Path staging = new Path(output + "-counting-tmp");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(staging, true);

        Job countJob = createCountJob(configuration, input, staging.toString());
        if (!countJob.waitForCompletion(true)) {
            return false;
        }
        try {
            Job topKJob = createTopKJob(configuration, staging.toString(), output, k);
            return topKJob.waitForCompletion(true);
        } finally {
            fileSystem.delete(staging, true);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TopKItemsJob <input> <output> <k>");
            return 2;
        }

        int k;
        try {
            k = Integer.parseInt(args[2]);
        } catch (NumberFormatException exception) {
            System.err.println("Invalid K (must be a positive integer): " + args[2]);
            return 2;
        }
        if (k <= 0) {
            System.err.println("Invalid K (must be a positive integer): " + args[2]);
            return 2;
        }

        return runTopK(getConf(), args[0], args[1], k) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TopKItemsJob(), args);
        System.exit(exitCode);
    }
}
