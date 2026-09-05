package com.example.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public final class IntegerSortJob extends Configured implements Tool {
    public enum InputCounters {
        EMPTY_LINES,
        MALFORMED_LINES
    }

    public static final class SortMapper
            extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        private final IntWritable outputKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                context.getCounter(InputCounters.EMPTY_LINES).increment(1);
                return;
            }

            try {
                outputKey.set(Integer.parseInt(line));
                context.write(outputKey, NullWritable.get());
            } catch (NumberFormatException exception) {
                context.getCounter(InputCounters.MALFORMED_LINES).increment(1);
            }
        }
    }

    public static final class SortReducer
            extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(
                IntWritable key,
                Iterable<NullWritable> values,
                Context context) throws IOException, InterruptedException {
            for (NullWritable ignored : values) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static Job createJob(Configuration configuration, String input, String output)
            throws IOException {
        Job job = Job.getInstance(configuration, "integer-sort");
        job.setJarByClass(IntegerSortJob.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // One reducer produces one globally sorted part file. With multiple reducers,
        // each part is sorted, but the collection of part files is not globally ordered.
        job.setNumReduceTasks(1);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: IntegerSortJob <input> <output>");
            return 2;
        }

        Job job = createJob(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new IntegerSortJob(), args);
        System.exit(exitCode);
    }
}
