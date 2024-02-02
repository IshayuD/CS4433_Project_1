import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

public class Task_B {

    public static class AccessCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pageAccessed = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            // Page dataset structure is 5 cols
            if (fields.length >= 5) {
                pageAccessed.set(fields[2]);
                context.write(pageAccessed, one);
            }
        }
    }

    public static class TopTenPagesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final int TOP_PAGES_COUNT = 10;
        private PriorityQueue<PageCount> topPages = new PriorityQueue<>(Comparator.reverseOrder());

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }


            topPages.add(new PageCount(key.toString(), sum));

            if (topPages.size() > TOP_PAGES_COUNT) {
                topPages.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top 10 pages
            while (!topPages.isEmpty()) {
                PageCount page = topPages.poll();
                context.write(new Text(page.getPageId()), new IntWritable(page.getCount()));
            }
        }

        private class PageCount implements Comparable<PageCount> {
            private String pageId;
            private int count;

            public PageCount(String pageId, int count) {
                this.pageId = pageId;
                this.count = count;
            }

            // getters
            public String getPageId() { return pageId; }
            public int getCount() { return count; }

            @Override
            public int compareTo(PageCount other) {
                // Reverse order to have higher counts first
                return Integer.compare(other.count, this.count);
            }
        }
    }


    public static class ReplicatedJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> accessCount = new HashMap<>();
        private final Text outK = new Text();
        private final Text outV = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

            // read the record line by line
            String line;
            while(StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] split = line.split("\t");
                accessCount.put(split[0], split[1]);
            }

            // close the stream
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] splits = value.toString().split(",");

            // use the ID field pulled from the page dataset to retrieve count from the lookup table
            String count = accessCount.get(splits[0]);

            if (count != null){
                outK.set(splits[0]);
                outV.set(splits[1] + "\t" + splits[2] + "\t" + count);

                context.write(outK, outV);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();

        //JOB 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Top 10 accessed pages");

        job1.setJarByClass(Task_B.class);

        job1.setMapperClass(AccessCountMapper.class);
        job1.setReducerClass(TopTenPagesReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        Path outputPath1 = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskb_job1");
        FileSystem fs1 = outputPath1.getFileSystem(conf1);
        if (fs1.exists(outputPath1)) {
            fs1.delete(outputPath1, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job1, new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/data/access_logs.csv"));
        FileOutputFormat.setOutputPath(job1, outputPath1);
        job1.waitForCompletion(true);

        //JOB 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Map-side Join");

        job2.setJarByClass(Task_B.class);

        job2.setMapperClass(ReplicatedJoinMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(0);

        job2.addCacheFile(new URI("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskb_job1/part-r-00000"));

        Path outputPath2 = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskb_final");
        FileSystem fs2 = outputPath2.getFileSystem(conf2);
        if (fs2.exists(outputPath2)) {
            fs2.delete(outputPath2, true); // true will delete recursively
        }
        FileInputFormat.setInputPaths(job2, new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/data/pages.csv"));
        FileOutputFormat.setOutputPath(job2, outputPath2);

        job2.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
