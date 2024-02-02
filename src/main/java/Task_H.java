import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task_H {

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text id = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elts = line.split(",");
            id.set(elts[1]);
            context.write(id, one);
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> friendSums = new HashMap<String, Integer>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            if(friendSums.containsKey(key.toString())) {
                int curSum = friendSums.get(key.toString());
                int newSum = curSum + sum;
                friendSums.put(key.toString(), new Integer(newSum));
            } else {
                friendSums.put(key.toString(), new Integer(sum));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            int total = 0;
            for(int val : friendSums.values()) {
                total = total + val;
            }
            double avg = (double) total / (double) friendSums.size();

            for(Map.Entry<String, Integer> e : friendSums.entrySet()) {
                if(e.getValue() >= avg) {
                    context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "friend counts");

        job.setJarByClass(Task_H.class);

        job.setMapperClass(Task_H.AverageMapper.class);
        job.setReducerClass(Task_H.AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskh_final");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job, new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/data/friends.csv"));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
