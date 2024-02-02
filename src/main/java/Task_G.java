import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

public class Task_G {

    public static class AccessDateMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text id = new Text();
        private Text date = new Text();
        private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elts = line.split(",");
            try {
                Date curDate = fmt.parse(elts[4]);
                String idStr = elts[1];
                id.set(idStr);
                date.set(elts[4]);
                context.write(id, date);
            } catch (ParseException e) {
                System.out.println("parse error");
                throw new RuntimeException(e);
            }
        }
    }

    public static class AccessDateReducer
            extends Reducer<Text, Text, Text, Text> {

        private Map<String, Date> accessMap = new HashMap<String, Date>();
        private SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private LocalDate today = LocalDate.now();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String idStr = key.toString();
            for (Text val : values) {
                try {
                    Date curDate = fmt.parse(val.toString());
                    if (accessMap.containsKey(idStr)) {
                        Date check = accessMap.get(idStr);
                        if (curDate.after(check)) {
                            accessMap.put(idStr, curDate);
                        }
                    } else {
                        accessMap.put(idStr, curDate);
                    }
                } catch (ParseException e) {
                    System.out.println("parse error in reduce");
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Map.Entry<String, Date> e : accessMap.entrySet()) {
                Date curDate = e.getValue();
                String curId = e.getKey();
                LocalDate curLocal = curDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                long days = ChronoUnit.DAYS.between(curLocal, today);
                if (days > 14) {
                    context.write(new Text(curId), new Text(fmt.format(curDate)));
                }

            }
        }
    }

    public static class ReplicatedJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, String> accessDates = new HashMap<>();
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
                accessDates.put(split[0], split[1]);
            }

            // close the stream
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] splits = value.toString().split(",");

            // use the ID field pulled from the page dataset to retrieve count from the lookup table
            String date = accessDates.get(splits[0]);

            if (date != null){
                outK.set(splits[0]);
                outV.set(splits[1]);

                context.write(outK, outV);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();

        //JOB 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Map Reduce, Disconnected ID and Access Dates");

        job1.setJarByClass(Task_G.class);

        job1.setMapperClass(Task_G.AccessDateMapper.class);
        job1.setReducerClass(Task_G.AccessDateReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        Path outputPath1 = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskg_job1");
        FileSystem fs1 = outputPath1.getFileSystem(conf1);
        if (fs1.exists(outputPath1)) {
            fs1.delete(outputPath1, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job1, new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/data/access_logs.csv"));
        FileOutputFormat.setOutputPath(job1, outputPath1);
        job1.waitForCompletion(true);

        //JOB 2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Map-side Join, ID and Name");

        job2.setJarByClass(Task_G.class);

        job2.setMapperClass(Task_G.ReplicatedJoinMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(0);

        job2.addCacheFile(new URI("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskg_job1/part-r-00000"));

        Path outputPath2 = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taskg_final");
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
