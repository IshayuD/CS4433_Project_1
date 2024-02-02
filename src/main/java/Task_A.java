import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task_A {

    public static class UserMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static String nationality = "Germany";
        private Text name = new Text();
        private Text hobby = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elts = line.split(",");
            String curName = elts[1];
            String curNat = elts[2];
            String curHobby = elts[4];
            if (curNat.equals(nationality)) {
                name.set(curName);
                hobby.set(curHobby);
                context.write(name, hobby);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        long timeNow = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "user of nationality");

        job.setJarByClass(Task_A.class);

        job.setMapperClass(UserMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/output/output_taska_final");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }
        FileInputFormat.addInputPath(job, new Path("file:///C:/Users/jackl/IntelliJ_Projects/CS4433_Project_1/data/pages.csv"));
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
