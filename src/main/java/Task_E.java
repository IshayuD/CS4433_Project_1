import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Task_E {
    public static class FacebookPageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text pageOwner = new Text();
        private Text pageID = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming CSV format: PageOwner,UserID,PageID,OtherColumns...
            String[] columns = value.toString().split(",");
            
            if (columns.length >= 3) {
                pageOwner.set(columns[0]);
                pageID.set(columns[2]);
                context.write(pageOwner, pageID);
            }
        }
    }

    

}
