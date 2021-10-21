package bmstu;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        String[]  = line.split(",");
        if(key.get() > 0){
            //DEST_AIRPORT_ID = 15 ARR_DELAY = 19 AIR_TIME = 22
            int airId = word
        }
    }
}

