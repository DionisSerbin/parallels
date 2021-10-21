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
        String[] column = line.split(",");
        if(key.get() > 0){
            //DEST_AIRPORT_ID = 15 ARR_DELAY = 19 AIR_TIME = 22 IS_CANCELLED = 20
            int airportId = Integer.parseInt(column[15]);
            double delayTime = Double.parseDouble(column[19]);
            double airTime = Double.parseDouble(column[22]);
            double cancelled = Double.parseDouble(column[20]);
            if(cancelled == 1.00) {

            }else {

            }
        }
    }
}

