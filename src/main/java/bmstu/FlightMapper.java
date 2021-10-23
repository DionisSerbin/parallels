package bmstu;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, AirWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        String[] column = line.split(",");
        if(key.get() > 0){
            //DEST_AIRPORT_ID = 14 ARR_DELAY = 18 AIR_TIME = 21 IS_CANCELLED = 19
            int airportId = Integer.parseInt(column[14]);
            float delayTime = Float.parseFloat(column[18]);
            if (!column[18].equals("")) {
                context.write(new AirWritableComparable());
            }

        }
    }
}

