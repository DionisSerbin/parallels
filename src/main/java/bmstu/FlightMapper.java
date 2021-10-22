package bmstu;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, FlightComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        String[] column = line.split(",");
        if(key.get() > 0){
            //DEST_AIRPORT_ID = 14 ARR_DELAY = 18 AIR_TIME = 21 IS_CANCELLED = 19
            int airportId = Integer.parseInt(column[14]);
            double delayTime = Double.parseDouble(column[18]);
            double airTime = Double.parseDouble(column[21]);
            double cancelled = Double.parseDouble(column[19]);
            if(cancelled == 1.00) {
                context.write(new FlightComparable(airportId, 0.00, 0.00, true), value);
            }else if (delayTime > 0.00){
                context.write(new FlightComparable(airportId, delayTime, airTime, false), value);
            }
        }
    }
}

