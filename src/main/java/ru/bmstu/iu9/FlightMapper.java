package ru.bmstu.iu9;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightMapper extends Mapper<LongWritable, Text, AirWritableComparable, Text> {
    private static final int DEST_AIRPORT_ID = 14;
    private static final int ARR_DELAY = 17;
    private static final String NULL_STR = "";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        String[] column = line.split(",");
        if(column[DEST_AIRPORT_ID].equals("DEST_AIRPORT_ID")){
            //DEST_AIRPORT_ID = 14 ARR_DELAY = 18 AIR_TIME = 21 IS_CANCELLED = 19
            int airportId = Integer.parseInt(column[DEST_AIRPORT_ID]);
            float delayTime = Float.parseFloat(column[ARR_DELAY]);
            if (!column[ARR_DELAY].equals(NULL_STR)) {
                context.write(new AirWritableComparable(new IntWritable(airportId),
                        new IntWritable(1)), new Text(column[ARR_DELAY]));
            }

        }
    }
}

