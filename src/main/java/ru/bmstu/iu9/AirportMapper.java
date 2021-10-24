package ru.bmstu.iu9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {

    private static final String CODE = "Code";
    private static final String POINT = ",";
    private static final String QUOTES = "\"";
    private static final String NULL_STR = "";
    private static final int


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] column = line.split(POINT, 2);
        String airportId = column[0].replaceAll(QUOTES, NULL_STR);
        String airportName = column[1].replaceAll(QUOTES, NULL_STR);
        if(!(airportId.equals(CODE))){
            context.write(new AirportWritableComparable(
                    new IntWritable(Integer.parseInt(airportId)), new IntWritable(0) ),
                    new Text(airportName));
        }
    }
}
