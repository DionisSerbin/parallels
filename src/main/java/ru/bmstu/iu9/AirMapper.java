package ru.bmstu.iu9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirMapper extends Mapper<LongWritable, Text, AirWritableComparable, Text> {

    private static final String CODE = "Code";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] column = line.split(",", 2);
        String airportId = column[0].replaceAll("\"", "");
        String airportName = column[1].replaceAll("\"", "");
        if(!(airportId.equals(CODE))){
            context.write(new AirWritableComparable(
                    new IntWritable(Integer.parseInt(airportId)), new IntWritable(0) ),
                    new Text(airportName));
        }
    }
}
