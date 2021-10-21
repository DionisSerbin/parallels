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
        String[] words = line.split("[\\p{Punct}\\p{Space}-]");
        for (String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}

