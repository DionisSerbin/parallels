package bmstu;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FlightReducer extends Reducer<FlightComparable, Text, String, Text> {

    @Override
    protected void reduce(FlightComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text ){
            context.write(key.toString(), new Tex);
        }
    }
}