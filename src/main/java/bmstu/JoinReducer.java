package bmstu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<AirWritableComparable, Text, Text, Text> {

    @Override
    protected void reduce(AirWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airName = new Text(iter.next().toString());
    }
}