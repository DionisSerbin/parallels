package bmstu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<FlightWritableComparable, Text> {

    @Override
    public int getPartition(FlightWritableComparable flightWritableComparable, Text text, int amountReducers) {
        int returned = Float.hashCode(flightWritableComparable.delayTime) & Integer.MAX_VALUE;
        return returned % amountReducers;
    }
}
