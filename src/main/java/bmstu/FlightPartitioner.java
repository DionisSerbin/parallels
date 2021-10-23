package bmstu;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<AirWritableComparable, Text> {

    @Override
    public int getPartition(AirWritableComparable airWritableComparable, Text text, int amountReducers) {
        int returned = Float.hashCode(airWritableComparable.delayTime) & Integer.MAX_VALUE;
        return returned % amountReducers;
    }
}
