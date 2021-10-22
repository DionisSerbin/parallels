package bmstu;

import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<FlightComparable, Text> {

    @Override
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
