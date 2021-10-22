package bmstu;

import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner {

    @Override
    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
