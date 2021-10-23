package ru.bmstu.iu9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlightPartitioner extends Partitioner<AirWritableComparable, Text> {

    @Override
    public int getPartition(AirWritableComparable airWritableComparable, Text text, int amountReducers) {
        int returned = Math.abs(airWritableComparable.getAirportId().hashCode());
        return returned % amountReducers;
    }
}
