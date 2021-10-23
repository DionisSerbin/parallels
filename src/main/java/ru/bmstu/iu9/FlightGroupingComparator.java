package ru.bmstu.iu9;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupingComparator extends WritableComparator {

    protected FlightGroupingComparator(){
        super(AirWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirWritableComparable first = (AirWritableComparable) a;
        AirWritableComparable second = (AirWritableComparable) b;
        return first.getAirportId().compareTo(second.getAirportId());
    }
}
