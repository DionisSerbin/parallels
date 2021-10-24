package ru.bmstu.iu9;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportGroupingComparator extends WritableComparator {

    protected AirportGroupingComparator(){
        super(AirportWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirportWritableComparable first = (AirportWritableComparable) a;
        AirportWritableComparable second = (AirportWritableComparable) b;
        return first.getAirportId().compareTo(second.getAirportId());
    }
}
