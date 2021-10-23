package bmstu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupingComparator extends WritableComparator {

    protected FlightGroupingComparator(){
        super(AirWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AirWritableComparable aa = (AirWritableComparable) a;
        AirWritableComparable bb = (AirWritableComparable) b;
        return aa.delayCompare(bb);
    }
}
