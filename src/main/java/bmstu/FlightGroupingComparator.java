package bmstu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupingComparator extends WritableComparator {

    protected FlightGroupingComparator(){
        super(FlightWritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlightWritableComparable aa = (FlightWritableComparable) a;
        FlightWritableComparable bb = (FlightWritableComparable) b;
        return aa.delayCompare(bb);
    }
}
