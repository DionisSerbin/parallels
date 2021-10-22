package bmstu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupingComparator extends WritableComparator {

    protected FlightGroupingComparator(){
        super(FlightComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlightComparable aa = (FlightComparable) a;
        FlightComparable bb = (FlightComparable) b;
        return aa.delayCompare(bb);
    }
}
