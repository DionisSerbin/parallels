package bmstu;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightGroupingComparator extends WritableComparator {

    public FlightGroupingComparator(){
        super(FlightComparable.class);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlightComparable aa = (FlightComparable) a;
        FlightComparable bb = (FlightComparable) b;
        return super.compare(a, b);
    }
}
