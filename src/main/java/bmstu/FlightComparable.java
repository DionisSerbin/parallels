package bmstu;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightComparable implements WritableComparable {

    int airportId;
    double delayTime;
    double airTime;
    boolean cancelled;

    public FlightComparable(int airportId, double delayTime, double airTime, boolean cancelled){
        this.airportId = airportId;
        this.delayTime = delayTime;
        this.airTime = airTime;
        this.cancelled = cancelled;
    }




    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(key1);
        d.writeInt(key2);
        d.writeInt(key3);

    }

    @Override
    public void readFields(DataInput di) throws IOException {
        key1 = di.readUTF();
        key2 = di.readInt();
        key3 = di.readInt();
    }


    @Override
    public int compareTo(FlightComparable t) {


    }

}
