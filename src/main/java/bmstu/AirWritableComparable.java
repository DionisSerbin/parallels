package bmstu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirWritableComparable implements WritableComparable<AirWritableComparable> {

    IntWritable airportId;
    IntWritable index;


    public AirWritableComparable(IntWritable intWritable, IntWritable intWritable1) {
        this.airportId = intWritable;
        this.index = intWritable1;
    }

    public AirWritableComparable(){
        airportId = new IntWritable(0);
        index = new IntWritable(0);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        airportId.write(d);
        index.write(d);
    }

    @Override
    public void readFields(DataInput d) throws IOException {
        airportId.readFields(d);
        index.readFields(d);
    }


//    @Override
//    public int hashCode() {
//        return this.toString().hashCode();
//    }



    @Override
    public int compareTo(AirWritableComparable o) {
        if(this.airportId.compareTo(o.airportId) == 0){
            return this.index.compareTo(o.index);
        } else {
            return 0;
        }
    }
}
