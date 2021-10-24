package ru.bmstu.iu9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritableComparable implements WritableComparable<AirportWritableComparable> {

    private IntWritable airportId;
    private IntWritable index;

    protected IntWritable getAirportId(){
        return this.airportId;
    }

    protected IntWritable getIndex(){
        return this.index;
    }

    public AirportWritableComparable(IntWritable intWritable, IntWritable intWritable1) {
        this.airportId = intWritable;
        this.index = intWritable1;
    }

    public AirportWritableComparable(){
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
    public int compareTo(AirportWritableComparable o) {
        if(this.getAirportId().compareTo(o.getAirportId()) == 0){
            return this.getIndex().compareTo(o.getIndex());
        } else {
            return 0;
        }
    }
}
