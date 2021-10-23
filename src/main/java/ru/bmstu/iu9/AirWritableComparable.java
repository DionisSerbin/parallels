package ru.bmstu.iu9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirWritableComparable implements WritableComparable<AirWritableComparable> {

    private IntWritable airportId;
    private IntWritable index;

    protected IntWritable getAirportId(){
        return this.airportId;
    }

    protected IntWritable getIndex(){
        return this.index;
    }

    protected void setIndex(IntWritable a){
        this.index = a;
    }

    protected void setAirportId(IntWritable a){
        this.airportId = a;
    }


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
