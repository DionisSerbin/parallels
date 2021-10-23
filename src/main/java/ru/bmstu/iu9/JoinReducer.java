package ru.bmstu.iu9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<AirWritableComparable, Text, Text, Text> {

    @Override
    protected void reduce(AirWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airName = new Text(iter.next().toString());
        ArrayList<String> delayTime = new ArrayList<>();
        if(delayTime.size() > 0){

        }
        // add delays
        while (iter.hasNext()){
            if (iter.next().toString().matches("^\\d+\\.\\d+$")){
                String added = iter.next().toString();
                delayTime.add(added);
            }
        }
        //calculate delays
        float min = Float.MAX_VALUE;
        float max = -1;
        float summ = 0;
        int i;
        for (i = 0; i < delayTime.size(); i++){
            float delayTimeNow = Float.parseFloat(delayTime.get(i));
            summ += delayTimeNow;
            if(delayTimeNow < min){
                min = delayTimeNow;
            }
            if(delayTimeNow > max){
                max = delayTimeNow;
            }
        }
        context.write(airName, new Text("Average summ of delays = " + summ / i
                + ", minimal delay = " + min + ", maximal delay" + max));

    }

    protected ArrayList<String> makeDelay(Iterator<Text> iter){
        ArrayList<String> delayTime;
        while (iter.hasNext()){
            if (iter.next().toString().matches("^\\d+\\.\\d+$")){
                String added = iter.next().toString();
                delayTime.add(added);
            }
        }
    }


}