package ru.bmstu.iu9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {

    private static final String FLOAT_REGEX = "^\\d+\\.\\d+$";

    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airportName = new Text(
                iter.next().toString()
        );
        ArrayList<String> delaysTime = getDelays(iter);

        if(delaysTime.size() > 0){
            context.write(
                    airportName,
                    makeMinMaxAverage(delaysTime)
            );
        }
    }

    protected ArrayList<String> getDelays(Iterator<Text> iter){
        ArrayList<String> delaysTime = new ArrayList<>();
        while (iter.hasNext()){
            String value = iter.next().toString();
            if (value.matches(FLOAT_REGEX)){
                delaysTime.add(value);
            }
        }
        return delaysTime;
    }

    protected Text makeMinMaxAverage(ArrayList<String> delayTime){
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
        return new Text("Average summ of delays = " + summ / delayTime.size()
                + ", minimal delay = " + min + ", maximal delay = " + max);
    }


}