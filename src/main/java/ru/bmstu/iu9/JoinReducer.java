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

        if(!delaysTime.isEmpty()){
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

    protected Text makeMinMaxAverage(ArrayList<String> delaysTime){
        float min = Float.MAX_VALUE;
        float max = 0;
        float sum = 0;

        for (int i = 0; i < delaysTime.size(); i++){
            float delayTimeNow = Float.parseFloat(
                    delaysTime.get(i)
            );
            if(delayTimeNow < min){
                min = delayTimeNow;
            }
            if(delayTimeNow > max){
                max = delayTimeNow;
            }
            sum += delayTimeNow;
        }
        return new Text(
                "Average sum of delays = " + sum / delaysTime.size()
                + ", minimal delay = " + min
                        + ", maximal delay = " + max
        );
    }


}