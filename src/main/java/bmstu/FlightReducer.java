package bmstu;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FlightReducer extends Reducer<FlightComparable, Text, String, Text> {

}