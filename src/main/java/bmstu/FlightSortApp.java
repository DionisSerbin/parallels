package bmstu;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlightSortApp {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: FlightSortApp <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(FlightSortApp.class);
        job.setJobName("Flight sort");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setPartitionerClass(FlightPartitioner.class);
        job.setGroupingComparatorClass(FlightGroupingComparator.class);
        job.setMapOutputKeyClass(FlightWritableComparable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}