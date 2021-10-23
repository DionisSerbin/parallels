package ru.bmstu.iu9;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JoinApp {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: JoinApp <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(JoinApp.class);
        job.setJobName("Make join");

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, FlightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, AirMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(FlightPartitioner.class);
        job.setGroupingComparatorClass(FlightGroupingComparator.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(AirWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}