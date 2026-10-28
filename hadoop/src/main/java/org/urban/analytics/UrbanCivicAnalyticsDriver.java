package org.urban.analytics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * UrbanCivicAnalyticsDriver configures and executes the Hadoop MapReduce job
 * for NYC 311 civic service resolution and operational intelligence.
 */
public class UrbanCivicAnalyticsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: UrbanCivicAnalyticsDriver <input_path> <output_path> [num_reducers]");
            return -1;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int numReducers = args.length >= 3 ? Integer.parseInt(args[2]) : 1;

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Urban Civic Complaint Analytics - NYC 311 Resolution Engine");

        job.setJarByClass(UrbanCivicAnalyticsDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(UrbanCivicMapper.class);
        job.setCombinerClass(UrbanCivicCombiner.class);
        job.setReducerClass(UrbanCivicReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MetricWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(numReducers);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.out.println("==================================================================");
        System.out.println("STARTING HADOOP MAPREDUCE: Urban Civic Complaint Analytics");
        System.out.println("Input HDFS Path  : " + inputPath);
        System.out.println("Output HDFS Path : " + outputPath);
        System.out.println("Reducer Tasks    : " + numReducers);
        System.out.println("==================================================================");

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long elapsed = System.currentTimeMillis() - startTime;

        System.out.println("==================================================================");
        System.out.println("MAPREDUCE JOB STATUS: " + (success ? "SUCCESS" : "FAILED"));
        System.out.println("Job ID             : " + job.getJobID());
        System.out.println("Execution Time     : " + (elapsed / 1000.0) + " seconds");
        System.out.println("==================================================================");

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new UrbanCivicAnalyticsDriver(), args);
        System.exit(exitCode);
    }
}
