package com.mapreduce.classes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MainDriver {

	public static void main(String[] args) throws Exception {

		// Load the configuration files and
		// add them to the the conf object
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// The input and the output path have to sent to the main/driver program
		if (otherArgs.length != 2) {
			System.err.println("Usage: FrequencyCounter <in> <out>");
			System.exit(2);
		}

		// Create a job object and give the job a name
		// It allows the user to configure the job, submit it,
		// control its execution, and query the state.
		Job job = new Job(conf, "FrequencyCounter");

		// Specify the jar which contains the required classes
		// for the job to run.
		job.setJarByClass(MainDriver.class);

		job.setMapperClass(BookToYearMapper.class);
		job.setCombinerClass(YearWiseCountCalculator.class);
		job.setReducerClass(YearWiseCountCalculator.class);

		// Set the output key and the value class for the entire job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the Input (format and location) and similarly for the output also
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setNumReduceTasks(2);

		// Submit the job and wait for it to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
