package com.mapreduce.classes;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

public class MainDriver extends Configured implements Tool {
	

	static int printUsage() {
		System.out
				.println("BookPubicationAnalysis [-m <maps>] [-r <reduces>] <RatingssDetailInput> <RatingsOutput> <BooksDetailsFile> <YOPOutput>");
		return 0;
	}

	@Override
	public int run(String[] args) throws IOException {
		return 0;
	}

	public static void main(String[] args) throws IOException {
		//first Job to map the Book, yop and ratings from ratings Input file
		JobConf conf = new JobConf(MainDriver.class);
		conf.setJobName("BookPubicationAnalysisJOB1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(RatingsMapper.class);
		conf.setReducerClass(RatingsReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		
		//second program to map books, yop and ratings from Books Input file.
		JobConf conf2 = new JobConf(MainDriver.class);
		conf2.setJobName("BookPubicationAnalysisJOB2");

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(YOPMapper.class);
		//conf2.setReducerClass(YOPReducer.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path(args[2]));
		FileOutputFormat.setOutputPath(conf2, new Path(args[3]));

		JobClient.runJob(conf2);
	}
}
