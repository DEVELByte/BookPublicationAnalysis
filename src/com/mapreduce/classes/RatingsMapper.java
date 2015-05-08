package com.mapreduce.classes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RatingsMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	private Text isbn;
	private Text rating;

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] rows = value.toString().split("\";\"");
		isbn = new Text(rows[1]);// second field is ISBN Number of the book
		rating = new Text("0000" + "\t"
				+ rows[2].substring(0, rows[2].length() - 1));// third field
																// is rating
																// of the
																// book
		output.collect(isbn, rating);
	}
}