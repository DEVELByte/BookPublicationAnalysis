package com.mapreduce.classes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class YOPMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	private Text isbn;
	private Text yop;

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] rows = value.toString().split("\";\"");
		isbn = new Text(rows[0].substring(1));
		yop = new Text(rows[3].substring(0, rows[3].length()) + "\t" + "00");
		output.collect(isbn, yop);
	}
}