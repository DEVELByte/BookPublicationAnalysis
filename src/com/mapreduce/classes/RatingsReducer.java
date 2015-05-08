package com.mapreduce.classes;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RatingsReducer extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {
@Override
public void reduce(Text key, Iterator<Text> values,
	OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException {

Text redVal = new Text();
int sum = 0;
int count = 0;
int avgRatings = 0;
String yop = null;
String[] tempVal = null;

while (values.hasNext()) {
	tempVal = (values.next().toString()).split("\t");
	sum = sum + Integer.parseInt(tempVal[1]);
	count++; 
}

avgRatings = Math.round(sum / count);
yop = tempVal[0];
redVal.set(yop + "\t" + avgRatings);
System.out.println("-----------> Reducer Values : " + redVal);
output.collect(key, redVal);
}
}
