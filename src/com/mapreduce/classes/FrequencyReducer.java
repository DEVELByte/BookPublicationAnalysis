package com.mapreduce.classes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends
Reducer<Text, IntWritable, Text, IntWritable>{

}
