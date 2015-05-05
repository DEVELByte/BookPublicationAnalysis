package com.mapreduce.classes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookToYearMapper extends
Mapper<Object, Text, Text, IntWritable> {

}
