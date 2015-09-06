package kollipal_Wordcount;

import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{
       private final static IntWritable one = new IntWritable(1);
       private Text word = new Text();
       public void map(LongWritable key, Text value, Context context)          
       throws IOException, InterruptedException 
       {
                  String line = value.toString();
                  StringTokenizer tokenizer = new StringTokenizer(line);
                  while (tokenizer.hasMoreTokens()) 
                  {
                              word.set(tokenizer.nextToken());
                              context.write(word, one);
                  }
       }
}