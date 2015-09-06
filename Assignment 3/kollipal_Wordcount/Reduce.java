package kollipal_Wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.*;
import java.util.*;


public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>
{
		int max_sum = 0;
    	Text max_occured_key = new Text();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			   throws IOException, InterruptedException 
			   {
			            int sum = 0;
			            
			            for(IntWritable value: values){
			                sum+= value.get();
			            	}
			            if (sum > max_sum)
			 	       	{
			 	    	   max_sum = sum;
			 	    	   max_occured_key.set(key);
			 	       	}
			            //context.write(key, new IntWritable(sum));
			 	    	   
			   }
			            @Override
			            protected void cleanup(Context context)
			            throws IOException,InterruptedException
			 	       {
			 	    	   context.write(max_occured_key, new IntWritable(max_sum));
			 	       } 
	}
	
