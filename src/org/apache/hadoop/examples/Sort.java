package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
   
public class Sort {
    public Sort() {}  
    
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
    	private IntWritable out_key = new IntWritable();
    	private static final IntWritable out_value = new IntWritable(0);
    	
        public void map(Object key, Text value,
        		Mapper<Object, Text, IntWritable, IntWritable>.Context context)
        		throws IOException, InterruptedException{
        	
        	String[] lines = value.toString().split("\n");
            for(String num: lines){
            	out_key.set(Integer.parseInt(num));
                context.write(out_key, out_value);
            }
        }
    }
    
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
    	private static int count = 0;
    	private IntWritable out_key = new IntWritable(count);
    	
        public void reduce(IntWritable key, Iterable<IntWritable> values,
        		Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
        		throws IOException, InterruptedException{
        	
        	for(Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext(); iterator.next()){
        		out_key.set(++count);
        		context.write(out_key, key);
        	}
        }
    }

    public static void main(String[] args) throws Exception {
        
    	String[] inputs = {"hdfs://localhost:9000/user/hadoop/sort/"};
    	String output = "hdfs://localhost:9000/user/hadoop/output_sort";
    	
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort.Map.class);
        job.setReducerClass(Sort.Reduce.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
   
        for(int i = 0; i < inputs.length; i++)
            FileInputFormat.addInputPath(job, new Path(inputs[i]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }  
}