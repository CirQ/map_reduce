package org.apache.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
   
public class Deduplication {
    public Deduplication() {}  
    
    public static class Map extends Mapper<Object, Text, Text, NullWritable>{
        public void map(Object key, Text value,
        		Mapper<Object, Text, Text, NullWritable>.Context context)
        		throws IOException, InterruptedException{
        	
        	String[] lines = value.toString().split("\n");
            for(String line: lines)
                context.write(new Text(line), NullWritable.get());
        }
    }
    
    public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable>{  
        public void reduce(Text key, Iterable<NullWritable> values,
        		Reducer<Text, NullWritable, Text, NullWritable>.Context context)
        		throws IOException, InterruptedException{
        	
            context.write(key, NullWritable.get());
        }
    }
   
    public static void main(String[] args) throws Exception {
        
    	String[] inputs = {"hdfs://localhost:9000/user/hadoop/deduplication/"};
    	String output = "hdfs://localhost:9000/user/hadoop/output_deduplication";
    	
    	Configuration conf = new Configuration();   
        Job job = Job.getInstance(conf, "deduplication");  
        job.setJarByClass(Deduplication.class);  
        job.setMapperClass(Deduplication.Map.class);  
        job.setCombinerClass(Deduplication.Reduce.class);
        job.setReducerClass(Deduplication.Reduce.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(NullWritable.class);  
   
        for(int i = 0; i < inputs.length; i++)
            FileInputFormat.addInputPath(job, new Path(inputs[i]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }  
}