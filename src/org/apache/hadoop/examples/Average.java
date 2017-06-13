package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   
public class Average {
    public Average() {}  
    
    public static class Map extends Mapper<Object, Text, Text, FloatWritable>{
    	private Text out_key = new Text();
    	private FloatWritable out_value = new FloatWritable();
    	
        public void map(Object key, Text value,
        		Mapper<Object, Text, Text, FloatWritable>.Context context)
        		throws IOException, InterruptedException{
        	
        	BufferedReader reader = new BufferedReader(new StringReader(value.toString()));
        	String line = "";
            while((line = reader.readLine()) != null){
            	String[] pair = line.split(" ");
            	out_key.set(pair[0]);
            	out_value.set(Float.parseFloat(pair[1]));
                context.write(out_key, out_value);
            }
        }
    }
    
    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    	private FloatWritable out_value = new FloatWritable();
    	
        public void reduce(Text key, Iterable<FloatWritable> values,
        		Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
        		throws IOException, InterruptedException{
        	
        	float sum = 0.0f;
        	int count = 0;
        	for(FloatWritable value: values){
        		sum += value.get();
        		count++;
        	}
        	out_value.set(sum / count);
        	context.write(key, out_value);
        }
    }

    public static void main(String[] args) throws Exception {
        
    	String[] inputs = {"hdfs://localhost:9000/user/hadoop/average/"};
    	String output = "hdfs://localhost:9000/user/hadoop/output_average";
    	
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average");
        job.setJarByClass(Average.class);
        job.setMapperClass(Average.Map.class);
        job.setReducerClass(Average.Reduce.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        for(int i = 0; i < inputs.length; i++)
            FileInputFormat.addInputPath(job, new Path(inputs[i]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }  
}