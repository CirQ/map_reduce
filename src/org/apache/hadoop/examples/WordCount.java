package org.apache.hadoop.examples;

import java.io.IOException;  
import java.util.Iterator;  
import java.util.StringTokenizer;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
   
public class WordCount {
    public WordCount() {}  
   
    public static void main(String[] args) throws Exception {
        
    	String[] inputs = {"hdfs://localhost:9000/user/hadoop/input/"};
    	String output = "hdfs://localhost:9000/user/hadoop/output_WordCount";
    	
    	Configuration conf = new Configuration();   
        Job job = Job.getInstance(conf, "word count");  
        job.setJarByClass(WordCount.class);  
        job.setMapperClass(WordCount.TokenizerMapper.class);  
        job.setCombinerClass(WordCount.IntSumReducer.class);  
        job.setReducerClass(WordCount.IntSumReducer.class);  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);  
   
        for(int i = 0; i < inputs.length; i++)
            FileInputFormat.addInputPath(job, new Path(inputs[i]));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }  
   
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{  

        public void reduce(Text key, Iterable<IntWritable> values,
        		Reducer<Text, IntWritable, Text, IntWritable>.Context context)
        		throws IOException, InterruptedException{
        	
            int sum = 0;
            IntWritable value;
            for(Iterator<IntWritable> i = values.iterator(); i.hasNext(); sum += value.get())
                value = i.next();
            context.write(key, new IntWritable(sum));
        }
    }
   
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value,
        		Mapper<Object, Text, Text, IntWritable>.Context context)
        		throws IOException, InterruptedException{
        	
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while(tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }
}