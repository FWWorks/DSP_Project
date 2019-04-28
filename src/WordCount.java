import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class WordCount extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    //our own partitioner
    //WordCountPartitioner wcp = new WordCountPartitioner();
    MyPartitioner.estimate("hdfs://localhost:9000/user/cc/input/test.txt");
    System.out.println(MyPartitioner.key_partition_map);
    //Partitioner.getPartition(null, null, 1);

    Configuration conf = getConf();
    Job job = new Job(conf, this.getClass().toString());
    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setJobName("WordCount");
    job.setJarByClass(WordCount.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //
    job.setNumReduceTasks(2);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
//    job.setCombinerClass(Reduce.class);
//    job.setReducerClass(Reduce.class);
    //
    //job.setPartitionerClass(MyPartitioner.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    Mapper.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

  public static class MyPartitioner extends BucketPartitioner{
	private int a;
	//@Override
	/*
	public int getPartition(Text key, IntWritable value, int numReduceTasks){
        	System.out.println("$$$$$$$");
		System.out.println(numReduceTasks);
		if(numReduceTasks==0)
            		return 0;
        	if(key.equals(new Text("Cricket")) && !value.equals(new Text("India")))
            		return 0;
        	if(key.equals(new Text("Cricket")) && value.equals(new Text("India")))
            		return 1;
        	else
            		return 2;
        
		
	}
	*/
        	}

	
	
  

}

