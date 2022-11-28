package in.olc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriverTemp  {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      String input="hdfs://localhost:9000/user/hduser/temperature";
      String output="hdfs://localhost:9000/user/hduser/temperature_output";

      Path inputPath = new Path(input);
      Path outputPath = new Path(output);
	
      Configuration config= new Configuration();
      Job job = Job.getInstance(config);
      
      job.setMapperClass(MyMapperTemp.class);
      job.setReducerClass(MyReducerTemp.class);
      job.setJarByClass(MyDriverTemp.class);
      /*
       * as set ReduceTask as 0 it will execute only Mapper 
       * as we have 3 input file/block it will generate 3 mapper output file
       */
     // job.setNumReduceTasks(0);  
     
      // This is Mapper output key and value type
      // By default hadoop expect Mapper output key => LongWritable and value=> Text
      // But in our case ouput looks like apple 1 i.e. key => Text and value=> IntWritable
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      
      FileInputFormat.addInputPath(job,inputPath);
      FileOutputFormat.setOutputPath(job, outputPath); //.addOutputPath(job,outputPath);
      
      outputPath.getFileSystem(config).delete(outputPath, true);
      System.exit(job.waitForCompletion(true)?0:1);
      
      }

}
