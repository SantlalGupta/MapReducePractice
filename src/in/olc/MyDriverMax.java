package in.olc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriverMax {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      String input="hdfs://localhost:9000/user/hduser/MaxRowWise";
      String output="hdfs://localhost:9000/user/hduser/MaxRowWise_output";

      Path inputPath = new Path(input);
      Path outputPath = new Path(output);
	
      Configuration config= new Configuration();
      Job job = Job.getInstance(config);
      
      job.setMapperClass(MyMapperMax.class);
      job.setReducerClass(MyReducerMax.class);
      job.setJarByClass(MyDriverMax.class);
     
      // This is Mapper output key and value type
      // By default hadoop expect Mapper output key => LongWritable and value=> Text
      // But in our case ouput looks like apple 1 i.e. key => Text and value=> IntWritable
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);
      
      FileInputFormat.addInputPath(job,inputPath);
      FileOutputFormat.setOutputPath(job, outputPath); //.addOutputPath(job,outputPath);
      
      outputPath.getFileSystem(config).delete(outputPath, true);
      System.exit(job.waitForCompletion(true)?0:1);
      
      }

}
