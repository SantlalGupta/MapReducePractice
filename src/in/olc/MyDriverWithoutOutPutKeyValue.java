package in.olc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Here we want to count number of words availabe in all input file 
 * i.e. total count:13
 * @author hduser
 *
 */
public class MyDriverWithoutOutPutKeyValue  {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String input="/home/hduser/Desktop/wordcount_input";
	      String output="/home/hduser/Desktop/wordcount_output";

	      Path inputPath = new Path(input);
	      Path outputPath = new Path(output);
		
	      Configuration config= new Configuration();
	      Job job = Job.getInstance(config);
	      
	      job.setMapperClass(MyMapperDefaultOutputKeyValueClass.class);
	      job.setReducerClass(MyReducerDefaultInputClassFromMap.class);
	      job.setJarByClass(MyDriverWithoutOutPutKeyValue.class);
	      /** by default 1 reducer so only one output will be created 
	        * When we are increasing number of reducer to 2 than 2 output file will be created. 
	        * ! reducer will get one key data
	        * 2nd reducer will get other key.  It will be never happened that one reducer get parts of key and other parts will go to next reducer. 
	        * 
	        * if we set 5 reducer than it will create 5 reducer object will be created and they call reduce method
	        * Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
	        * Reducer object created
			* reduce(-,-,-) : key apple context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@171fa707
			* Reducer object created
			* reduce(-,-,-) : key ball context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@a807909
			* Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
	        * Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
			*
	        */
	      job.setNumReduceTasks(1);
	      
	      // This is Mapper output key and value type
	      // By default hadoop expect Mapper output key => LongWritable and value=> Text
	      // But in our case ouput looks like apple 1 i.e. key => Text and value=> IntWritable
	      
	      //This is not required as we have handled in Mapper class by providing same Input and output key value type
	      //job.setOutputKeyClass(Text.class);
	      //job.setOutputValueClass(IntWritable.class);
	      
	      FileInputFormat.addInputPath(job,inputPath);
	      FileOutputFormat.setOutputPath(job, outputPath); //.addOutputPath(job,outputPath);
	      
	      outputPath.getFileSystem(config).delete(outputPath, true);
	      System.exit(job.waitForCompletion(true)?0:1);

	}

}
