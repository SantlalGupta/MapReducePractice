package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Here we want to count number of words availabe in all input file 
 * i.e. total count:13
 * @author hduser
 *
 */

public class MyMapperDefaultOutputKeyValueClass extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		String words[] = line.split(" ");
		for(String word:words) {
			context.write(new LongWritable(1), new Text(word));
		}	
	}
}
