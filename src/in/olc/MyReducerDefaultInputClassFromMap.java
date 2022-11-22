package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducerDefaultInputClassFromMap extends Reducer<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> allValue,
			Context context) throws IOException, InterruptedException {
		int total=0;
		for(Text text:allValue) {
			total += 1;
		}
		context.write(new Text("total"), new IntWritable(total));
	}

}
