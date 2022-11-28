package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducerTemp extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Context context) throws IOException, InterruptedException {
		
		float max=Float.MIN_VALUE;
		for(FloatWritable value : values) {
			if(value.get()>max) {
				max=value.get();
			}
		}
		context.write(new Text(key), new FloatWritable(max));
	}

}
