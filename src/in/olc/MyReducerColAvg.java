package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducerColAvg extends Reducer<Text, IntWritable, Text, FloatWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		int tElements=0;
		for(IntWritable iw:values) {
			sum=sum+iw.get();
			tElements++;
		}
		
		context.write(new Text(key), new FloatWritable(sum/Float.parseFloat(tElements+"")));
	}

}
