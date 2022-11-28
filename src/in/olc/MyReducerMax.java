package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducerMax extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		
		Long max=null;
		for(LongWritable value : values) {
			if(max==null || value.get()>max) {
				max=value.get();
			}
		}
		context.write(new Text(key), new LongWritable(max));
	}

}
