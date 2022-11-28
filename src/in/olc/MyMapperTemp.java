package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MyMapperTemp extends Mapper<LongWritable,Text, Text, FloatWritable>{
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		if(line.trim().isEmpty()) return;
		
		String year=line.substring(15,19);
		float temp = Integer.parseInt(line.substring(87,92))/10.0f;
		System.out.println(year +" => " + temp);
		context.write(new Text(year),new FloatWritable(temp));
	}

}
