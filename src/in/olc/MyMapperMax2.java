package in.olc; 

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
 file1.txt
 1 2 3 4
5 6 7
8 9 10


file2.txt
7 8 9 2
10
11 14 17 19
30


file3.txt
1
2
3
4
45
 */
public class MyMapperMax2 extends Mapper<LongWritable,Text, Text, IntWritable>{
	
	int rownum=0;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		if(line.trim().isEmpty()) return;
		
		rownum++;
		String [] nums= line.split(" ");
				
		for (String num:nums) {
			context.write(new Text("row"+rownum), new IntWritable(Integer.parseInt(num)));	
		}
			
		
		
	}

}
