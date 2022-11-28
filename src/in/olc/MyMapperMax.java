package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapperMax extends Mapper<LongWritable,Text, Text, LongWritable>{
	String okey = "row";
	int rownum=0;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String [] nums= value.toString().split(" ");
		Long max=null;
		rownum +=1 ; 
		String rkey=okey + rownum;
		
		for (String num:nums) {
			Long n = Long.parseLong(num);
			if(max==null || n>max) {
				max=n;
			}
		}
			
		context.write(new Text(rkey), new LongWritable(max));
		
	}

}
