package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 file1.txt:
 1 2 A 4 5
6 B 8 9
10 11 C
13 14

file2.txt:
7 A 9 2 6
B 1 2 7
4 C 1
7 8
12


find out average column wise, here some column contain value as A,B,C this should be elimited 
So after elimination file looks like
 file1.txt:
1 2 4 5
6 8 9
10 11 
13 14

file2.txt:
7 9 2 6
1 2 7
4 1
7 8
12
 
 so avg : 
 col1 : (1+6+10+13+7+1+4+7+12)/9 = 61/9 = 6.77777
 col2 = (2+8+11+14+9+2+1+8)/8= 55/8=6.875
 col3 = (4+9+2+7)/4 = 22/4 =5.5
 col4 = (5+6)/2 = 11/2  = 5.5
 ouptut: 
col1	6.7777777
col2	6.875
col3	5.5
col4	5.5
 */

public class MyMapperColAvg extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		if (line.trim().isEmpty())
			return;

		String[] nums = line.split(" ");
		int colnum = 0;
		for (String num : nums) {

			colnum++;
			try {
				int i = Integer.parseInt(num);
				context.write(new Text("col" + colnum), new IntWritable(i));
			} catch (Exception e) {
				System.out.println("Unparseable  " + num);
				colnum--;
			}
		}

	}

}
