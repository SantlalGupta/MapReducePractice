package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
data shuffling with bring data together and it is sorted order on key
 row1	7
row1	8
row1	9
row1	2
row1	1
row1	2
row1	3
row1	4
row1	1

row2	10
row2	5
row2	6
row2	7
row2	2

row3	11
row3	14
row3	17
row3	19
row3	8
row3	9
row3	10
row3	3

row4	30
row4	4

row5	45

group will be generated on sorting and shufle output and it is input to reducer 
row1	[7,8,9,2,1,2,3,4,1]

row2	[10,5,6,7,2]

row3	[11,14,17,19,8,9,10,3]

row4	[30,4]

row5	[45]

 */
public class MyReducerMax2 extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		int max=Integer.MIN_VALUE;
		for(IntWritable value : values) {
			if(value.get()>max) {
				max=value.get();
			}
		}
		context.write(new Text(key), new IntWritable(max));
	}

}
