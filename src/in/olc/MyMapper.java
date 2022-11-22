package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * word1.txt
 * -----------------
 * apple ball apple
 * ball apple apple
 * ball
 * 
 * InputFormatter will take input file and create key value pair out of it and send to Mapper to execute
 * Key(offset)  value(line)
 * 0             apple ball apple             (length => apple(5)+ space(1) +ball(4)+space(1)+apple(5) + \n => 17 ) next offset will be 0 +17
 * 17            ball apple apple			(length => ball(4)+space(1) + apple(5)+ space(1) +apple(5) + \n => 17 ) next off set = 17 + 17 => 34
 * 34            ball 
 * 
 * Mapper:
 * input => offset , line => LongWriteable(long), Text(String)
 * output =>  apple, 1 => text, IntWriteable
 * @author hduser
 *
 */
public class MyMapper  extends Mapper<LongWritable,Text, Text,IntWritable>{
	
	
	/**
	 * As we have two input file and size of each file is less than 128MB default block size, 
	 * 1 block will be created for each file
	 * so two mapper will be created one for each block
	 * So this constructor is called two time.
	 * 
	 * IF we have one file of 200Mb than 
	 * b0 => 128 MB  b1=> 200-128=72MB blocks so here two mapper object will be created to process this file in parallel
	 */
	public MyMapper() {
		System.out.println("Mapper object created");
	}

	/**
	 * Number of time map() will be called:
	 * map() funtion will be called as number of line input data have 
	 * example if we have 20K records in the file than 20K time map() will be called
	 * 
	 * In word count example: 
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@1259a2a9
	 * key : 0  value : apple ball apple
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@1259a2a9
	 * key : 17  value : ball apple apple
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@1259a2a9
	 * key : 34  value : ball apple
	 * 
	 * word2.txt have 5 line so 5 time map() will be called
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * key : 0  value : apple
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * key : 6  value : ball
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * key : 11  value : apple
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * key : 17  value : ball
	 * Map(-,-,-) called  context:org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * key : 22  value : apple

	 * 
	 * Number of context object:
	 * Each mapper have it's own context object, Context object is not shared between mappers.
	 * number of context object == number of Mapper object created
	 * As we have two Mapper object, here we have two context object is created
	 * org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@1259a2a9
	 * org.apache.hadoop.mapreduce.lib.map.WrappedMapper$Context@2c1ee203
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("Map(-,-,-) called  context:"+ context);
		
		String line = value.toString();
		System.out.println("key : " + key + "  " + "value : " + line);
		
		String words[] = line.split(" ");
		for(String word:words) {
			// return type of Mapper is void, so anything need to send to next layer need to pass within Context object
			context.write(new Text(word), new IntWritable(1));
		}
		
	}

}
