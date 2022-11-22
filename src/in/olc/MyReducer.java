package in.olc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * Mapper output i.e. 
 * apple 1 
 * ball 1
 * apple 1
 * ...
 * will be input to reducer 
 * 
 * Before reducer, framework will 
 * shuffle(get together all key value),   => apple 1    apple 1     ball 1      ball  1  apple 1
 * sort()  => apple 1        apple 1        apple 1             ball 1        ball 1
 *  group() => apple [1,1,1]    ball[1,1]
 *  
 *  so group output will be input to Reducer
 *  Reducer input =>
 *  apple [1,1,1]  => Text Intwritable
 *  
 *  output => 
 *  apple 3  => Text, Intwritable
 *  
 *  and output of reducer will be apple 3(sum[1,1,1])  ball 2(sum[1,1])
 *
 * @author hduser
 *
 */

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	/**
	 * By default only one reducer object is created. 
	 * To create more object we need to set job.setNumReduceTasks(<number of Reducer>)
	 */
	public MyReducer() {
		System.out.println("Reducer object created");
	}

	/**
	 * Number of time reduce method will be called: 
	 * reduce method will be called as number of groups.
	 * in our example as we have two two groups key apple and ball so reduce method is called two time.
	 * 
	 * reduce(-,-,-) : key apple context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@2bcd640b
	 * reduce(-,-,-) : key ball  context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@2bcd640b

     * 
     * So reduce method will be called depend on number of key
     * 
     * Number of context object = Number of Reducer object will be created
     * As we have only one reducer so all key will be go to one reducer
     * 
     * 
     * if we set 5 reducer than it will create 5 reducer object will be created and they call reduce method IF THEY HAVE DATA
     * Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
     * Reducer object created
	 * reduce(-,-,-) : key apple context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@171fa707
	 * Reducer object created
	 * reduce(-,-,-) : key ball context:org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer$Context@a807909
	 * Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
     * Reducer object created  --> DOES NOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATE, REDUCE METHOD IS NOT CALLED
	 *
	 * HERE AS WE CAN SEE WE HAVE ONLY TWO KEY SO TWO REDUCER GET DATA AND REDUCE METHOD IS CALLED 
	 * BUT 3 REDUCER DOESNOT HAVE ANY DATA SO ONLY REDUCER OBJECT IS CREATED BUT NO REDUCE METHOD IS CALLED
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> counts,
			Context context) throws IOException, InterruptedException {
	
		System.out.println("reduce(-,-,-) : key " + key.toString() + " context:" + context);
		int total=0;
		for(IntWritable count : counts) {
			total=total+count.get();
		}
		
		context.write(key, new IntWritable(total));
	}
	
}
