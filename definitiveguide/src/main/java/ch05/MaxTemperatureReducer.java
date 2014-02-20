package ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int max = Integer.MIN_VALUE;
		for(IntWritable value : values){
			max = Math.max(max, value.get());
		}
		
		context.write(key, new IntWritable(max));
	}

}
