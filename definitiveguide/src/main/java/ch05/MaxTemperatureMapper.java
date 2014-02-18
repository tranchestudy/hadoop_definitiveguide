package ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text year = new Text();
	private IntWritable temperature = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String strYear = line.substring(15, 19);
		String strTemperature = line.substring(87, 92);
		if(false == isMissing(strTemperature)){
			int airTemperature = Integer.parseInt(strTemperature);
			year.set(strYear);
			temperature.set(airTemperature);
			context.write( year, temperature );
		}
	}

	private boolean isMissing(String strTemperature) {
		return "+9999".equals(strTemperature);
	}
}
