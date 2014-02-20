package ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text year = new Text();
	private IntWritable temperature = new IntWritable();
	
	private NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		parser.parse(value);
		if (parser.isValidTemperature()) {
			year.set(parser.getYear());
			temperature.set(parser.getAirTemperature());
			context.write(year, temperature);
		}
	}
}
