package ch05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text year = new Text();
	private IntWritable temperature = new IntWritable();
	
	/**
	 * COUNTER FOR OVER 100 degrees
	 * @author ionia
	 *
	 */
	enum Temperature{
		OVER_100
	}
	
	private NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		parser.parse(value);
		if (parser.isValidTemperature()) {
			int airTemp = parser.getAirTemperature();
			if(airTemp > 1000){
				System.err.println("Temperature over 100 degrees for input: " + value);  
				context.setStatus("Detected possibly corrupt record: see logs."); // updating the map's status message.
				context.getCounter(Temperature.OVER_100).increment(1);
			}
			year.set(parser.getYear());
			temperature.set(parser.getAirTemperature());
			context.write(year, temperature);
		}
	}
}
