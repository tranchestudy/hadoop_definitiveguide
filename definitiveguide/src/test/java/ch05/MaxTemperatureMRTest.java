/**
 * Unit Test for MapReduce using mockito
 */
package ch05;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author ionia
 *
 */
public class MaxTemperatureMRTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void processValidRecord() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"
				                       //Year ^^^^
				+ "99999V0203201N00261220001CN9999999N9-00111+99999999999");
		                               //Temperature   ^^^^^
		@SuppressWarnings("unchecked")
		MaxTemperatureMapper.Context context = mock(MaxTemperatureMapper.Context.class);
		
		mapper.map(null, value, context);
		
		verify(context).write(new Text("1950"), new IntWritable(-11)); // verifying that Context.write method was called. 
	}

	@Test
	public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382"
           				               //Year ^^^^
				+ "99999V0203201N00261220001CN9999999N9+99991+99999999999");
				                       //Temperature   ^^^^^

		@SuppressWarnings("unchecked")
		MaxTemperatureMapper.Context context = mock(MaxTemperatureMapper.Context.class);
		
		mapper.map(null, value, context);
		
		verify(context, never()).write(any(Text.class), any(IntWritable.class)); // verifying that Context.write was never called.
	}
	
	/**
	 * MaxTemperatureReducer Test
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void returnsMaximumIntegerInValues() 
			throws IOException, InterruptedException {
		MaxTemperatureReducer reducer = new MaxTemperatureReducer();
		
		Text key = new Text("1950");
		List<IntWritable> values = Arrays.asList(
				new IntWritable(10), new IntWritable(5));
		
		@SuppressWarnings("unchecked")
		MaxTemperatureReducer.Context context = 
				mock(MaxTemperatureReducer.Context.class);
		
		reducer.reduce(key, values, context);
		
		verify(context).write(key, new IntWritable(10));
	}
}
