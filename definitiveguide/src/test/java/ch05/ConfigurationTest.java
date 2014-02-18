/**
 * 
 */
package ch05;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author ionia
 *
 */
public class ConfigurationTest {

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

	/**
	 * Configurations read properties from resources--XML files.
	 * Each property is named by a String and the type of a value may be one 
	 * of several types, boolean, int, float, String, Class, java.io.File, collections of Strings. 
	 */
	@Test
	public void testHadoopConfiguration_configuration1() {
		Configuration conf = new Configuration();
		conf.addResource("ch05/configuration-1.xml");
		
		assertThat(conf.get("color"), is("yellow"));
		assertThat(conf.getInt("size", 0), is(10));
		assertThat(conf.get("breadth", "wide"), is("wide"));
		assertThat(conf.get("size-weight"), is("10,heavy"));
	}
	
	/**
	 * Properties defined in resources that are added later override the earlier definitions.
	 * Property size is overridden.
	 * 
	 * Properties that are marked as final cannot be overridden in later definitions.
	 * Property weight is final and is not overridden.
	 * 
	 * System properties take priority over properties defined in resource files.
	 * This feature is used as JVM arguments -Dproperty=value. 
	 */
	@Test
	public void testHadoopConfiguration_combining_resources(){
		Configuration conf = new Configuration();
		conf.addResource("ch05/configuration-1.xml");
		conf.addResource("ch05/configuration-2.xml");
		
		assertThat(conf.getInt("size", 0), is(12));
		assertThat(conf.get("weight"), is("heavy"));
		assertThat(conf.get("size-weight"), is("12,heavy"));
		
		System.setProperty("size", "14");
		assertThat(conf.get("size-weight"), is("14,heavy"));
		
		System.setProperty("length", "2"); // undefined properties is not valid in Configuration.
		assertThat(conf.get("length"), nullValue());
	}

}
