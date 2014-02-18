/**
 * GenericOptionParser, Tool, and ToolRunner
 * 
 * GenericOptionParser is a class that interprets common Hadoop command line options and
 * sets them on a Configuration object for applications to use as desired. However
 * GenericOptionParser is not used directly. ToolRunner runs an application that implements
 * Tool interface and it uses GenericOptionParser internally.
 */
package ch05;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool{

	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		for(Entry<String, String> entry : conf){
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}
	

}
