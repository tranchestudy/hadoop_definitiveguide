/**
 * 
 */
package ch05;

import org.apache.hadoop.io.Text;

/**
 * @author ionia
 *
 */
public class NcdcRecordParser {
	
	private static final int MISSING_TEMPERATURE = 9999;
	
	private String year;
	private int airTemp;
	private String quality;
	
	public void parse(String record){
		year = record.substring(15, 19);
		String airTempStr = null;
		// Remove leading plus sign as parseInt doesn't like them
		if (record.charAt(87) == '+') {
			airTempStr = record.substring(88, 92);
		} else {
			airTempStr = record.substring(87, 92);
		}
		
		airTemp = Integer.parseInt(airTempStr);
		quality = record.substring(92, 93);
	}
	
	public void parse(Text record){
		parse(record.toString());
	}
	
	public boolean isValidTemperature() {
		return airTemp != MISSING_TEMPERATURE && quality.matches("[01459]");
	}
	
	public String getYear() {
		return this.year;
	}
	
	public int getAirTemperature() {
		return this.airTemp;
	}
}
