package edu.uta.project3.mapperreducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StateSeperatorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text outputkey = new Text();
	private Text outputvalue = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputcollector, Reporter reporter) throws IOException {
		
		String line = value.toString();
		String[] linearr = line.split(",");
		String state = linearr[5];
		state = getStateNameFromCode(state);
		if(state!=null){
			String year = linearr[0].trim();
			year = year.substring(0,4);
			
			String weight = linearr[7];
			String gender = linearr[69];
			gender = getGenderFromCode(gender);
			
			String keyout = year+","+state+","+gender;
			outputkey.set(keyout);
			
			outputvalue.set(weight);
			outputcollector.collect(outputkey,outputvalue);
		}
	}
	
	private static String getGenderFromCode(String gender){
		if(gender.equalsIgnoreCase("1")){
			return "M";
		}
		else{
			return "F";
		}
	}
	private static String getStateNameFromCode(String code){
		if(code.equalsIgnoreCase("06")){
			return "CA";
		}
		else if(code.equalsIgnoreCase("08")){
			return "CO";
		}
		else if(code.equalsIgnoreCase("01")){
			return "WY";
		}
		else if(code.equalsIgnoreCase("12")){
			return "FL";
		}
		else if(code.equalsIgnoreCase("23")){
			return "ME";
		}
		else if(code.equalsIgnoreCase("36")){
			return "NY";
		}
		else if(code.equalsIgnoreCase("38")){
			return "ND";
		}
		else if(code.equalsIgnoreCase("48")){
			return "TX";
		}
		else{
			return null;
		}
	}

}
