package edu.uta.project3.mapperreducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AveragerReducer extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> outputcollector, Reporter reporter)
			throws IOException {
		double totalsum = 0;
		int count = 0;
		while(values.hasNext()){
			String value = values.next().toString();
			double weight = Double.parseDouble(value);
			totalsum = totalsum + weight;
			count++;
		}
		if(count!=0){
			double avg = totalsum / count;
			outputcollector.collect(key, new DoubleWritable(avg));
		}
		else{
			outputcollector.collect(key, new DoubleWritable(0.0));
		}
	}


}
