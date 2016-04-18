package edu.uta.project3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;

import edu.uta.project3.mapperreducer.AveragerReducer;
import edu.uta.project3.mapperreducer.StateSeperatorMapper;

public class AverageWeightComputer extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), AverageWeightComputer.class);
		conf.setJobName("averageweight");
		conf.setJar("averageweight.jar");
		conf.set("mapred.textoutputformat.separator",",");
		
		FileInputFormat.setInputPaths(conf, args[0]);
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		JobConf yearMapper = new JobConf(false);
		
		ChainMapper.addMapper(conf,StateSeperatorMapper.class, LongWritable.class, Text.class, Text.class, Text.class,
				true, yearMapper);
		
		JobConf reduceConf = new JobConf(false);
		ChainReducer.setReducer(conf, AveragerReducer.class, Text.class, Text.class, Text.class, DoubleWritable.class, true,
				reduceConf);

		JobClient.runJob(conf);
		
		return 0;
	}

	

}
