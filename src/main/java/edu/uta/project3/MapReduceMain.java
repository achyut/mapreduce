package edu.uta.project3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceMain {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 2.");
			System.exit(printUsage());
		}
		int res = ToolRunner.run(new Configuration(), new AverageWeightComputer(), args);
		System.exit(res);
	}
	
	static int printUsage() {
		System.out.println("averageweight <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}


}
