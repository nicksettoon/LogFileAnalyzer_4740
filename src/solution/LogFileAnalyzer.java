/**
 * 
 */
package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * @author Nicklaus Settoon
 *
 */
public class LogFileAnalyzer {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 2)
		{// if command-line arguments is not exactly 2, terminate job
			System.out.printf(
					"Usage: LogFileAnalyzer <input dir> <output dir>\n");
	      System.exit(-1);
	    }
		
		// job setup
		Configuration config = new Configuration(); // new config object
		Job job_ = Job.getInstance(config, "Log File Analyzer"); // get job instance
		job_.setJarByClass(LogFileAnalyzer.class); // set the class for the job to run
		
		// file input and output formats are normal so don't need anything complicated here
		// just specifiy paths
		FileInputFormat.setInputPaths(job_, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_, new Path(args[1]));

		// set the mapper and reducer classes
		job_.setMapperClass(LogMapper.class);
		job_.setReducerClass(IP_DateReducer.class);

		// mapper and reducer outputs are same so this isn't needed
//		job_.setMapOutputKeyClass(Text.class);
//		job_.setMapOutputValueClass(Text.class);

		//set output key/value classes
		job_.setOutputKeyClass(Text.class);
		job_.setOutputValueClass(Text.class);

		// start job_ and wait for its completion
		boolean success = job_.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}