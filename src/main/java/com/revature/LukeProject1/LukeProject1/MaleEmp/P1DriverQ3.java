package com.revature.LukeProject1.LukeProject1.MaleEmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.revature.LukeProject1.LukeProject1.AveInc.P1DriverQ2;

/**
 * OVERARCHING GOAL: "Identify special programs aimed at women across the globe."
 * 
 * Prompt: "List the % of change in male employment from the year 2000."
 *      
 *      Notable Assumptions:
 * 1. I answered this question for every country with available data
 * 2. If data from the year 2000 was not available for a particular country, I assumed that 
 *    1999 or 2001 would be acceptable
 * 3. This data includes up to 2016.  I assumed that if the 2016 data was not available for
 *    a particular country, that 2015 or 2014 would be acceptable
 * 
 * MAPPER:
 *      All relevant data for each individual country were self-contained within a single row.
 *      Therefore, a reducer was not necessary.  Within the Mapper, I extract and filter
 *      through the relevant information of each individual row.  I then took the male employment
 *      percentage from 2016 and subtracted the male employment percentage from 2000, rounding this
 *      number to two decimal places.  This number was then the value in my key:value pair output,
 *      country being the key.
 *      
 * REDUCER:
 * 		N/A
 * 
 * @author Luke Davis
 *
 */
public class P1DriverQ3 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: Project1Driver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());

		job.setJarByClass(P1DriverQ3.class);
		job.setJobName("Countries with females grad-rate < 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaleEmpMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new P1DriverQ3(), args);
		System.exit(exitCode);
	}
}