package com.revature.LukeProject1.LukeProject1.AdoleFertility;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * OVERARCHING GOAL: "Identify special programs aimed at women across the globe."
 * 
 * Self-Prompt: "List the % of change in female employment from the year 2000."
 *      
 *      Notable Assumptions:
 * 1. I answered this question for every country with available data
 * 2. If data from the year 2000 was not available for a particular country, I assumed that 
 *    1999 or 2001 would be acceptable
 * 3. This data includes up to 2016.  I assumed that if the 2016 data was not available for
 *    a particular country, that 2015 or 2014 would be acceptable
 * 
 * APPROACH:
 *      All relevant data for each individual country were self-contained within a single row.
 *      Therefore, a reducer was not necessary.  Within the Mapper, I extract and filter
 *      through the relevant information of each individual row.  I then took the female employment
 *      percentage from 2016 and subtracted the female employment percentage from 2000, rounding this
 *      number to two decimal places.  This number was then the value in my key:value pair output,
 *      country being the key.
 * 
 * @author Luke Davis
 *
 */
public class P1DriverQ5 {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: Project1Driver <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(P1DriverQ5.class);
		job.setJobName("Countries with females grad-rate < 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AdoleFertilityMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}