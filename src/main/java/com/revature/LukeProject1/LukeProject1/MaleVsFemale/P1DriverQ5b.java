package com.revature.LukeProject1.LukeProject1.MaleVsFemale;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * OVERARCHING GOAL: "Identify special programs aimed at women across the globe."
 * 
 * Idea: Identify the top male vs female disparities in Bachelor's degree attainment (by country).  My hope is that this 
 *      information might aide a relevant organization in understanding potential causes of various gender disparities.
 *      
 * MAPPER:
 *      Extract the most recent available Bachelor's degree information for both male and females in every country.
 * REDUCER:
 *      I take the difference between the male and female data, outputting anything above 5% difference
 * 
 * @author Luke Davis
 *
 */
public class P1DriverQ5b {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: Project1Driver <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(P1DriverQ5b.class);
		job.setJobName("Highest male vs female disparity in Bachelor's degree attainment by country.");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaleVsFemaleMapper.class);
		job.setReducerClass(MaleVsFemaleReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}