package com.revature.LukeProject1.LukeProject1.AveInc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.LukeProject1.LukeProject1.FemaleGradReducer;

/**
 * OVERARCHING GOAL: "Identify special programs aimed at women across the globe."
 * 
 * Prompt: "List the average increase in female education in the U.S. from the year 2000."
 * My Interpretation: "Take the average of the year-over-year differences in High School and Bachelor's 
 * education in the U.S. from the year 2000."
 * 
 *      Notable Assumptions:
 * 1. the prompt was referring to High School Diplomas and Bachelor's degrees
 * 
 * APPROACH:
 *      Notes: 
 *           1. All of the data asked for in the prompt is contained within one row.  Considering the nature of MapReduce and
 *      processing power necessary to run a MapReduce job, I elected to include information to help put the requested U.S.
 *      data into context.  I included countries whose High School graduation rates were similar to the U.S. from 2004-2006.
 *           2. I included anomaly detection after I noticed that the U.S. data changed dramatically over a single year.  If
 *      a year-over-year change is observed to be greater than 5%, that anomaly is noted in the output. 
 * MAPPER:
 *      I extract and filter through the relevant information of each individual row, outputting Country:GradRate
 *      key:value pairs.  I include Bachelor's degrees alongside High School graduation.
 * REDUCER:
 *      I combine the graduation data from each individual country.  If the High School graduation data is not similar enough to the
 *      U.S. data, it is not included in the output.
 * 
 * @author Luke Davis
 *
 */
public class P1DriverQ2 {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: Project1Driver <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(P1DriverQ2.class);
		job.setJobName("Average increase in female education in the U.S. from the year 2000.");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FemaleAveIncreaseMapper.class);
		job.setReducerClass(FemaleAveIncreaseReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}