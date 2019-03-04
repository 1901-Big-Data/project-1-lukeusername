package com.revature.LukeProject1.LukeProject1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * OVERARCHING GOAL: "Identify special programs aimed at women across the globe."
 * 
 * Prompt: "Identify the countries where % of female graduates is less than 30%."
 *      
 *      Notable Assumptions:
 * 1. the prompt was referring high school graduate rates (although I included all higher 
 *    education) in the output
 * 2. the prompt was asking for recent data (within the past few years)
 * 
 *      Notes:
 *           1. I grabbed the most recent available year of data cutting off at 2011.  
 *           2. If the data was available, I took the median of those five years to add resistance to an
 *    	outlier or anomaly.
 * MAPPER:
 *      I extract and filter through the relevant information of each individual row, outputting Country:GradRate
 *      key:value pairs.  I include Bachelor's, Master's, and Doctorate degrees alongside High School graduation.
 *      The only data left out are when High School graduation is below 30% or if there is no data as recent as 2011
 * REDUCER:
 *      I combine the graduation data from each country.  If the High School graduation data does not exist, that entire
 *      country is not included in the output (meaning the rate was below 30%).
 * 
 * @author Luke Davis
 *
 */
public class Project1Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: Project1Driver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(Project1Driver.class);
		job.setJobName("Countries with females grad-rate < 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FemaleGradMapper.class);
		job.setReducerClass(FemaleGradReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Project1Driver(), args);
		System.exit(exitCode);
	}
}