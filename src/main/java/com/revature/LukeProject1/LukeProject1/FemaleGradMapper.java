package com.revature.LukeProject1.LukeProject1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author luked
 *
 */
public class FemaleGradMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		/*
		 *  Represents the percentage of women who have completed High School, Bachelor's, Master's, 
		 *  and Doctorate, respectively
		 */
		String education;
		if (line.contains("SE.SEC.HIAT.UP.FE.ZS")) {
			education = "High School";
		}
		else if (line.contains("SE.TER.HIAT.BA.FE.ZS")) {
			education = "Bachelor's";
		}
		else if (line.contains("SE.TER.HIAT.MS.FE.ZS")) {
			education = "Master's";
		}
		else if (line.contains("SE.TER.HIAT.DO.FE.ZS")) {
			education = "Doctorate";
		}
		else
			return;
		
		String[] columns = line.split("\"[,][\"]");
		String country = columns[0].substring(1);
		
		/*
		 * Code to retrieve educational attainment percentage
		 */
		double gradRate = -1.0;
		// we are grabbing the grad rate values from five years (2011-2015)
		for (int i = 58; i > 53; i--) {
			if (columns[i].contains(".")) {
				gradRate = Double.parseDouble(columns[i]);
				break;
			}
		}
		// if there is no data from these years...
		if (gradRate < 0)
			return;
		gradRate = (Math.round(gradRate*100.0))/100.0;
		
		if (education.equals("High School")) {
			if (gradRate >= 30)
				return;
			context.write(new Text(country), new Text(education + ": " + gradRate));
		}
		else {
			context.write(new Text(country), new Text(education + ": " + gradRate));
		}
	}
}
