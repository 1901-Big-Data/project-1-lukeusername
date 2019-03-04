package com.revature.LukeProject1.LukeProject1.TopAveInc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopAveIncMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

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
		
		double averageInc = 0.0;
		// we are grabbing increase in grad rate from 2013-2015
		if (columns[57].contains(".") && columns[58].contains(".") && columns[59].contains(".")) {
			averageInc = Double.parseDouble(columns[58]) - Double.parseDouble(columns[57]);
			averageInc += Double.parseDouble(columns[59]) - Double.parseDouble(columns[58]);
			averageInc = averageInc / 2.0;
		}
		else {
			if (education.equals("High School"))
				return;
			context.write(new Text(country), new Text(education + ": insufficient data"));
			return;
		}
		// we only want countries with high school grad rate on the rise
		if (education.equals("High School") && averageInc < 0.3) {
			return;
		}

		// formatting
		averageInc=Math.round((averageInc)*100000.0)/100000.0;
		
		context.write(new Text(country), new Text(education + ": " + averageInc));
	}
}
