package com.revature.LukeProject1.LukeProject1.MaleEmp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleEmpMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		// Represents the percentage of men employed in that particular country
		if (!line.contains("SL.EMP.TOTL.SP.MA.NE.ZS")) {
			return;
		}
		
		String[] columns = line.split("\"[,][\"]");
		String country = columns[0].substring(1);
		
		// year comparison will depend on available data
		int beginIndex = 0, endIndex = 0;
		
		// column for year 2000
		if (columns[44].contains(".")) {
			beginIndex = 44;
		}
		// column for year 1999
		else if (columns[43].contains(".")) {
			beginIndex = 43;
		}
		// column for year 2001
		else if (columns[45].contains(".")) {
			beginIndex = 45;
		}
		// else we have insufficient data so do not include in output
		else
			return;
		
		// column for year 2016
		if (columns[60].contains(".")) {
			endIndex = 60;
			columns[60] = columns[60].substring(0,columns[60].length()-4);
		}
		// column for year 2015
		else if (columns[59].contains(".")) {
			endIndex = 59;
		}
		// column for year 2014
		else if (columns[58].contains(".")) {
			endIndex = 58;
		}
		// else we have insufficient data so do not include in output
		else
			return;
		
		double change = Double.parseDouble(columns[endIndex])-Double.parseDouble(columns[beginIndex]);
		
		change = (Math.round(change*100.0))/100.0;
		
		context.write(new Text(country), new DoubleWritable(change));
	}
}
