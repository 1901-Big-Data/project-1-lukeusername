package com.revature.LukeProject1.LukeProject1.MaleVsFemale;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaleVsFemaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String gender;
		if (line.contains("SE.TER.HIAT.BA.FE.ZS")) {
			gender = "Female";
		}
		else if (line.contains("SE.TER.HIAT.BA.MA.ZS")) {
			gender = "Male";
		}
		else
			return;
		
		String[] columns = line.split("\"[,][\"]");
		String country = columns[0].substring(1);
		
		double rawValue = 0.0;
		if (columns[59].contains(".")) {
			rawValue = Double.parseDouble(columns[59]);
		}
		else if (columns[58].contains(".")) {
			rawValue = Double.parseDouble(columns[58]);
		}
		else if (columns[57].contains(".")) {
			rawValue = Double.parseDouble(columns[57]);
		}
		else
			return;
		
		if (gender.equals("Male"))
			rawValue = rawValue*-1;
		
		context.write(new Text(country), new DoubleWritable(rawValue));
	}
}
