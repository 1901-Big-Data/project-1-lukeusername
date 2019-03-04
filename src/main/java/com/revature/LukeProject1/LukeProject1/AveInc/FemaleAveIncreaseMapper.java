package com.revature.LukeProject1.LukeProject1.AveInc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleAveIncreaseMapper extends Mapper<LongWritable, Text, Text, Text> {
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
		else
			return;
		
		String[] columns = line.split("\"[,][\"]");
		String country = columns[0].substring(1);
		
		// Don't include countries that don't have a HS grad rate within 10 of USA from years 2004-2006
		if (education.equals("High School")) {
			for (int i = 48; i < 51; i++) {
				if (columns[i].contains(".")) {
					if (Double.parseDouble(columns[i]) >  60.0 || Double.parseDouble(columns[i]) < 40.0) {
						return;
					}
					// if we have data within the acceptable range, we're all good
					else {
						break;
					}
				}
				else if (i == 50)
					return;
			// if no data exists from those three years, don't include
			}
		}
		
		ArrayList<Double> increases = new ArrayList<Double>();
		double averageCume = 0.0;
		Double diff = 0.0;
		String outlierMsg = "";
		// we are grabbing increase in grad rate from 2000-2016
		for (int i = 45; i < 60; i++) {
			// if two consecutive years of data exist, get the difference
			if (columns[i-1].contains(".") && columns[i].contains(".")) {
				diff = Double.parseDouble(columns[i]) - Double.parseDouble(columns[i-1]);
				if (Math.abs(diff) > 5.0)
					outlierMsg += " (" + education +" outlier: " + (i-1 + 1956) + "-" + (i + 1956) + ", " + diff.toString().substring(0, 6) + ")";
				// the difference between these two particular years
				increases.add(diff);
			}
		}
		// must have data on at least 2 separate consecutive years
		if (increases.size() < 2)
			return;
		else {
			// calculate the average
			for(int i = 0; i < increases.size(); i++) {
				averageCume+=increases.get(i);
			}
			averageCume=Math.round((averageCume/(double)increases.size())*100.0)/100.0;
		}
		
		context.write(new Text(country), new Text(education + ": " + averageCume + outlierMsg));
	}
}
