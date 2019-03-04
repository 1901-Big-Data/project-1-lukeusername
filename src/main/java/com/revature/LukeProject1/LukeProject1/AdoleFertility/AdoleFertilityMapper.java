package com.revature.LukeProject1.LukeProject1.AdoleFertility;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdoleFertilityMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String data_90s = "", data_2000s = "", data_2010s = "";
		double start = 0.0, end = 0.0;
		String line = value.toString();
		// Adolescent fertility rate (births per 1,000 women ages 15-19)
		if (!line.contains("SP.ADO.TFRT")) {
			return;
		}
		String[] columns = line.split("\"[,][\"]");
		String country = columns[0].substring(1);
		
		/*
		 * 90s calculations
		 */
		ArrayList<Double> increases = new ArrayList<Double>();
		double averageCume = 0.0;
		// calculating average change in fertility rate through the 90's (index 34 is 1990)
		for (int i = 34; i < 43; i++) {
			// if two consecutive years of data exist, get the difference
			if (columns[i-1].contains(".") && columns[i].contains(".")) {
				// the difference between these two particular years
				increases.add(Double.parseDouble(columns[i]) - Double.parseDouble(columns[i-1]));
			}
			else
				return;
		}
		// calculate the average
		for(int i = 0; i < increases.size(); i++) {
			averageCume+=increases.get(i);
		}
		averageCume=Math.round((averageCume/(double)increases.size())*100.0)/100.0;
		start = Math.round((Double.parseDouble(columns[34])*100.0))/100.0;
		end = Math.round((Double.parseDouble(columns[43])*100.0))/100.0;
		data_90s = "1990-1999: " + start + "," + end + "," + averageCume + " ||| ";
		
		
		/*
		 * 2000s calculations
		 */
		increases = new ArrayList<Double>();
		averageCume = 0.0;
		// calculating average change in fertility rate through the 90's (index 34 is 1990)
		for (int i = 44; i < 53; i++) {
			// if two consecutive years of data exist, get the difference
			if (columns[i-1].contains(".") && columns[i].contains(".")) {
				// the difference between these two particular years
				increases.add(Double.parseDouble(columns[i]) - Double.parseDouble(columns[i-1]));
			}
			else
				return;
		}
		// calculate the average
		for(int i = 0; i < increases.size(); i++) {
			averageCume+=increases.get(i);
		}
		averageCume=Math.round((averageCume/(double)increases.size())*100.0)/100.0;
		start = Math.round((Double.parseDouble(columns[44])*100.0))/100.0;
		end = Math.round((Double.parseDouble(columns[53])*100.0))/100.0;
		data_2000s = "2000-2009: " + start + "," + end + "," + averageCume + " ||| ";
		
		/*
		 * 2010s calculations
		 */
		increases = new ArrayList<Double>();
		averageCume = 0.0;
		// calculating average change in fertility rate through the 90's (index 34 is 1990)
		for (int i = 54; i < 59; i++) {
			// if two consecutive years of data exist, get the difference
			if (columns[i-1].contains(".") && columns[i].contains(".")) {
				// the difference between these two particular years
				increases.add(Double.parseDouble(columns[i]) - Double.parseDouble(columns[i-1]));
			}
			else
				return;
		}
		// calculate the average
		for(int i = 0; i < increases.size(); i++) {
			averageCume+=increases.get(i);
		}
		averageCume=Math.round((averageCume/(double)increases.size())*100.0)/100.0;
		start = Math.round((Double.parseDouble(columns[54])*100.0))/100.0;
		end = Math.round((Double.parseDouble(columns[59])*100.0))/100.0;
		data_2010s = "2010-2015: " + start + "," + end + "," + averageCume;
		
		
		context.write(new Text(country), new Text(data_90s + data_2000s + data_2010s));
	}
}
