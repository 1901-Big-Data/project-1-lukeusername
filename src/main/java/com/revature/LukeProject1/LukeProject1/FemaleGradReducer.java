package com.revature.LukeProject1.LukeProject1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class FemaleGradReducer extends Reducer<Text, Text, Text, Text> {

	/*
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework.
	 * The method receives a key of type Text, a set of values of type
	 * IntWritable, and a Context object.
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String dataHS="", dataBA="", dataMS="", dataDO="";
		String temp;

		for (Text value : values) {
			if (value.getLength() > 0) {
				temp = value.toString();
				if (temp.charAt(0) == 'H') 
					dataHS = temp;
				else if (temp.charAt(0) == 'B')
					dataBA = temp;
				else if (temp.charAt(0) == 'M')
					dataMS = temp;
				else
					dataDO = temp;
			}
		}
		
		if (dataHS == "")
			return;

		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new Text(dataHS + ", " + dataBA + ", " + dataMS + ", " + dataDO));
	}
}