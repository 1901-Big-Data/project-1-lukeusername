package com.revature.LukeProject1.LukeProject1.MaleVsFemale;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaleVsFemaleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double maleVal = 1, femaleVal = -1;

		int count = 0;
		for (DoubleWritable value : values) {
			count++;
			if (value.get() < 0.0)
				maleVal = value.get();
			else
				femaleVal = value.get();
		}
		
		if (count > 2 || maleVal > 0 || femaleVal < 0)
			return;
		
		double diff = 0.0;
		diff = femaleVal+maleVal;
		diff=Math.round((diff)*100000.0)/100000.0;
		
		if (Math.abs(diff) < 5.0)
			return;
		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new DoubleWritable(diff));
	}
}