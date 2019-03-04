package com.revature.LukeProject1.LukeProject1;
import com.revature.LukeProject1.LukeProject1.MaleVsFemale.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MaleVsFemaleTests {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		MaleVsFemaleMapper mapper = new MaleVsFemaleMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		MaleVsFemaleReducer reducer = new MaleVsFemaleReducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapperPosFemale() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Cayman Islands\",\"CYM\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.29317\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.65493\",\"25.46769\",\"25.18721\",\"35.6955\",\"40.40238\",\"\",\"\",\"\",\"\",\"\",\"33.08515\",\"22.98506\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(0.0));
		mapDriver.runTest();
	}
	@Test
	public void testMapperPosMale() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Cayman Islands\",\"CYM\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, male (%)\",\"SE.TER.HIAT.BA.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.99664\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.1518\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"26.40114\",\"23.28305\",\"22.28403\",\"34.07075\",\"33.75652\",\"\",\"\",\"\",\"\",\"\",\"30.55279\",\"17.29588\",\"\",\r\n" + 
				""));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducerPos() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(22.98506));
		values.add(new DoubleWritable(-17.29588));

		reduceDriver.withInput(new Text("CaymanIslands"), values);
		reduceDriver.withOutput(new Text("United States"), new DoubleWritable(0.0));
		reduceDriver.runTest();
	}
	@Test
	public void testReducerNeg() {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(15.67));
		values.add(new DoubleWritable(-24.598766));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new DoubleWritable(0.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducePos() {
		Text input1 = new Text("\"Cayman Islands\",\"CYM\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.29317\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.65493\",\"25.46769\",\"25.18721\",\"35.6955\",\"40.40238\",\"\",\"\",\"\",\"\",\"\",\"33.08515\",\"22.98506\",\"\",\r\n" + 
				"");
		Text input2 = new Text("\"Cayman Islands\",\"CYM\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, male (%)\",\"SE.TER.HIAT.BA.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.99664\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.1518\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"26.40114\",\"23.28305\",\"22.28403\",\"34.07075\",\"33.75652\",\"\",\"\",\"\",\"\",\"\",\"30.55279\",\"17.29588\",\"\",\r\n" + 
				"");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.withOutput(new Text("Cayman Islands"), new DoubleWritable(5.68918));
		mapReduceDriver.runTest();
	}
	@Test
	public void testMapReduceNeg() {
		Text input1 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, male (%)\",\"SE.TER.HIAT.BA.MA.ZS\",\"\",\"12.80416\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.20019\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"25.42861\",\"13.56811\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"38.09709\",\"\",\"38.09709\",\"39.29174\",\"\",\"39.77195\",\"40.32772\",\"41.7526\",\"41.3881\",\"17.41226\",\"18.21406\",\"18.60306\",\"\",\r\n" + 
				"");
		Text input2 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.41621\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20.9\",\"9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.31597\",\"\",\"41.21073\",\"43.44227\",\"\",\"44.87483\",\"45.58211\",\"45.9417\",\"47.31916\",\"21.03799\",\"22.04126\",\"22.14368\",\"\",\r\n" + 
				"");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.runTest();
	}
}