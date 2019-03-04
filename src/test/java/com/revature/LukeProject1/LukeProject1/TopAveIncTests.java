package com.revature.LukeProject1.LukeProject1;
import com.revature.LukeProject1.LukeProject1.TopAveInc.*;

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

public class TopAveIncTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		TopAveIncMapper mapper = new TopAveIncMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		TopAveIncReducer reducer = new TopAveIncReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapperTop() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Ecuador\",\"ECU\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.76629\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"8.65367\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"63.87377\",\"18.95309\",\"20.43572\",\"\",\"\",\"24.05737\",\"25.97021\",\"27.73087\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new Text("placeholder"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperNotTop() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Israel\",\"ISR\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.154\",\"\",\"31.67816\",\"32.9518\",\"\",\"32.14155\",\"31.7677\",\"32.39906\",\"32.10819\",\"32.01087\",\"31.05902\",\"31.29998\",\"\",\r\n" + 
				""));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducerPos() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("High School: 17.19"));
		values.add(new Text("Bachelor's: 12.96"));
		values.add(new Text("Master's: 12.96"));
		values.add(new Text("Doctorate: 12.96"));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new Text("High School: 17.19, Bachelor's: 12.96, Master's: 12.96, Doctorate: 12.96"));
		reduceDriver.runTest();
	}
	@Test
	public void testReducerNeg() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text(""));
		values.add(new Text("Bachelor's: 12.96"));
		values.add(new Text("Master's: 12.96"));
		values.add(new Text("Doctorate: 12.96"));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducePos() {
		Text input1 = new Text("\"Ecuador\",\"ECU\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.76629\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"8.65367\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"63.87377\",\"18.95309\",\"20.43572\",\"\",\"\",\"24.05737\",\"25.97021\",\"27.73087\",\"\",\r\n" + 
				"");
		Text input2 = new Text("\"Ecuador\",\"ECU\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"0.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.65966\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.56166\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"14.45048\",\"11.25122\",\"11.77575\",\"\",\"\",\"13.5782\",\"9.89357\",\"11.68336\",\"\",\r\n" + 
				"");
		Text input3 = new Text("\"Ecuador\",\"ECU\",\"Educational attainment, completed Master's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.10955\",\"0.81488\",\"1.04982\",\"\",\r\n" + 
				"");
		Text input4 = new Text("\"Ecuador\",\"ECU\",\"Educational attainment, completed Doctoral or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\r\n" + 
				"");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.withInput(new LongWritable(1), input3);
		mapReduceDriver.withInput(new LongWritable(1), input4);
		mapReduceDriver.runTest();
	}
	@Test
	public void testMapReduceNeg() {
		Text input1 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.154\",\"\",\"31.67816\",\"32.9518\",\"\",\"32.14155\",\"31.7677\",\"32.39906\",\"32.10819\",\"32.01087\",\"31.05902\",\"31.29998\",\"\",\r\n" + 
				"");
		Text input2 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.41621\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20.9\",\"9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.31597\",\"\",\"41.21073\",\"43.44227\",\"\",\"44.87483\",\"45.58211\",\"45.9417\",\"47.31916\",\"21.03799\",\"22.04126\",\"22.14368\",\"\",\r\n" + 
				"");
		Text input3 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed Master's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.31507\",\"11.8431\",\"12.09941\",\"\",\r\n" + 
				"");
		Text input4 = new Text("\"Israel\",\"ISR\",\"Educational attainment, completed Doctoral or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.03311\",\"1.09616\",\"1.14957\",\"\",\r\n" + 
				"");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.withInput(new LongWritable(1), input3);
		mapReduceDriver.withInput(new LongWritable(1), input4);
		mapReduceDriver.runTest();
	}
}