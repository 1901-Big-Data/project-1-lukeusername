package com.revature.LukeProject1.LukeProject1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class Project1Tests {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {
		FemaleGradMapper mapper = new FemaleGradMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		FemaleGradReducer reducer = new FemaleGradReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapperHS() {
		mapDriver.withInput(new LongWritable(0), new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"4.70073\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.74554\",\"\",\"\",\"15.54716\",\"\",\"13.15425\",\"13.56239\",\"13.29657\",\"17.10869\",\"17.90698\",\"16.65435\",\"17.19287\",\"17.54984\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("Uruguay"), new Text("High School: 17.19"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperHSnegative() {
		mapDriver.withInput(new LongWritable(0), new Text("\"United States\",\"USA\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"48.1\",\"\",\"\",\"\",\"\",\"74.1\",\"\",\"\",\"\",\"69.9\",\"49.0785\",\"68.86887\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"46.81118\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"50.05921\",\"49.43142\",\"48.8585\",\"\",\"48.78573\",\"47.97969\",\"47.76093\",\"47.51506\",\"46.91442\",\"46.4593\",\"45.98511\",\"45.32604\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new Text("High School: 17.19"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperBA() {
		mapDriver.withInput(new LongWritable(0), new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6.76926\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"8.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.15207\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.89402\",\"\",\"\",\"11.34989\",\"\",\"11.31747\",\"11.57324\",\"11.1543\",\"12.96718\",\"12.96355\",\"6.15701\",\"6.24904\",\"10.95902\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("Uruguay"), new Text("Bachelor's: 12.96"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperMS() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Master's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.36489\",\"1.75143\",\"1.79331\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("Uruguay"), new Text("Master's: 1.75"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperDO() {
		mapDriver.withInput(new LongWritable(0), new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Doctoral or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.06197\",\"\",\"\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("Uruguay"), new Text("Doctorate: 0.06"));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("High School: 17.19"));
		values.add(new Text("Bachelor's: 12.96"));
		values.add(new Text("Master's: 1.75"));
		values.add(new Text("Doctorate: 0.06"));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new Text("High School: 17.19, Bachelor's: 12.96, Master's: 1.75, Doctorate: 0.06"));
		reduceDriver.runTest();
	}
	
	@Test
	public void testReducerNoHS() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text(""));
		values.add(new Text("Bachelor's: 12.96"));
		values.add(new Text("Master's: 1.75"));
		values.add(new Text("Doctorate: 0.06"));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		Text input1 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"4.70073\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.74554\",\"\",\"\",\"15.54716\",\"\",\"13.15425\",\"13.56239\",\"13.29657\",\"17.10869\",\"17.90698\",\"16.65435\",\"17.19287\",\"17.54984\",\"\",\r\n");
		Text input2 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6.76926\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"8.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.15207\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.89402\",\"\",\"\",\"11.34989\",\"\",\"11.31747\",\"11.57324\",\"11.1543\",\"12.96718\",\"12.96355\",\"6.15701\",\"6.24904\",\"10.95902\",\"\",\r\n");
		Text input3 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Master's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.36489\",\"1.75143\",\"1.79331\",\"\",\r\n");
		Text input4 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Doctoral or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.DO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.06197\",\"\",\"\",\"\",\r\n");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.withInput(new LongWritable(1), input3);
		mapReduceDriver.withInput(new LongWritable(1), input4);
		mapReduceDriver.addOutput(new Text("Uruguay"), new Text("High School: 17.11, Bachelor's: 11.15, Master's: 1.75, Doctorate: 0.06"));
		mapReduceDriver.runTest();
	}
}