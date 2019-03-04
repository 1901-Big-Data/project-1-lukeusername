package com.revature.LukeProject1.LukeProject1;
import com.revature.LukeProject1.LukeProject1.AveInc.*;

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

public class AveIncTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		FemaleAveIncreaseMapper mapper = new FemaleAveIncreaseMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		FemaleAveIncreaseReducer reducer = new FemaleAveIncreaseReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	@Test
	public void testMapperHS() {

		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"48.1\",\"\",\"\",\"\",\"\",\"74.1\",\"\",\"\",\"\",\"69.9\",\"49.0785\",\"68.86887\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"46.81118\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"50.05921\",\"49.43142\",\"48.8585\",\"\",\"48.78573\",\"47.97969\",\"47.76093\",\"47.51506\",\"46.91442\",\"46.4593\",\"45.98511\",\"45.32604\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new Text("placeholder"));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperNotUSAHS() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Brazil\",\"BRA\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"4.00803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"21.87047\",\"\",\"24.40829\",\"24.89787\",\"25.86688\",\"26.75596\",\"25.00621\",\"28.29667\",\"28.99427\",\"29.76304\",\"29.82821\",\"\",\"\",\r\n" + 
				""));
		mapDriver.runTest();
	}
	@Test
	public void testMapperBA() {
		mapDriver.withInput(new LongWritable(0), new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new Text("placeholder"));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("High School: 17.19"));
		values.add(new Text("Bachelor's: 12.96"));

		reduceDriver.withInput(new Text("United States"), values);
		reduceDriver.withOutput(new Text("United States"), new Text("placeholder"));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduceNegative() {
		Text input1 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"4.70073\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.74554\",\"\",\"\",\"15.54716\",\"\",\"13.15425\",\"13.56239\",\"13.29657\",\"17.10869\",\"17.90698\",\"16.65435\",\"17.19287\",\"17.54984\",\"\",\r\n");
		Text input2 = new Text("\"Uruguay\",\"URY\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6.76926\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"8.9\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.15207\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.89402\",\"\",\"\",\"11.34989\",\"\",\"11.31747\",\"11.57324\",\"11.1543\",\"12.96718\",\"12.96355\",\"6.15701\",\"6.24904\",\"10.95902\",\"\",\r\n");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.runTest();
	}
	@Test
	public void testMapReducePositive() {
		Text input1 = new Text("\"United States\",\"USA\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"48.1\",\"\",\"\",\"\",\"\",\"74.1\",\"\",\"\",\"\",\"69.9\",\"49.0785\",\"68.86887\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"46.81118\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"50.05921\",\"49.43142\",\"48.8585\",\"\",\"48.78573\",\"47.97969\",\"47.76093\",\"47.51506\",\"46.91442\",\"46.4593\",\"45.98511\",\"45.32604\",\"\",\r\n");
		Text input2 = new Text("\"United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"\",\r\n");
		mapReduceDriver.withInput(new LongWritable(1), input1);
		mapReduceDriver.withInput(new LongWritable(1), input2);
		mapReduceDriver.addOutput(new Text("Uruguay"), new Text("placeholder"));
		mapReduceDriver.runTest();
	}
}