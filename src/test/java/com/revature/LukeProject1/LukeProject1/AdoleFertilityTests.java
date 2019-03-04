package com.revature.LukeProject1.LukeProject1;
import com.revature.LukeProject1.LukeProject1.AdoleFertility.*;

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

public class AdoleFertilityTests {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	@Before
	public void setUp() {
		AdoleFertilityMapper mapper = new AdoleFertilityMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

	}
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Adolescent fertility rate (births per 1,000 women ages 15-19)\",\"SP.ADO.TFRT\",\"84.9872\",\"82.7346\",\"80.482\",\"77.9506\",\"75.4192\",\"72.8878\",\"70.3564\",\"67.825\",\"66.466\",\"65.107\",\"63.748\",\"62.389\",\"61.03\",\"59.3726\",\"57.7152\",\"56.0578\",\"54.4004\",\"52.743\",\"52.5098\",\"52.2766\",\"52.0434\",\"51.8102\",\"51.577\",\"51.9174\",\"52.2578\",\"52.5982\",\"52.9386\",\"53.279\",\"54.5456\",\"55.8122\",\"57.0788\",\"58.3454\",\"59.612\",\"57.9556\",\"56.2992\",\"54.6428\",\"52.9864\",\"51.33\",\"49.7042\",\"48.0784\",\"46.4526\",\"44.8268\",\"43.201\",\"42.4946\",\"41.7882\",\"41.0818\",\"40.3754\",\"39.669\",\"37.7398\",\"35.8106\",\"33.8814\",\"31.9522\",\"30.023\",\"27.0666\",\"24.1102\",\"21.1538\",\"\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new Text("hello"));
		mapDriver.runTest();
	}
}