package com.revature.LukeProject1.LukeProject1;
import com.revature.LukeProject1.LukeProject1.FemEmp.*;

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

public class FemaleEmpTests {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	@Before
	public void setUp() {
		FemaleEmpMapper mapper = new FemaleEmpMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
	}
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (national estimate)\",\"SL.EMP.TOTL.SP.FE.NE.ZS\",\"35.5200004577637\",\"35.3499984741211\",\"35.5699996948242\",\"35.8300018310547\",\"36.310001373291\",\"37.0900001525879\",\"38.3199996948242\",\"38.9900016784668\",\"39.6199989318848\",\"40.7099990844727\",\"40.7900009155273\",\"40.3600006103516\",\"40.9700012207031\",\"42.0499992370605\",\"42.5800018310547\",\"42.0299987792969\",\"43.2299995422363\",\"44.4799995422363\",\"46.3699989318848\",\"47.4599990844727\",\"47.6699981689453\",\"47.9799995422363\",\"47.6699981689453\",\"48.0400009155273\",\"49.4900016784668\",\"50.4199981689453\",\"51.3800010681152\",\"52.5099983215332\",\"53.4300003051758\",\"54.310001373291\",\"54.3499984741211\",\"53.689998626709\",\"53.7599983215332\",\"54.0999984741211\",\"55.25\",\"55.6300010681152\",\"56.0400009155273\",\"56.7999992370605\",\"57.0800018310547\",\"57.4300003051758\",\"57.4900016784668\",\"57\",\"56.2700004577637\",\"56.1300010681152\",\"55.9700012207031\",\"56.2400016784668\",\"56.6199989318848\",\"56.6399993896484\",\"56.25\",\"54.4199981689453\",\"53.5699996948242\",\"53.189998626709\",\"53.1300010681152\",\"53.1599998474121\",\"53.5200004577637\",\"53.7400016784668\",\"54.0800018310547\",\r\n" + 
				""));
		mapDriver.withOutput(new Text("United States"), new DoubleWritable(-3.41));
		mapDriver.runTest();
	}
}