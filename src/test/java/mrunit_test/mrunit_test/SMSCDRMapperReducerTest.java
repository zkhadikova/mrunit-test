package mrunit_test.mrunit_test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mrunit_test.mrunit_test.SMSCDRMapper.CDRCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class SMSCDRMapperReducerTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.getConfiguration().set("delim", ";");
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.getConfiguration().set("delim", ";");
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver
				.withInput(new LongWritable(1),
						new Text("655209;1;796764372490213;804422938115889;6"))
				.withInput(new LongWritable(2),
						new Text("848232;1;893239399454843;434323435533244;3"))
				.withInput(new LongWritable(3),
						new Text("323234;1;343438438443;323723734544545;3"))
				.withInput(new LongWritable(4),
						new Text("323234;0;343438438443;323723734544545;3"));
		mapDriver.withOutput(new Text("6"), new IntWritable(1))
				.withOutput(new Text("3"), new IntWritable(1))
				.withOutput(new Text("3"), new IntWritable(1));
		mapDriver.runTest();
		
		assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
	              .findCounter(CDRCounter.NonSMSCDR).getValue());
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		mapReduceDriver
				.withInput(new LongWritable(),
						new Text("655209;1;796764372490213;804422938115889;6"))
				.withInput(new LongWritable(2),
						new Text("848232;1;893239399454843;434323435533244;3"))
				.withInput(new LongWritable(3),
						new Text("323234;1;343438438443;323723734544545;3"))
				.withInput(new LongWritable(4),
						new Text("323234;0;343438438443;323723734544545;3"));
		
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(2))
				.withOutput(new Text("6"), new IntWritable(1));
		
		mapReduceDriver.withCounter(CDRCounter.NonSMSCDR, 1);
		
		mapReduceDriver.runTest();
	}
}