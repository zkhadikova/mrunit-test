package mrunit_test.mrunit_test;

import mrunit_test.mrunit_test.SMSCDRMapper.CDRCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UserTrackTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Mapper mapper = new UserTrackMapper();
		Reducer reducer = new UserTrackReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.getConfiguration().set("delim", ";");
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.getConfiguration().set("delim", ";");
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.getConfiguration().set("ignored_track_id", "TrackId");
		mapDriver
	    	.withInput(new LongWritable(1),
		        	    new Text("{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"a8ecf232f2ae422898fb0ae6b05e6dab\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))

			.withInput(new LongWritable(1),
	     			    new Text("{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T20:00:00\", \"source_uri\": \"\", \"track_id\": \"4f037421b4664bbb97d8efedb32336c0\", \"source\": \"search\", \"length\": 255, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));

		// ignored record:  track_id="TrackId"
		mapDriver.withInput(new LongWritable(1),
						new Text("{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"TrackId\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));

		mapDriver.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(1));
		mapDriver.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(1));
		mapDriver.runTest();
		
		assertEquals(2, mapDriver.getCounters().findCounter(UserTrackMapper.Track.PROCESSED).getValue());
		assertEquals(1, mapDriver.getCounters().findCounter(UserTrackMapper.Track.IGNORED).getValue());
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