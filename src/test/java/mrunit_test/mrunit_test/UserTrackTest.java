package mrunit_test.mrunit_test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

import com.google.common.collect.Lists;

public class UserTrackTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Mapper<LongWritable, Text, Text, IntWritable> mapper = new UserTrackMapper();
		Reducer<Text, IntWritable, Text, IntWritable> reducer = new UserTrackReducer();
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
		reduceDriver
				.withInput(new Text("415c6e3901989a4aa5d7b6934605b609"), values)
				.withInput(new Text("d40f5eb47b8cca0452fd74752762f3b5"),
						Lists.newArrayList(new IntWritable(1), new IntWritable(1)))
				.withInput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"), Lists.newArrayList(new IntWritable(1)));

		reduceDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(2))
				.withOutput(new Text("d40f5eb47b8cca0452fd74752762f3b5"), new IntWritable(2))
				.withOutput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"), new IntWritable(1));
		
		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer() throws IOException {
		mapReduceDriver.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"6726c2a9af7e6b7fbdb565c1a1911d58\", \"cached\": \"\", \"timestamp\": \"20140923T19:15:00\", \"source_uri\": \"\", \"track_id\": \"a8ecf232f2ae422898fb0ae6b05e6dab\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"d40f5eb47b8cca0452fd74752762f3b5\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"f83d3a3b5cd849acb25a1e41951ec9a8\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"bc6700d62ff04dc5a750f994c9f9f062\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"6726c2a9af7e6b7fbdb565c1a1911d58\", \"cached\": \"\", \"timestamp\": \"20140923T19:15:00\", \"source_uri\": \"\", \"track_id\": \"ac6f626bae894cc986cacfde397fc958\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140924T20:00:00\", \"source_uri\": \"\", \"track_id\": \"4f037421b4664bbb97d8efedb32336c0\", \"source\": \"search\", \"length\": 255, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));

		mapReduceDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(2))
				.withOutput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"), new IntWritable(2))
				.withOutput(new Text("d40f5eb47b8cca0452fd74752762f3b5"), new IntWritable(1));
		
		mapReduceDriver.runTest();
	}
}