package mrunit.test;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UserTrackTest {

	public static final String IGNORED_TRACK_ID_VALUE = "TrackId";

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Mapper<LongWritable, Text, Text, Text> mapper = new UserTrackMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		// test configuration
		mapDriver.getConfiguration().set("ignored_track_id", IGNORED_TRACK_ID_VALUE);

		Reducer<Text, Text, Text, IntWritable> reducer = new UserTrackReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"a8ecf232f2ae422898fb0ae6b05e6dab\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140924T19:15:00\", \"source_uri\": \"\", \"track_id\": \"a8ecf232f2ae422898fb0ae6b05e6dab\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140921T20:00:00\", \"source_uri\": \"\", \"track_id\": \"4f037421b4664bbb97d8efedb32336c0\", \"source\": \"search\", \"length\": 255, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));
		// ignored record: track_id="TrackId"
		mapDriver
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140925T19:15:00\", \"source_uri\": \"\", \"track_id\": \"TrackId\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));

		mapDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new Text("a8ecf232f2ae422898fb0ae6b05e6dab"));
		mapDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new Text("a8ecf232f2ae422898fb0ae6b05e6dab"));
		mapDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new Text("4f037421b4664bbb97d8efedb32336c0"));

		mapDriver.runTest();

		assertEquals(3, mapDriver.getCounters().findCounter(UserTrackMapper.TrackCounter.PROCESSED).getValue());
		assertEquals(1, mapDriver.getCounters().findCounter(UserTrackMapper.TrackCounter.IGNORED).getValue());
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("a8ecf232f2ae422898fb0ae6b05e6dab"));
		values.add(new Text("4f037421b4664bbb97d8efedb32336c0"));
		values.add(new Text("4f037421b4664bbb97d8efedb32336c0")); // duplicated record on purpose
		reduceDriver
				.withInput(new Text("415c6e3901989a4aa5d7b6934605b609"), values)
				.withInput(
						new Text("d40f5eb47b8cca0452fd74752762f3b5"),
						Lists.newArrayList(new Text("f83d3a3b5cd849acb25a1e41951ec9a8"), new Text(
								"bc6700d62ff04dc5a750f994c9f9f062")))
				.withInput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"),
						Lists.newArrayList(new Text("ac6f626bae894cc986cacfde397fc958")));

		reduceDriver.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(2))
				.withOutput(new Text("d40f5eb47b8cca0452fd74752762f3b5"), new IntWritable(2))
				.withOutput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"), new IntWritable(1));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer() throws IOException {
		mapReduceDriver
				.withInput(
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
								"{\"user_id\": \"6726c2a9af7e6b7fbdb565c1a1911d58\", \"cached\": \"\", \"timestamp\": \"20140923T19:15:00\", \"source_uri\": \"\", \"track_id\": \"ac6f626bae894cc986cacfde397fc958\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"bc6700d62ff04dc5a750f994c9f9f062\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"d40f5eb47b8cca0452fd74752762f3b5\", \"cached\": \"\", \"timestamp\": \"20140922T19:15:00\", \"source_uri\": \"\", \"track_id\": \"f83d3a3b5cd849acb25a1e41951ec9a8\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"6726c2a9af7e6b7fbdb565c1a1911d58\", \"cached\": \"\", \"timestamp\": \"20140923T19:15:00\", \"source_uri\": \"\", \"track_id\": \"ac6f626bae894cc986cacfde397fc958\", \"source\": \"search\", \"length\": 288, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"))
				.withInput(
						new LongWritable(1),
						new Text(
								"{\"user_id\": \"415c6e3901989a4aa5d7b6934605b609\", \"cached\": \"\", \"timestamp\": \"20140924T20:00:00\", \"source_uri\": \"\", \"track_id\": \"4f037421b4664bbb97d8efedb32336c0\", \"source\": \"search\", \"length\": 255, \"version\": 2, \"device_type\": \"tablet\", \"offline_timestamp\": \"\", \"message\": \"APIStreamData\", \"os\": \"iOS\"}"));

		// output sorted by key
		mapReduceDriver
				.withOutput(new Text("415c6e3901989a4aa5d7b6934605b609"), new IntWritable(2))
				.withOutput(new Text("6726c2a9af7e6b7fbdb565c1a1911d58"), new IntWritable(2))
				.withOutput(new Text("d40f5eb47b8cca0452fd74752762f3b5"), new IntWritable(1));

		mapReduceDriver.runTest();
	}

	/**
	 *	Check {@linkplain UserTrackMapper} and {@linkplain UserTrackReducer} implementations
	 *	against test file `resources/streams_20140922_AD` .
	 */
	@Test
	public void userTracksCount_streams_20140922_AD() throws IOException, JSONException, URISyntaxException {
		//      user_id, user tracks
		HashMap<String, Collection<String>> userStat = new HashMap<>();

		InputStream is = getClass().getResourceAsStream("/streams_20140922_AD");
		try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				mapReduceDriver.withInput(new LongWritable(1), new Text(line));
				JSONObject rec = new JSONObject(line);
        		updateStat(userStat, rec);
			}
		}

		List<Pair<Text, IntWritable>> pairs = mapReduceDriver.run(true);

		System.out.printf("=====================\n");
		System.out.printf("user_id: tracks_count\n");
		System.out.printf("=====================\n");
		for (Pair<Text, IntWritable> pair : pairs) {
			System.out.printf("%s: %s\n", pair.getFirst(), pair.getSecond());
			String userId = new String(pair.getFirst().getBytes(), Charset.defaultCharset());
			int tracksCount = pair.getSecond().get();
			int expectedTracksCount = userStat.get(userId).size();
			userStat.remove(userId);
			assertEquals(expectedTracksCount, tracksCount);
		}

		if (!userStat.isEmpty()) {
			System.out.println("\nuserStat: " + userStat);
			assertTrue("There should be no extra records", false);
		}

		System.out.printf("=====================\n");
		System.out.println("records processed: "
				+ mapReduceDriver.getCounters().findCounter(UserTrackMapper.TrackCounter.PROCESSED).getValue());
		System.out.println("records ignored: "
				+ mapReduceDriver.getCounters().findCounter(UserTrackMapper.TrackCounter.IGNORED).getValue());
	}

	private void updateStat(HashMap<String, Collection<String>> stat, JSONObject rec) throws JSONException {
		// do not count records with ignored track_id value
		if (IGNORED_TRACK_ID_VALUE.equalsIgnoreCase(rec.getString("track_id"))) {
			return;
		}

		String userId = rec.getString("user_id");
		String track = rec.getString("track_id");
		Collection<String> tracks = stat.get(userId);
		if (tracks == null) {
			tracks = new HashSet<>(); // using set, because we do not count duplicated records
			stat.put(userId, tracks);
		}

        tracks.add(track);
	}
}