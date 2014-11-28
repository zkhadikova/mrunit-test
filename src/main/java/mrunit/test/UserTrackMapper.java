package mrunit.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class UserTrackMapper extends Mapper<LongWritable, Text, Text, Text> {

	// to keep count of processed and ignored records
	static enum TrackCounter {
		PROCESSED, IGNORED;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			JSONObject userObject = new JSONObject(value.toString());
			String ignoredTrackId = context.getConfiguration().get("ignored_track_id"); // getting options from configuration
			String trackId = userObject.getString("track_id");
			if (trackId.equals(ignoredTrackId)) {
				context.getCounter(TrackCounter.IGNORED).increment(1); 
			} else {
				context.write(new Text(userObject.getString("user_id")), new Text(trackId));
				context.getCounter(TrackCounter.PROCESSED).increment(1);
			}
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
}
