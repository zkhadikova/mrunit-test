package mrunit.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class UserTrackMapper extends Mapper<LongWritable, Text, Text, Text> {

	static enum Track {
		PROCESSED, IGNORED;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject userObject;
		try {
			userObject = new JSONObject(value.toString());
			String ignoredTrackId = context.getConfiguration().get("ignored_track_id");
			if (userObject.getString("track_id").equals(ignoredTrackId)) {
				context.getCounter(Track.IGNORED).increment(1);
			} else {
				context.write(new Text(userObject.getString("user_id")), new Text(userObject.getString("track_id")));
				context.getCounter(Track.PROCESSED).increment(1);
			}
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
}
