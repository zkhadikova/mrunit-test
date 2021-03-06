package mrunit.test;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

public class UserTrackReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//remove user's duplicated tracks
		HashSet<Text> uniqueValues = Sets.newHashSet(values);
		context.write(key, new IntWritable(uniqueValues.size()));
	}
}