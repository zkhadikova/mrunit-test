package mrunit_test.mrunit_test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Sets;

public class UserTrackReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws java.io.IOException, InterruptedException {
		context.write(key, new IntWritable(Sets.newHashSet(values).size()));
	}
}