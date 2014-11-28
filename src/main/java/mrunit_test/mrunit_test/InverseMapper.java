package mrunit_test.mrunit_test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InverseMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		String delim = context.getConfiguration().get("delim");
		String[] line = value.toString().split(delim);
		String val = line[0];
		for (int i = 1; i < line.length; i++) {
			context.write(new Text(line[i]), new Text(val));
		}
	}
}
