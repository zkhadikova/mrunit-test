package mrunit_test.mrunit_test;

//import java.util.stream.Collectors;
//import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InverseReducer extends
		Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
//		String string = StreamSupport.stream(values.spliterator(), false).map(v -> v.toString()).collect(Collectors.joining(";"));
//		context.write(key, new Text(string));
	}
}