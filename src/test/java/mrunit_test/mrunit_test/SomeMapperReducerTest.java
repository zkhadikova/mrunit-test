package mrunit_test.mrunit_test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SomeMapperReducerTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		InverseMapper mapper = new InverseMapper();
		InverseReducer reducer = new InverseReducer();
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
						new Text("Ana;depeche_mode;ramstien;korn;rhcp"))
				.withInput(
						new LongWritable(2),
						new Text(
								"Kame;nirvana;korn;foo_fighters;bfmv;the_killers"))
				.withInput(
						new LongWritable(3),
						new Text(
								"James;guns_and_roses;bfmv;the_killers;slipknot"));
		mapDriver.withOutput(new Text("depeche_mode"), new Text("Ana"))
				.withOutput(new Text("ramstien"), new Text("Ana"))
				.withOutput(new Text("korn"), new Text("Ana"))
				.withOutput(new Text("rhcp"), new Text("Ana"))
				.withOutput(new Text("nirvana"), new Text("Kame"))
				.withOutput(new Text("korn"), new Text("Kame"))
				.withOutput(new Text("foo_fighters"), new Text("Kame"))
				.withOutput(new Text("bfmv"), new Text("Kame"))
				.withOutput(new Text("the_killers"), new Text("Kame"))
				.withOutput(new Text("guns_and_roses"), new Text("James"))
				.withOutput(new Text("bfmv"), new Text("James"))
				.withOutput(new Text("the_killers"), new Text("James"))
				.withOutput(new Text("slipknot"), new Text("James"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		reduceDriver
				.withInput(new Text("depeche_mode"),
						Lists.newArrayList(new Text("Ana")))
				.withInput(new Text("ramstien"),
						Lists.newArrayList(new Text("Ana")))
				.withInput(new Text("korn"),
						Lists.newArrayList(new Text("Ana"), new Text("Kame")))
				.withInput(new Text("rhcp"),
						Lists.newArrayList(new Text("Ana")))
				.withInput(new Text("nirvana"),
						Lists.newArrayList(new Text("Kame")))
				.withInput(new Text("foo_fighters"),
						Lists.newArrayList(new Text("Kame")))
				.withInput(new Text("bfmv"),
						Lists.newArrayList(new Text("Kame"), new Text("James")))
				.withInput(new Text("the_killers"),
						Lists.newArrayList(new Text("Kame"), new Text("James")))
				.withInput(new Text("slipknot"),
						Lists.newArrayList(new Text("James")))
				.withInput(new Text("guns_and_roses"), Lists.newArrayList(new Text("James")));
		reduceDriver.withOutput(new Text("depeche_mode"), new Text("Ana"))
				.withOutput(new Text("ramstien"), new Text("Ana"))
				.withOutput(new Text("korn"), new Text("Ana;Kame"))
				.withOutput(new Text("rhcp"), new Text("Ana"))
				.withOutput(new Text("nirvana"), new Text("Kame"))
				.withOutput(new Text("foo_fighters"), new Text("Kame"))
				.withOutput(new Text("bfmv"), new Text("Kame;James"))
				.withOutput(new Text("the_killers"), new Text("Kame;James"))
				.withOutput(new Text("slipknot"), new Text("James"))
				.withOutput(new Text("guns_and_roses"), new Text("James"));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		Sca
		mapReduceDriver
				.withInput(new LongWritable(1),
						new Text("Ana;depeche_mode;ramstien;korn;rhcp"))
				.withInput(
						new LongWritable(2),
						new Text(
								"Kame;nirvana;korn;foo_fighters;bfmv;the_killers"))
				.withInput(
						new LongWritable(3),
						new Text(
								"James;guns_and_roses;bfmv;the_killers;slipknot"));
		mapReduceDriver.withOutput(new Text("bfmv"), new Text("Kame;James"))
				.withOutput(new Text("depeche_mode"), new Text("Ana"))
				.withOutput(new Text("foo_fighters"), new Text("Kame"))
				.withOutput(new Text("guns_and_roses"), new Text("James"))
				.withOutput(new Text("korn"), new Text("Ana;Kame"))
				.withOutput(new Text("nirvana"), new Text("Kame"))
				.withOutput(new Text("ramstien"), new Text("Ana"))
				.withOutput(new Text("rhcp"), new Text("Ana"))
				.withOutput(new Text("slipknot"), new Text("James"))
				.withOutput(new Text("the_killers"), new Text("Kame;James"));
		mapReduceDriver.runTest();
	}
}