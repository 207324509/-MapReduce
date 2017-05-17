package hadoop.mapreduce;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import hadoop.mapreduce.v2.MaxTemperature.MaxTemperatureMapper;
import hadoop.mapreduce.v2.MaxTemperature.MaxTemperatureReducer;
import hadoop.mapreduce.v3.MaxTemperatureDriver;

/**
 * 
 * @author hefa
 *
 */
public class MaxTemperatureTest {

	@Test
	public void processesValidReceore() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		Text value = new Text("012299999996408201601152105I+63452-150875CRN05+067899999V02099999999999999999N999999999-01281+99999999999ADDCB105+0000010CG1+0537410CG2+0539310CG3+0530310CH105-012910084110CO199-09CT1-012810CT2-012810CT3-012810CW110840102917010");
		Mapper<LongWritable, Text, Text, IntWritable>.Context context = mock(Mapper.Context.class);
		mapper.map(null, value, context);
		verify(context).write(new Text("2016"), new IntWritable(-128));
	}

	@Test
	public void ignoresMissingTemperatureRecord() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		Text value = new Text("012299999996408201601152105I+63452-150875CRN05+067899999V02099999999999999999N999999999+99991+99999999999ADDCB105+0000010CG1+0537410CG2+0539310CG3+0530310CH105-012910084110CO199-09CT1-012810CT2-012810CT3-012810CW110840102917010");
		Mapper<LongWritable, Text, Text, IntWritable>.Context context = mock(Mapper.Context.class);
		mapper.map(null, value, context);
		verify(context, never()).write(any(Text.class), any(IntWritable.class));
	}

	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
		MaxTemperatureReducer reducer = new MaxTemperatureReducer();
		Text key = new Text("2016");
		Iterable<IntWritable> values = Arrays.asList(new IntWritable(10), new IntWritable(50), new IntWritable(5));
		Reducer<Text, IntWritable, Text, IntWritable>.Context context = mock(Reducer.Context.class);
		reducer.reduce(key, values, context);
		verify(context).write(new Text("2016"), new IntWritable(50));
	}

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000/");
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		Path input = new Path("hdfs://localhost:9000/user/hefa/input/sample.txt");
		Path output = new Path("hdfs://localhost:9000/user/hefa/output");

		FileSystem fs = FileSystem.get(conf);
		fs.delete(output, true); // delete old output

		driver.setConf(conf);

		int exitCode = driver.run(new String[] { input.toString(), output.toString() });
		assertThat(exitCode, is(0));
		checkOutput(conf, output);
	}

	private void checkOutput(Configuration conf, Path output) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
		assertThat(outputFiles.length, is(2));

		BufferedReader actual = asBufferedReader(fs.open(outputFiles[1]));
		BufferedReader expected = asBufferedReader(getClass().getResourceAsStream("/expected.txt"));
		String expectedLine;
		while ((expectedLine = expected.readLine()) != null) {
			assertThat(actual.readLine(), is(expectedLine));
		}
		assertThat(actual.readLine(), nullValue());
		actual.close();
		expected.close();
	}

	private BufferedReader asBufferedReader(InputStream in) throws IOException {
		return new BufferedReader(new InputStreamReader(in));
	}
}
