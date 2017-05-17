package hadoop.mapreduce.v4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.Test;

import hadoop.mapreduce.v4.MaxTemperature.MaxTemperatureMapper;

/**
 * MaxTemperatureTest
 * 
 * @author hefa
 *
 */
public class MaxTemperatureTest {

	@Test
	public void parsesMalformedTemperature() throws IOException, InterruptedException {
		MaxTemperatureMapper mapper = new MaxTemperatureMapper();
		mapper.ct = new GenericCounter("ErrorCounter", "OVER_250");
		Text value = new Text("012299999996408201601152105I+63452-150875CRN05+067899999V02099999999999999999N999999999$0128L+99999999999ADDCB105+0000010CG1+0537410CG2+0539310CG3+0530310CH105-012910084110CO199-09CT1-012810CT2-012810CT3-012810CW110840102917010");
		Mapper<LongWritable, Text, Text, IntWritable>.Context context = mock(Mapper.Context.class);
		mapper.map(null, value, context);
		verify(context,never()).write(any(Text.class),any(IntWritable.class));
		assertThat(mapper.ct.getValue(),is(1L));
	}
}
