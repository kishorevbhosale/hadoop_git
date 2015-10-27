

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RTemp extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxval = Integer.MIN_VALUE;
		for (IntWritable val : values) {
			maxval = Math.max(maxval, val.get());
		}
		context.write(key, new IntWritable(maxval));
	}
}
