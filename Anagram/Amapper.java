import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Amapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String wordString = value.toString().trim();
		char[] wordArray = wordString.toCharArray();
		Arrays.sort(wordArray);
		String wordStringSorted = String.valueOf(wordArray);
		context.write(new Text(wordStringSorted), new Text(wordString));
	}

}