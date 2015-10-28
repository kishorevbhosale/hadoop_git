import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


	public class WHMap extends Mapper <LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] csvFields = line.split(",");  
			
			if(csvFields.length > 1) {
				String name = csvFields[0] + " " +csvFields[1];
				String tempOutput = name.toLowerCase();
				word.set(tempOutput);
				context.write(word, one);
			}
		}
	}
