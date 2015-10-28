import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WHouse {

	public static void main(String[] args) throws Exception, IOException {
		if (args.length != 2) {
			System.out.println("IOError");
			System.exit(-1);
		}
		@SuppressWarnings("deprecation")
		Job job1 = new Job();
		job1.setJobName("WHouse");

		job1.setJarByClass(WHouse.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(WHMap.class);
		job1.setReducerClass(WHReducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

	}

}
