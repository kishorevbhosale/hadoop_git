import java.io.IOException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.println("IO error");
			System.exit(-1);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(Driver.class);
		job.setJobName("Driver");
		job.setMapperClass(WMapper.class);
		job.setReducerClass(WReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
