import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Areducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, Context context)
			throws IOException, InterruptedException {

		String newKey = "";
		String newValue = "";

		while (values.hasNext()) {
			if (newKey.length() == 0) {
				newKey = values.next().toString();
			} else {
				newValue += ", " + values.next().toString();
			}
		}
		if (newValue.length() > 0) {
			newValue = newValue.substring(2);
			context.write(new Text(newKey), new Text(newValue));
		}

	}
}