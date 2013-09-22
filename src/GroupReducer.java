import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GroupReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String members = "";
		int num = 0;
		while (values.hasNext()) {
			members += values.next().toString() + ":";
			num++;
		}
		members = num + ":" + members.substring(0, members.length()-1);
		output.collect(key, new Text(members));
	}
}