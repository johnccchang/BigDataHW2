import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GroupMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String line = value.toString();
		
		if (line.length() > 0) {
			String[] tokens = line.split(",\"");
			System.out.println(line);
			System.out.println(tokens.length);
			System.out.println();
			word.set(tokens[0]);
			output.collect(word, new Text(tokens[1]));
		}
	}
}