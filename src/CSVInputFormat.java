import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class CSVInputFormat extends TextInputFormat {
	public static String CSV_TOKEN_SEPARATOR_CONFIG = "csvinputformat.token.delimiter";

	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
		String csvDelimiter = job.get(CSV_TOKEN_SEPARATOR_CONFIG);
		//System.out.println(csvDelimiter);

		Character separator = null;
		if (csvDelimiter != null && csvDelimiter.length() == 1)
			separator = csvDelimiter.charAt(0);
		try {

			return new CSVRecordReader((FileSplit) genericSplit, job, separator);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
}
