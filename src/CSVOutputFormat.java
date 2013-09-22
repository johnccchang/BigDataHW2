import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

class CSVOutputFormat extends TextOutputFormat<Text, Text> {
	public static String CSV_TOKEN_SEPARATOR_CONFIG = "csvoutputformat.token.delimiter";

	public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
		
		Path file = new Path(FileOutputFormat.getOutputPath(job) + "/" + name + ".csv");
		FileSystem fs = file.getFileSystem(job);
		System.out.println(name+"=="+file);
		FSDataOutputStream fileOut = fs.create(file, progress);
		String keyValueSeparator = job.get(CSV_TOKEN_SEPARATOR_CONFIG, ",");
		return new CSVRecordWriter(fileOut, keyValueSeparator);
	}

	protected static class CSVRecordWriter implements RecordWriter<Text, Text> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		private final String csvSeparator;
		protected DataOutputStream outStream;

		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		public CSVRecordWriter(DataOutputStream out, String csvSeparator) {
			this.outStream = out;
			this.csvSeparator = csvSeparator;
		}

		@Override
		public void write(Text key, Text value) throws IOException {
			if (key == null) return;
			boolean first = true;
			// for (Writable field : key.get()) {
			//System.out.println(key.toString() + "<===>" + value.toString());
			writeObject(first, key);
			first = false;
			writeObject(first, value);
			// }
			this.outStream.write(newline);
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(boolean first, Writable o) throws IOException {

			if (!first) {
				this.outStream.write(csvSeparator.getBytes(utf8));
			}

			boolean encloseQuotes = true;
			if (o.toString().contains(csvSeparator)) {
				encloseQuotes = true;
			}

			if (encloseQuotes) {
				this.outStream.write("\"".getBytes(utf8));
			}
			if (o instanceof Text) {
				Text to = (Text) o;
				this.outStream.write(to.getBytes(), 0, to.getLength());
			} else {
				this.outStream.write(o.toString().getBytes(utf8));
			}
			if (encloseQuotes) {
				this.outStream.write("\"".getBytes(utf8));
			}
		}

		public synchronized void close(Reporter reporter) throws IOException {
			outStream.close();
		}
	}
}