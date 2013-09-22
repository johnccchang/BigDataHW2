import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class CSVRecordReader implements RecordReader<LongWritable, Text> {
	private Text value;
	private long start;
	private long end;
	private int count = 0;
	private FSDataInputStream fsin;
	private LineReader csvrecord; 

	public CSVRecordReader(FileSplit split, JobConf jobConf, Character csvDelimiter) throws IOException, InterruptedException {
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		System.out.println("path: " + file);
		FileSystem fs = file.getFileSystem(jobConf);
		fsin = fs.open(split.getPath());
		csvrecord = new LineReader(fsin);
	}
	
	@Override
	public void close() throws IOException {
		csvrecord.close();
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return fsin.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		float progress = (fsin.getPos() - start) / (float) (end - start);
		//System.out.println("progress: " + progress);
		return progress;
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		if (loadCSVLine()) {
			value.set(this.value.toString());
			return true;
		}
		return false;
	}
	
	private boolean loadCSVLine() throws IOException {
		Text line  = new Text();
		String result = "";
		String tag = "}}\"";
		boolean isEOF = false;
		do {
			csvrecord.readLine(line);
			if (line.getLength() == 0) {
				count++;
				if (count == 2) isEOF = true;
				continue;
			}
			result += line.toString();
			count = 0;
		} while(!line.toString().endsWith(tag) && result.length() > 0); 
		System.out.println("-> " + result);
		this.value = new Text(result);
		
		if (isEOF && result.length() == 0) return false;
		else return true;
	}
}