import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

public class LoadData {

	public void run(String[] args) throws IOException {

		JobConf conf = new JobConf(LoadData.class);
		conf.setJobName("LoadData");
		conf.setStrings("file.encoding", "UTF-8");
		
		conf.setMapperClass(GroupMapper.class);
		//conf.setReducerClass(GroupReducer.class);

		conf.setInputFormat(CSVInputFormat.class);
		
		conf.setOutputFormat(CSVOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		LoadData myInfobox = new LoadData();
		myInfobox.run(args);
	}

}
