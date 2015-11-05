package lab3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NCalculator {

	public static class NCalcMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		private final static Text one = new Text("one") ;
		private final static LongWritable oneVal = new LongWritable(1) ;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(one, oneVal);
		}
	}

	public static class NCalcReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long N = 0 ;
			for (Iterator<LongWritable> iterator = values.iterator(); iterator
					.hasNext();) {
				iterator.next();
				N += 1 ;
			}
			context.write(new Text("N"), new LongWritable(N));
		}
	}

	public void calculateN(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration() ;

		conf.set("mapred.textoutputformat.separator", "=");
		
		Job job = new Job(conf, "calculaten") ;
		job.setJarByClass(NCalculator.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(NCalcMapper.class);
		job.setReducerClass(NCalcReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

	}
}

