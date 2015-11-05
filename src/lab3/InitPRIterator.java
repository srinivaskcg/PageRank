package lab3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InitPRIterator {

	public static class InitPRIterateMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			long titlesCount = Long.parseLong(conf.get("TITLE_COUNT")); 
			float initialRank = (float) (1.0/titlesCount);

			int titleIndex = value.find("\t");
			String title = Text.decode(value.getBytes(),0,titleIndex) ;
			String outLinks = Text.decode(value.getBytes(),titleIndex+1, value.getLength()-(titleIndex+1));
			context.write(new Text(title+"\t"+initialRank), new Text(outLinks));
		}
	}

	public void initPRIterator(String inputPath0, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		Path pt = new Path(inputPath0);
		FileSystem fs = FileSystem.get(new URI(inputPath0), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line = br.readLine();
		String numTitles = line.substring(2);
		conf.set("TITLE_COUNT", numTitles) ;

		Job job = new Job(conf, "initpriterator") ;
		job.setJarByClass(InitPRIterator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(InitPRIterateMapper.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);

	}

}
