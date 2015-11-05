package lab3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PRSorter {

	public static class sortPRMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration() ;
			
			long numTitles = Long.parseLong(conf.get("TITLES_COUNT")) ;   // Integer.parseInt(NUM_TITLES) ;
			float compareVal = (float) (5.0/numTitles) ;
			
			int titleInd = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,titleInd) ;
			
			int rankInd = value.find("\t", titleInd+1) ;
			
			String rankStr = null ;

			if (rankInd != -1) 
				rankStr = Text.decode(value.getBytes(), titleInd+1, rankInd-(titleInd+1)) ;
			else
				rankStr = Text.decode(value.getBytes(), titleInd+1, value.getLength()-(titleInd+1)) ;

			float currentRank = Float.parseFloat(rankStr) ;
			
			if (currentRank < compareVal) return ;
			
			FloatWritable keyRank = new FloatWritable(currentRank) ;
			Text valueTitle = new Text(title) ;
			context.write(keyRank, valueTitle);			
		}
	}

	public static class sortPRReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

		public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext())
				context.write(iterator.next(), key);
		}
	}
	
	public static class SortFloatComparator extends WritableComparator {		 
		public SortFloatComparator() {
			super(FloatWritable.class, true);
		}
		
		@SuppressWarnings("rawtypes")
	 
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			FloatWritable k1 = (FloatWritable)w1;
			FloatWritable k2 = (FloatWritable)w2;
			
			return -1 * k1.compareTo(k2);
		}
	}

	public void PRSort(String inPath0, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

		Configuration conf = new Configuration() ;
		
		Path pt = new Path(inPath0) ;
		FileSystem fs = FileSystem.get(new URI(inPath0), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt))) ;
		String line = br.readLine() ;
		String numTitles = line.substring(2) ;
		conf.set("TITLES_COUNT", numTitles) ;

		Job job = new Job(conf, "prsort") ;
		job.setJarByClass(PRSorter.class);
		
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(sortPRMapper.class);
		job.setReducerClass(sortPRReducer.class) ;
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setSortComparatorClass(SortFloatComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
	}
} 