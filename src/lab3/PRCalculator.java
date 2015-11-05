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

public class PRCalculator {
	
	static int counter = 0;

	public static class PRCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int titleInd = value.find("\t") ;
			String title = Text.decode(value.getBytes(),0,titleInd) ;
			context.write(new Text(title), new Text("!")) ;

			int rankInd = value.find("\t", titleInd+1) ;

			if (rankInd != -1) {
				String links = Text.decode(value.getBytes(), rankInd+1, value.getLength()-(rankInd+1)) ;
				context.write(new Text(title), new Text("|"+links));

				float currentRank = Float.parseFloat(Text.decode(value.getBytes(), titleInd+1, rankInd-(titleInd+1))) ;
				String[] allLinks = links.split("\t") ;
				int numLinks = allLinks.length ;

				float rankContributed = currentRank/numLinks ;
				Text valRank = new Text(String.valueOf(rankContributed)) ; 

				for (String otherLink: allLinks){
					otherLink = otherLink.trim() ;
					if (otherLink != null)
						if (!otherLink.isEmpty())
							context.write(new Text(otherLink), valRank);
				}
			}
		}
	}

	public static class PRCalcReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String valMarkerOrLinksOrRank ;
			String allLinks = "" ;
			
			//changes
			double d = 0.85;			
			double sumOfRankShares = (1-d)/counter ;
			double remaining = 0;
			
			boolean existingPageFlag = false ;
			Iterator<Text> iterator = values.iterator() ;
						
			while(iterator.hasNext()) {
				valMarkerOrLinksOrRank = iterator.next().toString() ;

				if (valMarkerOrLinksOrRank.equals("!")) 
					existingPageFlag = true ;
				else if (valMarkerOrLinksOrRank.startsWith("|")) 
					allLinks = valMarkerOrLinksOrRank.substring(1) ;
				else
					remaining += Float.parseFloat(valMarkerOrLinksOrRank) ;
			}

			//change
			sumOfRankShares +=  remaining*d;

			if (existingPageFlag) 
				context.write(key, new Text(sumOfRankShares+"\t"+allLinks)) ;
		}
	}

	public void runPRCalculator(String inputPath, String outputPath, int count) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration() ;

		Job job = new Job(conf, "prcalculator") ;
		job.setJarByClass(PRCalculator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PRCalcMapper.class);
		job.setReducerClass(PRCalcReducer.class) ;

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		counter = count;

		job.waitForCompletion(true);
	}
} // end outermost class