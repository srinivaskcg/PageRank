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

public class RedLinksFilter {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int titleTabIndex = value.find("\t") ;		

			if (titleTabIndex == -1) {
				
				context.write(new Text(value.toString()), new Text("!")) ;
			} 
			else { 
				String title = Text.decode(value.getBytes(),0,titleTabIndex) ;
				Text keyTitle = new Text(title) ;
				context.write(keyTitle, new Text("!"));
				String links = Text.decode(value.getBytes(), titleTabIndex+1, value.getLength()-(titleTabIndex+1)) ;
				String[] allLinks = links.split("\t") ;

				for (String otherLink: allLinks){
					otherLink = otherLink.trim() ;
					if (otherLink != null)
						if (!otherLink.isEmpty())
							context.write(new Text(otherLink), keyTitle);
				}
			}	
		}	
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String valMarkerOrLinksOrRank ;
			String allLinks = "" ;
			
			boolean existingPageFlag = false ;
			boolean first = true ;
			
			Iterator<Text> iterator = values.iterator() ;
			
			while(iterator.hasNext()) {
				valMarkerOrLinksOrRank = iterator.next().toString() ;
				
				if (valMarkerOrLinksOrRank.equals("!")) 
					existingPageFlag = true ;
				else {
					if (!first)
						allLinks = allLinks+"\t"+valMarkerOrLinksOrRank ;
					else {
						allLinks = allLinks+valMarkerOrLinksOrRank ;
						first = false ;
					}
				}
			}
			if (existingPageFlag) 
				context.write(key, new Text(allLinks)) ;
		}
	}

	public void redLinkFilter(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		
		Job job = new Job(conf, "redLinkfilter") ;
		job.setJarByClass(RedLinksFilter.class);

		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class) ;
	    	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        
	    job.waitForCompletion(true);
	}
} // end outermost class

