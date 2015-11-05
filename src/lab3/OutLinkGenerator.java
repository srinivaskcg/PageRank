package lab3;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import lab3.XmlInputFormat;

public class OutLinkGenerator {

	public  static class Map extends Mapper<LongWritable, Text, Text, Text> {

		//private static final Pattern linkPattern = Pattern.compile("\\[.+?\\]") ;
		  private static final Pattern linkPattern = Pattern.compile("\\[\\[([^:\\]]*?)\\]\\]");
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			int start = value.find("<title>") ;
			int end = value.find("</title>", start) ;
			
			if (start == -1 || end == -1) return ;
				start += 7 ;
			
			String title = Text.decode(value.getBytes(), start, end-start) ;
			
			title = title.replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replace(' ', '_');
			
			Text keyPage = new Text(title) ;
			
			String outLinkValues = "" ;

				start = value.find("<text") ; 
				if (start == -1) {
					context.write(keyPage, new Text(outLinkValues));
					return ;
				}
				start = value.find(">", start) ;
				if (start == -1) {
					context.write(keyPage, new Text(outLinkValues));
					return ;
				}
				end = value.find("</text>") ; 
				if (end == -1) {
					context.write(keyPage, new Text(outLinkValues));
					return ;
				}
				start += 1 ;

				String text = Text.decode(value.getBytes(), start, end-start) ;

				Matcher linkMatcher = linkPattern.matcher(text) ;

				LinkedList<String> duplicateList = new LinkedList<String>()  ;
				
				while (linkMatcher.find()){
					String outLinkPage = linkMatcher.group() ;
					outLinkPage = getWikiPageFromLink(outLinkPage) ;

					if (outLinkPage != null){
						if (!outLinkPage.isEmpty()){
							outLinkPage = outLinkPage.trim() ;
							duplicateList.add(outLinkPage) ;
						}
					}
				}

				LinkedHashSet<String> listToSet = new LinkedHashSet<String>(duplicateList) ;
				LinkedList<String> listWithoutDuplicates = new LinkedList<String>(listToSet) ;

				boolean first = true ; 
				
				for (String vals: listWithoutDuplicates){
					if (!vals.equals(title)) {
						if (!first) 
							outLinkValues += "\t" ;
						outLinkValues += vals ;
						first = false ;
					}
				}

				context.write(keyPage, new Text(outLinkValues));
		} 
		
		private String getWikiPageFromLink(String link){
			
			if (isNotWikiLink(link)) return null ;

			int start = 1 ;
			if(link.startsWith("[["))
				start = 2 ;

			int end = link.indexOf("]") ;

			int pipePosition = link.indexOf("|") ;
			if (pipePosition > 0){
				end = pipePosition ;
			}
			link = link.substring(start, end) ;
			link = link.replaceAll("\\s", "_").replaceAll("&amp;", "&").replaceAll("&quot;", "\"");

			return link ;
		}

		private boolean isNotWikiLink(String link) {
			int start = 1;
			if(link.startsWith("[[")){
				start = 2;
			}

			if( link.length() < start+2 || link.length() > 100) return true;
			/*char firstChar = link.charAt(start);

			switch(firstChar){
			case '#': return true;
			case ',': return true;
			case '.': return true;
			case '&': return true;
			case '\'':return true;
			case '-': return true;
			case '{': return true;
			}	        
			
			if( link.contains(":")) return true;
			if( link.contains(",")) return true;
			if( link.contains("&")) return true;*/
			
			if (link.endsWith(".jpg") || link.endsWith(".gif")
                    || link.endsWith(".png") ||  link == null || link.isEmpty() 
                    || link.contains("Help:") || link.startsWith("../") || link.startsWith("."))
				return true;

			return false;
		}

	}

	public void parseXML(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration() ;
		
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;

		Job job = new Job(conf, "parsexml") ;
		job.setJarByClass(OutLinkGenerator.class);

		FileInputFormat.addInputPath(job, new Path(inputPath)) ;
		job.setInputFormatClass(XmlInputFormat.class) ;
		job.setMapperClass(Map.class) ;

		FileOutputFormat.setOutputPath(job, new Path(outputPath)) ;
		job.setOutputFormatClass(TextOutputFormat.class) ;
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true); 

	}
}
