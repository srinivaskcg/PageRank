package lab3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import lab3.OutLinkGenerator;


public class PageRank {

	// Input Path
	public static String INPUTPATH;
	
	// Output Path
	public static String OUTPUTPATH;
		
	// Number of iterations for calculation
	public static int ITERATIONS_COUNT = 8;
	

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub

		INPUTPATH = args[0];
		OUTPUTPATH = args[1];
		
		// Intermediate location for storing computation files
		String arg1 = OUTPUTPATH + "/tmp/1";
		String arg2 = OUTPUTPATH + "/tmp/2";
		String arg3 = OUTPUTPATH + "/tmp/3";
		String arg4 = OUTPUTPATH + "/tmp/4";
		String arg5 = OUTPUTPATH + "/tmp/5";
		String arg6 = OUTPUTPATH + "/tmp/6";
		String arg7 = OUTPUTPATH + "/tmp/7";
		String arg8 = OUTPUTPATH + "/tmp/8";
		
		// Storing the results
		String outLink1 = OUTPUTPATH + "/results/PageRank.outlink.out";
		String outLink2 = OUTPUTPATH + "/results/PageRank.n.out";
		String outLink3 = OUTPUTPATH + "/results/PageRank.iter1.out";
		String outLink4 = OUTPUTPATH + "/results/PageRank.iter8.out";

		//Generate OutLinks
		//OutLinkGenerator outLinks = new OutLinkGenerator() ;
		//outLinks.parseXML(INPUTPATH, arg1);

		//Filter Red Links
		//RedLinksFilter redLinksFilter = new RedLinksFilter() ;
		//redLinksFilter.redLinkFilter(arg1, arg5); 
		//redLinksFilter.redLinkFilter(arg5, arg6); 

		//Calculate N
		//NCalculator calcN = new NCalculator() ;
		//calcN.calculateN(arg6, arg2);


		Configuration conf = new Configuration() ;
		Path pt = new Path(outLink2) ;
		FileSystem fs = FileSystem.get(new URI(outLink2), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt))) ;
		String line = br.readLine() ;
		int count = Integer.parseInt(line.substring(2)) ;
	
		
		//Page Rank Calculation
		InitPRIterator initPRIterator = new InitPRIterator() ;

		//String path = arg2+"/part-r-00000" ;
		initPRIterator.initPRIterator(outLink2, outLink1, arg3);

		PRCalculator prcalc = new PRCalculator() ;
		prcalc.runPRCalculator(arg3, arg4+"1",count);
		for(int run = 1; run< ITERATIONS_COUNT;run++)
			prcalc.runPRCalculator(arg4+run, arg4+(run+1),count);

		FileSystem fileSystem;
		fileSystem = FileSystem.get(new URI(OUTPUTPATH), conf);
		/*Path source1 = new Path(arg6);
		Path destination1 = new Path(outLink1);
		FileUtil.copyMerge(fileSystem, source1, fileSystem, destination1, false, conf, "");

		Path source2 = new Path(arg2);
		Path destination2 = new Path(outLink2);
		FileUtil.copyMerge(fileSystem, source2, fileSystem, destination2, false, conf, "");*/

		//PageRank Sorting
		PRSorter prsorter = new PRSorter() ;
		prsorter.PRSort(outLink2, arg4+"1", arg7);
		prsorter.PRSort(outLink2, arg4+"8", arg8);

		Path source3 = new Path(arg7);
		Path destination3 = new Path(outLink3);
		FileUtil.copyMerge(fileSystem, source3, fileSystem, destination3, false, conf, "");

		Path source4 = new Path(arg8);
		Path destination4 = new Path(outLink4);
		FileUtil.copyMerge(fileSystem, source4, fileSystem, destination4, false, conf, "");

	}

}
