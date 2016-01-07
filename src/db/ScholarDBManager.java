/*
 * Main class. Features:
 * 
 * 1) create authors list
 * 2) create articles DB
 * 3) perform operations on the articles DB
 * 
 * 
 */
package db;

import web.GSscraper;
import web.HtmlHashParser;
import web.ProxiesVector;

import mapred.*;

import java.io.IOException;
import java.util.Vector;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;

public class ScholarDBManager {

	//objects shared between concurrent threads:
	public static ProxiesVector proxies;
	public static HtmlHashParser hp;
	public static int number_of_fields;
	public static int number_of_words=0;//total number of words in the DB titles and abstract
	public static Map<String,Integer> keywords_hash;//frequencies of the words
	public static int keywords_size;
	
	public static void main(String[] args) {
		  
		System.out.println("Scholar Database Manager");
		
		try{
		
			if((new String(args[0])).equals("-new_auth")){
				
				/*
				 * 1st argument: DB filepath. authors will be extracted from there.
				 * 2nd argument: output path
				 */

				JobConf conf = new JobConf(AuthorsListCreator.class);
				conf.setJobName("Authors List Creator");

				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);

				conf.setMapperClass(mapred.AuthorsListCreator.Map.class);
				conf.setReducerClass(mapred.AuthorsListCreator.Reduce.class);	
				conf.setPartitionerClass(mapred.AuthorsListCreator.MyPartitioner.class);
				conf.setOutputValueGroupingComparator(mapred.AuthorsListCreator.GroupComparator.class);

				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);

				FileInputFormat.setInputPaths(conf, new Path(args[1]));
				FileOutputFormat.setOutputPath(conf, new Path(args[2]));

				try{					
					JobClient.runJob(conf);
				}catch(IOException e){
					e.printStackTrace();
				}finally{
					System.out.println("Authors list created successfully in " + args[2]);
				}
				
			}else if((new String(args[0])).equals("-new_db")){
		        
				proxies  = new ProxiesVector(new String(args[3]),false);
				
				Vector<String> paths = new Vector<String>(); 
				paths.add(new String("div[id=gs_ab_md]"));//results
				paths.add(new String("div[class=gs_rs]"));//abstract
				paths.add(new String("h3[class=gs_rt]"));//title
				paths.add(new String("div[class=gs_a]"));//publication
				paths.add(new String("div[class=gs_fl]"));//citations
				hp = new HtmlHashParser(paths,100);
				number_of_fields = paths.size()-1;//exclude results
				
				System.out.println("Reading authors list from " + args[1]);
				System.out.println("Storing database in " + args[2]);
				
				JobConf conf = new JobConf(DBCreator.class);
				conf.setJobName("DB Creator");

				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);

				conf.setMapperClass(mapred.DBCreator.Map.class);
				conf.setReducerClass(mapred.DBCreator.Reduce.class);	

				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);

				FileInputFormat.setInputPaths(conf, new Path(args[1]));
				FileOutputFormat.setOutputPath(conf, new Path(args[2]));

				try{					
					JobClient.runJob(conf);				
				}catch(IOException e){					
					e.printStackTrace();					
				}
				
			}else if((new String(args[0])).equals("-help")){
				
				System.out.println("Scholar DB Manager 1.0. Arguments:\n");
				System.out.println("\t-new_auth arg1 arg2 arg3 : Extend authors list in arg1 (with collaborations) and save it in file ");
				System.out.println("\t arg2. Use the proxy list in arg3.");
	
				System.out.println("\t-new_db arg1 arg2 arg3: Create new articles database and save it in file arg2. A file arg1 containing ");
				System.out.println("\t the authors list must be present. Use the proxy list in arg3.\n");
				
			}else if((new String(args[0])).equals("-topic_trending")){
				
				/*
				 * 1st argument: comma-separated list of keywords
				 * 2nd argument: input DB folder
				 * 3rd argument: output folder.
				 *
				 */
								
				keywords_hash = new HashMap<String,Integer>();//keyword->index
				StringTokenizer key_tokens = new StringTokenizer(new String(args[1]),",");
				keywords_size=0;
				while(key_tokens.hasMoreTokens()){
					keywords_hash.put(key_tokens.nextToken().toLowerCase(),keywords_size);
					keywords_size++;
				}
				
				JobConf conf = new JobConf(TopicTrending.class);
				conf.setJobName("Topic Trending");

				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(Text.class);

				conf.setMapperClass(mapred.TopicTrending.Map.class);
				conf.setReducerClass(mapred.TopicTrending.Reduce.class);	
				conf.setPartitionerClass(mapred.TopicTrending.MyPartitioner.class);
				conf.setOutputValueGroupingComparator(mapred.TopicTrending.GroupComparator.class);
				
				conf.setInputFormat(TextInputFormat.class);
				conf.setOutputFormat(TextOutputFormat.class);

				FileInputFormat.setInputPaths(conf, new Path(args[2]));
				FileOutputFormat.setOutputPath(conf, new Path(args[3])  );
				
				try{				
					JobClient.runJob(conf);
				}catch(IOException e){		
					e.printStackTrace();		
				}
				
			}else{
				
				System.out.println("Error: unrecognized command \"" + args[0] + "\"");
				
			}
		
		}catch(ArrayIndexOutOfBoundsException e){
			
			System.out.println("Error: not enough arguments. Try -help for a complete list of the commands.");
			
		}
			
		System.exit(0);
		
	}//main
	
	public static void searchByAuthor(String name){

		  GSscraper gs = new GSscraper();
		  gs.setName(name);

		  String[] s = new String[1];
		  
		  int num_of_results = gs.numberOfResults();
		  int index;
		  
		  for(int i=0;i<GSscraper.number_of_fields*num_of_results;i++){
			  
			  index = gs.getField(s);
			  
			  if(index==GSscraper.TITLE)
				  System.out.println(" +++ title: " + gs.format(s[0]));
			  else if(index==GSscraper.ABSTRACT)
				  System.out.println(" +++ abstract: " + gs.format(s[0]));
			  else if(index==GSscraper.PUBLICATION){
				  
				  gs.setPublication(gs.format(s[0]));
				  
				  String author = gs.getAuthor();
				  
				  while(!author.equals("")){	  
					  System.out.println(" +++ Author: " + author);
					  author = gs.getAuthor();
				  }
				  System.out.println("--- END AUTHORS");
				  
				  int year;
				  if((year=gs.getYear())>-1)
					  System.out.println(" +++ year: " + year);
				  
			  }
			  else if(index==GSscraper.CITATIONS){
				  
				  int citations = GSscraper.getCitations(s[0]);
				  if(citations>=0)
					  System.out.println(" +++ citations: " + citations);
				  
			  }
			  else
				  System.out.println("NULL FIELD");
			  
			 
		  }

		  System.out.println("SUCCESS");

	  }//searchByAuthor
	
}//ScholarDBManager
