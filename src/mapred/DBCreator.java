package mapred;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.lang.Thread;

import web.GSscraper;

public class DBCreator {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	    String authors = "";
	    String auth;
	    
		public void map(LongWritable key, Text value, final OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    	 		    
		    authors=value.toString();
		    Vector<Thread> threads = new Vector<Thread>();
		    
		    int i=0;//position in authors
		    int j=0;//number of author
		    while(i<authors.length()){
		    
		    	auth="";
		    	while(i<authors.length() && authors.charAt(i)!='\t'){
		    		auth+=authors.charAt(i);
		    		i++;
		    	}
		    	final String author=auth;
		    	
		    	Thread t = new Thread(){	
		    		public void run(){
				    	processAuthor(author,output);
				    	System.out.println("Author " + author + " processed.");
		    		}
		    	};
		    	threads.add(t);
		    	t.start();
		    	j++;
		    	i++;
		    }
	    	
		    //wait until all the threads have finished
		    boolean finish;
		    do{
		    	finish=true;
		    	for(int k=0;k<threads.size();k++)
		    		finish = finish && (!threads.elementAt(k).isAlive());  		
		    }while(!finish);
		    		    
	    }//map
		
		/***
		 * 
		 * @param author
		 * @param output
		 * @return downloads from GS results of the author and emits correspondent key-values pairs
		 */
		private void processAuthor(String author, OutputCollector<Text, Text> output){
			
			GSscraper gs = new GSscraper();//object used to scrape GS

		    //progressive counters: when a counter becomes > current_counter, before to increase it emit new key-value pair using all values with counter = current_counter
		    int current_counter = 1;
		    int title_cnt=0;
		    int public_cnt=0;
		    int cit_cnt=0;
		    int abst_cnt=0;
		    
			String[] s = new String[1];
			String title = "";//article title
			String publication = "";//authors + year
			int c;//number of citations (int)
			String cit="";//number of citations (string)
		    String abst = "";//abstract
		    int type = 0;//field type

	    	gs.setName(author);
	    	
			int num_of_results = gs.numberOfResults();
	    	
	    	for(int i=0;i<GSscraper.number_of_fields*num_of_results;i++){

	    		type = gs.getField(s);
				
	    		//System.out.println("Field extracted.");
	    		
	    		if(type==GSscraper.TITLE){
	    			    			
	    			if(title_cnt+1 > current_counter){
	    				emit(author, publication, title, cit, abst, title_cnt, public_cnt, cit_cnt, abst_cnt, current_counter, output, gs);
		    			public_cnt=current_counter;
		    			cit_cnt=current_counter;
		    			abst_cnt=current_counter;
		    			title_cnt=current_counter;
		    			
		    			title = "";
		    			abst = "";
		    			publication = "";
		    			cit="";
	
		    			current_counter++;
	    			}
	    	
	    			title = gs.format(s[0]);
	    			title_cnt++;
	    			
	    		}else if(type==GSscraper.ABSTRACT){
	    		
	    			if(abst_cnt+1 > current_counter){
	    				emit(author, publication, title, cit, abst, title_cnt, public_cnt, cit_cnt, abst_cnt, current_counter, output, gs);
	    				public_cnt=current_counter;
	    				cit_cnt=current_counter;
	    				abst_cnt=current_counter;
	    				title_cnt=current_counter;
	    				
	    				title = "";
	    				abst = "";
	    				publication = "";
	    				cit="";

	    				current_counter++;
	    			}
	    		
	    			abst = gs.format(s[0]);
	    			abst_cnt++;
	    				    		
	    		}else if(type==GSscraper.PUBLICATION){
					  
	    			if(public_cnt+1 > current_counter){
	    				emit(author, publication, title, cit, abst, title_cnt, public_cnt, cit_cnt, abst_cnt, current_counter, output, gs);
	    				public_cnt=current_counter;
	    				cit_cnt=current_counter;
	    				abst_cnt=current_counter;
	    				title_cnt=current_counter;
	    				
	    				title = "";
	    				abst = "";
	    				publication = "";
	    				cit="";

	    				current_counter++;
	    			}
	    			
	    			publication = gs.format(s[0]);
	    			public_cnt++;
	    			
	    		}else if(type==GSscraper.CITATIONS){
	    			
	    			if((c = GSscraper.getCitations(s[0]))>=0){
	    				
		    			if(cit_cnt+1 > current_counter){
		    				emit(author, publication, title, cit, abst, title_cnt, public_cnt, cit_cnt, abst_cnt, current_counter, output, gs);
		    				public_cnt=current_counter;
		    				cit_cnt=current_counter;
		    				abst_cnt=current_counter;
		    				title_cnt=current_counter;
		    				
		    				title = "";
		    				abst = "";
		    				publication = "";
		    				cit="";

		    				current_counter++;
		    			}
		    			
	    				cit = c + "";		
	    				cit_cnt++;
	    			
	    			}
					  
	    		}
				  
	    	}//for
	    	
			emit(author, publication, title, cit, abst, title_cnt, public_cnt, cit_cnt, abst_cnt, current_counter, output, gs);//emit last result
	    	
		}
		
		private void emit(String author, String publication, String title, String cit, String abst, int title_cnt, int public_cnt, int cit_cnt, int abst_cnt, int current_counter, OutputCollector<Text, Text> output, GSscraper gs){
		
			/*
			 * KEY-VALUE PAIRS protocol: <title, 0citations	year	abstract> and <title, 1author> for every author
			 * 
			 * <"title", "auth1,auth2,auth3	citations	year	abstract">
			 * 
			 */
		
		    Text K = new Text();//used to emit key-value pairs without creating new objects
		    Text V = new Text();//used to emit key-value pairs without creating new objects

			if(public_cnt==current_counter && title_cnt==current_counter){//emit key-values only if title and authors are present
				
				String coauthor="";
				int year;
				
				try{

					K.set(title);
					
				// 1) Emit <title, author> for the current author
				
					V.set( 1 + author );
					
					output.collect(K,V);
					
				// 2) Emit <title, author> for every listed author

					gs.setPublication(publication);
					
					coauthor = gs.getAuthor();
					  
					while(!coauthor.equals("")){	
						
						V.set( 1 + coauthor );

						output.collect(K,V);
						
						coauthor = gs.getAuthor();
					}
					  
				// 3) Emit title, citations, year, abstract	
					
					if(abst_cnt!=current_counter)
						abst="NULL";
					
					if(cit_cnt!=current_counter)
						cit="NULL";
					
					if((year=gs.getYear())>-1){		
						
						V.set( 0 + cit + "\t" + year + "\t" + abst );
						output.collect(K,V);
						
					}else{
						
						V.set( 0 + cit + "\tNULL\t" + abst );
						output.collect(K,V);
										
					}
					
				}catch(IOException e){
					
					e.printStackTrace();
					
				}

			}//if			

		}//emit
	    
	}//Map
	 	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		TreeSet<String> authors = new TreeSet<String>();
		Iterator<String> it;
		String otherfields = "";
		String field = "";
		String auth = "";
		Text V = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			while(values.hasNext()){

				if((field=values.next().toString()).charAt(0) == '0')//the value contains citation, abstract, ecc..				
					otherfields = field.substring(1);
				else
					authors.add(field.substring(1));//the value contains one author
				
			}//while
			
			it = authors.iterator();
			
			auth = it.next();
			
			while(it.hasNext())
				auth += ( "," + it.next());
			
			V.set(auth + "\t" + otherfields);
			
			output.collect(key,V);
			
			authors.clear();
			
		}

	}//Reduce
	
}