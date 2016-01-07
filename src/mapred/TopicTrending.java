package mapred;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import db.ScholarDBManager;

public class TopicTrending {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	    Text K = new Text();
	    Text V = new Text();
	    String str_value="";
	    String zeros = "";
	    String year;
	    String next_year;
	    
		public void map(LongWritable key, Text value, final OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    	
			/*
			 * Emit for each article a line:
			 * 		year	#1	#2	...	#other	0	0	...	0
			 * where #1,.. are the occurrencies of the keywords and #other are the occurrencies of the other words, and a line
			 * 		year+1	0	0	...	#1	#2	...	#other
			 *  
			 */
			String[] title_tokens =  null;
			String[] abstract_tokens = null;
			
			try{
			    StringTokenizer tokens = new StringTokenizer(value.toString(),"\t");
			    title_tokens = tokens.nextToken().split("[ \\[\\].,()}{$\\/:*;]");
			    tokens.nextToken();//skip authors
			    tokens.nextToken();//skip citations
			    year = tokens.nextToken();//read year
			    abstract_tokens = tokens.nextToken().split("[ \\[\\].,()}{$\\/:*;]");
			}catch(Exception e){}

		    if(!year.equals("NULL") && Integer.parseInt(year)>1600){
		    	
		    	next_year = (Integer.parseInt(year) + 1) + "";
		    	
				int[] number_of_occurrencies = new int[ScholarDBManager.keywords_size+1];//number of occurrencies of each keyword
				for(int i=0;i<ScholarDBManager.keywords_size+1;i++)
					number_of_occurrencies[i]=0;
		    	
				int type;
				
				//in which category falls the word? (keyword or other?)
				if(title_tokens!=null)
				    for(int i=0;i<title_tokens.length;i++){
				    	if(!title_tokens[i].equals("")){
				    		if(ScholarDBManager.keywords_hash.get(title_tokens[i].toLowerCase())==null)//not a keyword
				    			type = ScholarDBManager.keywords_size;
				    		else
				    			type = ScholarDBManager.keywords_hash.get(title_tokens[i].toLowerCase());//a keyword
				    		number_of_occurrencies[type]++;
				    	}
				    }
				if(abstract_tokens!=null)
				    for(int i=0;i<abstract_tokens.length;i++){
				    	if(!abstract_tokens[i].equals("")){
				    		if(ScholarDBManager.keywords_hash.get(abstract_tokens[i].toLowerCase())==null)
				    			type = ScholarDBManager.keywords_size;
				    		else
				    			type = ScholarDBManager.keywords_hash.get(abstract_tokens[i].toLowerCase());
				    		number_of_occurrencies[type]++;
				    	}
				    }
				
				str_value = number_of_occurrencies[0] + "";
				for(int i=1;i<ScholarDBManager.keywords_size+1;i++)
					str_value += "\t" + number_of_occurrencies[i];
				
				zeros = "0";
				for(int i=1;i<ScholarDBManager.keywords_size+1;i++)
					zeros += "\t0";
				
				K.set(year);
				V.set(str_value + "\t" + zeros);
				output.collect(K,V);
				
				K.set(next_year);
				V.set(zeros + "\t" + str_value);
				output.collect(K,V);
				
		    }//if(!year.equals("NULL"))
		    
	    }//map
		
	}//Map
	 	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		Text V = new Text();
		String str_val;
		int tot_words;
		double chi;
		
		int[] actual_frequencies;//number of occurrencies of the words THIS year
		int[] previous_frequencies;//number of occurrencies of the words the PREVIOUS year
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			/*
			 * For each year receive tuples of values: the first part are the occurrencies of words in the year, the second part in the previous year.
			 */
		
			actual_frequencies = new int[ScholarDBManager.keywords_size+1];
			previous_frequencies = new int[ScholarDBManager.keywords_size+1];	
			for(int i=0;i<ScholarDBManager.keywords_size+1;i++){
				actual_frequencies[i]=0;
				previous_frequencies[i]=0;
			}
						
			while(values.hasNext()){
				StringTokenizer tokens = new StringTokenizer(values.next().toString());
				
				for(int i=0;i<ScholarDBManager.keywords_size+1;i++)
					actual_frequencies[i] += Integer.parseInt(tokens.nextToken());
				
				for(int i=0;i<ScholarDBManager.keywords_size+1;i++)
					previous_frequencies[i] += Integer.parseInt(tokens.nextToken());
			}
			
			tot_words=0;
			for(int i=0;i<ScholarDBManager.keywords_size+1;i++)
				tot_words+=actual_frequencies[i];
			
			/*str_val=actual_frequencies[0]+"";
			for(int i=1;i<ScholarDBManager.keywords_size+1;i++)
				str_val += "\t" + actual_frequencies[i];
			for(int i=0;i<ScholarDBManager.keywords_size+1;i++)
				str_val += "\t" + previous_frequencies[i];
			
			V.set(str_val);
			output.collect(key,V);*/
			
			chi=0;
			for(int i=0;i<ScholarDBManager.keywords_size;i++)
				if(actual_frequencies[i]>previous_frequencies[i])
					chi += Math.pow((double)(actual_frequencies[i]-previous_frequencies[i]), 2)/(double)(previous_frequencies[i]+1);
			
			V.set(chi+"");
			output.collect(key,V);
			
		}//reduce

	}//Reduce
	
	public static class MyPartitioner implements Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {	 
			int hash = (new StringTokenizer(key.toString())).nextToken().toLowerCase().hashCode() % numPartitions;//only year					
			return hash;	
		}	 		 
		@Override
		public void configure(JobConf arg0) {}	 
	}
	 	
	public static class GroupComparator extends WritableComparator {
		 protected GroupComparator() {
			 super(Text.class, true);
		 }
		 
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
			 return ((new StringTokenizer(w1.toString())).nextToken().toLowerCase()).compareTo((new StringTokenizer(w2.toString())).nextToken().toLowerCase());
		 }
	}
	
}