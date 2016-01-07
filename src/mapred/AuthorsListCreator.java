package mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AuthorsListCreator {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
	    Text K = new Text();//used to emit key-value pairs without creating new objects
	    Text V = new Text("");//empty
	    
	    int type = 0;//field type
	    	    
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			StringTokenizer tokens = new StringTokenizer(value.toString(),"\t");
			tokens.nextToken();//discard title
			String authors = tokens.nextToken();
			
			tokens = new StringTokenizer(authors,",");
	    	
			while(tokens.hasMoreTokens()){
				K.set(tokens.nextToken());
				output.collect(K, V);
			}
	    		    	

	    }//map
		
	}//Map
	 	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		Text V = new Text("");
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			output.collect(key,V);
			
		}

	}//Reduce
	
	public static class MyPartitioner implements Partitioner<Text,Text> {
		
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
	    
			return key.toString().toLowerCase().hashCode() % numPartitions;
			
		}
	 		 
		@Override
		public void configure(JobConf arg0) {}
	 
	}//MyPartitioner
	 	
	public static class GroupComparator extends WritableComparator {

		 protected GroupComparator() {
			 super(Text.class, true);
		 }
		 
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
		 		 
			return ((Text)w1).toString().toLowerCase().compareTo(((Text)w2).toString().toLowerCase());
		 
		 }
	}//GroupComparator
	
}
