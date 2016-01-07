package web;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.commons.httpclient.params.HttpMethodParams;

import java.io.*;
import java.util.Calendar;

import db.ScholarDBManager;
import utils.StringStream;

import java.util.concurrent.*;

public class GSscraper{
	
	class UnicodeToAscii{//to convert characters to ascii: try to uniform authors names
		
		int table[];
		int size = 576;
		
		UnicodeToAscii(){
			
			table = new int[size];
			
			for(int i=0;i<size;i++)
				table[i]=-1;
			
			for(int i=32;i<127;i++)//ASCII chars
				table[i] = i;
			
			for(int i=192;i<=198;i++)//A
				table[i] = (int)'A';
			
			table[199] = (int)'C';
			
			for(int i=200;i<=203;i++)
				table[i] = (int)'E';
			
			for(int i=204;i<=207;i++)
				table[i] = (int)'I';
			
			table[208] = (int)'D';
			table[209] = (int)'N';

			for(int i=210;i<=214;i++)
				table[i] = (int)'O';
			
			for(int i=217;i<=220;i++)
				table[i] = (int)'U';
			
			table[221] = (int)'Y';

			for(int i=224;i<=230;i++)
				table[i] = (int)'a';
			
			table[231] = (int)'c';

			for(int i=232;i<=235;i++)
				table[i] = (int)'e';
			
			for(int i=236;i<=239;i++)
				table[i] = (int)'i';
			
			table[241] = (int)'n';

			for(int i=242;i<=246;i++)
				table[i] = (int)'o';
			
			for(int i=249;i<=252;i++)
				table[i] = (int)'u';
			
			for(int i=256;i<=261;i++)
				table[i] = (int)'a';
			
			for(int i=262;i<=269;i++)
				table[i] = (int)'c';

			for(int i=274;i<=283;i++)
				table[i] = (int)'e';
			
			for(int i=296;i<=305;i++)
				table[i] = (int)'i';
			
			for(int i=332;i<=337;i++)
				table[i] = (int)'o';
			
			for(int i=360;i<=371;i++)
				table[i] = (int)'u';
			
		}//constructor
		
		int toAscii(int uni){
			
			if(uni<size)
				return table[uni];
			
			return -1;
			
		}
		
	}
	
	//private String PROXY_HOST = "203.189.138.146";
    //private int PROXY_PORT = 8080;
    
    private ProxiesVector proxies = null;
    private Proxy currentProxy = null;
    
    private int time_limit = 5;//kill a http request/connection after this number of seconds if no result is obtained
    
	String formattedName;
	HttpClient client;
	String url;
	
	HtmlHashParser hp;
	
	int num_of_results;
	int author_index_start;
	int author_index_end;
	boolean no_more_authors = false;
	
	String publication;
	
	StringStream is = null;
	String[] s;
	int current_field;
	
	public static int number_of_fields;
	
	public static int ABSTRACT = 1;
	public static int TITLE = 2;	
	public static int PUBLICATION = 3;
	public static int CITATIONS = 4;
	
	int current_year;//used to check validity of the publication year

	private UnicodeToAscii uToA;
	
	public GSscraper(){
		
		//use shared objects
		proxies = ScholarDBManager.proxies;
		hp = ScholarDBManager.hp;
		number_of_fields = ScholarDBManager.number_of_fields;
		
		currentProxy = proxies.currentProxy();
		
		uToA = new UnicodeToAscii();
		s = new String[1];
		formattedName = "";
		
		client = new HttpClient();
		client.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY);
		
		client.getParams().setParameter( HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(0, false));		
		
		current_year = Calendar.getInstance().get(Calendar.YEAR);
				
	}
	
	public static int getCitations(String cit){
		
		int i=0;
		while(i<cit.length() && ((int)cit.charAt(i) < 48 || (int)cit.charAt(i)>57))
			i++;
				
		if(cit.substring(0, i).equals("Cited by ")){
			
			int citations=0;
			
			while(i<cit.length() && (int)cit.charAt(i) >= 48 && (int)cit.charAt(i)<=57){
				
				citations = citations*10 + (int)cit.charAt(i) - 48;
				i++;
				
			}
			
			return citations;			
		}
		
		return -1;
		
	}
	
	public void setPublication(String p){
		publication = p;
		author_index_start = -1;
		author_index_end = -1;
		no_more_authors=false;
		
		if(publication.charAt(0) == ','){//sometimes the author list begins with ',' : skip
			author_index_start = 0;
			author_index_end = 0;
		}
		
	}
	
	public String getAuthor(){
		
		if(no_more_authors)
			return "";
		else{
			
			try{
				
				author_index_end++;
				author_index_start = author_index_end;
				
				if(author_index_end >= publication.length()){//should never occurr if the publication field is well formed
					no_more_authors = true;
					return "";
				}
				
				while(author_index_start < publication.length() && publication.charAt(author_index_start)==' ')//skip spaces
					author_index_start++;
						
				while(author_index_end < publication.length() && publication.charAt(author_index_end) != ',' && publication.charAt(author_index_end) != '-')
					author_index_end++;
							
				if(publication.charAt(author_index_end) == ',')
					return publication.substring(author_index_start, author_index_end);
				else if(publication.charAt(author_index_end) == '-'){
					no_more_authors = true;
					
					if(author_index_end==0 || author_index_start<=author_index_end)
						return "";
									
					return publication.substring(author_index_start, author_index_end-1);//there is a space before -
				}
				
			}catch(Exception e){//catch exceptions caused by non well formed fields
			
				return "";
				
			}
			
		}
		
		
		return "";
		
	}
	
	public int getYear(){
		
		if(author_index_end==-1){
			//System.out.println("Error: called getYear() before getAuthor()");
		}
		
		int i=author_index_end;
		while(i<publication.length() && ((int)publication.charAt(i) < 48 || (int)publication.charAt(i)>57))
			i++;
		
		int year=0;
		
		while(i<publication.length() && (int)publication.charAt(i) >= 48 && (int)publication.charAt(i)<=57){
			
			year = year*10 + (int)publication.charAt(i) - 48;
			i++;
			
		}
		
		if(year<100 || year > current_year)//impossible year
			year=-1;
		
		return year;
		
	}
	
	public String format(String t){//removes from the title [PDF], [HTML], ecc...; removes strange chracters; remove &hellip; ecc...; remove \n
		
		int i=0;
		
		String res="";
		
		while(i<t.length()){
			
			if(t.charAt(i)=='&'){
				
				while(t.charAt(i)!=';' && i<t.length())
					i++;
				
				i++;//skip ;
				
			}
			
			if(i<t.length() && uToA.toAscii((int)t.charAt(i))>=0 && t.charAt(i)!='&')
				res += (char)uToA.toAscii((int)t.charAt(i));
			
			if(i<t.length() && t.charAt(i)==']')
				res="";
			
			if(i<t.length() && t.charAt(i)!='&')
				i++;
			
		}
		
		return res;
		
	}

	public void setName(String name) {
	
		formattedName = "";
		
		for(int i=0;i<name.length();i++)		
			if(name.charAt(i)!=' ')
				formattedName = formattedName + name.charAt(i);
			else
				formattedName = formattedName + "%20"; 

		current_field = 0;

		url = "http://scholar.google.com/scholar?start=" + current_field + "&q=author:%22" + formattedName + "%22&hl=en&num=100&as_sdt=1,5&as_vis=1";	
	    
		downloadDataRotation();
	      
	   // System.out.println("getting number of results for author " + name +" ...");
		hp.getNextField(is,s);//number of results
		//System.out.println("Results obtained: Name = "+ name + ". num results = " + s[0]);
		
		//System.out.println("++++++++++++ RESULTS:" + s[0] + "+++++++++++++");
		
		num_of_results=0;
		
		if(s[0] == null){//if for some reasons the request has failed, retry with another proxy
			
			setName(name);			
			
		}else{
		
			int i=0;
			while(i < s[0].length() && ( (int)s[0].charAt(i) < 48 || (int)s[0].charAt(i)>57) )
				i++;
			
			while(i < s[0].length() && s[0].charAt(i)!=' '){
				num_of_results = (num_of_results*10) + (int)s[0].charAt(i) - 48;
				i++;
			}
			
			if(num_of_results>1000)
				num_of_results=1000;
			
			//System.out.println(num_of_results + " RESULTS");
		
		}
		
	}//setName
		
	public int numberOfResults(){
		
		return num_of_results;
		
	}
		
	public int getField(String[] res){
		
		if(current_field < num_of_results*number_of_fields){
					
			if(current_field>0 && current_field%(100*number_of_fields)==0){//if new page
				
				//System.out.println("Loading new results page ... ");
				
				url = "http://scholar.google.com/scholar?start=" + (current_field/number_of_fields) + "&q=author:%22" + formattedName + "%22&hl=en&num=100&as_sdt=1,5&as_vis=1";	
				
				downloadDataRotation();
				
				//System.out.println("getting field ... ");
				hp.getNextField(is,s);//skip number of results
				//System.out.println("Field obtained. ");

			}
			
			current_field++;
						
			return hp.getNextField(is, res);//save in res the text
			
		}
			
		return -1;
		
	}
	
	
	private void downloadDataRotation(){//try proxies to download data. Time limit is time_limit for every trial.
				
		StringStream result = null;

		while(result == null){
		
		//	System.out.print("Rotating proxy ... ");
	        currentProxy = proxies.getNext();//random proxy
	     //   System.out.println("done.");
	        
			FutureTask<StringStream> future = new FutureTask<StringStream>(new Callable<StringStream>() {
			    public StringStream call() {
			    	
			    	try{

			    		return downloadData();
			    		
			    	}catch(Exception e){
			    		
			    		return null;
			    		
			    	}
			    		
			    }   
			} ); 
						
			try{
				
				final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
				THREAD_POOL.execute(future);
				result = future.get(time_limit,TimeUnit.SECONDS);
				
				if(result==null){}
					//System.out.println("NULL result returned!");
				
			}catch(TimeoutException te){//computation exceeded timeout
						
				//System.out.println("downloadData() timed out! killed.");
				result = null;
				
			}catch(Exception e){
				//System.out.println(e.getMessage());
			}		
			
			future.cancel(true);
			
			if(result==null){
				//System.out.println(" ... Proxy not good.");
			}else{
				proxies.addVerifiedProxy(currentProxy);
				//System.out.println("Data downloaded correctly.");
			}
		}//while
		
		//System.out.print("Returning data ... ");
		is = result;
		
	}//downloadDataRotation
	
	private StringStream downloadData(){
		
		int status = -1;
		GetMethod method = null;
		StringStream stream = null;
		boolean error = false;
	
		//System.out.println("Trying to connect ... ");

		// --------
		final HttpState state = client.getState();
	    final HostConfiguration config = client.getHostConfiguration();
        config.setProxy(currentProxy.IP, currentProxy.port);
        state.clear();//clear cookies
		method = new GetMethod(url);
     
		//System.out.println("Trying to execute method ... ");
        try{
        	status = client.executeMethod(method);
        }catch(IOException e){    	
			//System.out.println("Method failed.");
        	//System.out.println(e.getMessage());
        	error=true;
        }
        
        if(status == HttpStatus.SC_OK){}
        	//System.out.println("Method executed.");
        else
        	error=true;

		// --------   
        
        if(!error){
        	
        	//System.out.println("Connection estabilished. ");
	    	    	
		    try{	    	
		    	
		    	//System.out.println("Trying to download data ... ");
				stream = new StringStream(method.getResponseBodyAsString());
				//System.out.println("Data downloaded.");						
			
		    }catch(IOException e){//1st kind of error: socket exception
		    	
		    	stream = null;
		    	//System.out.println(e.getMessage());
		    
		    }
	    
        }
        
        //System.out.println("Releasing connection ... ");								
    	method.releaseConnection();
    	//System.out.println("Connection released.");		

    	// if(error == true || stream == null)
	    	//System.out.println("Something gone wrong trying to download data. Trying again ...");
	    
		return stream;
		
	}//downloadData()
	
}