package web;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;
import java.util.Random;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class ProxiesVector{

	int current;
	int size;
	Proxy[] proxies;
	String path;
	boolean filter;//select good proxies and save the new list of proxies
	Vector<Proxy> verifiedProxies;
	String last_verified_proxies_path;
	
	public ProxiesVector(String path,boolean filter){

		last_verified_proxies_path = path;
		this.filter=false;
		this.path = path;
		verifiedProxies=new Vector<Proxy>();
		loadProxies();

	}//LOAD PROXIES

	private void loadProxies(){
		
		Vector<Proxy> pr = new Vector<Proxy>();
		
		current = 0;
		
		String IP = "";
		String port_s = "";
		int port = 0;
		
		System.out.print("Loading proxies list from " + last_verified_proxies_path + " ... ");
		
		try{
			
			Path pt=new Path(last_verified_proxies_path);
            FileSystem hdfs = FileSystem.get(new Configuration());
            BufferedReader fs=new BufferedReader(new InputStreamReader(hdfs.open(pt)));
            
			//FileInputStream fs = new FileInputStream(path);
		
			int c = fs.read();
			
			while(  c !=-1 ){//while not EOF
			
				IP = "";
				port = 0;
				port_s = "";
				
				while( (char)c != ':' && c!=-1){
					IP += (char)c;
					c = fs.read();
				}
				
				if(c==-1)
					break;
				
				while( (c = fs.read()) >= 48 && c <= 57 )
					port_s += (char)c;				
									
				if(c==-1)
					break;
				
				port = Integer.parseInt(port_s);
				
				pr.add( new Proxy(IP,port,port_s) );
								
				while( ((c = fs.read()) < 48 || c > 57) && c!=-1 ){}
				
			}
			
			System.out.println("done.");
			
		}catch(FileNotFoundException e){
			
			System.out.println("Error: Proxy file not found in " + last_verified_proxies_path);
			System.exit(1);
						
		}catch(IOException e){
			
			e.printStackTrace();
			System.exit(1);
			
		}
		
		size = pr.size();
		System.out.print("Allocating memory for the proxies ... ");
		proxies = new Proxy[size()];
		
		for(int i=0;i<size();i++)
			proxies[i] = pr.elementAt(i);
		System.out.println(" done.");
		
		randomPermute();
		
		System.out.println("Proxies list loaded correctly " + last_verified_proxies_path);
		
		//for(int i=0;i<size();i++)			
			//System.out.println("IP = " + elementAt(i).IP + ", PORT = " + elementAt(i).port);
		
	}
	
	public int size(){
		return size;
	}
	
	public Proxy elementAt(int i){
		return proxies[i];
	}
	
	public synchronized Proxy getNext(){
		
		current = (current + 1) % size();
		
		if(current==0)
			save();
		
		return elementAt(current);
		
	}
	
	public Proxy currentProxy(){
		
		return elementAt(current);
		
	}
	
	private void swap(int i,int j){
		
		Proxy p = elementAt(i);
		proxies[i] = elementAt(j);
		proxies[j] = p;
		
	}

	/*public Proxy randomProxy(){
		Random r = new Random();	
		return proxies[current=r.nextInt(size)];
	}*/
	
	private void randomPermute(){
		
		System.out.print("Random permutation of the proxies ... ");
		
		Random r = new Random();
		
		for(int i=size()-1;i>1;i--)
			swap(i,r.nextInt(i));
		
		System.out.println("done.");
		
	}
	
	public void addVerifiedProxy(Proxy p){	
		if(filter){
	        verifiedProxies.add(p);
	        System.out.println(verifiedProxies.size() + "/" + (current+1) + " verified good proxies until now. "); 
		}
	}
	
	private void save(){
		
		if(filter && verifiedProxies.size()>1000){
			
			last_verified_proxies_path = path + "_verified_" + verifiedProxies.size();
			System.out.println("Saving good proxies in " + last_verified_proxies_path);		
			try{
				
				BufferedWriter o = new BufferedWriter(new FileWriter(new File(last_verified_proxies_path)));
						
				for(int i=0;i<verifiedProxies.size();i++){
					
					for(int j=0;j<verifiedProxies.elementAt(i).IP.length();j++)
						o.write((int)verifiedProxies.elementAt(i).IP.charAt(j));
					
					o.write((int)':');
					
					for(int j=0;j<verifiedProxies.elementAt(i).port_s.length();j++)
						o.write((int)verifiedProxies.elementAt(i).port_s.charAt(j));
					
					o.write((int)'\n');
					
				
				}//for
				
				o.close();
				
			}catch(FileNotFoundException e){
				
				System.out.println("Proxy output file not found: " + last_verified_proxies_path);
				
			}catch(IOException io){
				
				System.out.println("Error in creating proxy file in " + last_verified_proxies_path);
				io.printStackTrace();
				
			}
				
			System.out.println("Good proxies stored in " + last_verified_proxies_path);
			
		}

		verifiedProxies.clear();
		loadProxies();
		randomPermute();//permute order so the proxies are not visited after regular intervals
		
	}//save
	
}
