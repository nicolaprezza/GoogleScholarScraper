package web;

public class Proxy {

	public final String IP;
	public final int port;
	boolean is_good;
	public final String port_s;
	
	public Proxy(String IP,int port, String port_s){
		
		this.IP = IP;
		this.port = port;
		this.port_s = port_s;
		is_good = true;
		
	}
	
	public void notGood(){
		is_good=false;
	}
	
}
