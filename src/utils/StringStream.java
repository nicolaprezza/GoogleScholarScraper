package utils;

public class StringStream {

	private String s;
	private int pos;
	
	public StringStream(String s){
		
		this.s = s;
		pos = 0;
		
	}
	
	public int read(){
	
		int c;
		
		if(pos < s.length()){
			c = s.charAt(pos);
			pos++;
		}else
			c = -1;
	
		return c;
		
	}
	
}
