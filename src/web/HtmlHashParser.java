package web;

import utils.StringStream;
import java.io.IOException;
import java.util.Vector;

public class HtmlHashParser {
	
	private int[] ptrs;//ptrs[ hashVal(<element>) ] = index of the field in result
	private String[][] FP;//FP[ hashVal(<element>) ] = name, attr name, attr value: to avoid false positives
	private int mod;

	public HtmlHashParser(Vector<String> paths, int min_hash_size){

		Vector<String[]> decomposedPaths = new Vector<String[]>(); // for each path, 3 fields: element name, attribute name, attribute value

		String elem,att_n,att_v;
		
		for(int i=0;i<paths.size();i++){
			
			decomposedPaths.add( new String[3] );
			
			int j=0;
			elem = "";
			att_n = "";
			att_v="";
			
			while(paths.elementAt(i).charAt(j) != '[' && j< paths.elementAt(i).length()){
				elem += paths.elementAt(i).charAt(j);
				j++;
			}
			
			if(paths.elementAt(i).charAt(j) == '['){
				
				j++;
				while(paths.elementAt(i).charAt(j) != '=' && j< paths.elementAt(i).length()){
					att_n += paths.elementAt(i).charAt(j);
					j++;
				}
				
				j++;	
				while(paths.elementAt(i).charAt(j) != ']' && j< paths.elementAt(i).length()){
					att_v += paths.elementAt(i).charAt(j);
					j++;
				}
	
			}

			decomposedPaths.elementAt(i)[0] = new String(elem);
			decomposedPaths.elementAt(i)[1] = new String(att_n);
			decomposedPaths.elementAt(i)[2] = new String(att_v);
						
		}

		mod = min_hash_size-1;
		
		boolean unique = false;//do the paths give different hash values?
		
		while(!unique){
			
			mod++;
			boolean[] h = new boolean[mod];
			for(int i=0;i<mod;i++)
				h[i] = false;
			
			unique = true;
			for(int i=0;i<decomposedPaths.size();i++){
				
				if( h[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ] )
					unique=false;
				
				h[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ] = true;
				
			}
			
		}
		
		ptrs = new int[mod];
		
		for(int i=0;i<mod;i++)
			ptrs[i] = -1;
		
		for(int i=0;i<decomposedPaths.size();i++)
			ptrs[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ] = i;	
		
		FP = new String[mod][3];
		
		for(int i=0;i<decomposedPaths.size();i++){
			
			FP[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ][0] = new String(decomposedPaths.elementAt(i)[0]);
			FP[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ][1] = new String(decomposedPaths.elementAt(i)[1]);
			FP[ hashVal(decomposedPaths.elementAt(i)[0],decomposedPaths.elementAt(i)[1],decomposedPaths.elementAt(i)[2]) ][2] = new String(decomposedPaths.elementAt(i)[2]);

		}

		//System.out.println("min mod = " + mod);
		
	}//constructor
	
	public int hashVal(String name){
		
		return name.hashCode() % mod;
		
	}
	
	public int hashVal(String name, String att_n, String att_v){
				
		return ((abs(name.hashCode())%mod) + (abs(att_n.hashCode())%mod) + (abs(att_v.hashCode())%mod) ) % mod;
		
	}
	
	private int ptr(String name, String att_n, String att_v){
		
		return ptrs[ hashVal(name,att_n,att_v) ];
		
	}
	
	private int abs(int a){
		
		return (a<0?-a:a);
		
	}
	
	/*
	 * Given a list of paths a,b,c, each time inside the path paths[i] write
	 * in result[j][i] the content in that path ignoring <fields>. Example:
	 * if paths[3] = "/html/body/div[@class='gs_r']/font/span[@class='gs_fl']",
	 * document = <html><body><div class="gs_r"><font><span class="gs_fl"> TEXT1 <br> TEXT2 </span></font></div>...
	 * then paths[j][3] = "TEXT1TEXT2", where j is the current pointerfor that tree node.
	 */
	
	public int getNextField(StringStream st, String[] result){//copy in result the text and return index of the extracted field
		
		int c;
		String text = "";
		String field = "";
		String attr_name = "";
		String attr_val = "";
		
		int copying_from_ptr=-1;
		
		String copying_from = "";//field name from which text is currently copied
		
		boolean close = false;//exiting from a html field?

		c = st.read();

		while(c != -1){

			if((char)c == '<'){//inside a field <>

				 close = false;
				 field = "";
				 
				 if((char)(c = st.read())=='/')
					 close = true;
				 else
					 field += (char)c;
				 
				 while((char)(c = st.read())!=' ' && (char)c!='>' && c != -1)//read field type
					 field += (char)c;
				 
				 if(c==-1){
					 //System.out.println("End of html document in the wrong place.");
					 return -1;//error: field not closed
				 }
				 
				 if(close){
				
					 if(copying_from.equals(field)){
						 result[0] = text;
						 
						 //System.out.println("Returning field ... ");
						 
						 return copying_from_ptr;
					 }
					 
					 
				 }else if((char)c!='>'){
					 			
					 if(copying_from.length() == 0){
						 
						 while(copying_from.length() == 0 && (char)c == ' '){
							 
							 attr_name = "";
							 attr_val = "";
							 
							 while((char)(c = st.read()) != '=' && c != -1)//read attribute name
								 attr_name += (char)c;
							 
							 
							 if(c==-1){
								 
								// System.out.println("End of html document in the wrong place.");
								 return -1;//error: end of document in the wrong place
							 }
							 
							 //st.read();//skip "
							 
							 while((char)(c = st.read())!=' ' && (char)c != '>' && c != -1)//read attribute value
								 attr_val += (char)c;
							 
							 
							 if(c==-1){
								 //System.out.println("End of html document in the wrong place.");
								 return -1;//error: end of document in the wrong place
							 }
							 
							 if(attr_val.length() > 1 && attr_val.charAt(0) == '"')
								 attr_val = attr_val.substring(1,attr_val.length()-1);//remove " in the beginning and end
							 
							 
							 //c = st.read();//read next char (can be ' ' or >)
							 
							 if(ptr(field, attr_name, attr_val)>-1){//hash entry occupied: verify if FP 
								 
								 int p = hashVal(field,attr_name,attr_val);
								 
								 if(FP[p][0].equals(field) && FP[p][1].equals(attr_name) && FP[p][2].equals(attr_val)){//not false positive
									 copying_from_ptr = ptr(field,attr_name,attr_val);
									 copying_from = field;//start to copy text
									 text="";
								 }
								
							 }
							 
							 
						 }
 
						 if((char)c != '>')
							 while((char)(c = st.read())!='>' && c != -1){}
						 
						 if(c==-1)
							 return -1;//error: end of document in the wrong place
					        
					 }else{//copying text: skip until '>'
						 
						 while((char)(c = st.read())!='>'&& c != -1){}
						 
						 if(c==-1){
							 //System.out.println("End of html document in the wrong place.");
							 return -1;//error: end of document in the wrong place
						 }
						 
					 }
					 
				 }
				 			
				field = "";

			}//inside a field <>
			
			while((char)(c = st.read()) != '<' && c != -1)
				text += (char)c;
					
		}
	
		//st.close();
		
		return -1;
		
	}//parse
	
}
