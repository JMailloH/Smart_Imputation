package utils;

import java.io.IOException;
import java.util.ArrayList;

//import org.apache.mahout.keel.Algorithms.Instance_Generation.Basic.Prototype;
import keel.Dataset.Attributes;
import keel.Dataset.InstanceAttributes;
import keel.Dataset.InstanceParser;
import keel.Dataset.InstanceSet;

public class Utils implements java.io.Serializable{
	
	  //Added by DMM - read header from File then readHeader(String)
	  public static synchronized InstanceAttributes readHeaderFromFile(String fileName) throws IOException{
		  String cabecera = null;
		  InstanceParser parser = new InstanceParser( fileName, true );
		  
		  String line;
		  while((line=parser.getLine())!=null) {
			  cabecera+=line;
		  }
		  
		  return readHeader(cabecera);
	  }
	
	  public static InstanceAttributes readHeader(String cabecera) throws IOException{
		  
		   if(Attributes.getNumAttributes()>0) return null;
		   System.out.println("------------------------------------" + Attributes.getNumAttributes());
		   Attributes.clearAll();//BUGBUGBUG
		   InstanceSet training = new InstanceSet();     
		      
		   ArrayList<String> header = new ArrayList<String>();
		   
		   String parts[]= cabecera.split("@");
		   
		   for(int i=0; i<parts.length;i++){
			   header.add("@"+parts[i]);
			   //System.out.println(parts[i]);
		   }
				   
		   training.parseHeaderFromString(header,true);
		   training.setAttributesAsNonStatic();
	       InstanceAttributes att = InstanceSet.getAttributeDefinitions();
	       
	
	
	     //  Prototype.setAttributesTypes(att);  
	        
	       return att;
	  }
}

