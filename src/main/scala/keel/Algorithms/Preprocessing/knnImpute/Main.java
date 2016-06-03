/***********************************************************************

	This file is part of KEEL-software, the Data Mining tool for regression, 
	classification, clustering, pattern mining and so on.

	Copyright (C) 2004-2010
	
	F. Herrera (herrera@decsai.ugr.es)
    L. S�nchez (luciano@uniovi.es)
    J. Alcal�-Fdez (jalcala@decsai.ugr.es)
    S. Garc�a (sglopez@ujaen.es)
    A. Fern�ndez (alberto.fernandez@ujaen.es)
    J. Luengo (julianlm@decsai.ugr.es)

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see http://www.gnu.org/licenses/
  
**********************************************************************/

/**
 * <p>
 * @author Written by Juli�n Luengo Mart�n 31/12/2006
 * @version 0.3
 * @since JDK 1.5
 * </p>
 */
package keel.Algorithms.Preprocessing.knnImpute;

/*
import org.joptsimple.OptionException;
import org.joptsimple.OptionParser;
import org.joptsimple.OptionSet;
*/
/**
 *
 * @author Juli�n Luengo Mart�n
 */
public class Main {
    
    /** Creates a new instance of Main */
    public Main() {
    }
    
    public static void printHelp() {
        System.err.println();

        System.err.println("Example of usage:");
        System.err.println();
        System.err.println("--------------");
        System.err.println("-t <training INPUT file> -T <test INPUT file> -o <training OUTPUT file> -O <test OUTPUT file> -k <number of neighbors for KNNI>");
        System.err.println();
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        knnImpute  M = null;
        if (args.length == 1){
        	M = new knnImpute (args[0]);
        }else{
        	if(args.length == 0){
        		printHelp();
        		System.exit(-1);
        	}/*else{
	        	OptionParser parser = new OptionParser() {
	                {
	                    accepts("t", "INPUNT training file").withRequiredArg()
	                            .ofType(String.class)
	                            .describedAs("Text File in \"path\"");
	                    accepts("T", "INPUT test file").withRequiredArg()
	                            .ofType(String.class)
	                            .describedAs("Text File in \"path\"");
	                    accepts("o", "OUTPUT training file").withRequiredArg()
			                    .ofType(String.class)
			                    .describedAs("Text File in \"path\"");
			            accepts("O", "OUTPUT test file").withRequiredArg()
			                    .ofType(String.class)
			                    .describedAs("Text File in \"path\"");
	                    accepts("k", "Neighbors for KNNI")
	                            .withRequiredArg()
	                            .ofType(Integer.class)
	                            .defaultsTo( 10 );
	                    accepts("?", "prints help")
	                            .forHelp();

	                }
	            };
	        	*/
	        	
	        	try {
		        	/*OptionSet options = parser.parse(args);
		        	int neighbors = (Integer)options.valueOf("k");
		            String input_tra = (String) options.valueOf("t");
		            String input_tst = (String) options.valueOf("T");
		            String output_tra = (String) options.valueOf("o");
		            String output_tst = (String) options.valueOf("O");*/
	        		
	        		
		        	//M = new knnImpute(input_tra,input_tst,output_tra,output_tst,neighbors);
	        		M = new knnImpute("","","","",1);
	        	} catch (Exception e) {
	                System.exit(1);
	            }
//	        }
	        
	        
	    }
        //M.impute();
    }
    
}

