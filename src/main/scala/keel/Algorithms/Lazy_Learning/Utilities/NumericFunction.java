/***********************************************************************

	This file is part of KEEL-software, the Data Mining tool for regression, 
	classification, clustering, pattern mining and so on.

	Copyright (C) 2004-2010
	
	F. Herrera (herrera@decsai.ugr.es)
    L. Sanchez (luciano@uniovi.es)
    J. Alcala-Fdez (jalcala@decsai.ugr.es)
    S. Garcia (sglopez@ujaen.es)
    A. Fernandez (alberto.fernandez@ujaen.es)
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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package keel.Algorithms.Lazy_Learning.Utilities;

/**
 *
 * @author diegoj
 */
public class NumericFunction<IndexType>
{
    IndexType one;
    IndexType two;
    double defaultValue = 0.0;
    
    NumericFunction(IndexType a, IndexType b)
    {
        one = a;
        two = b;
    }
    
     double make()
     {
        return defaultValue;
     }
}

