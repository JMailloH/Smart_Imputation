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
 * @author Written by Juli�n Luengo Mart�n 05/11/2006
 * @version 0.2
 * @since JDK 1.5
 * </p>
 */
package keel.Algorithms.Preprocessing.knnImpute;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import keel.Dataset.*;

/**
 * <p>
 * This class computes the mean (numerical) or mode (nominal) value of the
 * attributes with missing values for the selected neighbours for a given
 * instance with missing values
 * </p>
 */
public class knnImpute {

	double[] mean = null;
	double[] std_dev = null;
	double tempData = 0;
	String[][] X = null; // matrix of transformed data
	String[] mostCommon;
	boolean[] filtered = null;

	int ndatos = 0;
	int nentradas = 0;
	int tipo = 0;
	int direccion = 0;
	int nvariables = 0;
	int nsalidas = 0;
	int nneigh = 1; // number of neighbours
	int ennNeighbors = 1; // number of neighbors for ENN filtering
	boolean useENN = true;
	double cleanThreshold = 0.7;

	InstanceSet IS, IStest;
	String input_train_name = new String();
	String input_test_name = new String();
	String output_train_name = new String();
	String output_test_name = new String();
	String temp = new String();
	String data_out = new String("");

	public knnImpute() {
		ndatos = 0;
		nentradas = 0;
		tipo = 0;
		direccion = 0;
		nvariables = 0;
		nsalidas = 0;
		nneigh = 1; // number of neighbours
	}

	/**
	 * Creates a new instance of MostCommonValue
	 * 
	 * @param fileParam
	 *            The path to the configuration file with all the parameters in
	 *            KEEL format
	 */
	public knnImpute(String fileParam) {
		config_read(fileParam);
		IS = new InstanceSet();
		IStest = new InstanceSet();
	}

	/**
	 * Creates a new instance of MostCommonValue
	 * 
	 * @param fileParam
	 *            The path to the configuration file with all the parameters in
	 *            KEEL format
	 */
	public knnImpute(InstanceSet tra, int neighbors) {
		nneigh = neighbors;
		IS = tra;
	}

	/**
	 * Creates a new instance of MostCommonValue
	 * 
	 * @param fileParam
	 *            The path to the configuration file with all the parameters in
	 *            KEEL format
	 */
	public knnImpute(InstanceSet tra, InstanceSet MVs, int neighbors) {
		nneigh = neighbors;
		IS = tra;
		IStest = MVs;
	}

	/**
	 * Creates a new instance of MostCommonValue
	 * 
	 * @param fileParam
	 *            The path to the configuration file with all the parameters in
	 *            KEEL format
	 */
	public knnImpute(String input_tra, String input_tst, String output_tra,
			String output_tst, int neighbors) {
		input_train_name = input_tra;
		input_test_name = input_tst;
		output_train_name = output_tra;
		output_test_name = output_tst;
		nneigh = neighbors;
		IS = new InstanceSet();
		IStest = new InstanceSet();
	}

	// Write data matrix X to disk, in KEEL format
	protected void write_results(String output) {
		// File OutputFile = new File(output_train_name.substring(1,
		// output_train_name.length()-1));
		try {
			FileWriter file_write = new FileWriter(output);

			file_write.write(IS.getHeader());

			// now, print the normalized data
			file_write.write("@data\n");
			for (int i = 0; i < ndatos; i++) {
				if (!filtered[i]) {
					file_write.write(X[i][0]);
					for (int j = 1; j < nvariables; j++) {
						file_write.write("," + X[i][j]);
					}
					file_write.write("\n");
				}
			}
			file_write.close();
		} catch (IOException e) {
			System.out.println("IO exception = " + e);
			System.exit(-1);
		}
	}

	// Read the pattern file, and parse data into strings
	protected void config_read(String fileParam) {
		File inputFile = new File(fileParam);

		if (inputFile == null || !inputFile.exists()) {
			System.out.println("parameter " + fileParam
					+ " file doesn't exists!");
			System.exit(-1);
		}
		// begin the configuration read from file
		try {
			FileReader file_reader = new FileReader(inputFile);
			BufferedReader buf_reader = new BufferedReader(file_reader);
			// FileWriter file_write = new FileWriter(outputFile);

			String line;

			do {
				line = buf_reader.readLine();
			} while (line.length() == 0); // avoid empty lines for processing ->
											// produce exec failure
			String out[] = line.split("algorithm = ");
			// alg_name = new String(out[1]); //catch the algorithm name
			// input & output filenames
			do {
				line = buf_reader.readLine();
			} while (line.length() == 0);
			out = line.split("inputData = ");
			out = out[1].split("\\s\"");
			input_train_name = new String(out[0].substring(1,
					out[0].length() - 1));
			input_test_name = new String(out[1].substring(0,
					out[1].length() - 1));
			if (input_test_name.charAt(input_test_name.length() - 1) == '"')
				input_test_name = input_test_name.substring(0,
						input_test_name.length() - 1);

			do {
				line = buf_reader.readLine();
			} while (line.length() == 0);
			out = line.split("outputData = ");
			out = out[1].split("\\s\"");
			output_train_name = new String(out[0].substring(1,
					out[0].length() - 1));
			output_test_name = new String(out[1].substring(0,
					out[1].length() - 1));
			if (output_test_name.charAt(output_test_name.length() - 1) == '"')
				output_test_name = output_test_name.substring(0,
						output_test_name.length() - 1);

			// parameters
			do {
				line = buf_reader.readLine();
			} while (line.length() == 0);
			out = line.split("k = ");
			nneigh = (new Integer(out[1])).intValue(); // parse the string into
														// a double

			do {
				line = buf_reader.readLine();
			} while (line.length() == 0);
			out = line.split("enn = ");
			ennNeighbors = (new Integer(out[1])).intValue(); // parse the string
																// into a
																// integer

			do {
				line = buf_reader.readLine();
			} while (line.length() == 0);
			out = line.split("eta = ");
			cleanThreshold = (new Double(out[1])).doubleValue(); // parse the
																	// string
																	// into a
																	// double

			file_reader.close();

		} catch (IOException e) {
			System.out.println("IO exception = " + e);
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * <p>
	 * Computes the distance between two instances (without previous
	 * normalization)
	 * </p>
	 * 
	 * @param i
	 *            First instance
	 * @param j
	 *            Second instance
	 * @return The Euclidean distance between i and j
	 */
	protected double distance(Instance i, Instance j, InstanceAttributes atts) {
		
		//System.out.println("\n\n\n@nvariables => " + nvariables + "  |  @getInputNumAttributes => " + atts.getInputNumAttributes() + "\n\n");
		boolean sameMVs = true;
		double dist = 0;
		int in = 0;
		int out = 0;

		for (int l = 0; l < nentradas && sameMVs; l++) {
			Attribute a = atts.getAttribute(l);

			direccion = a.getDirectionAttribute();
			tipo = a.getType();
			if (i.getInputMissingValues(l) != j.getInputMissingValues(l))
				sameMVs = false;
			
			if (tipo != Attribute.NOMINAL && !i.getInputMissingValues(in)) {
				double range = a.getMaxAttribute() - a.getMinAttribute();
				// real value, apply euclidean distance
				dist += ((i.getInputRealValues(in) - j
						.getInputRealValues(in)) / range)
						* ((i.getInputRealValues(in) - j
								.getInputRealValues(in)) / range);
			} else {
				if (!i.getInputMissingValues(in)
						&& i.getInputNominalValues(in) != j
								.getInputNominalValues(in))
					dist += 1;
			}
			in++;
		}

		if(sameMVs)
			return -1;
		else
			return Math.sqrt(dist);
	}

	/**
	 * <p>
	 * Computes the distance between two instances (without previous
	 * normalization)
	 * </p>
	 * 
	 * @param i
	 *            First instance
	 * @param j
	 *            Second instance
	 * @return The Euclidean distance between i and j
	 */
	protected double distanceOld(Instance i, Instance j, InstanceAttributes atts) {
		double dist = 0;
		int in = 0;
		int out = 0;

		for (int l = 0; l < nvariables; l++) {
			Attribute a = atts.getAttribute(l);

			direccion = a.getDirectionAttribute();
			tipo = a.getType();

			if (direccion == Attribute.INPUT) {
				if (tipo != Attribute.NOMINAL && !i.getInputMissingValues(in)) {
					double range = a.getMaxAttribute() - a.getMinAttribute();
					// real value, apply euclidean distance
					dist += ((i.getInputRealValues(in) - j
							.getInputRealValues(in)) / range)
							* ((i.getInputRealValues(in) - j
									.getInputRealValues(in)) / range);
				} else {
					if (!i.getInputMissingValues(in)
							&& i.getInputNominalValues(in) != j
									.getInputNominalValues(in))
						dist += 1;
				}
				in++;
			}

		}
		return Math.sqrt(dist);
	}

	/**
	 * <p>
	 * Checks if two instances present MVs for the same attributes
	 * </p>
	 * 
	 * @param inst1
	 *            the first instance
	 * @param inst2
	 *            the second instance
	 * @return true if both instances have missing values for the same
	 *         attributes, false otherwise
	 */
	protected boolean sameMissingInputAttributes(Instance inst1,
			Instance inst2, InstanceAttributes atts) {
		boolean sameMVs = true;

		for (int i = 0; i < atts.getInputNumAttributes() && sameMVs; i++) {
			if (inst1.getInputMissingValues(i) != inst2
					.getInputMissingValues(i))
				sameMVs = false;
		}

		return sameMVs;
	}

	/**
	 * Finds the nearest neighbor with a valid value in the specified attribute
	 * 
	 * @param inst
	 *            the instance to be taken as reference
	 * @param a
	 *            the attribute which will be checked
	 * @return the nearest instance that has a valid value in the attribute 'a'
	 */
	protected Instance nearestValidNeighbor(Instance inst, int a,
			InstanceAttributes atts) {
		double distance = Double.POSITIVE_INFINITY;
		Instance inst2;
		int nn = 0;

		for (int i = 0; i < IS.getNumInstances(); i++) {
			inst2 = IS.getInstance(i);
			if (inst != inst2 && !inst2.getInputMissingValues(a)
					&& distance(inst, inst2, atts) < distance) {
				distance = distance(inst, inst2, atts);
				nn = i;
			}

		}

		return IS.getInstance(nn);
	}

	/**
	 * <p>
	 * Takes a value and checks if it belongs to the attribute interval. If not,
	 * it returns the nearest limit. IT DOES NOT CHECK IF THE ATTRIBUTE IS NOT
	 * NOMINAL
	 * </p>
	 * 
	 * @param value
	 *            the value to be checked
	 * @param a
	 *            the attribute to which the value will be checked against
	 * @return the original value if it was in the interval limits of the
	 *         attribute, or the nearest boundary limit otherwise.
	 */
	public double boundValueToAttributeLimits(double value, Attribute a) {

		if (value < a.getMinAttribute())
			value = a.getMinAttribute();
		else if (value > a.getMaxAttribute())
			value = a.getMaxAttribute();

		return value;
	}

	/**
	 * <p>
	 * Process the training and test files provided in the parameters file to
	 * the constructor.
	 * </p>
	 */
	public String[][] impute(InstanceAttributes atts) {
		Instance neighbor;
		double dist, mean;
		int actual, totalN;
		int[] N = new int[nneigh];
		double[] Ndist = new double[nneigh];
		boolean allNull;

		try {

			// Load in memory a dataset that contains a classification problem
			int in = 0;
			int out = 0;

			ndatos = IS.getNumInstances();
			nvariables = atts.getNumAttributes();
			nentradas = atts.getInputNumAttributes();
			nsalidas = atts.getOutputNumAttributes();

			X = new String[ndatos][nvariables];// matrix with transformed data

			mostCommon = new String[nvariables];
			filtered = new boolean[ndatos];

			for (int i = 0; i < ndatos; i++) {
				Instance inst = IS.getInstance(i);

				filtered[i] = false;
				in = 0;
				out = 0;
				if (inst.existsAnyMissingValue()) {

					// since exists MVs, first we must compute the nearest
					// neighbors for our instance
					for (int n = 0; n < nneigh; n++) {
						Ndist[n] = Double.MAX_VALUE;
						N[n] = -1;
					}
					for (int k = 0; k < ndatos; k++) {
						neighbor = IS.getInstance(k);

						if (!sameMissingInputAttributes(inst, neighbor, atts)) {
							dist = distance(inst, neighbor, atts);

							actual = -1;
							for (int n = 0; n < nneigh; n++) {
								if (dist < Ndist[n]) {
									if (actual != -1) {
										if (Ndist[n] > Ndist[actual]) {
											actual = n;
										}
									} else
										actual = n;
								}
							}
							if (actual != -1) {
								N[actual] = k;
								Ndist[actual] = dist;
							}
						}

					}

				}
				for (int j = 0; j < nvariables; j++) {
					Attribute a = atts.getAttribute(j);

					direccion = a.getDirectionAttribute();
					tipo = a.getType();

					if (direccion == Attribute.INPUT) {
						if (tipo != Attribute.NOMINAL
								&& !inst.getInputMissingValues(in)) {
							if (tipo == Attribute.INTEGER)
								X[i][j] = new String(String.valueOf((int) inst
										.getInputRealValues(in)));
							else
								X[i][j] = new String(String.valueOf(inst
										.getInputRealValues(in)));
						} else {
							if (!inst.getInputMissingValues(in))
								X[i][j] = inst.getInputNominalValues(in);
							else {
								// save computing time
								allNull = true;
								if (tipo != Attribute.NOMINAL) {
									mean = 0.0;
									totalN = 0;
									for (int m = 0; m < nneigh; m++) {
										if (N[m] != -1) {
											Instance inst2 = IS
													.getInstance(N[m]);
											if (!inst2
													.getInputMissingValues(in)) {
												mean += inst2
														.getInputRealValues(in);
												totalN++;
												allNull = false;
											}

										}
									}
									if (!allNull) {
										if (tipo == Attribute.INTEGER)
											mean = Math.round(mean);

										mean = this
												.boundValueToAttributeLimits(
														mean, a);
										if (tipo == Attribute.INTEGER)
											X[i][j] = new String(
													String.valueOf((int) mean));
										else
											X[i][j] = new String(
													String.valueOf(mean));
									} else
										// if no option left, lets take the
										// nearest neighbor with a valid
										// attribute value
										X[i][j] = String
												.valueOf(nearestValidNeighbor(
														inst, in, atts)
														.getInputRealValues(in));
								} else {
									HashMap<String, Integer> popMap = new HashMap<String, Integer>();
									String popular = "";
									int count = 0;
									for (int m = 0; m < nneigh; m++) {
										Instance inst2 = IS.getInstance(N[m]);
										if (!popMap.containsKey(inst2
												.getOutputNominalValues(in))) {
											popMap.put(
													inst2.getOutputNominalValues(in),
													1);
										} else {
											popMap.put(
													inst2.getOutputNominalValues(in),
													popMap.get(inst2
															.getOutputNominalValues(in)) + 1);
										}
									}

									Iterator it = popMap.entrySet().iterator();
									while (it.hasNext()) {
										Map.Entry pair = (Map.Entry) it.next();
										if (count < (Integer) pair.getValue()) {
											popular = (String) pair.getKey();
											count = (Integer) pair.getValue();
										}
									}

									X[i][j] = new String(popular);
								}

							}
						}
						in++;
					} else {
						X[i][j] = inst.getOutputNominalValues(out);

					}
				}
			}
		} catch (Exception e) {
			System.out.println("Dataset exception = " + e);
			e.printStackTrace();
			System.exit(-1);
		}
		// Return the dataset imputed.
		return X;

	}

	/**
	 * Process the training and test files provided in the parameters file to
	 * the constructor.
	 */
	public ArrayList<Double>[] imputeDistributed(InstanceAttributes atts) {
		BigDecimal begTime = BigDecimal.valueOf(System.currentTimeMillis());
		BigDecimal sum_neigh = new BigDecimal(0);
		BigDecimal sum = new BigDecimal(0);

		Instance neighbor;
		double dist,maxdist,candidateDist;
		int actual;
		int[] N = new int[nneigh];
		double[] Ndist = new double[nneigh];
		int ndatos2 = IStest.getNumInstances();
		// ArrayList<Neighbor> Neigh = new ArrayList<Neighbor>();
		ndatos = IS.getNumInstances();
		nvariables = atts.getNumAttributes();
		nentradas = atts.getInputNumAttributes();
		nsalidas = atts.getOutputNumAttributes();

		ArrayList<Double>[] result = (ArrayList<Double>[]) new ArrayList[ndatos2];

		try {

			for (int i = 0; i < ndatos2; i++) {
				begTime = BigDecimal.valueOf(System.currentTimeMillis());
				Instance inst = IStest.getInstance(i);

				// since exists MVs, first we must compute the nearest
				// Neighbors for our instance
				maxdist = Double.MAX_VALUE;
				for (int n = 0; n < nneigh; n++) {
					Ndist[n] = Double.MAX_VALUE;
					N[n] = -1;
				}
				
				for (int k = 0; k < ndatos; k++) {
					neighbor = IS.getInstance(k);
					
					dist = distance(inst, neighbor, atts);
					if (dist != -1){ //Leave one out + dont have the same missing attributes
					//if (!sameMissingInputAttributes(inst, neighbor, atts)) {
						//dist = distanceOld(inst, neighbor, atts);
						if(dist < maxdist){
							actual = -1;
							candidateDist = Ndist[0];
							for (int n = 0; n < nneigh; n++) {

								if (dist < Ndist[n]) {
									if (actual != -1) {
										if (Ndist[n] > Ndist[actual]) {
											candidateDist = Ndist[actual];
											actual = n;
										}
									} else
										actual = n;
								}
							}
							if (actual != -1) {
								N[actual] = k;
								Ndist[actual] = dist;
								maxdist = Math.max(candidateDist,dist);
							}
						}
					}
				}

				sum_neigh = sum_neigh.add(BigDecimal.valueOf(System.currentTimeMillis())).subtract(begTime);
				begTime = BigDecimal.valueOf(System.currentTimeMillis());
				// Add the key as first element
				result[i] = new ArrayList<Double>();
				for (int t = 0; t < nentradas; t++) {
					if (inst.getInputMissingValues(t)) {
						// Add the index of the missing value
						result[i].add((double) t);
						for (int g = 0; g < nneigh; g++) {
							// Add the distance to the neighbor
							result[i].add((double) Ndist[g]);
							// Add the value of the neighbor for this feature
							result[i].add((double) IS.getInstance(N[g])
									.getAllInputValues()[t]);
						}
					}
				}

				sum = sum.add(BigDecimal.valueOf(System.currentTimeMillis())).subtract(begTime);
			}
		} catch (Exception e) {
			System.out.println("Dataset exception = " + e);
			e.printStackTrace();
			System.exit(-1);
		}
		System.out.println("@PROMEDIO\tTiempo vecinos => " + (sum_neigh.divide(new BigDecimal(1000))).divide(new BigDecimal(ndatos2),15,RoundingMode.HALF_UP) + " Tiempo estructura => " + (sum.divide(new BigDecimal(1000))).divide(new BigDecimal(ndatos2),15,RoundingMode.HALF_UP));
		System.out.println("@TOTAL\tTiempo vecinos => " + (sum_neigh.divide(new BigDecimal(1000))) + " Tiempo estructura => " + (sum.divide(new BigDecimal(1000))));
		return result;

		/*
		 * Instance neighbor; double dist; int actual; int ndatos2 =
		 * IStest.getNumInstances(); ArrayList<Neighbor> Neigh = null;
		 * 
		 * ndatos = IS.getNumInstances(); nvariables = atts.getNumAttributes();
		 * nentradas = atts.getInputNumAttributes(); nsalidas =
		 * atts.getOutputNumAttributes();
		 * 
		 * ArrayList<Double>[] result = (ArrayList<Double>[]) new
		 * ArrayList[ndatos2];
		 * 
		 * try {
		 * 
		 * for (int i = 0; i < ndatos2; i++) { Instance inst =
		 * IStest.getInstance(i);
		 * 
		 * Neigh = new ArrayList<Neighbor>();
		 * 
		 * for (int k = 0; k < ndatos; k++) { neighbor = IS.getInstance(k);
		 * 
		 * if (!sameMissingInputAttributes(inst, neighbor, atts)) { dist =
		 * distance(inst, neighbor, atts); Neighbor aux_neigh = new Neighbor(k,
		 * dist);
		 * 
		 * //System.out.println("\n\n\n@Como estoy ahora =>"); //for (int asdf =
		 * 0 ; asdf < Neigh.size() ; asdf++){ // System.out.println(asdf +
		 * " - Dist = " + Neigh.get(asdf).getDist()); //}
		 * Neigh.add(Math.abs(Collections.binarySearch(Neigh,aux_neigh) + 1),
		 * aux_neigh);
		 * 
		 * 
		 * //System.out.println("@Voy a meter este elemento: " + dist); //for
		 * (int asdf = 0 ; asdf < Neigh.size() ; asdf++){ //
		 * System.out.println(asdf + " - Dist = " + Neigh.get(asdf).getDist());
		 * //} //System.out.println("\n\n");
		 * 
		 * 
		 * 
		 * if (Neigh.size() > nneigh) { Neigh.remove(nneigh); } } }
		 * 
		 * // Add the key as first element result[i] = new ArrayList<Double>();
		 * for (int t = 0; t < nentradas; t++) {
		 * 
		 * if (inst.getInputMissingValues(t)) { // Add the index of the missing
		 * value result[i].add((double) t); // System.out.println("key = " +
		 * (double) t);
		 * 
		 * for (int g = 0; g < nneigh; g++) { // Add the distance to the
		 * neighbor result[i].add(Neigh.get(g).getDist()); // Add the value of
		 * the neighbor for this features result[i].add((double) IS.getInstance(
		 * Neigh.get(g).getIndex()) .getAllInputValues()[t]); //
		 * System.out.println("value = " + (double) //
		 * IS.getInstance(Neigh.get(g).getIndex()).getAllInputValues()[t]);
		 * 
		 * } } } } } catch (Exception e) {
		 * System.out.println("Dataset exception = " + e); e.printStackTrace();
		 * System.exit(-1); }
		 * 
		 * return result;
		 */
	}
}
