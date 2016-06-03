/**
 * <p>
 * @author Written by Jesus Maillo 23/05/2016
 * @version 0.1
 * @since JDK 1.5
 * </p>
 */
package keel.Algorithms.Preprocessing.knnImpute;

/**
 * <p>
 * This class is used to sort the neighbors relative to his distance
 * </p>
 */
public class Neighbor implements Comparable<Neighbor> {
	protected Integer index;
	protected Double dist;

	/**
	 * <p>
	 * Default constructor. Initializes all to zero and allocates memory.
	 * </p>
	 */
	
	public int getIndex(){
		return index;
	}
	
	public double getDist(){
		return dist;
	}
	
	/**
	 * <p>
	 * Constructor. Initializes index and distance of the neighbor.
	 * </p>
	 */
	public Neighbor(Integer index, Double dist) {
		this.index = index;
		this.dist = dist;
	}

	@Override
	public int compareTo(Neighbor other) {
		if (other.dist < dist)
			return +1;
		if (other.dist > dist)
			return -1;
		return 0;
	}
}
