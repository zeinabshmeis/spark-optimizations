package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class models the count action in Spark
 *
 */
public class Count extends Action {

	/**
	 * 
	 * @param flow a Flow object that represents the computation that
	 *             creates the RDD on which the action is invoked
	 */
	public Count(Flow flow) { super(flow); }

	private Count() { super(null); }

	/**
	 * @return returns a new instance of this object but without the input flow
	 */
	public Action getClone() { return new Count(); }
	
	public String toString() { return "count() º " + getInput().toString();	}
}
