package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class models an RDD action operation
 *
 */
public abstract class Action {
	
	/**
	 * input represent the computation that creates the RDD on which the action is invoked
	 */
	private Flow input;
	
	/**
	 * 
	 * @param flow a Flow object that represents the computation that
	 *             creates the RDD on which the action is invoked
	 */
	protected Action(Flow flow) { input = flow; }
	
	/**
	 * 
	 * @return a Flow object that represents the computation that
	 *         creates the RDD on which the action is invoked
	 */
	public Flow getInput() { return input; }
	
	/**
	 * 
	 * @param flow a Flow object that represents the computation that
	 *             creates the RDD on which the action is invoked
	 */
	public void setInput(Flow flow) { input = flow; }
	
	/**
	 * 
	 * @return a new copy of this object but without the input flow
	 */
	public abstract Action getClone();
}
