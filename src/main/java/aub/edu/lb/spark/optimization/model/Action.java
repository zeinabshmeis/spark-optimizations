package aub.edu.lb.spark.optimization.model;

/**
 * 
 * A model of an RDD Action operation
 *
 */
public abstract class Action {
	
	/**
	 * The field represent the computation that creates the input data set received by the action
	 */
	private Flow input;
	
	protected Action(Flow flow) { input = flow; }
	
	public Flow getInput() { return input; }
	public void setInput(Flow flow) { input = flow; }
	
	public abstract Action getClone();
}
