package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class represent represent Spark transformations that performs an operation on one RDD
 *
 */
public abstract class SingleRDDTransformation implements RDDTransformation{

	/**
	 * input represents the computation that creates the RDD on which the transformation is invoked
	 */
	private Flow input;
	
	/**
	 * visited is used to check if this object is scanned by the optimizer
	 */
	private boolean visited;
	
	protected SingleRDDTransformation(Flow flow) { input = flow; }
	
	public Flow getInput() { return input; }
	public void setInput(Flow flow) { input = flow; }
	
	public boolean isVisited() { return visited; } 
	public void setVisited(boolean visited) { this.visited = visited; }
	
	//dummy methods
	public Flow getInput1() { return null; }
	public Flow getInput2() { return null; }
	public void setInput1(Flow flow) {	}
	public void setInput2(Flow flow) {	}
	
	
}
