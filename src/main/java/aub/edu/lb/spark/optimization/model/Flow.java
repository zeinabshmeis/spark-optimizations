package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This interface represent any operation that can create an RDD
 *
 */
public interface Flow {
	
	/**
	 * 
	 * @return a copy of the object without the inputs
	 */
	public Flow getClone();
	
	/**
	 * 
	 * @return true => object was scanned by the optimizer || false => otherwise
	 */
	public boolean isVisited();
	
	/**
	 * 
	 * @param visited to turn on/off the visited fields 
	 */
	public void setVisited(boolean visited);
	
	/**
	 * 
	 * @return the input flow of the current object in case of transformations on single RDD
	 */
	public Flow getInput();
	
	/**
	 * 
	 * @return the first input flow of the current object in case of transformations that needs two RDDs
	 */
	public Flow getInput1();
	
	/**
	 * 
	 * @return the second input flow of the current object in case of transformations that needs two RDDs
	 */
	public Flow getInput2();
	
	/**
	 * 
	 * @param flow represents the computation that creates the RDD on which the transformation 
	 *             is invoked in case of transformations that needs one RDD
	 */
	public void setInput(Flow flow);
	
	/**
	 * 
	 * @param flow represents the computation that creates the RDD on which the transformation 
	 *             is invoked in case of transformations that needs two RDD
	 */
	public void setInput1(Flow flow);
	
	/**
	 * 
	 * @param flow represents the computation that creates the RDD provided in the argument 
	 * 			   in case of transformations that needs one RDD
	 */
	public void setInput2(Flow flow);

}
