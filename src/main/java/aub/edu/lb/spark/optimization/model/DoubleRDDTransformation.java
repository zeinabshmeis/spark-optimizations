package aub.edu.lb.spark.optimization.model;

/**
 * 
 * This class represent represent Spark transformations that performs an operation on two RDDs
 *
 */
public abstract class DoubleRDDTransformation implements RDDTransformation{
	
	/**
	 * input1 represents the computation that creates the RDD on which the transformation is invoked
	 */
	private Flow input1;
	
	/**
	 * input2 represents the computation that creates the RDD provided in the argument
	 */
	private Flow input2;
	
	/**
	 * 
	 * @param flow1 a Flow object that represents the computation that creates the RDD 
	 *              on which the transformation is invoked
	 * @param flow2 a Flow object that represents the computation that creates the RDD 
	 *              provided in the argument
	 */
	protected DoubleRDDTransformation(Flow flow1, Flow flow2) {
		input1 = flow1;
		input2 = flow2;
	}
	
	public Flow getInput1() { return input1; }
	
	public Flow getInput2() { return input2; }
	
	public void setInput1(Flow flow) { input1 = flow; }
	
	public void setInput2(Flow flow) { input2 = flow; }

	// dummy methods
	public Flow getInput() {return null; }
	public void setInput(Flow flow) { }
	
}
