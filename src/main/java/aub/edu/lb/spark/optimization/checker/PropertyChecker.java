package aub.edu.lb.spark.optimization.checker;

import aub.edu.lb.spark.optimization.model.Flow;
import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * Interface containing all the possible properties needed to be checked inside the optimizer
 *
 */
public interface PropertyChecker {
	
	/**
	 * Test if a unary function is an identity function or not
	 * 
	 * @param operation a unary function
	 * @return true -> operation is identity function || false -> otherwise
	 */
	public boolean isIdentityOperation(UDF operation);
	
	/**
	 * Test if a unary function is distributive over a binary function
	 * 
	 * @param operation1 a unary function
	 * @param operation2 a binary function
	 * @return true -> operation1 is distributive over operation2 || false -> otherwise
	 */
	public boolean isDistributive(UDF operation1, UDF operation2);	
	
	/**
	 * Test if two RDD transformations have ReadWrite conflict
	 * 
	 * @param flow1 an RDD operation
	 * @param flow2 an RDD operation
	 * @return true -> flow1 read set intersect flow2 write set || false -> otherwise
	 */
	public boolean hasReadWriteConflict(Flow flow1, Flow flow2);
	
	/**
	 * Test if the read set of an RDD transformation conflict with the 
	 * variables used by the other transformation
	 * 
	 * @param flow1 an RDD operation
	 * @param flow2 an RDD operation
	 * @return true -> flow1 read set intersect with flow2 variable set || false -> otherwise
	 */
	public boolean readSetIsSubsetOfVarSet(Flow flow1, Flow flow2);
	
}
