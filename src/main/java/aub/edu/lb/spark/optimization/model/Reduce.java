package aub.edu.lb.spark.optimization.model;
import aub.edu.lb.spark.optimization.udf.BinaryOperator;
import aub.edu.lb.spark.optimization.udf.Functions;

/**
 * 
 * This class models the reduce action in Spark
 *
 */
public class Reduce<V> extends Action{

	/**
	 * The user defined function provided the reduce function
	 */
	private BinaryOperator<V> udf;
	
	/**
	 * 
	 * @param flow
	 * @param function
	 */
	public Reduce(Flow flow, BinaryOperator<V> function) {
		super(flow);
		udf = function;
	}

	private Reduce(BinaryOperator<V> function) {
		super(null);
		udf = function;
	}
	
	public BinaryOperator<V> getUDF() { return udf; }
	public void setUDF(BinaryOperator<V> function) { udf = function; }

	/**
	 * 
	 * @return a new copy of this object but without the input flow
	 */
	public Action getClone() { return new Reduce<V>(udf); }
	
	public String toString() { return "reduce( " + Functions.binaryFunctionNames.get(udf) + " ) º " + getInput().toString(); }
	
}
