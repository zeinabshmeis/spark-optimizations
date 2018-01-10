package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.Functions;
import aub.edu.lb.spark.optimization.udf.Predicate;

/**
 * 
 * This class represents the spark filter transformation
 *
 * @param <V>
 */
public class SparkFilter<V> extends SingleRDDTransformation {

	/**
	 * The user defined function provided the reduce function
	 */
	private Predicate<V> udf;
	
	public SparkFilter(Flow flow, Predicate<V> predicate) {
		super(flow);
		udf = predicate;
	}
	
	private SparkFilter(Predicate<V> predicate, boolean visited) {
		super(null);
		setVisited(visited);
		udf = predicate;
	}
	
	public Predicate<V> getUDF() { return udf; }
	public void setUDF(Predicate<V> predicate) { udf = predicate; }

	public Flow getClone() { return new SparkFilter<V>(udf, super.isVisited()); }
	
	public String toString() { return "filter( " + Functions.unaryFunctionNames.get(udf) + " ) º " + getInput().toString();	}

}
