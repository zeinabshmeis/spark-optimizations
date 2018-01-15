package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.*;

/**
 * 
 * This class represents the spark filter transformation
 *
 * @param <V>
 */
public class SparkFilter extends SingleRDDTransformation {

	/**
	 * The user defined function provided the reduce function
	 */
	private UDF udf;
	
	public SparkFilter(Flow flow, UDF predicate) {
		super(flow);
		udf = predicate;
	}
	
	private SparkFilter(UDF predicate, boolean visited) {
		super(null);
		setVisited(visited);
		udf = predicate;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF predicate) { udf = predicate; }

	public Flow getClone() { return new SparkFilter(udf, super.isVisited()); }
	
	public String toString() { return "filter( " + udf + " ) º " + getInput().toString();	}

}
