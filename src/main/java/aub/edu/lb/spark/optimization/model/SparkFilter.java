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
	
	private SparkFilter(UDF predicate) {
		super(null);
		udf = predicate;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF predicate) { udf = predicate; }

	public Flow getClone() { return new SparkFilter(udf); }
	
	public String toString() { return "filter( " + udf + " ) • " + getInput().toString();	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkFilter) {
			SparkFilter filter = (SparkFilter) obj;
			return udf.equals(filter.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 61 * udf.hashCode();
	}
	

}
