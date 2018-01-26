package aub.edu.lb.spark.optimization.model;

import aub.edu.lb.spark.optimization.udf.*;


public class SparkFlatMap extends SingleRDDTransformation {

	private UDF udf;

	public SparkFlatMap(Flow flow, UDF function) {
		super(flow);
		udf = function;
	}
	
	private SparkFlatMap(UDF function) {
		super(null);
		udf = function;
	}
	
	public UDF getUDF() { return udf; }
	public void setUDF(UDF function) { udf = function; }
	
	public Flow getClone() { return new SparkFlatMap(udf); }
	
	public String toString() { return "flatMap( " + udf + " ) • " + getInput().toString(); }
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SparkFlatMap) {
			SparkFlatMap flatMap = (SparkFlatMap) obj;
			return udf.equals(flatMap.getUDF());
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 73 * udf.hashCode();
	}
}
